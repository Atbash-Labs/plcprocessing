#!/usr/bin/env python3
"""
Parser for .sc (structured code) files exported from L5X.
Extracts tags, logic, metadata, and relationships for ontology generation.
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set
from pathlib import Path


@dataclass
class Tag:
    """Represents a PLC tag (parameter or local variable)."""
    name: str
    data_type: str
    direction: Optional[str] = None  # INPUT, OUTPUT, IN_OUT, or None for local
    description: Optional[str] = None
    default_value: Optional[str] = None
    is_array: bool = False
    array_bounds: Optional[str] = None


@dataclass
class LogicRung:
    """Represents a single rung of ladder logic."""
    number: int
    comment: Optional[str]
    logic: str


@dataclass
class SCFile:
    """Parsed representation of a .sc file."""
    file_path: str
    name: str
    type: str  # AOI, UDT, etc.
    revision: Optional[str] = None
    vendor: Optional[str] = None
    description: Optional[str] = None

    # Tags
    input_tags: List[Tag] = field(default_factory=list)
    output_tags: List[Tag] = field(default_factory=list)
    inout_tags: List[Tag] = field(default_factory=list)
    local_tags: List[Tag] = field(default_factory=list)

    # Logic
    routines: List[Dict] = field(default_factory=list)  # {name, type, rungs}

    # Raw text sections for LLM analysis
    raw_implementation: Optional[str] = None


class SCParser:
    """Parser for .sc files."""

    # Regex patterns
    HEADER_PATTERN = r'\(\*\s*(\w+):\s*(.+?)\s*\*\)'
    VAR_SECTION_PATTERN = r'VAR_(INPUT|OUTPUT|IN_OUT)\s+(.*?)\s+END_VAR'
    VAR_LOCAL_PATTERN = r'VAR\s+(.*?)\s+END_VAR'
    TAG_PATTERN = r'^\s*(\w+)\s*:\s*([^;/]+?)\s*(?::=\s*(.+?))?\s*;(?:\s*//\s*(.+))?$'
    ARRAY_PATTERN = r'ARRAY\[(.*?)\]\s+OF\s+(\w+)'
    ROUTINE_PATTERN = r'\(\*\s*ROUTINE:\s*(.+?)\s*\[(\w+)\]\s*\*\)'
    RUNG_PATTERN = r'//\s*Rung\s+(\d+)(?::\s*(.+))?'

    def parse_file(self, file_path: str) -> SCFile:
        """Parse a .sc file and extract all information."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        sc_file = SCFile(file_path=file_path, name="", type="")

        # Extract header metadata
        self._parse_headers(content, sc_file)

        # Extract variable sections
        self._parse_var_sections(content, sc_file)

        # Extract implementation (logic)
        self._parse_implementation(content, sc_file)

        return sc_file

    def _parse_headers(self, content: str, sc_file: SCFile):
        """Extract metadata from header comments."""
        for match in re.finditer(self.HEADER_PATTERN, content):
            key = match.group(1).lower()
            value = match.group(2).strip()

            if key in ('aoi', 'udt', 'pou'):
                sc_file.name = value
                sc_file.type = key.upper()
            elif key == 'type':
                pass  # Already captured
            elif key == 'revision':
                sc_file.revision = value
            elif key == 'vendor':
                sc_file.vendor = value
            elif key == 'description':
                sc_file.description = value

    def _parse_var_sections(self, content: str, sc_file: SCFile):
        """Extract all variable declarations."""

        # Parse INPUT/OUTPUT/IN_OUT sections
        for match in re.finditer(self.VAR_SECTION_PATTERN, content, re.DOTALL):
            direction = match.group(1)
            vars_text = match.group(2)
            tags = self._parse_tags(vars_text, direction)

            if direction == 'INPUT':
                sc_file.input_tags.extend(tags)
            elif direction == 'OUTPUT':
                sc_file.output_tags.extend(tags)
            elif direction == 'IN_OUT':
                sc_file.inout_tags.extend(tags)

        # Parse local VAR section
        for match in re.finditer(self.VAR_LOCAL_PATTERN, content, re.DOTALL):
            vars_text = match.group(1)
            tags = self._parse_tags(vars_text, None)
            sc_file.local_tags.extend(tags)

    def _parse_tags(self, vars_text: str, direction: Optional[str]) -> List[Tag]:
        """Parse individual tag declarations."""
        tags = []

        for line in vars_text.split('\n'):
            line = line.strip()
            if not line or line.startswith('(*'):
                continue

            match = re.match(self.TAG_PATTERN, line)
            if match:
                name = match.group(1)
                type_str = match.group(2).strip()
                default = match.group(3).strip() if match.group(3) else None
                description = match.group(4).strip() if match.group(4) else None

                # Check if it's an array
                array_match = re.search(self.ARRAY_PATTERN, type_str)
                if array_match:
                    is_array = True
                    array_bounds = array_match.group(1)
                    data_type = array_match.group(2)
                else:
                    is_array = False
                    array_bounds = None
                    data_type = type_str

                tag = Tag(
                    name=name,
                    data_type=data_type,
                    direction=direction,
                    description=description,
                    default_value=default,
                    is_array=is_array,
                    array_bounds=array_bounds
                )
                tags.append(tag)

        return tags

    def _parse_implementation(self, content: str, sc_file: SCFile):
        """Extract implementation logic."""

        # Find implementation section
        impl_match = re.search(r'\(\*\s*IMPLEMENTATION\s*\*\)(.*)', content, re.DOTALL)
        if not impl_match:
            return

        impl_content = impl_match.group(1)
        sc_file.raw_implementation = impl_content.strip()

        # Parse routines
        routines = []
        routine_splits = re.split(self.ROUTINE_PATTERN, impl_content)

        if len(routine_splits) > 1:
            # Multiple routines found
            for i in range(1, len(routine_splits), 3):
                if i + 2 <= len(routine_splits):
                    routine_name = routine_splits[i].strip()
                    routine_type = routine_splits[i + 1].strip()
                    routine_content = routine_splits[i + 2] if i + 2 < len(routine_splits) else ""

                    rungs = self._parse_rungs(routine_content)

                    routines.append({
                        'name': routine_name,
                        'type': routine_type,
                        'rungs': rungs,
                        'raw_content': routine_content.strip()
                    })
        else:
            # Single routine (no explicit routine markers)
            rungs = self._parse_rungs(impl_content)
            routines.append({
                'name': 'Main',
                'type': 'Unknown',
                'rungs': rungs,
                'raw_content': impl_content.strip()
            })

        sc_file.routines = routines

    def _parse_rungs(self, content: str) -> List[LogicRung]:
        """Parse ladder logic rungs."""
        rungs = []

        lines = content.split('\n')
        current_rung = None
        current_comment = None

        for line in lines:
            line = line.strip()

            # Check for rung marker
            rung_match = re.match(self.RUNG_PATTERN, line)
            if rung_match:
                # Save previous rung if exists
                if current_rung is not None:
                    rungs.append(current_rung)

                rung_num = int(rung_match.group(1))
                rung_comment = rung_match.group(2).strip() if rung_match.group(2) else None
                current_rung = LogicRung(number=rung_num, comment=rung_comment, logic="")
                current_comment = rung_comment
            elif current_rung is not None and line and not line.startswith('(*'):
                # Accumulate logic for current rung
                if current_rung.logic:
                    current_rung.logic += '\n'
                current_rung.logic += line

        # Save last rung
        if current_rung is not None:
            rungs.append(current_rung)

        return rungs

    def get_all_tags(self, sc_file: SCFile) -> List[Tag]:
        """Get all tags from a parsed SC file."""
        return (sc_file.input_tags + sc_file.output_tags +
                sc_file.inout_tags + sc_file.local_tags)

    def get_tag_names(self, sc_file: SCFile) -> Set[str]:
        """Get set of all tag names."""
        return {tag.name for tag in self.get_all_tags(sc_file)}

    def extract_referenced_tags(self, logic: str) -> Set[str]:
        """Extract tag names referenced in logic text."""
        # Simple extraction - looks for word-like tokens
        # In real ladder logic, this would need more sophisticated parsing
        potential_tags = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', logic)

        # Filter out common ladder logic instructions
        instructions = {
            'XIC', 'XIO', 'OTE', 'OTL', 'OTU', 'OSR', 'OSF',
            'TON', 'TOF', 'RTO', 'CTU', 'CTD', 'EQU', 'NEQ',
            'GRT', 'LES', 'GEQ', 'LEQ', 'MOV', 'ADD', 'SUB',
            'MUL', 'DIV', 'AND', 'OR', 'XOR', 'NOT'
        }

        return {tag for tag in potential_tags if tag not in instructions}


def main():
    """Test the parser on exported L5X files."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python sc_parser.py <path_to_sc_file>")
        sys.exit(1)

    file_path = sys.argv[1]
    parser = SCParser()
    sc_file = parser.parse_file(file_path)

    print(f"\n=== Parsed: {sc_file.name} ({sc_file.type}) ===")
    print(f"Revision: {sc_file.revision}")
    print(f"Vendor: {sc_file.vendor}")
    print(f"Description: {sc_file.description}")

    print(f"\n--- Input Tags ({len(sc_file.input_tags)}) ---")
    for tag in sc_file.input_tags:
        desc = f" // {tag.description}" if tag.description else ""
        print(f"  {tag.name}: {tag.data_type}{desc}")

    print(f"\n--- Output Tags ({len(sc_file.output_tags)}) ---")
    for tag in sc_file.output_tags:
        desc = f" // {tag.description}" if tag.description else ""
        print(f"  {tag.name}: {tag.data_type}{desc}")

    print(f"\n--- Local Tags ({len(sc_file.local_tags)}) ---")
    for tag in sc_file.local_tags[:5]:  # Show first 5
        desc = f" // {tag.description}" if tag.description else ""
        print(f"  {tag.name}: {tag.data_type}{desc}")
    if len(sc_file.local_tags) > 5:
        print(f"  ... and {len(sc_file.local_tags) - 5} more")

    print(f"\n--- Routines ({len(sc_file.routines)}) ---")
    for routine in sc_file.routines:
        print(f"  {routine['name']} [{routine['type']}]: {len(routine['rungs'])} rungs")
        for rung in routine['rungs'][:3]:  # Show first 3 rungs
            comment = f": {rung.comment}" if rung.comment else ""
            logic_preview = rung.logic[:60] + "..." if len(rung.logic) > 60 else rung.logic
            print(f"    Rung {rung.number}{comment}")
            print(f"      {logic_preview}")
        if len(routine['rungs']) > 3:
            print(f"    ... and {len(routine['rungs']) - 3} more rungs")


if __name__ == "__main__":
    main()
