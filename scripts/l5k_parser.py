#!/usr/bin/env python3
"""
Parser for Rockwell/Allen-Bradley L5K (ASCII text) project files.

L5K is a full-project text format exported from Studio 5000 / RSLogix 5000.
It uses keyword-delimited blocks (CONTROLLER, DATATYPE, TAG, PROGRAM,
ROUTINE, TASK, MODULE, ADD_ON_INSTRUCTION_DEFINITION) nested in a hierarchy:

    IE_VER := x.x;
    CONTROLLER <name> ...
      DATATYPE <name> ... END_DATATYPE
      MODULE <name> ... END_MODULE
      ADD_ON_INSTRUCTION_DEFINITION <name> ... END_ADD_ON_INSTRUCTION_DEFINITION
      TAG ... ;
      TASK <name> ...
        PROGRAM <name> ...
          TAG ... ;
          ROUTINE <name> ...
            <rung/logic content>
          END_ROUTINE
        END_PROGRAM
      END_TASK
    END_CONTROLLER

This parser converts L5K content into the same SCFile data structures used by
the existing sc_parser.py, enabling the ontology pipeline to process L5K files
without modification.

Reference: Rockwell Publication 1756-RM084 (L5K/L5X Import/Export Reference)
"""

import re
import sys
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from sc_parser import SCFile, Tag, LogicRung


# ---------------------------------------------------------------------------
# L5K block extraction helpers
# ---------------------------------------------------------------------------

def _find_block(text: str, keyword: str, end_keyword: str,
                start_pos: int = 0) -> Optional[Tuple[str, str, int]]:
    """Find next block delimited by keyword / end_keyword.

    Returns (block_name, block_body, end_position) or None.
    """
    pattern = re.compile(
        rf'^\s*{keyword}\s+(\S+)',
        re.MULTILINE,
    )
    m = pattern.search(text, start_pos)
    if not m:
        return None

    block_name = m.group(1).strip('(').strip()
    # Find the matching END_ keyword (same indentation level tracking)
    depth = 1
    pos = m.end()
    end_pat = re.compile(
        rf'^\s*({keyword}|{end_keyword})\b',
        re.MULTILINE,
    )
    while depth > 0:
        em = end_pat.search(text, pos)
        if not em:
            # No matching end — take rest of text
            return block_name, text[m.end():], len(text)
        if em.group(1) == end_keyword:
            depth -= 1
            if depth == 0:
                return block_name, text[m.end():em.start()], em.end()
        else:
            depth += 1
        pos = em.end()
    return None


def _find_all_blocks(text: str, keyword: str,
                     end_keyword: str) -> List[Tuple[str, str]]:
    """Find all top-level blocks of a given type. Returns [(name, body), ...]."""
    results = []
    pos = 0
    while True:
        result = _find_block(text, keyword, end_keyword, pos)
        if result is None:
            break
        name, body, end_pos = result
        results.append((name, body))
        pos = end_pos
    return results


# ---------------------------------------------------------------------------
# Tag / member parsing
# ---------------------------------------------------------------------------

# L5K tag declaration pattern:
#   TAG <name> : <type> (Description := "...", ...) := <value>;
# or multi-line within a block. We handle the common forms.
_TAG_LINE_RE = re.compile(
    r'TAG\s+(\w+)\s*:\s*(\S+)'
    r'(?:\s*\(([^)]*)\))?'       # optional attributes in parens
    r'(?:\s*:=\s*(\S+))?\s*;',   # optional default
    re.IGNORECASE,
)

# Simpler inline tag (inside PROGRAM blocks): name : type := value ;
_INLINE_TAG_RE = re.compile(
    r'^\s*(\w+)\s*:\s*([^;:=(]+?)\s*'
    r'(?:\(([^)]*)\))?\s*'
    r'(?::=\s*([^;]+?))?\s*;',
    re.MULTILINE,
)

_DESC_RE = re.compile(r'Description\s*:=\s*"([^"]*)"', re.IGNORECASE)
_USAGE_RE = re.compile(r'Usage\s*:=\s*(\w+)', re.IGNORECASE)
_EXTERNAL_ACCESS_RE = re.compile(r'ExternalAccess\s*:=\s*(\w+)', re.IGNORECASE)


def _parse_l5k_tag_attrs(attr_str: str) -> Dict[str, str]:
    """Parse parenthesized attribute list into a dict."""
    attrs: Dict[str, str] = {}
    if not attr_str:
        return attrs
    m = _DESC_RE.search(attr_str)
    if m:
        attrs['description'] = m.group(1)
    m = _USAGE_RE.search(attr_str)
    if m:
        attrs['usage'] = m.group(1)
    m = _EXTERNAL_ACCESS_RE.search(attr_str)
    if m:
        attrs['external_access'] = m.group(1)
    return attrs


def _parse_l5k_tags(text: str, scope: str = "controller") -> List[Tag]:
    """Parse TAG declarations from an L5K text block.

    The *scope* is used for labeling but doesn't affect parsing.
    """
    tags: List[Tag] = []

    for m in _TAG_LINE_RE.finditer(text):
        name = m.group(1)
        data_type = m.group(2).strip()
        attrs = _parse_l5k_tag_attrs(m.group(3) or "")
        default = m.group(4)

        # Map Usage attribute to direction
        usage = attrs.get('usage', '').lower()
        direction_map = {
            'input': 'INPUT',
            'output': 'OUTPUT',
            'inout': 'IN_OUT',
        }
        direction = direction_map.get(usage)

        # Detect arrays
        is_array = False
        array_bounds = None
        arr_match = re.match(r'(\w+)\[(.+?)\]', data_type)
        if arr_match:
            data_type = arr_match.group(1)
            array_bounds = arr_match.group(2)
            is_array = True

        tags.append(Tag(
            name=name,
            data_type=data_type,
            direction=direction,
            description=attrs.get('description'),
            default_value=default,
            is_array=is_array,
            array_bounds=array_bounds,
        ))

    return tags


# ---------------------------------------------------------------------------
# DATATYPE parsing → UDT SCFile objects
# ---------------------------------------------------------------------------

_MEMBER_RE = re.compile(
    r'MEMBER\s+(\w+)\s*:\s*(\S+?)\s*'
    r'(?:\(([^)]*)\))?\s*;',
    re.IGNORECASE,
)


def _parse_datatype_block(name: str, body: str,
                          source_file: str) -> SCFile:
    """Convert a DATATYPE block into an SCFile representing a UDT."""
    members: List[Tag] = []

    for m in _MEMBER_RE.finditer(body):
        mem_name = m.group(1)
        mem_type = m.group(2).strip()
        attrs = _parse_l5k_tag_attrs(m.group(3) or "")

        # Skip hidden helper members (like ZZZZZZZZZZ padding)
        if mem_name.startswith('ZZZZZZZZ'):
            continue

        is_array = False
        array_bounds = None
        arr = re.match(r'(\w+)\[(.+?)\]', mem_type)
        if arr:
            mem_type = arr.group(1)
            array_bounds = arr.group(2)
            is_array = True

        members.append(Tag(
            name=mem_name,
            data_type=mem_type,
            description=attrs.get('description'),
            is_array=is_array,
            array_bounds=array_bounds,
        ))

    # Also try to pick up members via bare "name : type ;" lines within the body
    # L5K DATATYPEs sometimes list members without the MEMBER keyword
    for m in _INLINE_TAG_RE.finditer(body):
        candidate = m.group(1)
        if candidate.upper() in ('MEMBER', 'DATATYPE', 'END_DATATYPE',
                                 'FAMILY', 'CLASS'):
            continue
        if candidate.startswith('ZZZZZZZZ'):
            continue
        # Avoid duplicates
        if any(t.name == candidate for t in members):
            continue
        mem_type = m.group(2).strip()
        attrs = _parse_l5k_tag_attrs(m.group(3) or "")

        is_array = False
        array_bounds = None
        arr = re.match(r'(\w+)\[(.+?)\]', mem_type)
        if arr:
            mem_type = arr.group(1)
            array_bounds = arr.group(2)
            is_array = True

        members.append(Tag(
            name=candidate,
            data_type=mem_type,
            description=attrs.get('description'),
            is_array=is_array,
            array_bounds=array_bounds,
        ))

    # Extract family / class info from body
    family_m = re.search(r'Family\s*:=\s*(\w+)', body, re.IGNORECASE)
    class_m = re.search(r'Class\s*:=\s*(\w+)', body, re.IGNORECASE)
    desc_m = re.search(r'Description\s*:=\s*"([^"]*)"', body, re.IGNORECASE)

    sc = SCFile(
        file_path=source_file,
        name=name,
        type='UDT',
        description=desc_m.group(1) if desc_m else None,
    )
    sc.local_tags = members
    return sc


# ---------------------------------------------------------------------------
# ROUTINE parsing → logic extraction
# ---------------------------------------------------------------------------

_RUNG_RE = re.compile(
    r'(?:RC\s*:=\s*"([^"]*?)"\s*;?\s*)?'  # optional rung comment
    r'(?:N|NR|D)?\s*:?\s*'                 # optional rung type prefix
    r'((?:[A-Z]{2,4}\([^;]*?\)\s*)*;)',    # ladder instructions ending with ;
    re.IGNORECASE | re.DOTALL,
)

_ST_BODY_RE = re.compile(
    r'ST\s*\((.*?)\)\s*;',
    re.DOTALL,
)


def _parse_routine_block(name: str, body: str) -> Dict:
    """Parse a ROUTINE block into a dict matching the SCFile routines format."""
    # Determine routine type
    type_m = re.search(r'Type\s*:=\s*(\w+)', body, re.IGNORECASE)
    routine_type = type_m.group(1).upper() if type_m else 'RLL'

    rungs: List[LogicRung] = []
    raw_content = body.strip()

    if routine_type in ('RLL', 'LADDER'):
        # Extract RLL content — look for rung lines
        # L5K rungs: N: <instructions>;  or just <instructions>;
        # Comments: RC := "comment text";
        rung_num = 0
        lines = body.split('\n')
        current_comment = None
        current_logic_lines: List[str] = []

        for line in lines:
            stripped = line.strip()

            # Rung comment
            rc_match = re.match(r'RC\s*:=\s*"(.*?)"\s*;', stripped)
            if rc_match:
                # Save previous rung
                if current_logic_lines:
                    logic_text = ' '.join(current_logic_lines).strip()
                    if logic_text:
                        rungs.append(LogicRung(
                            number=rung_num,
                            comment=current_comment,
                            logic=logic_text,
                        ))
                        rung_num += 1
                    current_logic_lines = []
                current_comment = rc_match.group(1)
                continue

            # Rung type markers (N: = normal, NR: = not retentive, D: = delete)
            type_match = re.match(r'^(N|NR|D)\s*:\s*(.*)', stripped)
            if type_match:
                # Save previous rung
                if current_logic_lines:
                    logic_text = ' '.join(current_logic_lines).strip()
                    if logic_text:
                        rungs.append(LogicRung(
                            number=rung_num,
                            comment=current_comment,
                            logic=logic_text,
                        ))
                        rung_num += 1
                    current_logic_lines = []
                    current_comment = None

                rest = type_match.group(2).strip()
                if rest:
                    current_logic_lines.append(rest)
                continue

            # Ladder instructions (XIC, XIO, OTE, TON, etc.)
            if stripped and re.match(r'[A-Z]', stripped) and '(' in stripped:
                current_logic_lines.append(stripped)
            elif stripped.endswith(';') and current_logic_lines:
                current_logic_lines.append(stripped)

        # Final rung
        if current_logic_lines:
            logic_text = ' '.join(current_logic_lines).strip()
            if logic_text:
                rungs.append(LogicRung(
                    number=rung_num,
                    comment=current_comment,
                    logic=logic_text,
                ))

    elif routine_type in ('ST', 'STRUCTURED_TEXT'):
        # Structured text — extract ST body
        # The entire body between the routine markers is the ST code
        pass  # raw_content will be used

    return {
        'name': name,
        'type': routine_type,
        'rungs': rungs,
        'raw_content': raw_content,
    }


# ---------------------------------------------------------------------------
# ADD_ON_INSTRUCTION_DEFINITION parsing → AOI SCFile objects
# ---------------------------------------------------------------------------

def _parse_aoi_block(name: str, body: str,
                     source_file: str) -> SCFile:
    """Parse an ADD_ON_INSTRUCTION_DEFINITION block into an SCFile."""
    # Extract metadata
    revision_m = re.search(r'Revision\s*:=\s*(\S+)', body, re.IGNORECASE)
    vendor_m = re.search(r'Vendor\s*:=\s*"([^"]*)"', body, re.IGNORECASE)
    desc_m = re.search(r'Description\s*:=\s*"([^"]*)"', body, re.IGNORECASE)

    sc = SCFile(
        file_path=source_file,
        name=name,
        type='AOI',
        revision=revision_m.group(1).rstrip(';') if revision_m else None,
        vendor=vendor_m.group(1) if vendor_m else None,
        description=desc_m.group(1) if desc_m else None,
    )

    # Extract parameters (they look like TAG declarations with Usage)
    tags = _parse_l5k_tags(body, scope="aoi")
    for tag in tags:
        if tag.name in ('EnableIn', 'EnableOut'):
            continue
        if tag.direction == 'INPUT':
            sc.input_tags.append(tag)
        elif tag.direction == 'OUTPUT':
            sc.output_tags.append(tag)
        elif tag.direction == 'IN_OUT':
            sc.inout_tags.append(tag)
        else:
            sc.local_tags.append(tag)

    # Also find LOCAL_TAG entries specific to AOIs
    local_tag_re = re.compile(
        r'LOCAL_TAG\s+(\w+)\s*:\s*(\S+?)\s*'
        r'(?:\(([^)]*)\))?\s*'
        r'(?::=\s*([^;]+?))?\s*;',
        re.IGNORECASE,
    )
    for m in local_tag_re.finditer(body):
        tag_name = m.group(1)
        if any(t.name == tag_name for t in sc.local_tags):
            continue
        data_type = m.group(2).strip()
        attrs = _parse_l5k_tag_attrs(m.group(3) or "")
        default = m.group(4)

        is_array = False
        array_bounds = None
        arr = re.match(r'(\w+)\[(.+?)\]', data_type)
        if arr:
            data_type = arr.group(1)
            array_bounds = arr.group(2)
            is_array = True

        sc.local_tags.append(Tag(
            name=tag_name,
            data_type=data_type,
            description=attrs.get('description'),
            default_value=default,
            is_array=is_array,
            array_bounds=array_bounds,
        ))

    # Extract routines
    routines = _find_all_blocks(body, 'ROUTINE', 'END_ROUTINE')
    for rname, rbody in routines:
        sc.routines.append(_parse_routine_block(rname, rbody))

    # Build raw implementation from all routine content
    if sc.routines:
        parts = []
        for r in sc.routines:
            parts.append(f"(* ROUTINE: {r['name']} [{r['type']}] *)")
            if r.get('raw_content'):
                parts.append(r['raw_content'])
        sc.raw_implementation = '\n'.join(parts)

    return sc


# ---------------------------------------------------------------------------
# PROGRAM parsing → Program SCFile objects
# ---------------------------------------------------------------------------

def _parse_program_block(name: str, body: str,
                         source_file: str) -> SCFile:
    """Parse a PROGRAM block into an SCFile."""
    desc_m = re.search(r'Description\s*:=\s*"([^"]*)"', body, re.IGNORECASE)

    sc = SCFile(
        file_path=source_file,
        name=name,
        type='PROGRAM',
        description=desc_m.group(1) if desc_m else None,
    )

    # Extract program-scoped tags
    tags = _parse_l5k_tags(body, scope="program")
    for tag in tags:
        if tag.direction == 'INPUT':
            sc.input_tags.append(tag)
        elif tag.direction == 'OUTPUT':
            sc.output_tags.append(tag)
        elif tag.direction == 'IN_OUT':
            sc.inout_tags.append(tag)
        else:
            sc.local_tags.append(tag)

    # Extract routines
    routines = _find_all_blocks(body, 'ROUTINE', 'END_ROUTINE')
    for rname, rbody in routines:
        sc.routines.append(_parse_routine_block(rname, rbody))

    if sc.routines:
        parts = []
        for r in sc.routines:
            parts.append(f"(* ROUTINE: {r['name']} [{r['type']}] *)")
            if r.get('raw_content'):
                parts.append(r['raw_content'])
        sc.raw_implementation = '\n'.join(parts)

    return sc


# ---------------------------------------------------------------------------
# Main L5K parser
# ---------------------------------------------------------------------------

class L5KParser:
    """Parser for Rockwell L5K (ASCII) project files.

    Parses the entire project and returns a list of SCFile objects
    (one per AOI, UDT, and PROGRAM found in the project).
    """

    def parse_file(self, file_path: str) -> List[SCFile]:
        """Parse an L5K file and return list of SCFile objects."""
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

        return self.parse_text(content, source_file=file_path)

    def parse_text(self, text: str, source_file: str = "<string>") -> List[SCFile]:
        """Parse L5K text content and return list of SCFile objects."""
        results: List[SCFile] = []

        # Extract controller name for context
        ctrl_m = re.search(r'CONTROLLER\s+(\w+)', text, re.IGNORECASE)
        controller_name = ctrl_m.group(1) if ctrl_m else "Unknown"

        # 1. Parse DATATYPEs → UDTs
        datatypes = _find_all_blocks(text, 'DATATYPE', 'END_DATATYPE')
        for dt_name, dt_body in datatypes:
            sc = _parse_datatype_block(dt_name, dt_body, source_file)
            results.append(sc)

        # 2. Parse ADD_ON_INSTRUCTION_DEFINITIONs → AOIs
        aois = _find_all_blocks(
            text,
            'ADD_ON_INSTRUCTION_DEFINITION',
            'END_ADD_ON_INSTRUCTION_DEFINITION',
        )
        for aoi_name, aoi_body in aois:
            sc = _parse_aoi_block(aoi_name, aoi_body, source_file)
            results.append(sc)

        # 3. Parse PROGRAMs
        programs = _find_all_blocks(text, 'PROGRAM', 'END_PROGRAM')
        for prog_name, prog_body in programs:
            sc = _parse_program_block(prog_name, prog_body, source_file)
            results.append(sc)

        # 4. Extract controller-scoped tags as a synthetic "CONTROLLER" SCFile
        #    (everything between CONTROLLER and the first nested block)
        ctrl_tags = _parse_l5k_tags(text, scope="controller")
        if ctrl_tags:
            ctrl_sc = SCFile(
                file_path=source_file,
                name=controller_name,
                type='CONTROLLER',
            )
            for tag in ctrl_tags:
                if tag.direction == 'INPUT':
                    ctrl_sc.input_tags.append(tag)
                elif tag.direction == 'OUTPUT':
                    ctrl_sc.output_tags.append(tag)
                else:
                    ctrl_sc.local_tags.append(tag)
            results.append(ctrl_sc)

        return results

    def export_to_sc(self, file_path: str, output_dir: str) -> int:
        """Parse L5K file and export each component as .sc files.

        Returns count of exported files.
        """
        import os
        os.makedirs(output_dir, exist_ok=True)

        sc_files = self.parse_file(file_path)
        count = 0

        for sc in sc_files:
            if sc.type == 'AOI':
                suffix = '.aoi.sc'
            elif sc.type == 'UDT':
                suffix = '.udt.sc'
            elif sc.type == 'PROGRAM':
                suffix = '.prog.sc'
            elif sc.type == 'CONTROLLER':
                suffix = '.ctrl.sc'
            else:
                suffix = '.sc'

            out_path = os.path.join(output_dir, f"{sc.name}{suffix}")
            _write_sc_file(sc, out_path)
            count += 1
            print(f"[OK] Exported {sc.type}: {sc.name}")

        return count


def _write_sc_file(sc: SCFile, path: str):
    """Write an SCFile to disk in the standard .sc text format."""
    with open(path, 'w', encoding='utf-8') as f:
        # Header
        type_label = sc.type
        if type_label == 'AOI':
            f.write(f"(* AOI: {sc.name} *)\n")
            f.write(f"(* Type: AddOnInstruction *)\n")
        elif type_label == 'UDT':
            f.write(f"(* UDT: {sc.name} *)\n")
            f.write(f"(* Type: UserDefinedType *)\n")
        elif type_label == 'PROGRAM':
            f.write(f"(* POU: {sc.name} *)\n")
            f.write(f"(* Type: Program *)\n")
        elif type_label == 'CONTROLLER':
            f.write(f"(* POU: {sc.name} *)\n")
            f.write(f"(* Type: Controller *)\n")
        else:
            f.write(f"(* POU: {sc.name} *)\n")
            f.write(f"(* Type: {type_label} *)\n")

        if sc.revision:
            f.write(f"(* Revision: {sc.revision} *)\n")
        if sc.vendor:
            f.write(f"(* Vendor: {sc.vendor} *)\n")
        if sc.description:
            f.write(f"(* Description: {sc.description} *)\n")
        f.write("\n")

        # UDT members
        if type_label == 'UDT' and sc.local_tags:
            f.write(f"TYPE {sc.name} :\n")
            f.write("STRUCT\n")
            for tag in sc.local_tags:
                desc = f"  // {tag.description}" if tag.description else ""
                if tag.is_array and tag.array_bounds:
                    f.write(f"\t{tag.name}: ARRAY[{tag.array_bounds}] OF {tag.data_type};{desc}\n")
                else:
                    f.write(f"\t{tag.name}: {tag.data_type};{desc}\n")
            f.write("END_STRUCT\n")
            f.write("END_TYPE\n")
            return

        # Parameters / tags
        if sc.input_tags:
            f.write("(* PARAMETERS *)\n")
            f.write("VAR_INPUT\n")
            for tag in sc.input_tags:
                desc = f"  // {tag.description}" if tag.description else ""
                f.write(f"\t{tag.name}: {tag.data_type};{desc}\n")
            f.write("END_VAR\n\n")

        if sc.output_tags:
            f.write("VAR_OUTPUT\n")
            for tag in sc.output_tags:
                desc = f"  // {tag.description}" if tag.description else ""
                f.write(f"\t{tag.name}: {tag.data_type};{desc}\n")
            f.write("END_VAR\n\n")

        if sc.inout_tags:
            f.write("VAR_IN_OUT\n")
            for tag in sc.inout_tags:
                desc = f"  // {tag.description}" if tag.description else ""
                f.write(f"\t{tag.name}: {tag.data_type};{desc}\n")
            f.write("END_VAR\n\n")

        if sc.local_tags:
            f.write("(* LOCAL TAGS *)\n")
            f.write("VAR\n")
            for tag in sc.local_tags:
                desc = f"  // {tag.description}" if tag.description else ""
                default = f" := {tag.default_value}" if tag.default_value else ""
                f.write(f"\t{tag.name}: {tag.data_type}{default};{desc}\n")
            f.write("END_VAR\n\n")

        # Implementation
        if sc.routines:
            f.write("(* IMPLEMENTATION *)\n")
            for routine in sc.routines:
                f.write(f"\n(* ROUTINE: {routine['name']} [{routine['type']}] *)\n")
                if routine.get('rungs'):
                    for rung in routine['rungs']:
                        if rung.comment:
                            f.write(f"\n// Rung {rung.number}: {rung.comment}\n")
                        else:
                            f.write(f"\n// Rung {rung.number}\n")
                        f.write(f"{rung.logic}\n")
                elif routine.get('raw_content'):
                    f.write(f"\n{routine['raw_content']}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """CLI for L5K parser."""
    if len(sys.argv) < 2:
        print("Usage: python l5k_parser.py <file.L5K> [output_dir]")
        print("  Parse L5K file and optionally export to .sc format")
        sys.exit(1)

    file_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    if not Path(file_path).exists():
        print(f"[ERROR] File not found: {file_path}")
        sys.exit(1)

    parser = L5KParser()

    if output_dir:
        count = parser.export_to_sc(file_path, output_dir)
        print(f"\n[OK] Exported {count} components to {output_dir}")
    else:
        sc_files = parser.parse_file(file_path)
        print(f"\n[INFO] Parsed {len(sc_files)} components from {Path(file_path).name}:")
        for sc in sc_files:
            tag_count = (len(sc.input_tags) + len(sc.output_tags) +
                         len(sc.inout_tags) + len(sc.local_tags))
            routine_count = len(sc.routines)
            desc = f" - {sc.description[:60]}..." if sc.description else ""
            print(f"  [{sc.type}] {sc.name}: {tag_count} tags, "
                  f"{routine_count} routines{desc}")


if __name__ == "__main__":
    main()
