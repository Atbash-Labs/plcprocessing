#!/usr/bin/env python3
"""
Parser for Siemens TIA Portal / AX Structured Text (.st) source files.
Extracts classes, types, programs, configurations, methods and variables
into the same SCFile/Tag/LogicRung dataclasses used by the Rockwell sc_parser,
so the downstream ontology pipeline works unchanged.

Supported constructs:
    NAMESPACE ... END_NAMESPACE
    TYPE ... STRUCT ... END_STRUCT END_TYPE
    CLASS ... END_CLASS  (Function Blocks)
        VAR PUBLIC / PRIVATE / PROTECTED
        METHOD ... END_METHOD
    PROGRAM ... END_PROGRAM
        VAR_EXTERNAL / VAR / VAR_INPUT / VAR_OUTPUT
    CONFIGURATION ... END_CONFIGURATION
        VAR_GLOBAL (with AT %IX/%QX addressing)

Usage:
    python siemens_parser.py <file.st>
    python siemens_parser.py <directory>   # parse all .st files
"""

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from sc_parser import SCFile, Tag, LogicRung


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _strip_comments(text: str) -> str:
    """Remove (* ... *) and // line comments, preserving line structure."""
    # Block comments (non-greedy, can span lines)
    text = re.sub(r"\(\*.*?\*\)", "", text, flags=re.DOTALL)
    # Line comments
    text = re.sub(r"//.*$", "", text, flags=re.MULTILINE)
    return text


def _strip_comments_preserve(text: str) -> str:
    """Strip block comments but keep // comments (they are useful as descriptions)."""
    text = re.sub(r"\(\*.*?\*\)", "", text, flags=re.DOTALL)
    return text


def _extract_inline_comment(line: str) -> Tuple[str, Optional[str]]:
    """Split a line into code part and optional // comment."""
    match = re.search(r"//\s*(.*?)\s*$", line)
    if match:
        code = line[: match.start()].rstrip()
        comment = match.group(1)
        return code, comment
    return line.rstrip(), None


# ---------------------------------------------------------------------------
# Variable / tag parsing
# ---------------------------------------------------------------------------

# Matches:  name : TYPE := default ;
# Handles AT %IX4.1 addresses, ARRAY, class instances
_VAR_LINE_RE = re.compile(
    r"^\s*"
    r"(\w+)"  # name
    r"(?:\s+AT\s+(%[A-Za-z0-9.]+))?"  # optional AT address
    r"\s*:\s*"  # colon
    r"([^;:=]+?)"  # data type (non-greedy up to ; or :=)
    r"(?:\s*:=\s*([^;]+?))?"  # optional default value
    r"\s*;"  # semicolon
    r"\s*$",
    re.MULTILINE,
)

_ARRAY_TYPE_RE = re.compile(r"ARRAY\s*\[\s*(.+?)\s*\]\s+OF\s+(\w+)", re.IGNORECASE)


def _parse_var_lines(text: str, direction: Optional[str] = None) -> List[Tag]:
    """Parse individual variable declaration lines from a VAR block body."""
    tags = []
    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("//") or stripped.startswith("(*"):
            continue

        code_part, description = _extract_inline_comment(stripped)
        if not code_part:
            continue

        m = _VAR_LINE_RE.match(
            code_part + ";" if not code_part.endswith(";") else code_part
        )
        if not m:
            # Try the line as-is with trailing semicolon ensured
            m = _VAR_LINE_RE.match(
                code_part if code_part.endswith(";") else code_part + ";"
            )
        if not m:
            continue

        name = m.group(1)
        at_addr = m.group(2)
        type_str = m.group(3).strip()
        default_val = m.group(4).strip() if m.group(4) else None

        # Append AT address info to description
        if at_addr:
            addr_note = f"AT {at_addr}"
            description = f"{addr_note} — {description}" if description else addr_note

        # Check for array types
        arr_match = _ARRAY_TYPE_RE.search(type_str)
        if arr_match:
            is_array = True
            array_bounds = arr_match.group(1)
            data_type = arr_match.group(2)
        else:
            is_array = False
            array_bounds = None
            data_type = type_str

        tags.append(
            Tag(
                name=name,
                data_type=data_type,
                direction=direction,
                description=description,
                default_value=default_val,
                is_array=is_array,
                array_bounds=array_bounds,
            )
        )

    return tags


# ---------------------------------------------------------------------------
# Block-level regex patterns
# ---------------------------------------------------------------------------

# VAR sections: VAR [PUBLIC|PRIVATE|PROTECTED|CONSTANT] ... END_VAR
_VAR_BLOCK_RE = re.compile(
    r"VAR(?:_(INPUT|OUTPUT|IN_OUT|EXTERNAL|GLOBAL))?\s*"
    r"(?:(PUBLIC|PRIVATE|PROTECTED|CONSTANT)\s*)?"
    r"\n(.*?)"
    r"END_VAR",
    re.DOTALL | re.IGNORECASE,
)

# TYPE ... END_TYPE  (contains STRUCT body)
_TYPE_BLOCK_RE = re.compile(
    r"TYPE\s+(\w+)\s*:\s*\s*STRUCT\s*\n(.*?)END_STRUCT\s*;?\s*END_TYPE",
    re.DOTALL | re.IGNORECASE,
)

# CLASS ... END_CLASS
_CLASS_BLOCK_RE = re.compile(
    r"CLASS\s+(\w+)(.*?)END_CLASS",
    re.DOTALL | re.IGNORECASE,
)

# METHOD ... END_METHOD
_METHOD_BLOCK_RE = re.compile(
    r"METHOD\s+(PUBLIC|PRIVATE|PROTECTED)?\s*(\w+)"
    r"(?:\s*:\s*(\w+))?\s*\n"  # optional return type
    r"(.*?)"
    r"END_METHOD",
    re.DOTALL | re.IGNORECASE,
)

# PROGRAM ... END_PROGRAM
_PROGRAM_BLOCK_RE = re.compile(
    r"PROGRAM\s+(\w+)\s*\n(.*?)END_PROGRAM",
    re.DOTALL | re.IGNORECASE,
)

# CONFIGURATION ... END_CONFIGURATION
_CONFIG_BLOCK_RE = re.compile(
    r"CONFIGURATION\s+(\w+)\s*\n(.*?)END_CONFIGURATION",
    re.DOTALL | re.IGNORECASE,
)

# NAMESPACE ... END_NAMESPACE
_NAMESPACE_BLOCK_RE = re.compile(
    r"NAMESPACE\s+([\w.]+)\s*\n(.*?)END_NAMESPACE",
    re.DOTALL | re.IGNORECASE,
)

# USING directives
_USING_RE = re.compile(r"^\s*USING\s+([\w.]+)\s*;", re.MULTILINE | re.IGNORECASE)

# TASK declaration inside CONFIGURATION
_TASK_RE = re.compile(
    r"TASK\s+(\w+)\s*\(([^)]*)\)\s*;",
    re.IGNORECASE,
)

# PROGRAM assignment: PROGRAM P1 WITH Main: ConveyorControl;
_PROG_ASSIGN_RE = re.compile(
    r"PROGRAM\s+(\w+)\s+WITH\s+(\w+)\s*:\s*(\w+)\s*;",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# SiemensSTParser
# ---------------------------------------------------------------------------


class SiemensSTParser:
    """
    Parse Siemens Structured Text (.st) source files.

    Returns a list of SCFile objects (one per top-level block found in the file).
    """

    def parse_file(self, file_path: str) -> List[SCFile]:
        """Parse a .st file and return all blocks as SCFile objects."""
        with open(file_path, "r", encoding="utf-8") as f:
            raw = f.read()

        # Collect USING directives before stripping them
        usings = _USING_RE.findall(raw)

        results: List[SCFile] = []

        # Check for NAMESPACE wrappers — parse inner content
        ns_matches = list(_NAMESPACE_BLOCK_RE.finditer(raw))
        if ns_matches:
            for ns_match in ns_matches:
                ns_name = ns_match.group(1)
                ns_body = ns_match.group(2)
                blocks = self._parse_content(
                    ns_body, file_path, namespace=ns_name, usings=usings
                )
                results.extend(blocks)

            # Also parse anything *outside* namespaces (e.g., top-level TYPE in tcpCommunication.st)
            outside = raw
            for ns_match in ns_matches:
                outside = outside.replace(ns_match.group(0), "")
            blocks = self._parse_content(
                outside, file_path, namespace=None, usings=usings
            )
            results.extend(blocks)
        else:
            results = self._parse_content(raw, file_path, namespace=None, usings=usings)

        return results

    def _parse_content(
        self,
        content: str,
        file_path: str,
        namespace: Optional[str],
        usings: List[str],
    ) -> List[SCFile]:
        """Parse content for TYPE, CLASS, PROGRAM, and CONFIGURATION blocks."""
        results: List[SCFile] = []

        # --- TYPE (UDT) blocks ---
        for m in _TYPE_BLOCK_RE.finditer(content):
            sc = self._parse_type_block(m, file_path, namespace, usings)
            results.append(sc)

        # --- CLASS (FB) blocks ---
        for m in _CLASS_BLOCK_RE.finditer(content):
            sc = self._parse_class_block(m, file_path, namespace, usings)
            results.append(sc)

        # --- PROGRAM blocks ---
        for m in _PROGRAM_BLOCK_RE.finditer(content):
            sc = self._parse_program_block(m, file_path, namespace, usings)
            results.append(sc)

        # --- CONFIGURATION blocks ---
        for m in _CONFIG_BLOCK_RE.finditer(content):
            sc = self._parse_config_block(m, file_path, namespace, usings)
            results.append(sc)

        return results

    # ------------------------------------------------------------------
    # TYPE (UDT)
    # ------------------------------------------------------------------
    def _parse_type_block(
        self,
        match: re.Match,
        file_path: str,
        namespace: Optional[str],
        usings: List[str],
    ) -> SCFile:
        type_name = match.group(1)
        struct_body = match.group(2)

        sc = SCFile(
            file_path=file_path,
            name=type_name,
            type="UDT",
            description=f"Siemens TYPE STRUCT{f' in namespace {namespace}' if namespace else ''}",
        )
        sc.local_tags = _parse_var_lines(struct_body, direction=None)
        return sc

    # ------------------------------------------------------------------
    # CLASS (Function Block)
    # ------------------------------------------------------------------
    def _parse_class_block(
        self,
        match: re.Match,
        file_path: str,
        namespace: Optional[str],
        usings: List[str],
    ) -> SCFile:
        class_name = match.group(1)
        class_body = match.group(2)

        sc = SCFile(
            file_path=file_path,
            name=class_name,
            type="FB",
            description=f"Siemens CLASS (Function Block){f' in namespace {namespace}' if namespace else ''}",
        )

        # Parse VAR sections (outside of methods)
        # First, blank out method bodies so we only get class-level vars
        body_no_methods = _METHOD_BLOCK_RE.sub("", class_body)
        self._parse_var_sections(body_no_methods, sc, context="class")

        # Parse methods as routines
        for mm in _METHOD_BLOCK_RE.finditer(class_body):
            routine = self._parse_method(mm)
            sc.routines.append(routine)

        # Build raw implementation from method bodies
        impl_parts = []
        for routine in sc.routines:
            impl_parts.append(
                f"(* METHOD: {routine['name']} [{routine['visibility']}] *)"
            )
            impl_parts.append(routine["raw_content"])
        if impl_parts:
            sc.raw_implementation = "\n\n".join(impl_parts)

        return sc

    # ------------------------------------------------------------------
    # PROGRAM
    # ------------------------------------------------------------------
    def _parse_program_block(
        self,
        match: re.Match,
        file_path: str,
        namespace: Optional[str],
        usings: List[str],
    ) -> SCFile:
        prog_name = match.group(1)
        prog_body = match.group(2)

        sc = SCFile(
            file_path=file_path,
            name=prog_name,
            type="PROGRAM",
            description=f"Siemens PROGRAM{f' in namespace {namespace}' if namespace else ''}",
        )

        # Parse VAR sections
        self._parse_var_sections(prog_body, sc, context="program")

        # Everything after the last END_VAR is the logic body
        logic_body = self._extract_logic_body(prog_body)
        if logic_body.strip():
            sc.raw_implementation = logic_body.strip()
            sc.routines.append(
                {
                    "name": "Main",
                    "type": "ST",
                    "visibility": "PUBLIC",
                    "rungs": [],
                    "raw_content": logic_body.strip(),
                }
            )

        return sc

    # ------------------------------------------------------------------
    # CONFIGURATION
    # ------------------------------------------------------------------
    def _parse_config_block(
        self,
        match: re.Match,
        file_path: str,
        namespace: Optional[str],
        usings: List[str],
    ) -> SCFile:
        config_name = match.group(1)
        config_body = match.group(2)

        sc = SCFile(
            file_path=file_path,
            name=config_name,
            type="CONFIGURATION",
            description=f"Siemens CONFIGURATION — task and I/O mapping",
        )

        # Parse TASK declarations
        tasks = []
        for tm in _TASK_RE.finditer(config_body):
            tasks.append(f"TASK {tm.group(1)}({tm.group(2)})")

        # Parse PROGRAM assignments
        prog_assigns = []
        for pm in _PROG_ASSIGN_RE.finditer(config_body):
            prog_assigns.append(
                f"PROGRAM {pm.group(1)} WITH {pm.group(2)}: {pm.group(3)}"
            )

        if tasks or prog_assigns:
            desc_parts = []
            if tasks:
                desc_parts.append("Tasks: " + "; ".join(tasks))
            if prog_assigns:
                desc_parts.append("Programs: " + "; ".join(prog_assigns))
            sc.description += " | " + " | ".join(desc_parts)

        # Parse VAR_GLOBAL — split inputs (%I) vs outputs (%Q) by address
        self._parse_var_sections(config_body, sc, context="config")

        return sc

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _parse_var_sections(self, body: str, sc: SCFile, context: str) -> None:
        """Extract all VAR blocks from a body and populate the SCFile."""
        for vm in _VAR_BLOCK_RE.finditer(body):
            qualifier = (
                vm.group(1) or ""
            ).upper()  # INPUT, OUTPUT, EXTERNAL, GLOBAL, or ''
            visibility = (
                vm.group(2) or ""
            ).upper()  # PUBLIC, PRIVATE, PROTECTED, CONSTANT, or ''
            var_body = vm.group(3)

            tags = _parse_var_lines(var_body)

            # Decide where to put these tags based on qualifier + visibility + context
            direction = self._resolve_direction(
                qualifier, visibility, context, var_body
            )

            for tag in tags:
                tag.direction = direction

            if direction == "INPUT":
                sc.input_tags.extend(tags)
            elif direction == "OUTPUT":
                sc.output_tags.extend(tags)
            elif direction in ("IN_OUT", "EXTERNAL"):
                sc.inout_tags.extend(tags)
            else:
                sc.local_tags.extend(tags)

    def _resolve_direction(
        self,
        qualifier: str,
        visibility: str,
        context: str,
        var_body: str,
    ) -> Optional[str]:
        """Determine the tag direction from qualifiers and context."""
        # Explicit IEC qualifiers always win
        if qualifier == "INPUT":
            return "INPUT"
        if qualifier == "OUTPUT":
            return "OUTPUT"
        if qualifier in ("IN_OUT", "EXTERNAL"):
            return "IN_OUT"

        # GLOBAL: split by AT address — %I = input, %Q = output, else local
        if qualifier == "GLOBAL":
            # This is handled per-tag, but we set a default direction.
            # Individual tags with AT addresses will have their direction
            # noted in the description. For the section as a whole,
            # treat as IN_OUT since globals are bidirectional.
            return "IN_OUT"

        # CLASS context: PUBLIC = output (externally visible), PRIVATE = local
        if context == "class":
            if visibility == "PUBLIC":
                return "OUTPUT"
            return None  # local

        # PROGRAM context: plain VAR = local
        return None

    def _parse_method(self, match: re.Match) -> Dict:
        """Parse a METHOD block into a routine dict."""
        visibility = (match.group(1) or "PUBLIC").upper()
        method_name = match.group(2)
        return_type = match.group(3)  # may be None
        method_body = match.group(4)

        # Extract local vars from the method
        local_tags = []
        for vm in _VAR_BLOCK_RE.finditer(method_body):
            local_tags.extend(_parse_var_lines(vm.group(3)))

        # Logic is everything after the last END_VAR
        logic_body = self._extract_logic_body(method_body)

        routine = {
            "name": method_name,
            "type": "ST",
            "visibility": visibility,
            "return_type": return_type,
            "local_tags": local_tags,
            "rungs": [],  # ST doesn't have rungs, but keep compatible
            "raw_content": logic_body.strip(),
        }

        return routine

    def _extract_logic_body(self, body: str) -> str:
        """
        Return everything after the last END_VAR in the body.
        If no END_VAR, return the whole body.
        """
        parts = re.split(r"END_VAR", body, flags=re.IGNORECASE)
        if len(parts) > 1:
            return parts[-1]
        return body


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_scfile_summary(sc: SCFile) -> None:
    """Print a human-readable summary of a parsed SCFile."""
    print(f"\n{'='*60}")
    print(f"  {sc.name}  ({sc.type})")
    print(f"  {sc.description or ''}")
    print(f"  Source: {sc.file_path}")
    print(f"{'='*60}")

    if sc.input_tags:
        print(f"\n  --- Input Tags ({len(sc.input_tags)}) ---")
        for t in sc.input_tags:
            desc = f"  // {t.description}" if t.description else ""
            default = f" := {t.default_value}" if t.default_value else ""
            print(f"    {t.name}: {t.data_type}{default}{desc}")

    if sc.output_tags:
        print(f"\n  --- Output Tags ({len(sc.output_tags)}) ---")
        for t in sc.output_tags:
            desc = f"  // {t.description}" if t.description else ""
            default = f" := {t.default_value}" if t.default_value else ""
            print(f"    {t.name}: {t.data_type}{default}{desc}")

    if sc.inout_tags:
        print(f"\n  --- InOut / External Tags ({len(sc.inout_tags)}) ---")
        for t in sc.inout_tags[:15]:
            desc = f"  // {t.description}" if t.description else ""
            default = f" := {t.default_value}" if t.default_value else ""
            print(f"    {t.name}: {t.data_type}{default}{desc}")
        if len(sc.inout_tags) > 15:
            print(f"    ... and {len(sc.inout_tags) - 15} more")

    if sc.local_tags:
        print(f"\n  --- Local / Struct Tags ({len(sc.local_tags)}) ---")
        for t in sc.local_tags[:15]:
            desc = f"  // {t.description}" if t.description else ""
            default = f" := {t.default_value}" if t.default_value else ""
            print(f"    {t.name}: {t.data_type}{default}{desc}")
        if len(sc.local_tags) > 15:
            print(f"    ... and {len(sc.local_tags) - 15} more")

    if sc.routines:
        print(f"\n  --- Routines ({len(sc.routines)}) ---")
        for r in sc.routines:
            vis = r.get("visibility", "")
            ret = f" : {r['return_type']}" if r.get("return_type") else ""
            content_preview = (
                r["raw_content"][:120].replace("\n", " ") + "..."
                if len(r["raw_content"]) > 120
                else r["raw_content"].replace("\n", " ")
            )
            print(f"    {vis} {r['name']}{ret}  ({r['type']})")
            print(f"      {content_preview}")


def main():
    """CLI: parse Siemens .st files and print summaries."""
    if len(sys.argv) < 2:
        print("Usage: python siemens_parser.py <file.st | directory>")
        sys.exit(1)

    target = Path(sys.argv[1])
    parser = SiemensSTParser()

    if target.is_dir():
        st_files = sorted(target.rglob("*.st"))
        if not st_files:
            print(f"[WARNING] No .st files found in {target}")
            sys.exit(1)
        print(f"[INFO] Found {len(st_files)} .st file(s) in {target}\n")
        for st_file in st_files:
            print(f"\n--- File: {st_file.name} ---")
            blocks = parser.parse_file(str(st_file))
            if not blocks:
                print("  (no parseable blocks found)")
            for sc in blocks:
                _print_scfile_summary(sc)
    elif target.is_file():
        blocks = parser.parse_file(str(target))
        if not blocks:
            print("  (no parseable blocks found)")
        for sc in blocks:
            _print_scfile_summary(sc)
    else:
        print(f"[ERROR] Path not found: {target}")
        sys.exit(1)


if __name__ == "__main__":
    main()
