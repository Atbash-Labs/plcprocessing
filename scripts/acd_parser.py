#!/usr/bin/env python3
"""
Parser for Rockwell/Allen-Bradley ACD (binary archive) project files.

ACD files are the native binary project format for Studio 5000 / RSLogix 5000.
They are archive files containing compressed XML, database files, and metadata.

This parser uses the 'acd-tools' library (pip install acd-tools) to extract
the project structure and converts it into SCFile objects compatible with
the ontology pipeline.

If acd-tools is not installed, this parser falls back to extracting the
embedded L5X/XML content from the ACD archive and parsing it with the
L5X parser.

Reference: https://github.com/hutcheb/acd
"""

import sys
import os
import re
import tempfile
import zipfile
import struct
from pathlib import Path
from typing import List, Optional

from sc_parser import SCFile, Tag, LogicRung


# Try to import acd-tools
_HAS_ACD_TOOLS = False
try:
    from acd import ImportProjectFromFile
    _HAS_ACD_TOOLS = True
except ImportError:
    pass


class ACDParser:
    """Parser for Rockwell ACD (binary) project files.

    Attempts to use the acd-tools library if available, otherwise
    falls back to extracting embedded XML from the ACD archive.
    """

    def parse_file(self, file_path: str) -> List[SCFile]:
        """Parse an ACD file and return list of SCFile objects."""
        if _HAS_ACD_TOOLS:
            return self._parse_with_acd_tools(file_path)
        else:
            return self._parse_with_fallback(file_path)

    def _parse_with_acd_tools(self, file_path: str) -> List[SCFile]:
        """Parse using the acd-tools library."""
        results: List[SCFile] = []

        try:
            project = ImportProjectFromFile(file_path).import_project()
            controller = project.controller
        except Exception as e:
            print(f"[ERROR] acd-tools failed to parse {file_path}: {e}")
            return self._parse_with_fallback(file_path)

        controller_name = getattr(controller, 'name', 'Unknown')

        # Extract data types
        if hasattr(controller, 'data_types') and controller.data_types:
            for dt in controller.data_types:
                sc = self._convert_datatype(dt, file_path)
                if sc:
                    results.append(sc)

        # Extract programs and routines
        if hasattr(controller, 'programs') and controller.programs:
            for program in controller.programs:
                sc = self._convert_program(program, file_path)
                if sc:
                    results.append(sc)

        # Extract tags
        if hasattr(controller, 'tags') and controller.tags:
            ctrl_sc = SCFile(
                file_path=file_path,
                name=controller_name,
                type='CONTROLLER',
            )
            for tag in controller.tags:
                tag_name = getattr(tag, 'name', None)
                tag_type = getattr(tag, 'data_type', 'BOOL')
                tag_desc = getattr(tag, 'description', None)
                if tag_name:
                    ctrl_sc.local_tags.append(Tag(
                        name=tag_name,
                        data_type=str(tag_type) if tag_type else 'BOOL',
                        description=str(tag_desc) if tag_desc else None,
                    ))
            if ctrl_sc.local_tags:
                results.append(ctrl_sc)

        return results

    def _convert_datatype(self, dt, source_file: str) -> Optional[SCFile]:
        """Convert an acd-tools data type to SCFile."""
        name = getattr(dt, 'name', None)
        if not name:
            return None

        sc = SCFile(
            file_path=source_file,
            name=name,
            type='UDT',
            description=getattr(dt, 'description', None),
        )

        if hasattr(dt, 'members'):
            for member in dt.members:
                mem_name = getattr(member, 'name', '')
                mem_type = getattr(member, 'data_type', 'SINT')
                mem_desc = getattr(member, 'description', None)
                if mem_name and not mem_name.startswith('ZZZZZZZZ'):
                    sc.local_tags.append(Tag(
                        name=mem_name,
                        data_type=str(mem_type) if mem_type else 'SINT',
                        description=str(mem_desc) if mem_desc else None,
                    ))

        return sc if sc.local_tags else None

    def _convert_program(self, program, source_file: str) -> Optional[SCFile]:
        """Convert an acd-tools program to SCFile."""
        name = getattr(program, 'name', None)
        if not name:
            return None

        sc = SCFile(
            file_path=source_file,
            name=name,
            type='PROGRAM',
            description=getattr(program, 'description', None),
        )

        # Extract routines
        if hasattr(program, 'routines') and program.routines:
            for routine in program.routines:
                r_name = getattr(routine, 'name', 'Main')
                r_type = getattr(routine, 'type', 'RLL')
                rungs: List[LogicRung] = []
                raw_parts = []

                if hasattr(routine, 'rungs') and routine.rungs:
                    for i, rung in enumerate(routine.rungs):
                        rung_text = str(rung) if rung else ""
                        comment = getattr(rung, 'comment', None)
                        rungs.append(LogicRung(
                            number=i,
                            comment=str(comment) if comment else None,
                            logic=rung_text,
                        ))
                        raw_parts.append(rung_text)

                sc.routines.append({
                    'name': r_name,
                    'type': str(r_type) if r_type else 'RLL',
                    'rungs': rungs,
                    'raw_content': '\n'.join(raw_parts),
                })

        # Extract program-scoped tags
        if hasattr(program, 'tags') and program.tags:
            for tag in program.tags:
                tag_name = getattr(tag, 'name', None)
                tag_type = getattr(tag, 'data_type', 'BOOL')
                tag_desc = getattr(tag, 'description', None)
                if tag_name:
                    sc.local_tags.append(Tag(
                        name=tag_name,
                        data_type=str(tag_type) if tag_type else 'BOOL',
                        description=str(tag_desc) if tag_desc else None,
                    ))

        # Build raw implementation
        if sc.routines:
            parts = []
            for r in sc.routines:
                parts.append(f"(* ROUTINE: {r['name']} [{r['type']}] *)")
                if r.get('raw_content'):
                    parts.append(r['raw_content'])
            sc.raw_implementation = '\n'.join(parts)

        return sc

    def _parse_with_fallback(self, file_path: str) -> List[SCFile]:
        """Fallback: extract embedded XML from ACD archive and parse with L5X parser.

        ACD files are essentially archives containing compressed XML among other data.
        We attempt to extract any XML content and parse it.
        """
        print("[INFO] acd-tools not available, using fallback XML extraction...")

        # Try to read the file and look for embedded XML
        xml_content = self._extract_xml_from_acd(file_path)

        if not xml_content:
            print(f"[WARNING] Could not extract XML from ACD file: {file_path}")
            print("[INFO] Install acd-tools for full ACD support: pip install acd-tools")
            return []

        # Parse the extracted XML with L5X parser
        from l5x_export import L5XParser
        import xml.etree.ElementTree as ET

        results: List[SCFile] = []
        l5x_parser = L5XParser()

        for xml_str in xml_content:
            try:
                root = ET.fromstring(xml_str)
                parsed = l5x_parser._parse_root(root, source_file=file_path)
                results.extend(parsed)
            except ET.ParseError:
                continue

        if not results:
            print(f"[WARNING] No parseable content found in ACD file: {file_path}")
            print("[INFO] Install acd-tools for full ACD support: pip install acd-tools")

        return results

    def _extract_xml_from_acd(self, file_path: str) -> List[str]:
        """Attempt to extract embedded XML content from an ACD binary file.

        ACD files contain compressed (zlib) XML sections. We scan the binary
        for zlib-compressed blocks and attempt to decompress them, then check
        if the result looks like XML.
        """
        import zlib

        xml_sections = []

        try:
            with open(file_path, 'rb') as f:
                data = f.read()
        except IOError as e:
            print(f"[ERROR] Cannot read ACD file: {e}")
            return []

        # Strategy 1: Look for zlib headers (0x78 0x9C, 0x78 0x01, 0x78 0xDA)
        zlib_headers = [b'\x78\x9c', b'\x78\x01', b'\x78\xda']

        for header in zlib_headers:
            pos = 0
            while True:
                idx = data.find(header, pos)
                if idx == -1:
                    break

                # Try to decompress from this position
                for length in [len(data) - idx, min(len(data) - idx, 1024 * 1024)]:
                    try:
                        decompressed = zlib.decompress(data[idx:idx + length])
                        text = decompressed.decode('utf-8', errors='ignore')
                        # Check if it contains XML-like content
                        if '<?xml' in text or '<Controller' in text or '<RSLogix5000Content' in text:
                            # Clean up to get valid XML
                            xml_start = text.find('<?xml')
                            if xml_start == -1:
                                xml_start = text.find('<RSLogix5000Content')
                            if xml_start == -1:
                                xml_start = text.find('<Controller')
                            if xml_start >= 0:
                                xml_sections.append(text[xml_start:])
                        break
                    except (zlib.error, UnicodeDecodeError):
                        continue

                pos = idx + 1

        # Strategy 2: Look for raw XML strings in the binary
        if not xml_sections:
            text = data.decode('utf-8', errors='ignore')
            # Find <Controller> blocks
            ctrl_matches = list(re.finditer(
                r'<Controller\s+[^>]*>.*?</Controller>',
                text,
                re.DOTALL,
            ))
            for m in ctrl_matches:
                xml_sections.append(m.group(0))

        return xml_sections

    def export_to_sc(self, file_path: str, output_dir: str) -> int:
        """Parse ACD file and export each component as .sc files."""
        from l5k_parser import _write_sc_file

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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """CLI for ACD parser."""
    if len(sys.argv) < 2:
        print("Usage: python acd_parser.py <file.ACD> [output_dir]")
        print("  Parse ACD file and optionally export to .sc format")
        if not _HAS_ACD_TOOLS:
            print("\n[INFO] For best results, install acd-tools: pip install acd-tools")
        sys.exit(1)

    file_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    if not Path(file_path).exists():
        print(f"[ERROR] File not found: {file_path}")
        sys.exit(1)

    parser = ACDParser()

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
