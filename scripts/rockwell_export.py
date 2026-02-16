#!/usr/bin/env python3
"""
Unified Rockwell/Allen-Bradley PLC file handler.

Auto-detects and processes all Rockwell PLC file formats:
  - .L5X  — XML-based project/component export (Studio 5000)
  - .L5K  — ASCII text-based full project export (Studio 5000 / RSLogix 5000)
  - .ACD  — Native binary project file (Studio 5000 / RSLogix 5000)
  - .RSS  — RSLogix 500 project file (SLC 500 / MicroLogix) [detection only]

Each format is parsed into a list of SCFile objects that feed into the
existing ontology pipeline (ontology_analyzer.py → Claude → Neo4j).

Usage:
    python rockwell_export.py <input_file_or_dir> [output_dir]
    python rockwell_export.py <input_file_or_dir> --parse-only

Examples:
    python rockwell_export.py project.L5X export/
    python rockwell_export.py project.L5K export/
    python rockwell_export.py project.ACD export/
    python rockwell_export.py plc_files/ export/    # Process all Rockwell files
"""

import sys
import os
from pathlib import Path
from typing import List, Optional, Tuple

from sc_parser import SCFile


# ---------------------------------------------------------------------------
# File format detection
# ---------------------------------------------------------------------------

# Supported Rockwell file extensions (case-insensitive)
ROCKWELL_EXTENSIONS = {
    '.l5x': 'L5X',
    '.l5k': 'L5K',
    '.acd': 'ACD',
    '.rss': 'RSS',
}


def detect_rockwell_format(file_path: str) -> Optional[str]:
    """Detect the Rockwell PLC file format from extension and content.

    Returns one of: 'L5X', 'L5K', 'ACD', 'RSS', or None if not recognized.
    """
    ext = Path(file_path).suffix.lower()

    # Extension-based detection
    fmt = ROCKWELL_EXTENSIONS.get(ext)
    if fmt:
        return fmt

    # Content-based detection for ambiguous extensions
    try:
        with open(file_path, 'rb') as f:
            header = f.read(512)

        # Check for XML (L5X)
        if b'<?xml' in header or b'<RSLogix5000Content' in header:
            return 'L5X'

        # Check for L5K text format
        text_header = header.decode('ascii', errors='ignore')
        if 'IE_VER' in text_header or 'CONTROLLER' in text_header:
            return 'L5K'

        # ACD files typically start with specific binary signatures
        # (they are archive/database files)
        if header[:4] in (b'RSBA', b'RSBa', b'\x00\x00\x00\x00') or ext == '.acd':
            return 'ACD'

    except IOError:
        pass

    return None


def is_rockwell_file(file_path: str) -> bool:
    """Check if a file is a recognized Rockwell PLC format."""
    return detect_rockwell_format(file_path) is not None


def find_rockwell_files(directory: str) -> List[Tuple[str, str]]:
    """Find all Rockwell PLC files in a directory.

    Returns list of (file_path, format) tuples.
    """
    results = []
    dir_path = Path(directory)

    for ext in ROCKWELL_EXTENSIONS:
        for f in dir_path.rglob(f"*{ext}"):
            fmt = detect_rockwell_format(str(f))
            if fmt:
                results.append((str(f), fmt))
        # Also check uppercase
        for f in dir_path.rglob(f"*{ext.upper()}"):
            fmt = detect_rockwell_format(str(f))
            if fmt and (str(f), fmt) not in results:
                results.append((str(f), fmt))

    return sorted(results, key=lambda x: x[0])


# ---------------------------------------------------------------------------
# Unified parsing
# ---------------------------------------------------------------------------

def parse_rockwell_file(file_path: str,
                        format_hint: Optional[str] = None) -> List[SCFile]:
    """Parse any Rockwell PLC file into SCFile objects.

    Args:
        file_path: Path to the Rockwell file
        format_hint: Optional format override ('L5X', 'L5K', 'ACD', 'RSS')

    Returns:
        List of SCFile objects extracted from the file.
    """
    fmt = format_hint or detect_rockwell_format(file_path)

    if fmt is None:
        print(f"[ERROR] Unrecognized file format: {file_path}")
        return []

    if fmt == 'L5X':
        from l5x_export import L5XParser
        parser = L5XParser()
        return parser.parse_file(file_path)

    elif fmt == 'L5K':
        from l5k_parser import L5KParser
        parser = L5KParser()
        return parser.parse_file(file_path)

    elif fmt == 'ACD':
        from acd_parser import ACDParser
        parser = ACDParser()
        return parser.parse_file(file_path)

    elif fmt == 'RSS':
        print(f"[WARNING] RSS (RSLogix 500) files are proprietary binary format.")
        print(f"[INFO] RSS support is limited to detection only.")
        print(f"[INFO] To process RSS files, first convert them to L5X or L5K")
        print(f"[INFO] using Rockwell's Logix Designer Export tool.")
        return []

    else:
        print(f"[ERROR] Unsupported format: {fmt}")
        return []


def parse_rockwell_directory(directory: str) -> List[SCFile]:
    """Parse all Rockwell PLC files in a directory.

    Returns combined list of SCFile objects from all files.
    """
    files = find_rockwell_files(directory)
    if not files:
        print(f"[WARNING] No Rockwell PLC files found in {directory}")
        return []

    # Group by format for reporting
    by_format = {}
    for fp, fmt in files:
        by_format.setdefault(fmt, []).append(fp)

    print(f"[INFO] Found {len(files)} Rockwell PLC file(s):")
    for fmt, fps in sorted(by_format.items()):
        print(f"  {fmt}: {len(fps)} file(s)")

    all_results: List[SCFile] = []
    for fp, fmt in files:
        print(f"\n[INFO] Processing [{fmt}]: {Path(fp).name}")
        try:
            results = parse_rockwell_file(fp, format_hint=fmt)
            all_results.extend(results)
            print(f"  [OK] Extracted {len(results)} components")
        except Exception as e:
            print(f"  [ERROR] Failed to process {Path(fp).name}: {e}")

    return all_results


# ---------------------------------------------------------------------------
# Export to .sc files
# ---------------------------------------------------------------------------

def export_rockwell_to_sc(input_path: str, output_dir: str) -> int:
    """Parse Rockwell file(s) and export to .sc format.

    Args:
        input_path: File or directory path
        output_dir: Output directory for .sc files

    Returns:
        Count of exported .sc files.
    """
    from l5k_parser import _write_sc_file

    os.makedirs(output_dir, exist_ok=True)

    if os.path.isdir(input_path):
        sc_files = parse_rockwell_directory(input_path)
    else:
        sc_files = parse_rockwell_file(input_path)

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

    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """CLI for unified Rockwell file handler."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Unified Rockwell PLC file handler (L5X, L5K, ACD)"
    )
    parser.add_argument(
        "input",
        help="Path to Rockwell PLC file (.L5X, .L5K, .ACD) or directory"
    )
    parser.add_argument(
        "output",
        nargs="?",
        help="Output directory for .sc files (omit for parse-only mode)"
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Parse and display summary without exporting"
    )
    parser.add_argument(
        "--detect",
        action="store_true",
        help="Detect file format only"
    )

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"[ERROR] Input not found: {args.input}")
        sys.exit(1)

    if args.detect:
        if os.path.isdir(args.input):
            files = find_rockwell_files(args.input)
            if files:
                print(f"Found {len(files)} Rockwell PLC file(s):")
                for fp, fmt in files:
                    print(f"  [{fmt}] {fp}")
            else:
                print("No Rockwell PLC files found.")
        else:
            fmt = detect_rockwell_format(args.input)
            if fmt:
                print(f"[{fmt}] {args.input}")
            else:
                print(f"Not a recognized Rockwell PLC file: {args.input}")
        return

    if args.parse_only or not args.output:
        # Parse and display summary
        if os.path.isdir(args.input):
            sc_files = parse_rockwell_directory(args.input)
        else:
            sc_files = parse_rockwell_file(args.input)

        print(f"\n{'=' * 60}")
        print(f"  Parsed {len(sc_files)} components")
        print(f"{'=' * 60}")

        # Group by type
        by_type = {}
        for sc in sc_files:
            by_type.setdefault(sc.type, []).append(sc)

        for sc_type, items in sorted(by_type.items()):
            print(f"\n  {sc_type} ({len(items)}):")
            for sc in items[:20]:
                tag_count = (len(sc.input_tags) + len(sc.output_tags) +
                             len(sc.inout_tags) + len(sc.local_tags))
                routine_count = len(sc.routines)
                desc = f" — {sc.description[:50]}..." if sc.description else ""
                print(f"    {sc.name}: {tag_count} tags, "
                      f"{routine_count} routines{desc}")
            if len(items) > 20:
                print(f"    ... and {len(items) - 20} more")

    else:
        # Export to .sc files
        count = export_rockwell_to_sc(args.input, args.output)
        print(f"\n[OK] Exported {count} components to {args.output}")


if __name__ == "__main__":
    main()
