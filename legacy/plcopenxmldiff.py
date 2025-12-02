#!/usr/bin/env python3
"""
Diff tool for comparing PLCopen XML files.
Extracts POUs, Methods, and GVLs from two XML files and generates unified diffs.

Usage:
    python plcopenxmldiff.py file1.xml file2.xml [output_dir]
"""

import sys
import os
from pathlib import Path
import difflib
from plcopenxmlprocessor import parse_plcopen_xml
import tempfile
import shutil


def extract_to_temp_dir(xml_path, label):
    """Extract all code from XML to a temporary directory."""
    temp_dir = tempfile.mkdtemp(prefix=f"plcopen_{label}_")
    try:
        parse_plcopen_xml(xml_path, temp_dir)
        return temp_dir
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise Exception(f"Failed to extract from {xml_path}: {e}")


def get_all_sc_files(directory):
    """Get all .sc files from a directory recursively."""
    sc_files = {}
    for sc_file in Path(directory).rglob("*.sc"):
        # Use relative path from directory as key
        rel_path = sc_file.relative_to(directory)
        sc_files[str(rel_path)] = sc_file
    return sc_files


def generate_unified_diff(file1_path, file2_path, file1_label="file1", file2_label="file2"):
    """Generate unified diff between two files."""
    # Convert Path objects to strings - use just the filename for cleaner diffs
    file1_str = Path(file1_path).name
    file2_str = Path(file2_path).name
    
    try:
        with open(file1_path, "r", encoding="utf-8") as f1:
            lines1 = f1.readlines()
        with open(file2_path, "r", encoding="utf-8") as f2:
            lines2 = f2.readlines()
    except Exception as e:
        return f"Error reading files: {e}\n"

    diff = difflib.unified_diff(
        lines1,
        lines2,
        fromfile=file1_str,
        tofile=file2_str,
        lineterm="",
        n=3,
    )
    diff_lines = list(diff)
    # Join with newlines to ensure proper formatting
    return "\n".join(diff_lines) + "\n" if diff_lines else ""


def compare_xml_files(xml1_path, xml2_path, output_dir=None):
    """Compare two XML files and generate diffs for all POUs/GVLs/Methods."""
    if output_dir is None:
        output_dir = "diffs"

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Extracting code from {xml1_path}...")
    temp_dir1 = extract_to_temp_dir(xml1_path, "file1")
    print(f"[INFO] Extracting code from {xml2_path}...")
    temp_dir2 = extract_to_temp_dir(xml2_path, "file2")

    try:
        files1 = get_all_sc_files(temp_dir1)
        files2 = get_all_sc_files(temp_dir2)

        # Find all unique files (in either set)
        all_files = set(files1.keys()) | set(files2.keys())

        diff_count = 0
        added_count = 0
        removed_count = 0
        unchanged_count = 0

        # Generate diff for each file
        for rel_path in sorted(all_files):
            file1_path = files1.get(rel_path)
            file2_path = files2.get(rel_path)

            if file1_path and file2_path:
                # Both files exist - generate diff
                diff_content = generate_unified_diff(file1_path, file2_path, xml1_path, xml2_path)
                
                # Check if there are actual differences
                if diff_content.strip():
                    diff_file = output_path / f"{rel_path}.diff"
                    diff_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(diff_file, "w", encoding="utf-8") as f:
                        f.write(diff_content)
                    print(f"[DIFF] {rel_path}")
                    diff_count += 1
                else:
                    unchanged_count += 1
            elif file1_path:
                # File exists only in file1 (removed)
                diff_file = output_path / f"{rel_path}.removed"
                diff_file.parent.mkdir(parents=True, exist_ok=True)
                with open(diff_file, "w", encoding="utf-8") as f:
                    f.write(f"--- {rel_path} (removed from {xml2_path})\n")
                    f.write(f"+++ /dev/null\n")
                    with open(file1_path, "r", encoding="utf-8") as f1:
                        for i, line in enumerate(f1, start=1):
                            f.write(f"-{line}")
                print(f"[REMOVED] {rel_path}")
                removed_count += 1
            elif file2_path:
                # File exists only in file2 (added)
                diff_file = output_path / f"{rel_path}.added"
                diff_file.parent.mkdir(parents=True, exist_ok=True)
                with open(diff_file, "w", encoding="utf-8") as f:
                    f.write(f"--- /dev/null\n")
                    f.write(f"+++ {rel_path} (added in {xml2_path})\n")
                    with open(file2_path, "r", encoding="utf-8") as f2:
                        for i, line in enumerate(f2, start=1):
                            f.write(f"+{line}")
                print(f"[ADDED] {rel_path}")
                added_count += 1

        # Generate summary
        summary_file = output_path / "diff_summary.txt"
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write(f"Diff Summary: {xml1_path} vs {xml2_path}\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Files with differences: {diff_count}\n")
            f.write(f"Files added: {added_count}\n")
            f.write(f"Files removed: {removed_count}\n")
            f.write(f"Files unchanged: {unchanged_count}\n")
            f.write(f"Total files: {len(all_files)}\n")

        print(f"\n[OK] Diff complete!")
        print(f"  Differences: {diff_count}")
        print(f"  Added: {added_count}")
        print(f"  Removed: {removed_count}")
        print(f"  Unchanged: {unchanged_count}")
        print(f"  Output directory: {output_dir}")

    finally:
        # Clean up temporary directories
        shutil.rmtree(temp_dir1, ignore_errors=True)
        shutil.rmtree(temp_dir2, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python plcopenxmldiff.py file1.xml file2.xml [output_dir]")
        sys.exit(1)

    xml_file1 = sys.argv[1]
    xml_file2 = sys.argv[2]
    output_dir = sys.argv[3] if len(sys.argv) > 3 else "diffs"

    if not os.path.exists(xml_file1):
        print(f"Error: File not found: {xml_file1}")
        sys.exit(1)

    if not os.path.exists(xml_file2):
        print(f"Error: File not found: {xml_file2}")
        sys.exit(1)

    try:
        compare_xml_files(xml_file1, xml_file2, output_dir)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

