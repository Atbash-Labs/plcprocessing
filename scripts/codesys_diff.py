#!/usr/bin/env python3
"""
Diff two directories of CODESYS text exports.
Generates unified diffs for source control.

Usage:
    python codesys_diff.py <dir1> <dir2> <output_dir>
"""

import sys
import os
from pathlib import Path
import difflib


def get_all_st_files(directory):
    """Get all .st files from a directory recursively."""
    st_files = {}
    for st_file in Path(directory).rglob("*.st"):
        rel_path = st_file.relative_to(directory)
        st_files[str(rel_path)] = st_file
    return st_files


def generate_unified_diff(file1_path, file2_path, file1_label="file1", file2_label="file2"):
    """Generate unified diff between two files."""
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
    return "\n".join(diff_lines) + "\n" if diff_lines else ""


def compare_directories(dir1_path, dir2_path, output_dir):
    """Compare two directories and generate diffs."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    files1 = get_all_st_files(dir1_path)
    files2 = get_all_st_files(dir2_path)
    
    # Find all unique files
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
            diff_content = generate_unified_diff(file1_path, file2_path, dir1_path, dir2_path)
            
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
            # File exists only in dir1 (removed)
            diff_file = output_path / f"{rel_path}.removed"
            diff_file.parent.mkdir(parents=True, exist_ok=True)
            with open(diff_file, "w", encoding="utf-8") as f:
                f.write(f"--- {rel_path} (removed)\n")
                f.write(f"+++ /dev/null\n")
                with open(file1_path, "r", encoding="utf-8") as f1:
                    for line in f1:
                        f.write(f"-{line}")
            print(f"[REMOVED] {rel_path}")
            removed_count += 1
        elif file2_path:
            # File exists only in dir2 (added)
            diff_file = output_path / f"{rel_path}.added"
            diff_file.parent.mkdir(parents=True, exist_ok=True)
            with open(diff_file, "w", encoding="utf-8") as f:
                f.write(f"--- /dev/null\n")
                f.write(f"+++ {rel_path} (added)\n")
                with open(file2_path, "r", encoding="utf-8") as f2:
                    for line in f2:
                        f.write(f"+{line}")
            print(f"[ADDED] {rel_path}")
            added_count += 1
    
    # Generate summary
    summary_file = output_path / "diff_summary.txt"
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(f"Diff Summary: {dir1_path} vs {dir2_path}\n")
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


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python codesys_diff.py <dir1> <dir2> <output_dir>")
        sys.exit(1)
    
    dir1 = sys.argv[1]
    dir2 = sys.argv[2]
    output_dir = sys.argv[3]
    
    if not os.path.exists(dir1):
        print(f"Error: Directory not found: {dir1}")
        sys.exit(1)
    
    if not os.path.exists(dir2):
        print(f"Error: Directory not found: {dir2}")
        sys.exit(1)
    
    try:
        compare_directories(dir1, dir2, output_dir)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

