#!/usr/bin/env python3
"""
Apply diffs to PLCopen XML files.
Takes a diff directory (from plcopenxmldiff.py) and applies it to a target XML file.

Usage:
    python plcopenxmlapply.py diff_dir/ target.xml output.xml
"""

import sys
import os
from pathlib import Path
import re
import shutil
import tempfile
from plcopenxmlprocessor import parse_plcopen_xml
from plcopenxmlmerge import update_xml_with_sc_content


def apply_unified_diff(target_lines, diff_lines):
    """Apply a unified diff to target lines. Returns new lines."""
    result = list(target_lines)
    i = 0

    while i < len(diff_lines):
        line = diff_lines[i]

        # Skip header lines
        if line.startswith("---") or line.startswith("+++"):
            i += 1
            continue

        # Hunk header: @@ -start,count +start,count @@
        if line.startswith("@@"):
            # Parse hunk header
            match = re.match(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
            if match:
                old_start = (
                    int(match.group(1)) - 1
                )  # Convert to 0-based (line numbers in diff are 1-based)
                old_count = int(match.group(2)) if match.group(2) else 1

                # Read hunk lines
                hunk_lines = []
                i += 1
                while i < len(diff_lines) and not diff_lines[i].startswith("@@"):
                    hunk_lines.append(diff_lines[i])
                    i += 1

                # Apply hunk: build replacement lines
                new_lines = []
                old_idx = old_start

                for hunk_line in hunk_lines:
                    if hunk_line.startswith(" "):
                        # Context line - keep from original
                        if old_idx < len(result):
                            new_lines.append(result[old_idx])
                        old_idx += 1
                    elif hunk_line.startswith("-"):
                        # Remove line - skip it in original
                        old_idx += 1
                    elif hunk_line.startswith("+"):
                        # Add new line
                        new_lines.append(hunk_line[1:])

                # Replace the range in result
                # Calculate how many lines we're replacing
                lines_to_remove = sum(
                    1 for l in hunk_lines if l.startswith(" ") or l.startswith("-")
                )
                result = (
                    result[:old_start]
                    + new_lines
                    + result[old_start + lines_to_remove :]
                )
            else:
                i += 1
        else:
            i += 1

    return result


def apply_diff_to_file(target_file, diff_file, output_file):
    """Apply a unified diff to a target file."""
    if not target_file.exists():
        # File doesn't exist - check if this is an "added" file
        if diff_file.suffix == ".added":
            # Create new file from diff
            with open(diff_file, "r", encoding="utf-8") as f:
                content = f.read()
            # Extract lines starting with + (but skip header lines like --- and +++)
            new_lines = []
            for line in content.split("\n"):
                if line.startswith("+") and not line.startswith("+++"):
                    # Remove + prefix
                    clean_line = line[1:]
                    # Add newline if not present
                    if not clean_line.endswith("\n"):
                        clean_line += "\n"
                    new_lines.append(clean_line)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            return True
        else:
            print(f"[WARN] Target file {target_file} doesn't exist, skipping")
            return False

    if diff_file.suffix == ".removed":
        # File should be removed - don't create output
        print(f"[INFO] File {target_file} marked for removal")
        return False

    # Read target file and diff file
    with open(target_file, "r", encoding="utf-8") as f:
        target_lines = f.readlines()

    with open(diff_file, "r", encoding="utf-8") as f:
        diff_lines = f.readlines()

    # Apply diff
    result_lines = apply_unified_diff(target_lines, diff_lines)

    # Write result
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.writelines(result_lines)

    return True


def apply_diffs(diff_dir, target_xml, output_xml):
    """Apply all diffs from diff_dir to target_xml, creating output_xml."""
    diff_path = Path(diff_dir)
    if not diff_path.exists():
        print(f"Error: Diff directory not found: {diff_dir}")
        return False

    # Extract target XML to temporary directory
    print(f"[INFO] Extracting code from {target_xml}...")
    temp_dir = tempfile.mkdtemp(prefix="plcopen_apply_")

    try:
        parse_plcopen_xml(target_xml, temp_dir)

        # Find all diff files
        diff_files = {}
        for diff_file in diff_path.rglob("*.diff"):
            rel_path = diff_file.relative_to(diff_path)
            # Remove .diff extension to get original filename
            sc_name = str(rel_path).replace(".diff", "")
            diff_files[sc_name] = diff_file

        # Also check for .added and .removed files
        for diff_file in diff_path.rglob("*.added"):
            rel_path = diff_file.relative_to(diff_path)
            sc_name = str(rel_path).replace(".added", "")
            diff_files[sc_name] = diff_file

        for diff_file in diff_path.rglob("*.removed"):
            rel_path = diff_file.relative_to(diff_path)
            sc_name = str(rel_path).replace(".removed", "")
            diff_files[sc_name] = diff_file

        # Apply each diff
        applied_count = 0
        for sc_name, diff_file in diff_files.items():
            target_file = Path(temp_dir) / sc_name
            output_file = Path(temp_dir) / sc_name

            if apply_diff_to_file(target_file, diff_file, output_file):
                applied_count += 1
                print(f"[OK] Applied diff to {sc_name}")

        print(f"\n[OK] Applied {applied_count} diffs to .sc files")

        # Try to merge back into XML
        print(f"\n[INFO] Merging changes back into XML...")
        try:
            # Pass both the temp_dir (for modified .sc files) and diff_dir (for .removed files)
            update_xml_with_sc_content(target_xml, temp_dir, output_xml, diff_path)
            print(f"[OK] Successfully created updated XML: {output_xml}")
        except Exception as e:
            print(f"[WARN] Could not automatically merge to XML: {e}")
            print(f"[INFO] Modified .sc files are in: {temp_dir}")
            print(
                f"[INFO] You can manually update {output_xml} or use plcopenxmlmerge.py"
            )

        return True

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        # Don't remove temp dir - user might want to inspect it
        print(f"[INFO] Temporary files preserved in: {temp_dir}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python plcopenxmlapply.py diff_dir/ target.xml output.xml")
        print("\nNote: This creates modified .sc files. You'll need to manually")
        print("      update the XML file or use a merge tool.")
        sys.exit(1)

    diff_dir = sys.argv[1]
    target_xml = sys.argv[2]
    output_xml = sys.argv[3]

    if not os.path.exists(diff_dir):
        print(f"Error: Diff directory not found: {diff_dir}")
        sys.exit(1)

    if not os.path.exists(target_xml):
        print(f"Error: Target XML file not found: {target_xml}")
        sys.exit(1)

    apply_diffs(diff_dir, target_xml, output_xml)
