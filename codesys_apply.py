#!/usr/bin/env python3
"""
Apply diffs to CODESYS text exports and import back to project.
Takes diff directory, applies changes to text files, then imports to project.

Usage:
    python codesys_apply.py <diff_dir> <target_dir> <project_path> [output_dir]
"""

import sys
import os
from pathlib import Path
import re
import shutil
import tempfile


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
            match = re.match(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
            if match:
                old_start = int(match.group(1)) - 1
                old_count = int(match.group(2)) if match.group(2) else 1
                
                # Read hunk lines
                hunk_lines = []
                i += 1
                while i < len(diff_lines) and not diff_lines[i].startswith("@@"):
                    hunk_lines.append(diff_lines[i])
                    i += 1
                
                # Extract the lines to match (context and removal lines)
                lines_to_match = []
                lines_to_remove = []
                for hunk_line in hunk_lines:
                    if hunk_line.startswith(" ") or hunk_line.startswith("-"):
                        # Context or removal line - normalize (strip leading space/dash)
                        normalized = hunk_line[1:] if len(hunk_line) > 0 else hunk_line
                        lines_to_match.append(normalized)
                        if hunk_line.startswith("-"):
                            lines_to_remove.append(normalized)
                
                # Try to find matching location in target (more flexible matching)
                # First try exact line number match
                match_start = old_start
                if match_start >= len(result) or match_start < 0:
                    # Line number out of range, try to find by content
                    match_start = -1
                    for j in range(len(result) - len(lines_to_match) + 1):
                        matches = True
                        for k, match_line in enumerate(lines_to_match):
                            if j + k >= len(result):
                                matches = False
                                break
                            # Normalize comparison (strip whitespace)
                            target_normalized = result[j + k].rstrip()
                            match_normalized = match_line.rstrip()
                            if target_normalized != match_normalized:
                                matches = False
                                break
                        if matches:
                            match_start = j
                            break
                
                if match_start >= 0 and match_start < len(result):
                    # Apply hunk
                    new_lines = []
                    old_idx = match_start
                    
                    for hunk_line in hunk_lines:
                        if hunk_line.startswith(" "):
                            # Context line - keep from original
                            if old_idx < len(result):
                                new_lines.append(result[old_idx])
                            old_idx += 1
                        elif hunk_line.startswith("-"):
                            # Remove line - skip it
                            old_idx += 1
                        elif hunk_line.startswith("+"):
                            # Add new line
                            new_lines.append(hunk_line[1:])
                    
                    # Replace the range in result
                    lines_to_remove_count = sum(
                        1 for l in hunk_lines if l.startswith(" ") or l.startswith("-")
                    )
                    result = (
                        result[:match_start]
                        + new_lines
                        + result[match_start + lines_to_remove_count:]
                    )
            else:
                i += 1
        else:
            i += 1
    
    return result


def apply_diff_to_file(target_file, diff_file, output_file):
    """Apply a unified diff to a target file."""
    if diff_file.suffix == ".removed":
        # File should be removed - don't create output
        print(f"[INFO] File {target_file} marked for removal")
        return False
    
    if diff_file.suffix == ".added":
        # Create new file from diff
        with open(diff_file, "r", encoding="utf-8") as f:
            content = f.read()
        # Extract lines starting with + (but skip header lines)
        new_lines = []
        for line in content.split("\n"):
            if line.startswith("+") and not line.startswith("+++"):
                new_lines.append(line[1:] + "\n")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        return True
    
    if not target_file.exists():
        print(f"[WARN] Target file {target_file} doesn't exist, skipping")
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


def apply_diffs(diff_dir, target_dir, project_path, output_dir=None):
    """Apply all diffs and import to project."""
    diff_path = Path(diff_dir)
    target_path = Path(target_dir)
    
    if not diff_path.exists():
        print(f"Error: Diff directory not found: {diff_dir}")
        return False
    
    if not target_path.exists():
        print(f"Error: Target directory not found: {target_dir}")
        return False
    
    # Create temporary directory for modified files
    if output_dir:
        temp_dir = Path(output_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_dir = Path(tempfile.mkdtemp(prefix="codesys_apply_"))
    
    try:
        # Copy target directory structure (always copy, even if output_dir is specified)
        shutil.copytree(target_dir, temp_dir, dirs_exist_ok=True)
        
        # Find all diff files
        # Handle both .sc and .st extensions
        diff_files = {}
        for diff_file in diff_path.rglob("*.diff"):
            rel_path = diff_file.relative_to(diff_path)
            base_name = str(rel_path).replace(".diff", "")
            # Handle .st files: GVL.gvl.st.diff -> GVL.gvl.st (keep full name)
            # Handle .sc files: GVL.sc.diff -> GVL (extract base)
            if base_name.endswith(".sc"):
                base_name = base_name[:-3]  # Remove .sc
            elif base_name.endswith(".prg.st"):
                base_name = base_name[:-7]  # Remove .prg.st
            elif base_name.endswith(".fb.st"):
                base_name = base_name[:-5]  # Remove .fb.st
            elif base_name.endswith(".fun.st"):
                base_name = base_name[:-6]  # Remove .fun.st
            elif base_name.endswith(".gvl.st"):
                base_name = base_name[:-6]  # Remove .gvl.st
            elif base_name.endswith(".meth.st"):
                base_name = base_name[:-7]  # Remove .meth.st
            elif base_name.endswith(".st"):
                base_name = base_name[:-3]  # Remove .st
            diff_files[base_name] = ('diff', diff_file, str(rel_path).replace(".diff", ""))
        
        for diff_file in diff_path.rglob("*.added"):
            rel_path = diff_file.relative_to(diff_path)
            base_name = str(rel_path).replace(".added", "")
            if base_name.endswith(".sc"):
                base_name = base_name[:-3]
            elif base_name.endswith(".prg.st"):
                base_name = base_name[:-7]
            elif base_name.endswith(".fb.st"):
                base_name = base_name[:-5]
            elif base_name.endswith(".fun.st"):
                base_name = base_name[:-6]
            elif base_name.endswith(".gvl.st"):
                base_name = base_name[:-6]
            elif base_name.endswith(".meth.st"):
                base_name = base_name[:-7]
            elif base_name.endswith(".st"):
                base_name = base_name[:-3]
            diff_files[base_name] = ('added', diff_file, str(rel_path).replace(".added", ""))
        
        for diff_file in diff_path.rglob("*.removed"):
            rel_path = diff_file.relative_to(diff_path)
            base_name = str(rel_path).replace(".removed", "")
            if base_name.endswith(".sc"):
                base_name = base_name[:-3]
            elif base_name.endswith(".prg.st"):
                base_name = base_name[:-7]
            elif base_name.endswith(".fb.st"):
                base_name = base_name[:-5]
            elif base_name.endswith(".fun.st"):
                base_name = base_name[:-6]
            elif base_name.endswith(".gvl.st"):
                base_name = base_name[:-6]
            elif base_name.endswith(".meth.st"):
                base_name = base_name[:-7]
            elif base_name.endswith(".st"):
                base_name = base_name[:-3]
            diff_files[base_name] = ('removed', diff_file, str(rel_path).replace(".removed", ""))
        
        # Helper function to find matching .st file
        def find_matching_st_file(base_name, original_filename=None):
            """Find matching .st file for a base name."""
            # If we have the original filename, try exact match first
            if original_filename:
                candidate = target_path / original_filename
                if candidate.exists():
                    return candidate
            
            # Try exact match with various extensions
            for ext in ['.prg.st', '.fb.st', '.fun.st', '.gvl.st', '.meth.st', '.st']:
                candidate = target_path / f"{base_name}{ext}"
                if candidate.exists():
                    return candidate
            
            # Try partial match (e.g., GVL -> GVL.gvl.st, PLC_PRG_METH -> PLC_PRG_METH.meth.st)
            for st_file in target_path.rglob("*.st"):
                # Check if base_name matches the stem (name without extension)
                stem_parts = st_file.stem.split('.')
                if base_name in stem_parts or stem_parts[0] == base_name:
                    return st_file
                # Also try if base_name starts with the stem
                if st_file.stem.startswith(base_name.split('_')[0]):
                    return st_file
            return None
        
        # Apply each diff
        applied_count = 0
        for base_name, (diff_type, diff_file, original_filename) in diff_files.items():
            # Find matching file
            target_file = find_matching_st_file(base_name, original_filename)
            
            if diff_type == 'removed':
                if target_file:
                    output_file = temp_dir / target_file.name
                    if output_file.exists():
                        output_file.unlink()
                        print(f"[OK] Removed {target_file.name}")
                        applied_count += 1
                else:
                    print(f"[INFO] File {base_name} not found (already removed?)")
            elif diff_type == 'added':
                # Determine extension from context or use .st
                output_file = temp_dir / f"{base_name}.st"
                if apply_diff_to_file(None, diff_file, output_file):
                    print(f"[OK] Added {output_file.name}")
                    applied_count += 1
            else:  # diff
                if target_file:
                    output_file = temp_dir / target_file.name
                    if apply_diff_to_file(target_file, diff_file, output_file):
                        print(f"[OK] Applied diff to {target_file.name}")
                        applied_count += 1
                else:
                    print(f"[WARN] No matching file found for diff {diff_file.name}")
        
        print(f"\n[OK] Applied {applied_count} diffs to text files")
        print(f"[INFO] Modified files in: {temp_dir}")
        print(f"\n[INFO] To import to project, run:")
        print(f'  codesys_import.py "{project_path}" "{temp_dir}"')
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python codesys_apply.py <diff_dir> <target_dir> <project_path> [output_dir]")
        print("\nExample:")
        print('  python codesys_apply.py diffs/ export1/ "C:\\Projects\\MyProject.project" modified/')
        sys.exit(1)
    
    diff_dir = sys.argv[1]
    target_dir = sys.argv[2]
    project_path = sys.argv[3]
    output_dir = sys.argv[4] if len(sys.argv) > 4 else None
    
    if not os.path.exists(project_path):
        print(f"Error: Project file not found: {project_path}")
        sys.exit(1)
    
    apply_diffs(diff_dir, target_dir, project_path, output_dir)

