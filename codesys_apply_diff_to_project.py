#!/usr/bin/env python3
"""
Apply a diff to a CODESYS project and import the results back.
Complete workflow: Export → Apply Diff → Import (overwrites project)

Usage:
    python codesys_apply_diff_to_project.py <project_path> <diff_dir> [--codesys-path PATH]
"""

import sys
import os
import subprocess
import tempfile
import shutil
from pathlib import Path


def find_codesys_exe(codesys_path=None):
    """Find CODESYS executable."""
    if codesys_path:
        exe = Path(codesys_path)
        if exe.exists():
            return str(exe)
        raise FileNotFoundError(f"CODESYS executable not found: {codesys_path}")
    
    common_paths = [
        r"C:\Program Files\CODESYS\CODESYS.exe",
        r"C:\Program Files (x86)\CODESYS\CODESYS.exe",
        r"C:\Program Files\CODESYS V3.5 SP21\CODESYS.exe",
        r"C:\Program Files\CODESYS V3.5 SP20\CODESYS.exe",
    ]
    
    for path in common_paths:
        if os.path.exists(path):
            return path
    
    raise FileNotFoundError(
        "CODESYS executable not found. Please specify with --codesys-path"
    )


def get_script_path(script_name):
    """Get absolute path to a script in the same directory."""
    script_dir = Path(__file__).parent.absolute()
    script_file = script_dir / script_name
    
    if not script_file.exists():
        raise FileNotFoundError(f"{script_name} not found at {script_file}")
    
    return str(script_file)


def run_codesys_script(codesys_exe, script_path, *args):
    """Run a script inside CODESYS headless."""
    cmd = [
        codesys_exe,
        "--noUI",
        f'--runscript={script_path}',
    ] + [str(arg) for arg in args]
    
    print(f"[INFO] Running: {' '.join(cmd)}")
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    
    return result.returncode == 0, result.stdout, result.stderr


def apply_diff_to_project(project_path, diff_dir, codesys_path=None, keep_temp=False):
    """
    Complete workflow: Export project → Apply diff → Import back.
    
    Args:
        project_path: Path to .project file
        diff_dir: Directory containing diff files (.diff, .added, .removed)
        codesys_path: Optional path to CODESYS.exe
        keep_temp: If True, keep temporary export directory
    
    Returns:
        bool: True if successful
    """
    project_path = Path(project_path).absolute()
    diff_dir = Path(diff_dir).absolute()
    
    if not project_path.exists():
        raise FileNotFoundError(f"Project file not found: {project_path}")
    
    if not diff_dir.exists():
        raise FileNotFoundError(f"Diff directory not found: {diff_dir}")
    
    codesys_exe = find_codesys_exe(codesys_path)
    
    # Step 1: Export project to text format
    print("\n" + "="*60)
    print("Step 1: Exporting project to text format...")
    print("="*60)
    
    temp_export_dir = tempfile.mkdtemp(prefix="codesys_export_")
    print(f"[INFO] Temporary export directory: {temp_export_dir}")
    
    export_script = get_script_path("codesys_export.py")
    success, stdout, stderr = run_codesys_script(
        codesys_exe,
        export_script,
        project_path,
        temp_export_dir
    )
    
    if not success:
        print(f"[ERROR] Export failed!")
        if not keep_temp:
            shutil.rmtree(temp_export_dir, ignore_errors=True)
        return False
    
    # Step 2: Apply diff to exported files
    print("\n" + "="*60)
    print("Step 2: Applying diff to exported files...")
    print("="*60)
    
    temp_modified_dir = tempfile.mkdtemp(prefix="codesys_modified_")
    print(f"[INFO] Temporary modified directory: {temp_modified_dir}")
    
    # Copy exported files to modified directory first
    shutil.copytree(temp_export_dir, temp_modified_dir, dirs_exist_ok=True)
    
    # Import apply functions from codesys_apply.py
    sys.path.insert(0, str(Path(__file__).parent))
    from codesys_apply import apply_diff_to_file, apply_unified_diff
    
    diff_path = Path(diff_dir)
    modified_path = Path(temp_modified_dir)
    export_path = Path(temp_export_dir)
    
    # Find all diff files and map them to exported files
    # Handle .sc -> .st conversion and name matching
    def find_matching_st_file(base_name):
        """Find matching .st file for a base name (handles .sc -> .st conversion)."""
        # Try exact match first
        for ext in ['.prg.st', '.fb.st', '.fun.st', '.gvl.st', '.st']:
            candidate = export_path / f"{base_name}{ext}"
            if candidate.exists():
                return candidate
        
        # Try partial match (e.g., PLC_PRG_METH -> PLC_PRG.prg.st)
        for st_file in export_path.rglob("*.st"):
            if base_name in st_file.stem or st_file.stem.startswith(base_name.split('_')[0]):
                return st_file
        
        return None
    
    # Find all diff files
    diff_files = {}
    for diff_file in diff_path.rglob("*.diff"):
        rel_path = diff_file.relative_to(diff_path)
        base_name = str(rel_path).replace(".diff", "").replace(".sc", "")
        diff_files[base_name] = ('diff', diff_file)
    
    for diff_file in diff_path.rglob("*.added"):
        rel_path = diff_file.relative_to(diff_path)
        base_name = str(rel_path).replace(".added", "").replace(".sc", "")
        diff_files[base_name] = ('added', diff_file)
    
    for diff_file in diff_path.rglob("*.removed"):
        rel_path = diff_file.relative_to(diff_path)
        base_name = str(rel_path).replace(".removed", "").replace(".sc", "")
        diff_files[base_name] = ('removed', diff_file)
    
    # Apply each diff
    applied_count = 0
    for base_name, (diff_type, diff_file) in diff_files.items():
        # Find matching exported file
        target_file = find_matching_st_file(base_name)
        
        if diff_type == 'removed':
            if target_file:
                # Remove the file from modified directory
                modified_file = modified_path / target_file.name
                if modified_file.exists():
                    modified_file.unlink()
                    print(f"[OK] Removed {target_file.name}")
                    applied_count += 1
            else:
                print(f"[INFO] File {base_name} not found in export (already removed?)")
        elif diff_type == 'added':
            # Create new file from diff
            # Try to determine extension from context
            output_file = modified_path / f"{base_name}.st"
            if apply_diff_to_file(None, diff_file, output_file):
                print(f"[OK] Added {output_file.name}")
                applied_count += 1
        else:  # diff
            if target_file:
                output_file = modified_path / target_file.name
                if apply_diff_to_file(target_file, diff_file, output_file):
                    print(f"[OK] Applied diff to {target_file.name}")
                    applied_count += 1
            else:
                print(f"[WARN] No matching file found for diff {diff_file.name}")
    
    print(f"\n[OK] Applied {applied_count} diffs to text files")
    
    # Check if modified directory has files
    modified_files = list(modified_path.rglob("*.st"))
    if not modified_files:
        print("[WARN] No modified files found. Using original export.")
        temp_modified_dir = temp_export_dir
    
    # Step 3: Import modified files back to project (overwrites)
    print("\n" + "="*60)
    print("Step 3: Importing modified files back to project (overwrites)...")
    print("="*60)
    
    import_script = get_script_path("codesys_import.py")
    success, stdout, stderr = run_codesys_script(
        codesys_exe,
        import_script,
        project_path,
        temp_modified_dir
    )
    
    if not success:
        print(f"[ERROR] Import failed!")
        if not keep_temp:
            shutil.rmtree(temp_export_dir, ignore_errors=True)
            shutil.rmtree(temp_modified_dir, ignore_errors=True)
        return False
    
    # Cleanup
    if not keep_temp:
        print(f"\n[INFO] Cleaning up temporary directories...")
        shutil.rmtree(temp_export_dir, ignore_errors=True)
        shutil.rmtree(temp_modified_dir, ignore_errors=True)
    else:
        print(f"\n[INFO] Temporary directories preserved:")
        print(f"  Export: {temp_export_dir}")
        print(f"  Modified: {temp_modified_dir}")
    
    print("\n" + "="*60)
    print("[OK] Complete! Diff applied and imported to project.")
    print("="*60)
    
    return True


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Apply diff to CODESYS project and import results (overwrites project)"
    )
    parser.add_argument("project_path", help="Path to .project file")
    parser.add_argument("diff_dir", help="Directory containing diff files")
    parser.add_argument(
        "--codesys-path",
        help="Path to CODESYS.exe (auto-detected if not specified)"
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary export/modified directories for debugging"
    )
    
    args = parser.parse_args()
    
    try:
        success = apply_diff_to_project(
            args.project_path,
            args.diff_dir,
            args.codesys_path,
            args.keep_temp
        )
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

