#!/usr/bin/env python3
"""
Import CODESYS text files into a project from outside CODESYS.
Uses subprocess to call CODESYS headless with the import script.

Usage:
    python codesys_import_external.py <project_path> <import_dir> [--dry-run] [--codesys-path PATH]
"""

import sys
import os
import subprocess
from pathlib import Path


def find_codesys_exe():
    """Find CODESYS executable in common installation locations."""
    common_paths = [
        r"C:\Program Files\CODESYS 3.5.21.30\CODESYS\Common\CODESYS.exe",
        r"C:\Program Files\CODESYS\CODESYS.exe",
        r"C:\Program Files (x86)\CODESYS\CODESYS.exe",
        r"C:\Program Files\CODESYS V3.5 SP21\CODESYS.exe",
        r"C:\Program Files\CODESYS V3.5 SP20\CODESYS.exe",
        r"C:\Program Files\CODESYS V3.5 SP19\CODESYS.exe",
    ]

    for path in common_paths:
        if os.path.exists(path):
            return path

    return None


def get_script_path():
    """Get the absolute path to codesys_import.py."""
    # Get directory where this script is located (scripts/)
    script_dir = Path(__file__).parent.absolute()
    import_script = script_dir / "codesys_import.py"

    if not import_script.exists():
        raise FileNotFoundError(f"codesys_import.py not found at {import_script}")

    return str(import_script)


def update_import_script_paths(
    import_script_path, project_path, import_dir, dry_run=False
):
    """Update hardcoded paths in codesys_import.py before running."""
    import_script_path = Path(import_script_path)

    # Read the script
    with open(import_script_path, "rb") as f:
        content_bytes = f.read()

    try:
        content = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        content = content_bytes.decode("latin-1")

    # Find and replace hardcoded paths using regex
    import re

    # Pattern to match paths - handles both single-line and multi-line (with parens) formats
    # Single-line: project_path = r"path"
    # Multi-line:  project_path = (\n        r"path"\n    )
    project_pattern = r'(project_path\s*=\s*(?:\(\s*)?r)["\']([^"\']+)["\']'
    import_pattern = r'(import_dir\s*=\s*(?:\(\s*)?r)["\']([^"\']+)["\']'
    # Match standalone assignment (with leading whitespace), not function parameter defaults
    dry_run_pattern = r"(^    dry_run = )(True|False)"

    # Replace paths - normalize to single-line format for consistency
    def replace_project(match):
        return f'project_path = r"{project_path}"'

    def replace_import(match):
        return f'import_dir = r"{import_dir}"'

    def replace_dry_run(match):
        return f"{match.group(1)}{str(dry_run)}"

    # Use DOTALL flag to match across newlines for multi-line formats
    content = re.sub(
        project_pattern, replace_project, content, count=1, flags=re.DOTALL
    )
    content = re.sub(import_pattern, replace_import, content, count=1, flags=re.DOTALL)
    content = re.sub(
        dry_run_pattern, replace_dry_run, content, count=1, flags=re.MULTILINE
    )

    # Clean up any orphaned closing parens from multi-line format
    content = re.sub(r'(import_dir = r"[^"]+"\s*)\n\s*\)', r"\1", content)
    content = re.sub(r'(project_path = r"[^"]+"\s*)\n\s*\)', r"\1", content)

    # Write back
    with open(import_script_path, "wb") as f:
        f.write(content.encode("utf-8"))

    return True


def import_to_project(
    project_path,
    import_dir,
    codesys_path=None,
    profile="CODESYS V3.5 SP21 Patch 3",
    dry_run=False,
):
    """Import text files to CODESYS project using subprocess.

    Args:
        project_path: Path to the CODESYS project file
        import_dir: Directory containing .st files to import
        codesys_path: Optional path to CODESYS.exe
        profile: CODESYS profile name
        dry_run: If True, only preview changes without modifying the project
    """

    # Validate inputs
    project_path = Path(project_path).absolute()
    import_dir = Path(import_dir).absolute()

    if not project_path.exists():
        raise FileNotFoundError(f"Project file not found: {project_path}")

    if not import_dir.exists():
        raise FileNotFoundError(f"Import directory not found: {import_dir}")

    # Find CODESYS executable
    if codesys_path:
        codesys_exe = Path(codesys_path)
        if not codesys_exe.exists():
            raise FileNotFoundError(f"CODESYS executable not found: {codesys_exe}")
    else:
        codesys_exe = find_codesys_exe()
        if not codesys_exe:
            raise FileNotFoundError(
                "CODESYS executable not found. Please specify path with --codesys-path option.\n"
                "Common locations:\n"
                "  - C:\\Program Files\\CODESYS\\CODESYS.exe\n"
                "  - C:\\Program Files (x86)\\CODESYS\\CODESYS.exe"
            )

    codesys_exe = Path(codesys_exe)

    # Get import script path
    import_script = get_script_path()

    # Update hardcoded paths in the import script before running
    print(f"[INFO] Updating paths in {import_script}")
    update_import_script_paths(
        import_script, str(project_path), str(import_dir), dry_run
    )

    # Build command
    # CODESYS.exe --noUI --profile="profile" --runscript="script.py"
    # Note: We don't pass args since they're hardcoded in the script now
    # Build command - CODESYS requires profile to be quoted
    # Use shell=True on Windows to handle quotes properly
    exe_str = f'"{codesys_exe}"'
    script_str = f'"{import_script}"'
    profile_str = f'"{profile}"'

    cmd_str = f"{exe_str} --noUI --profile={profile_str} --runscript={script_str}"

    print(f"[INFO] CODESYS executable: {codesys_exe}")
    print(f"[INFO] Profile: {profile}")
    print(f"[INFO] Project: {project_path}")
    print(f"[INFO] Import directory: {import_dir}")
    if dry_run:
        print(f"[INFO] Mode: DRY RUN (no changes will be made)")
    print(f"[INFO] Running: {cmd_str}\n")

    try:
        # Run CODESYS headless with shell=True for proper quote handling
        result = subprocess.run(
            cmd_str,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=True,
        )

        # Print output
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)

        if result.stderr:
            print("STDERR:")
            print(result.stderr)

        if result.returncode == 0:
            print("\n[OK] Import completed successfully!")
            return True
        else:
            print(f"\n[ERROR] Import failed with exit code: {result.returncode}")
            return False

    except FileNotFoundError as e:
        print(f"[ERROR] CODESYS executable not found: {e}")
        print(f"[INFO] Please specify the path to CODESYS.exe")
        return False
    except Exception as e:
        print(f"[ERROR] Failed to run import: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Import CODESYS text files to project (runs CODESYS headless)"
    )
    parser.add_argument("project_path", help="Path to .project file")
    parser.add_argument("import_dir", help="Directory containing .st files to import")
    parser.add_argument(
        "--codesys-path", help="Path to CODESYS.exe (auto-detected if not specified)"
    )
    parser.add_argument(
        "--profile",
        default="CODESYS V3.5 SP21 Patch 3",
        help="CODESYS profile name (default: CODESYS V3.5 SP21 Patch 3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying the project",
    )

    args = parser.parse_args()

    try:
        success = import_to_project(
            args.project_path,
            args.import_dir,
            args.codesys_path,
            args.profile,
            args.dry_run,
        )
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
