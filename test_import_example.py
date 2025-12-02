#!/usr/bin/env python3
"""
Example script showing how to use codesys_import_external.py programmatically.
"""

import subprocess
from pathlib import Path


def import_to_codesys_project(project_path, import_dir, codesys_path=None):
    """
    Import text files to CODESYS project using external wrapper.
    
    Args:
        project_path: Path to .project file
        import_dir: Directory containing .st files
        codesys_path: Optional path to CODESYS.exe (auto-detected if None)
    
    Returns:
        bool: True if successful, False otherwise
    """
    script_path = Path(__file__).parent / "codesys_import_external.py"
    
    cmd = [
        "python",
        str(script_path),
        str(project_path),
        str(import_dir)
    ]
    
    if codesys_path:
        cmd.extend(["--codesys-path", str(codesys_path)])
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        
        return result.returncode == 0
    except Exception as e:
        print(f"Error running import: {e}")
        return False


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python test_import_example.py <project_path> <import_dir>")
        sys.exit(1)
    
    project = sys.argv[1]
    import_dir = sys.argv[2]
    
    success = import_to_codesys_project(project, import_dir)
    sys.exit(0 if success else 1)

