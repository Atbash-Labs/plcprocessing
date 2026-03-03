#!/usr/bin/env python3
"""
Quick test script to verify import files are ready.
Shows what will be imported without needing CODESYS running.
"""

import os
from pathlib import Path


def analyze_import_directory(import_dir):
    """Analyze what will be imported."""
    import_path = Path(import_dir)
    
    if not import_path.exists():
        print(f"Error: Directory not found: {import_dir}")
        return
    
    print(f"Analyzing import directory: {import_dir}\n")
    print("=" * 60)
    
    pous = []
    gvls = []
    
    for st_file in import_path.rglob("*.st"):
        filename = st_file.name
        
        if filename.endswith('.prg.st'):
            name = filename.replace('.prg.st', '')
            pous.append(('Program', name, st_file))
        elif filename.endswith('.fb.st'):
            name = filename.replace('.fb.st', '')
            pous.append(('FunctionBlock', name, st_file))
        elif filename.endswith('.fun.st'):
            name = filename.replace('.fun.st', '')
            pous.append(('Function', name, st_file))
        elif filename.endswith('.gvl.st'):
            name = filename.replace('.gvl.st', '')
            gvls.append((name, st_file))
    
    print(f"\nPOUs to import: {len(pous)}")
    for pou_type, name, filepath in pous:
        print(f"  - {name} ({pou_type})")
        # Show first few lines
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()[:5]
            for line in lines:
                print(f"    {line.rstrip()}")
        print()
    
    print(f"\nGVLs to import: {len(gvls)}")
    for name, filepath in gvls:
        print(f"  - {name}")
        # Show content
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            print(f"    {content.strip()}")
        print()
    
    print("=" * 60)
    print(f"\nTotal: {len(pous)} POUs, {len(gvls)} GVLs")
    print(f"\nTo import, run inside CODESYS:")
    print(f'  codesys_import.py "<project_path>" "{import_dir}"')


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python quick_import_test.py <import_dir>")
        print("\nExample:")
        print("  python quick_import_test.py test_cross_applied_export")
        sys.exit(1)
    
    analyze_import_directory(sys.argv[1])

