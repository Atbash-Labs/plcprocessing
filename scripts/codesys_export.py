#!/usr/bin/env python3
"""
Export CODESYS project to text format using Scripting API.
Exports POUs and GVLs as .st files for source control and diffing.

Usage:
    Run inside CODESYS: Tools → Execute Script
    Or headless: CODESYS.exe --noUI --runscript="codesys_export.py" <project_path> <output_dir>
"""

from scriptengine import *
import os
import sys


def export_pou_to_text(pou, output_dir):
    """Export a POU to a text file."""
    try:
        # Get declaration and implementation
        decl = pou.textual_declaration.text
        impl = pou.textual_implementation.text
        
        # Determine file extension based on POU type
        pou_type = str(pou.type)
        if 'Program' in pou_type:
            ext = '.prg.st'
        elif 'FunctionBlock' in pou_type:
            ext = '.fb.st'
        elif 'Function' in pou_type:
            ext = '.fun.st'
        else:
            ext = '.st'
        
        filename = os.path.join(output_dir, f"{pou.name}{ext}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"(* POU: {pou.name} *)\n")
            f.write(f"(* Type: {pou_type} *)\n\n")
            
            if decl:
                f.write("(* DECLARATION *)\n")
                f.write(decl)
                f.write("\n\n")
            
            if impl:
                f.write("(* IMPLEMENTATION *)\n")
                f.write(impl)
                f.write("\n")
        
        print(f"[OK] Exported POU: {pou.name}")
        return True
        
    except Exception as e:
        print(f"[WARN] Could not export POU {pou.name}: {e}")
        return False


def export_gvl_to_text(gvl, output_dir):
    """Export a GVL to a text file."""
    try:
        decl = gvl.textual_declaration.text
        
        filename = os.path.join(output_dir, f"{gvl.name}.gvl.st")
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"(* GVL: {gvl.name} *)\n\n")
            if decl:
                f.write(decl)
                f.write("\n")
        
        print(f"[OK] Exported GVL: {gvl.name}")
        return True
        
    except Exception as e:
        print(f"[WARN] Could not export GVL {gvl.name}: {e}")
        return False


def export_project_to_text(project_path, output_dir):
    """Export entire project to text format."""
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Open project
    proj = projects.open(project_path)
    app = proj.active_application
    
    try:
        pous_count = 0
        gvls_count = 0
        
        # Export all objects
        for obj in app.get_children(recursive=True):
            obj_type = str(obj.type)
            
            if 'Pou' in obj_type:
                if export_pou_to_text(obj, output_dir):
                    pous_count += 1
            elif 'Gvl' in obj_type:
                if export_gvl_to_text(obj, output_dir):
                    gvls_count += 1
        
        print(f"\n[OK] Export complete: {pous_count} POUs, {gvls_count} GVLs")
        print(f"[INFO] Exported to: {output_dir}")
        
    finally:
        proj.close()


def main():
    """Main entry point."""
    if len(sys.argv) < 3:
        print("Usage: codesys_export.py <project_path> <output_dir>")
        print("\nExample:")
        print('  codesys_export.py "C:\\Projects\\MyProject.project" "C:\\Export"')
        sys.exit(1)
    
    project_path = sys.argv[1]
    output_dir = sys.argv[2]
    
    if not os.path.exists(project_path):
        print(f"Error: Project file not found: {project_path}")
        sys.exit(1)
    
    try:
        export_project_to_text(project_path, output_dir)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

