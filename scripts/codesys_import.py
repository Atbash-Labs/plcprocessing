#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import text files back into CODESYS project using Scripting API.
Reads .st files and applies them to the project.

Usage:
    Run inside CODESYS: Tools → Execute Script
    Or headless: CODESYS.exe --noUI --runscript="codesys_import.py" <project_path> <import_dir>
"""

from scriptengine import *
import os
import sys


def parse_st_file(filepath):
    """Parse a .st file and extract declaration and implementation."""
    # IronPython (Python 2.7) doesn't support encoding parameter in open()
    # Read as binary and decode manually
    with open(filepath, "rb") as f:
        content_bytes = f.read()
    # Decode from UTF-8
    try:
        content = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        # Fallback to latin-1 if UTF-8 fails
        content = content_bytes.decode("latin-1")

    # Split by sections
    decl = ""
    impl = ""

    # Check if it's a GVL file (has VAR_GLOBAL)
    if "VAR_GLOBAL" in content:
        # GVL files: extract everything between VAR_GLOBAL and END_VAR
        if "VAR_GLOBAL" in content and "END_VAR" in content:
            parts = content.split("VAR_GLOBAL", 1)
            if len(parts) > 1:
                var_part = parts[1].split("END_VAR", 1)[0].strip()
                # Reconstruct with VAR_GLOBAL/END_VAR
                decl = "VAR_GLOBAL\n\n" + var_part + "\n\nEND_VAR"
        return decl, impl

    if "(* DECLARATION *)" in content:
        parts = content.split("(* DECLARATION *)", 1)
        if len(parts) > 1:
            decl_part = parts[1]
            if "(* IMPLEMENTATION *)" in decl_part:
                decl = decl_part.split("(* IMPLEMENTATION *)", 1)[0].strip()
                impl = decl_part.split("(* IMPLEMENTATION *)", 1)[1].strip()
            else:
                decl = decl_part.strip()
    elif "(* IMPLEMENTATION *)" in content:
        parts = content.split("(* IMPLEMENTATION *)", 1)
        impl = parts[1].strip()
    else:
        # Assume it's all implementation
        impl = content.strip()

    return decl, impl


def import_st_file(project_path, filepath, app):
    """Import a single .st file into the project."""
    filename = os.path.basename(filepath)
    name_without_ext = os.path.splitext(os.path.splitext(filename)[0])[
        0
    ]  # Remove .st and .prg/.fb/.fun

    # Determine POU type from extension
    if filename.endswith(".prg.st"):
        pou_type = PouType.Program
        name = name_without_ext
    elif filename.endswith(".fb.st"):
        pou_type = PouType.FunctionBlock
        name = name_without_ext
    elif filename.endswith(".fun.st"):
        pou_type = PouType.Function
        name = name_without_ext
    elif filename.endswith(".meth.st"):
        # It's a method - handle separately
        # Format: POU_METHOD.meth.st -> method METHOD in POU POU
        if "_" in name_without_ext:
            parts = name_without_ext.rsplit("_", 1)
            if len(parts) == 2:
                pou_name, method_name = parts
                return import_method_file(filepath, pou_name, method_name, app)
        print("[WARN] Could not parse method name from: {}".format(filename))
        return False
    elif filename.endswith(".gvl.st"):
        # It's a GVL
        name = name_without_ext
        return import_gvl_file(filepath, name, app)
    else:
        # Default to Program
        pou_type = PouType.Program
        name = name_without_ext

    # Parse file
    decl, impl = parse_st_file(filepath)

    # Find or create POU
    found = app.find(name, recursive=True)

    if found and len(found) > 0:
        # POU exists - update it
        pou = found[0]
        if decl:
            pou.textual_declaration.replace(decl)
        if impl:
            pou.textual_implementation.replace(impl)

        # Methods will be handled in import_directory after we know what should exist

        print("[OK] Updated POU: {}".format(name))
        return True
    else:
        # Create new POU
        try:
            pou = app.create_pou(name, pou_type)
            if decl:
                pou.textual_declaration.replace(decl)
            if impl:
                pou.textual_implementation.replace(impl)
            print("[OK] Created POU: {}".format(name))
            return True
        except Exception as e:
            print("[ERROR] Could not create POU {}: {}".format(name, e))
            return False


def import_method_file(filepath, pou_name, method_name, app):
    """Import a method file into a POU."""
    decl, impl = parse_st_file(filepath)

    # Find the POU
    found = app.find(pou_name, recursive=True)
    if not found or len(found) == 0:
        print("[ERROR] POU {} not found for method {}".format(pou_name, method_name))
        return False

    pou = found[0]

    # Find or create method
    try:
        methods = pou.get_children()
        method = None
        for m in methods:
            if str(m.type) == "Method" and str(m.name) == method_name:
                method = m
                break

        if method:
            # Update existing method
            if decl:
                method.textual_declaration.replace(decl)
            if impl:
                method.textual_implementation.replace(impl)
            print("[OK] Updated method: {} in POU: {}".format(method_name, pou_name))
        else:
            # Create new method
            method = pou.create_method(method_name)
            if decl:
                method.textual_declaration.replace(decl)
            if impl:
                method.textual_implementation.replace(impl)
            print("[OK] Created method: {} in POU: {}".format(method_name, pou_name))
        return True
    except Exception as e:
        print(
            "[ERROR] Could not import method {} in POU {}: {}".format(
                method_name, pou_name, e
            )
        )
        return False


def import_gvl_file(filepath, name, app):
    """Import a GVL file - merges variables instead of overwriting."""
    decl, _ = parse_st_file(filepath)

    # Find or create GVL
    found = app.find(name, recursive=True)

    if found and len(found) > 0:
        gvl = found[0]
        # Debug: Print existing GVL content
        existing_decl = gvl.textual_declaration.text
        print("[DEBUG] GVL {} BEFORE update:".format(name))
        print("[DEBUG]   Length: {}".format(len(existing_decl) if existing_decl else 0))
        print(
            "[DEBUG]   Content: {}".format(
                repr(existing_decl) if existing_decl else "(empty)"
            )
        )
        if existing_decl:
            print("[DEBUG]   Content (readable):")
            for i, line in enumerate(existing_decl.split("\n")[:10]):
                print("[DEBUG]     {}: {}".format(i, repr(line)))

        if decl:
            print("[DEBUG] GVL {} NEW declaration to set:".format(name))
            print("[DEBUG]   Length: {}".format(len(decl)))
            print("[DEBUG]   Content: {}".format(repr(decl)))
            print("[DEBUG]   Content (readable):")
            for i, line in enumerate(decl.split("\n")[:10]):
                print("[DEBUG]     {}: {}".format(i, repr(line)))

            # Try to set the declaration
            try:
                gvl.textual_declaration.replace(decl)

                # Debug: Verify what was actually set
                verify_decl = gvl.textual_declaration.text
                print("[DEBUG] GVL {} AFTER replace:".format(name))
                print(
                    "[DEBUG]   Length: {}".format(
                        len(verify_decl) if verify_decl else 0
                    )
                )
                print(
                    "[DEBUG]   Content: {}".format(
                        repr(verify_decl) if verify_decl else "(empty)"
                    )
                )
                if verify_decl:
                    print("[DEBUG]   Content (readable):")
                    for i, line in enumerate(verify_decl.split("\n")[:10]):
                        print("[DEBUG]     {}: {}".format(i, repr(line)))

                if verify_decl and len(verify_decl.strip()) > 0:
                    print(
                        "[OK] Updated GVL: {} ({} chars)".format(name, len(verify_decl))
                    )
                else:
                    print(
                        "[WARN] GVL {} declaration appears empty after replace!".format(
                            name
                        )
                    )
            except Exception as e:
                print("[ERROR] Failed to update GVL {}: {}".format(name, e))
                import traceback

                traceback.print_exc()
        else:
            print("[WARN] GVL {} has no declaration content".format(name))
        return True
    else:
        try:
            gvl = app.create_gvl(name)
            if decl:
                gvl.textual_declaration.replace(decl)
            print(
                "[OK] Created GVL: {} (declaration length: {})".format(name, len(decl))
            )
            return True
        except Exception as e:
            print("[ERROR] Could not create GVL {}: {}".format(name, e))
            return False


def import_directory(project_path, import_dir):
    """Import all .st files from a directory and remove items not in the directory."""

    # Open project
    proj = projects.open(project_path)
    app = proj.active_application

    try:
        # Find all .st files recursively and track what we're importing
        st_files = []
        imported_pou_names = set()
        imported_gvl_names = set()
        imported_methods = set()  # (pou_name, method_name) tuples

        for root, dirs, files in os.walk(import_dir):
            for file in files:
                if file.endswith(".st"):
                    st_files.append(os.path.join(root, file))
                    # Track what we're importing
                    name_without_ext = os.path.splitext(os.path.splitext(file)[0])[0]
                    if file.endswith(".gvl.st"):
                        imported_gvl_names.add(name_without_ext)
                    elif file.endswith(".meth.st"):
                        # Format: POU_METHOD.meth.st
                        if "_" in name_without_ext:
                            parts = name_without_ext.rsplit("_", 1)
                            if len(parts) == 2:
                                imported_methods.add((parts[0], parts[1]))
                    else:
                        imported_pou_names.add(name_without_ext)

        imported_count = 0

        # Import all files
        for st_file in st_files:
            if import_st_file(project_path, st_file, app):
                imported_count += 1

        # For now, skip deletion logic - focus on additions
        # TODO: Implement proper deletion logic later

        # Save project
        proj.save()

        print("\n[OK] Import complete: {} files imported".format(imported_count))

    finally:
        proj.close()


def main():
    """Main entry point."""
    # Hardcoded paths for testing (will be overridden by sys.argv or env vars)
    project_path = r"C:\Users\leorf\OneDrive\Desktop\plcprocessing\Untitled1.project"
    import_dir = r"C:\Users\leorf\OneDrive\Desktop\plcprocessing\tests\test_additions"

    # Try to get from sys.argv if provided (for future use)
    if len(sys.argv) >= 3:
        project_path = sys.argv[1]
        import_dir = sys.argv[2]
    elif len(sys.argv) == 2:
        # Only script path provided, try to get from environment
        if os.environ.get("CODESYS_PROJECT_PATH"):
            project_path = os.environ.get("CODESYS_PROJECT_PATH")
        if os.environ.get("CODESYS_IMPORT_DIR"):
            import_dir = os.environ.get("CODESYS_IMPORT_DIR")

    if not os.path.exists(project_path):
        print("Error: Project file not found: {}".format(project_path))
        sys.exit(1)

    if not os.path.exists(import_dir):
        print("Error: Import directory not found: {}".format(import_dir))
        sys.exit(1)

    try:
        import_directory(project_path, import_dir)
    except Exception as e:
        print("Error: {}".format(e))
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
