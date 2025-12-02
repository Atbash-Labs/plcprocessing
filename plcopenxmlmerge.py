#!/usr/bin/env python3
"""
Merge modified .sc files back into PLCopen XML using the plcopen library.
Takes modified .sc files (from applying diffs) and updates the source XML file.

Usage:
    python plcopenxmlmerge.py modified_sc_dir/ source.xml output.xml
"""

import sys
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import html
import re
from plcopen import Project
from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.parsers.config import ParserConfig
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig


def extract_code_from_sc(sc_file):
    """Extract the actual code from a .sc file, removing comments."""
    with open(sc_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Check if this is a diff format file (starts with --- or +++)
    # This can happen if .added files weren't properly converted
    if content.startswith("---") or content.startswith("+++"):
        # Extract lines starting with + (but skip header lines)
        lines = content.split("\n")
        code_lines = []
        for line in lines:
            if line.startswith("+") and not line.startswith("+++"):
                code_lines.append(line[1:])  # Remove + prefix
        content = "\n".join(code_lines)

    # Remove comment lines (lines starting with (* or containing *)
    lines = content.split("\n")
    code_lines = []
    in_comment = False

    for line in lines:
        # Check for multi-line comments
        if "(*" in line:
            in_comment = True
        if "*)" in line:
            in_comment = False
            continue
        if in_comment:
            continue

        # Skip single-line comments
        if line.strip().startswith("(*") or line.strip().startswith("*"):
            continue

        # Skip empty lines at start/end
        if line.strip() or len(code_lines) > 0:
            code_lines.append(line)

    # Remove VAR_GLOBAL/END_VAR wrapper if present
    result = "\n".join(code_lines)
    result = re.sub(r"^\s*VAR_GLOBAL\s*\n", "", result, flags=re.MULTILINE)
    result = re.sub(r"\n\s*END_VAR\s*$", "", result, flags=re.MULTILINE)

    return result.strip()


def update_xml_with_sc_content(xml_path, sc_dir, output_path, diff_dir=None):
    """Update XML file with modified .sc file content using plcopen library."""
    # Parse as XML tree to detect namespace and make modifications
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Detect namespace - CODESYS uses tc6_0200 (default)
    ns_map = root.tag.split("}")[0].strip("{") if "}" in root.tag else ""
    if "tc6_0201" in ns_map:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0201"
    else:
        # Default to CODESYS namespace
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0200"

    XHTML_NS = "http://www.w3.org/1999/xhtml"
    print(f"[DEBUG] Detected namespace: {PLCOPEN_NS} (from tag: {root.tag})")

    sc_path = Path(sc_dir)
    updated_count = 0
    removed_count = 0

    # First, handle removed files - check both sc_dir and diff_dir
    removed_files_to_process = []

    # Check sc_dir for .removed files
    for removed_file in sc_path.rglob("*.removed"):
        # File is like "PLC_PRG.sc.removed", stem is "PLC_PRG.sc", need to remove .sc
        sc_name = removed_file.stem
        if sc_name.endswith(".sc"):
            sc_name = sc_name[:-3]  # Remove .sc extension
        removed_files_to_process.append(sc_name)

    # Also check diff_dir if provided (where .removed files are stored)
    if diff_dir:
        diff_path = Path(diff_dir)
        for removed_file in diff_path.rglob("*.removed"):
            # File is like "PLC_PRG.sc.removed", stem is "PLC_PRG.sc", need to remove .sc
            sc_name = removed_file.stem
            if sc_name.endswith(".sc"):
                sc_name = sc_name[:-3]  # Remove .sc extension
            if sc_name not in removed_files_to_process:
                removed_files_to_process.append(sc_name)

    # Process removed files
    for sc_name in removed_files_to_process:
        # Determine what type of file this is
        # Methods are named POU_METHOD, so if there's an underscore, check if it's a method
        # by looking for the pattern in the XML structure
        removed_this_item = False

        if "_" in sc_name:
            # Try as method first: POU_METHOD format
            parts = sc_name.rsplit("_", 1)
            if len(parts) == 2:
                pou_name, method_name = parts
                # Find and remove method from XML tree
                for data in root.findall(f".//{{{PLCOPEN_NS}}}data"):
                    if data.get("name") == "http://www.3s-software.com/plcopenxml/pou":
                        pou = data.find(f".//{{{PLCOPEN_NS}}}pou[@name='{pou_name}']")
                        if pou is not None:
                            # Find method data element
                            for method_data in list(
                                pou.findall(f".//{{{PLCOPEN_NS}}}data")
                            ):
                                if (
                                    method_data.get("name")
                                    == "http://www.3s-software.com/plcopenxml/method"
                                ):
                                    method = method_data.find(
                                        f".//{{{PLCOPEN_NS}}}Method[@name='{method_name}']"
                                    )
                                    if method is not None:
                                        # Remove the method data element
                                        # Find parent by iterating through pou's children
                                        for parent_elem in list(pou):
                                            if (
                                                method_data in list(parent_elem)
                                                or method_data == parent_elem
                                            ):
                                                # Found the parent - remove method_data
                                                if method_data in list(parent_elem):
                                                    parent_elem.remove(method_data)
                                                elif method_data == parent_elem:
                                                    pou.remove(method_data)
                                                removed_count += 1
                                                removed_this_item = True
                                                print(
                                                    f"[OK] Removed method {pou_name}.{method_name}"
                                                )
                                                break
                                        if removed_this_item:
                                            break
                            if removed_this_item:
                                break

                # If method wasn't found, try as POU instead
                if not removed_this_item:
                    # Look for POU with this exact name
                    for data in list(root.findall(f".//{{{PLCOPEN_NS}}}data")):
                        if (
                            data.get("name")
                            == "http://www.3s-software.com/plcopenxml/pou"
                        ):
                            pou = data.find(
                                f".//{{{PLCOPEN_NS}}}pou[@name='{sc_name}']"
                            )
                            if pou is not None:
                                # It's actually a POU, not a method
                                # Remove the entire data element containing the POU
                                for parent_elem in root.iter():
                                    if data in list(parent_elem):
                                        parent_elem.remove(data)
                                        removed_count += 1
                                        removed_this_item = True
                                        print(f"[OK] Removed POU {sc_name}")
                                        break
                                if removed_this_item:
                                    break
                    # If still not found, the POU might have been removed already
                    # (e.g., if we're processing methods after their parent POU was removed)
                    # This is fine - methods are removed when their parent POU is removed
        else:
            # Likely a POU
            pou_name = sc_name
            print(f"[DEBUG] Looking for POU {pou_name}")
            # Find and remove POU from XML tree
            for data in list(root.findall(f".//{{{PLCOPEN_NS}}}data")):
                if data.get("name") == "http://www.3s-software.com/plcopenxml/pou":
                    pou = data.find(f".//{{{PLCOPEN_NS}}}pou[@name='{pou_name}']")
                    if pou is not None:
                        print(f"[DEBUG] Found POU data element to remove")
                        # Remove the entire data element containing the POU
                        # Find parent by searching through root
                        for parent_elem in root.iter():
                            if data in list(parent_elem):
                                parent_elem.remove(data)
                                removed_count += 1
                                print(f"[OK] Removed POU {pou_name}")
                                break
                        else:
                            # Try searching in addData sections
                            for add_data in root.findall(f".//{{{PLCOPEN_NS}}}addData"):
                                if data in list(add_data):
                                    add_data.remove(data)
                                    removed_count += 1
                                    print(f"[OK] Removed POU {pou_name}")
                                    break

    # Process each .sc file for updates
    for sc_file in sc_path.rglob("*.sc"):
        sc_name = sc_file.stem  # filename without extension
        print(f"[DEBUG] Processing .sc file: {sc_name}")

        # Extract code from .sc file
        new_code = extract_code_from_sc(sc_file)
        if not new_code:
            print(f"[DEBUG] No code extracted from {sc_name}, skipping")
            continue
        print(f"[DEBUG] Extracted code from {sc_name}: {new_code[:50]}...")

        # Determine what type of file this is
        # Priority: GVL > POU > Method
        # First check if it's a GVL (already handled above)
        # Then try POU, then method

        # Check if it's likely a method: POU_METHOD format
        # Methods are typically short names like METH, INIT, EXIT, etc.
        # But we'll try POU first since POUs can have underscores too
        is_likely_method = False
        if "_" in sc_name:
            parts = sc_name.rsplit("_", 1)
            if len(parts) == 2:
                pou_name_part, method_name_part = parts
                # Heuristic: if the last part is all uppercase and VERY SHORT (<= 6 chars), it's likely a method
                # Common method names: METH, INIT, EXIT, REQ, etc.
                # But we'll still try POU first
                if method_name_part.isupper() and len(method_name_part) <= 6:
                    is_likely_method = True

        # Try POU first (most common case)
        pou_name = sc_name
        pou_updated = False
        print(f"[DEBUG] Trying as POU first: {pou_name}")

        # Method 1: CODESYS-specific format - POUs in addData sections
        for data in root.findall(f".//{{{PLCOPEN_NS}}}data"):
            if data.get("name") == "http://www.3s-software.com/plcopenxml/pou":
                # Find POU by iterating and checking name attribute
                for pou in data.findall(f".//{{{PLCOPEN_NS}}}pou"):
                    if pou.get("name") == pou_name:
                        body = pou.find(f".//{{{PLCOPEN_NS}}}body")
                        if body is not None:
                            st_elem = body.find(f".//{{{PLCOPEN_NS}}}ST")
                            if st_elem is not None:
                                xhtml = st_elem.find(f".//{{{XHTML_NS}}}xhtml")
                                if xhtml is not None:
                                    xhtml.text = new_code
                                    updated_count += 1
                                    pou_updated = True
                                    print(f"[OK] Updated POU {pou_name}")
                                    break
                if pou_updated:
                    break

        # Method 2: Standard PLCopen format - POUs in <types><pous> (fallback)
        if not pou_updated:
            types_elem = root.find(f".//{{{PLCOPEN_NS}}}types")
            if types_elem is not None:
                pous_elem = types_elem.find(f".//{{{PLCOPEN_NS}}}pous")
                if pous_elem is not None:
                    # Find POU by iterating and checking name attribute
                    for pou in pous_elem.findall(f".//{{{PLCOPEN_NS}}}pou"):
                        if pou.get("name") == pou_name:
                            print(f"[DEBUG] Found POU {pou_name}, updating...")
                            body = pou.find(f".//{{{PLCOPEN_NS}}}body")
                            if body is not None:
                                st_elem = body.find(f".//{{{PLCOPEN_NS}}}ST")
                                if st_elem is not None:
                                    # Try xhtml format (CODESYS)
                                    xhtml = st_elem.find(f".//{{{XHTML_NS}}}xhtml")
                                    if xhtml is not None:
                                        xhtml.text = new_code
                                        updated_count += 1
                                        pou_updated = True
                                        print(
                                            f"[OK] Updated POU {pou_name} (standard format)"
                                        )
                                        break
                                    else:
                                        # Try xhtml:p format (for compatibility)
                                        xhtml_p = st_elem.find(f".//{{{XHTML_NS}}}p")
                                        if xhtml_p is not None:
                                            xhtml_p.text = new_code
                                            updated_count += 1
                                            pou_updated = True
                                            print(
                                                f"[OK] Updated POU {pou_name} (standard format)"
                                            )
                                            break

        # If not found as POU and looks like a method, try method
        if not pou_updated and is_likely_method:
            # Likely a method: POU_METHOD
            parts = sc_name.rsplit("_", 1)
            pou_name, method_name = parts
            # Find and update method in XML tree
            for data in root.findall(f".//{{{PLCOPEN_NS}}}data"):
                if data.get("name") == "http://www.3s-software.com/plcopenxml/pou":
                    pou = data.find(f".//{{{PLCOPEN_NS}}}pou[@name='{pou_name}']")
                    if pou is not None:
                        # Find method
                        for method_data in pou.findall(f".//{{{PLCOPEN_NS}}}data"):
                            if (
                                method_data.get("name")
                                == "http://www.3s-software.com/plcopenxml/method"
                            ):
                                method = method_data.find(
                                    f".//{{{PLCOPEN_NS}}}Method[@name='{method_name}']"
                                )
                                if method is not None:
                                    body = method.find(f".//{{{PLCOPEN_NS}}}body")
                                    if body is not None:
                                        st_elem = body.find(f".//{{{PLCOPEN_NS}}}ST")
                                        if st_elem is not None:
                                            xhtml = st_elem.find(
                                                f".//{{{XHTML_NS}}}xhtml"
                                            )
                                            if xhtml is not None:
                                                xhtml.text = new_code
                                                updated_count += 1
                                                print(
                                                    f"[OK] Updated method {pou_name}.{method_name}"
                                                )
        elif (
            sc_name.startswith("GVL")
            or sc_name == "GVL"
            or sc_name.startswith("Global_vars")
            or sc_name == "Global_vars"
        ):
            print(f"[DEBUG] Processing as GVL: {sc_name}")
            # Global Variable List - update variables
            # Handle both "GVL" and "Global_vars" naming
            if sc_name.startswith("GVL"):
                gvl_name = sc_name.replace("GVL", "").strip() or "GVL"
            else:
                gvl_name = sc_name.replace("Global_vars", "").strip() or "Global_vars"

            # Parse VAR_GLOBAL structure to extract variable declarations
            var_declarations = {}
            for line in new_code.split("\n"):
                line = line.strip()
                if ":" in line and ";" in line:
                    # Format: VAR_NAME: TYPE;
                    parts = line.split(":")
                    if len(parts) == 2:
                        var_name = parts[0].strip()
                        type_part = parts[1].split(";")[0].strip()
                        var_declarations[var_name] = type_part

            # Update in XML tree (handles both standard and CODESYS formats)
            gvl_found = False
            for gvl in root.findall(
                f".//{{{PLCOPEN_NS}}}globalVars[@name='{gvl_name}']"
            ):
                # Parse VAR_GLOBAL to extract variable: type pairs
                var_declarations = {}
                for line in new_code.split("\n"):
                    line = line.strip()
                    if ":" in line and ";" in line:
                        # Format: VAR_NAME: TYPE;
                        parts = line.split(":")
                        if len(parts) == 2:
                            var_name = parts[0].strip()
                            type_part = parts[1].split(";")[0].strip()
                            var_declarations[var_name] = type_part

                # Update or remove variables in XML
                # Get direct children variables (CODESYS format)
                variables = gvl.findall(f"./{{{PLCOPEN_NS}}}variable")
                # Also check for variables in interface sections
                if not variables:
                    interface = gvl.find(f".//{{{PLCOPEN_NS}}}interface")
                    if interface is not None:
                        variables = interface.findall(f".//{{{PLCOPEN_NS}}}variable")

                for var in list(
                    variables
                ):  # Use list() to avoid modification during iteration
                    var_name = var.get("name", "")
                    if var_name in var_declarations:
                        # Variable exists in new code - update it
                        new_type = var_declarations[var_name]
                        # Update the type element
                        type_elem = var.find(f".//{{{PLCOPEN_NS}}}type")
                        if type_elem is not None:
                            # Remove old type children
                            for child in list(type_elem):
                                type_elem.remove(child)
                            # Add new type
                            new_type_elem = ET.SubElement(
                                type_elem, f"{{{PLCOPEN_NS}}}{new_type}"
                            )
                            updated_count += 1
                            print(
                                f"[OK] Updated GVL variable {gvl_name}.{var_name}: {new_type}"
                            )
                            gvl_found = True
                    else:
                        # Variable not in new code - remove it
                        # Find parent by iterating through GVL's children
                        for parent in list(gvl):
                            if var in list(parent) or var == parent:
                                if var in list(parent):
                                    parent.remove(var)
                                elif var == parent:
                                    gvl.remove(var)
                                removed_count += 1
                                print(
                                    f"[OK] Removed GVL variable {gvl_name}.{var_name}"
                                )
                                gvl_found = True
                                break
                        # If not found as direct child, try removing from GVL directly
                        if var in list(gvl):
                            gvl.remove(var)
                            removed_count += 1
                            print(f"[OK] Removed GVL variable {gvl_name}.{var_name}")
                            gvl_found = True

                if not gvl_found and variables:
                    print(
                        f"[INFO] GVL {gvl_name} found but no matching variables to update"
                    )
        else:
            # Likely a POU
            pou_name = sc_name
            pou_updated = False
            print(f"[DEBUG] Processing as POU: {pou_name}, namespace: {PLCOPEN_NS}")

            # Method 1: CODESYS-specific format - POUs in addData sections
            for data in root.findall(f".//{{{PLCOPEN_NS}}}data"):
                if data.get("name") == "http://www.3s-software.com/plcopenxml/pou":
                    # Find POU by iterating and checking name attribute
                    for pou in data.findall(f".//{{{PLCOPEN_NS}}}pou"):
                        if pou.get("name") == pou_name:
                            body = pou.find(f".//{{{PLCOPEN_NS}}}body")
                            if body is not None:
                                st_elem = body.find(f".//{{{PLCOPEN_NS}}}ST")
                                if st_elem is not None:
                                    xhtml = st_elem.find(f".//{{{XHTML_NS}}}xhtml")
                                    if xhtml is not None:
                                        xhtml.text = new_code
                                        updated_count += 1
                                        pou_updated = True
                                        print(f"[OK] Updated POU {pou_name}")
                                        break
                    if pou_updated:
                        break

            # Method 2: Standard PLCopen format - POUs in <types><pous> (fallback)
            if not pou_updated:
                types_elem = root.find(f".//{{{PLCOPEN_NS}}}types")
                if types_elem is not None:
                    print(f"[DEBUG] Found types element")
                    pous_elem = types_elem.find(f".//{{{PLCOPEN_NS}}}pous")
                    if pous_elem is not None:
                        print(
                            f"[DEBUG] Found pous element, searching for POU {pou_name}"
                        )
                        # Find POU by iterating and checking name attribute
                        for pou in pous_elem.findall(f".//{{{PLCOPEN_NS}}}pou"):
                            print(f"[DEBUG] Found POU: {pou.get('name')}")
                            if pou.get("name") == pou_name:
                                print(f"[DEBUG] Matched POU {pou_name}, updating...")
                                body = pou.find(f".//{{{PLCOPEN_NS}}}body")
                                if body is not None:
                                    st_elem = body.find(f".//{{{PLCOPEN_NS}}}ST")
                                    if st_elem is not None:
                                        # Try xhtml format (CODESYS)
                                        xhtml = st_elem.find(f".//{{{XHTML_NS}}}xhtml")
                                        if xhtml is not None:
                                            xhtml.text = new_code
                                            updated_count += 1
                                            pou_updated = True
                                            print(
                                                f"[OK] Updated POU {pou_name} (standard format)"
                                            )
                                            break
                                        else:
                                            # Try xhtml:p format (for compatibility)
                                            xhtml_p = st_elem.find(
                                                f".//{{{XHTML_NS}}}p"
                                            )
                                            if xhtml_p is not None:
                                                xhtml_p.text = new_code
                                                updated_count += 1
                                                pou_updated = True
                                                print(
                                                    f"[OK] Updated POU {pou_name} (standard format)"
                                                )
                                                break

    # Write updated XML
    # We use ElementTree to preserve CODESYS extensions, but ideally we'd use xsdata serializer
    # for standard parts. For now, ElementTree works for both standard and extended parts.
    ET.register_namespace("", PLCOPEN_NS)
    ET.register_namespace("xhtml", XHTML_NS)

    # Write with proper formatting
    tree.write(output_path, encoding="utf-8", xml_declaration=True)

    # Fix the XML declaration format (ElementTree writes it differently)
    with open(output_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Remove any duplicate XML declarations (ElementTree may add one, and original may have one)
    lines = content.split("\n")
    xml_decl_count = 0
    cleaned_lines = []
    for line in lines:
        if line.strip().startswith("<?xml"):
            xml_decl_count += 1
            # Keep only the first XML declaration, and ensure it's in the correct format
            if xml_decl_count == 1:
                cleaned_lines.append('<?xml version="1.0" encoding="utf-8"?>')
            # Skip subsequent declarations
        else:
            cleaned_lines.append(line)

    content = "\n".join(cleaned_lines)

    # Ensure proper XML declaration if none exists
    if not content.strip().startswith("<?xml"):
        content = '<?xml version="1.0" encoding="utf-8"?>\n' + content

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(
        f"\n[OK] Updated {updated_count} items and removed {removed_count} items in XML file: {output_path}"
    )
    print(
        f"[INFO] Note: Using ElementTree for XML output (preserves CODESYS extensions)"
    )

    return updated_count + removed_count


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python plcopenxmlmerge.py modified_sc_dir/ source.xml output.xml")
        sys.exit(1)

    sc_dir = sys.argv[1]
    source_xml = sys.argv[2]
    output_xml = sys.argv[3]

    if not os.path.exists(sc_dir):
        print(f"Error: SC directory not found: {sc_dir}")
        sys.exit(1)

    if not os.path.exists(source_xml):
        print(f"Error: Source XML file not found: {source_xml}")
        sys.exit(1)

    update_xml_with_sc_content(source_xml, sc_dir, output_xml)
