#!/usr/bin/env python3
"""
Extract Structured Text and Global Variables from PLCopen XML files.
Usage: python plcopenxmlprocessor.py input.xml output_dir/
"""

import sys
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import html


def extract_variable_declarations(var_list_element, namespace=None):
    """Extract variable declarations from a variable list element and convert to ST format."""
    if namespace:
        PLCOPEN_NS = namespace
    else:
        # Detect namespace from the element
        ns_map = (
            var_list_element.tag.split("}")[0].strip("{")
            if "}" in var_list_element.tag
            else ""
        )
        if "tc6_0201" in ns_map:
            PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0201"
        else:
            PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0200"

    if var_list_element is None:
        return None

    declarations = []

    # Find all variable elements
    variables = var_list_element.findall(f".//{{{PLCOPEN_NS}}}variable")
    for var in variables:
        var_name = var.get("name", "")
        var_type = None

        # Try to find the type
        type_elem = var.find(f".//{{{PLCOPEN_NS}}}type")
        if type_elem is not None:
            # Check for derived type
            derived = type_elem.find(f".//{{{PLCOPEN_NS}}}derived")
            if derived is not None:
                var_type = derived.get("name", "")
            else:
                # Check for base type
                base_type = type_elem.find(f".//{{{PLCOPEN_NS}}}BOOL")
                if base_type is not None:
                    var_type = "BOOL"
                else:
                    base_type = type_elem.find(f".//{{{PLCOPEN_NS}}}INT")
                    if base_type is not None:
                        var_type = "INT"
                    else:
                        base_type = type_elem.find(f".//{{{PLCOPEN_NS}}}CHAR")
                        if base_type is not None:
                            var_type = "CHAR"
                        else:
                            base_type = type_elem.find(f".//{{{PLCOPEN_NS}}}REAL")
                            if base_type is not None:
                                var_type = "REAL"
                            else:
                                base_type = type_elem.find(f".//{{{PLCOPEN_NS}}}STRING")
                                if base_type is not None:
                                    var_type = "STRING"

        if var_name and var_type:
            # Check for initial value
            initial_value = var.find(f".//{{{PLCOPEN_NS}}}initialValue")
            init_val = ""
            if initial_value is not None:
                const_value = initial_value.find(f".//{{{PLCOPEN_NS}}}constant")
                if const_value is not None:
                    const_val_elem = const_value.find(f".//{{{PLCOPEN_NS}}}simpleValue")
                    if const_val_elem is not None:
                        init_val = f" := {const_val_elem.get('value', '')}"

            declarations.append(f"\t{var_name}: {var_type};{init_val}")

    return "\n".join(declarations) if declarations else None


def extract_st_from_xhtml(st_element):
    """Extract ST code from <ST><xhtml> structure."""
    if st_element is None:
        return None

    XHTML_NS = "http://www.w3.org/1999/xhtml"

    # Look for xhtml:p element (Arduino format with CDATA)
    xhtml_p = st_element.find(f".//{{{XHTML_NS}}}p")
    if xhtml_p is not None:
        # Get CDATA content
        text = xhtml_p.text or ""
        # Unescape HTML entities
        return html.unescape(text.strip())

    # Look for xhtml element inside ST (CODESYS format)
    xhtml = st_element.find(f".//{{{XHTML_NS}}}xhtml")
    if xhtml is not None:
        # Get text content, handling CDATA and text nodes
        text = xhtml.text or ""
        # Also check tail text from xhtml element
        if xhtml.tail:
            text += xhtml.tail
        # Unescape HTML entities
        return html.unescape(text.strip())

    # Fallback: try direct text content
    if st_element.text:
        return html.unescape(st_element.text.strip())

    return None


def extract_pou_code(pou_element, output_path):
    """Extract code from a POU element. Returns (pou_extracted, method_count)."""
    # Detect namespace from the element
    ns_map = pou_element.tag.split("}")[0].strip("{") if "}" in pou_element.tag else ""
    if "tc6_0201" in ns_map:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0201"
    else:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0200"

    pou_name = pou_element.get("name", "Unknown")
    pou_type = pou_element.get("pouType", "unknown")

    pou_extracted = False
    method_count = 0

    # Find body/ST content
    body = pou_element.find(f".//{{{PLCOPEN_NS}}}body")
    if body is not None:
        st_element = body.find(f".//{{{PLCOPEN_NS}}}ST")
        if st_element is not None:
            st_code = extract_st_from_xhtml(st_element)
            if st_code:
                output_file = output_path / f"{pou_name}.sc"
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(f"(* POU: {pou_name} *)\n")
                    f.write(f"(* Type: {pou_type} *)\n\n")
                    f.write(st_code)
                    f.write("\n")
                print(f"[OK] Extracted POU: {pou_name}.sc")
                pou_extracted = True

    # Also check for methods inside the POU's addData sections
    # Methods are in data elements with name="http://www.3s-software.com/plcopenxml/method"
    for data in pou_element.findall(f".//{{{PLCOPEN_NS}}}data"):
        if data.get("name") == "http://www.3s-software.com/plcopenxml/method":
            method = data.find(f".//{{{PLCOPEN_NS}}}Method")
            if method is not None:
                method_name = method.get("name", "Unknown")
                method_body = method.find(f".//{{{PLCOPEN_NS}}}body")
                if method_body is not None:
                    st_element = method_body.find(f".//{{{PLCOPEN_NS}}}ST")
                    if st_element is not None:
                        st_code = extract_st_from_xhtml(st_element)
                        if st_code:
                            output_file = output_path / f"{pou_name}_{method_name}.sc"
                            with open(output_file, "w", encoding="utf-8") as f:
                                f.write(
                                    f"(* Method: {method_name} in POU: {pou_name} *)\n\n"
                                )
                                f.write(st_code)
                                f.write("\n")
                            print(f"[OK] Extracted Method: {pou_name}_{method_name}.sc")
                            method_count += 1

    return (pou_extracted, method_count)


def parse_plcopen_xml(xml_path, output_dir):
    """Parse PLCopen XML and extract ST code and global variables to .sc files."""

    # Parse XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Detect namespace - Arduino uses tc6_0201, CODESYS uses tc6_0200
    # Get namespace from root element
    ns_map = root.tag.split("}")[0].strip("{") if "}" in root.tag else ""
    if "tc6_0201" in ns_map:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0201"
    else:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0200"

    XHTML_NS = "http://www.w3.org/1999/xhtml"

    # Register namespaces for easier searching
    ET.register_namespace("", PLCOPEN_NS)
    ET.register_namespace("xhtml", XHTML_NS)

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    extracted_pous = 0
    extracted_methods = 0
    extracted_gvls = 0

    # Extract POUs - check both standard location and CODESYS-specific location
    # Method 1: Standard PLCopen format (Arduino, etc.) - POUs in <types><pous>
    types_elem = root.find(f".//{{{PLCOPEN_NS}}}types")
    if types_elem is not None:
        pous_elem = types_elem.find(f".//{{{PLCOPEN_NS}}}pous")
        if pous_elem is not None:
            for pou in pous_elem.findall(f".//{{{PLCOPEN_NS}}}pou"):
                pou_extracted, method_count = extract_pou_code(pou, output_path)
                if pou_extracted:
                    extracted_pous += 1
                extracted_methods += method_count

    # Method 2: CODESYS-specific format - POUs in addData sections
    # Look for data elements with name="http://www.3s-software.com/plcopenxml/pou"
    for data in root.findall(f".//{{{PLCOPEN_NS}}}data"):
        if data.get("name") == "http://www.3s-software.com/plcopenxml/pou":
            # Find pou element inside this data element
            pou = data.find(f".//{{{PLCOPEN_NS}}}pou")
            if pou is not None:
                pou_extracted, method_count = extract_pou_code(pou, output_path)
                if pou_extracted:
                    extracted_pous += 1
                extracted_methods += method_count

    # Extract Global Variables
    # GVLs can have variables in interface sections, as direct children, or in addData sections (CODESYS-specific)
    global_vars_elements = root.findall(f".//{{{PLCOPEN_NS}}}globalVars")
    for gvl in global_vars_elements:
        gvl_name = gvl.get("name", "GVL")
        gvl_content = []
        gvl_extracted = False

        # Method 0: Check if variables are direct children of globalVars (CODESYS/Arduino format)
        direct_vars = gvl.findall(f"./{{{PLCOPEN_NS}}}variable")
        if direct_vars:
            # Extract variables directly
            var_list_wrapper = ET.Element("variableList")
            for var in direct_vars:
                var_list_wrapper.append(var)
            st_vars = extract_variable_declarations(var_list_wrapper, PLCOPEN_NS)
            if st_vars:
                gvl_content.append(st_vars)
                gvl_extracted = True

        # Method 1: Check for interface section with variable declarations
        if not gvl_extracted:
            interface = gvl.find(f".//{{{PLCOPEN_NS}}}interface")
            if interface is not None:
                # Look for variableList elements first (standard PLCopen structure)
                var_lists = interface.findall(f".//{{{PLCOPEN_NS}}}variableList")
                if not var_lists:
                    # Fallback: look for variable elements directly
                    var_lists = interface.findall(f".//{{{PLCOPEN_NS}}}variable")

                if var_lists:
                    for var_list in var_lists:
                        # Try to extract as ST format first
                        st_vars = extract_variable_declarations(var_list, PLCOPEN_NS)
                        if st_vars:
                            gvl_content.append(st_vars)
                        else:
                            # Fallback to XML format
                            var_text = ET.tostring(
                                var_list, encoding="unicode", method="xml"
                            )
                            gvl_content.append(var_text)
                    gvl_extracted = True

        # Method 2: Check for GVL content in addData sections (CODESYS-specific)
        # Similar to how POUs are stored, GVLs might be in addData with specific names
        if not gvl_extracted:
            # Look for GVL data in addData sections
            for data in gvl.findall(f".//{{{PLCOPEN_NS}}}data"):
                data_name = data.get("name", "")
                # Check if this data contains GVL variable declarations
                # CODESYS might store GVL content similar to POUs
                if "gvl" in data_name.lower() or "variable" in data_name.lower():
                    # Look for variable declarations
                    var_lists = data.findall(f".//{{{PLCOPEN_NS}}}variable")
                    if var_lists:
                        for var_list in var_lists:
                            st_vars = extract_variable_declarations(var_list)
                            if st_vars:
                                gvl_content.append(st_vars)
                            else:
                                var_text = ET.tostring(
                                    var_list, encoding="unicode", method="xml"
                                )
                                gvl_content.append(var_text)
                        gvl_extracted = True
                        break

        # Method 3: Check if GVL has an interface child directly
        if not gvl_extracted:
            direct_interface = gvl.find(f"./{{{PLCOPEN_NS}}}interface")
            if direct_interface is not None:
                var_lists = direct_interface.findall(f".//{{{PLCOPEN_NS}}}variable")
                if var_lists:
                    for var_list in var_lists:
                        st_vars = extract_variable_declarations(var_list)
                        if st_vars:
                            gvl_content.append(st_vars)
                        else:
                            var_text = ET.tostring(
                                var_list, encoding="unicode", method="xml"
                            )
                            gvl_content.append(var_text)
                    gvl_extracted = True

        # Method 4: Check if GVL variables are stored as ST text (like POUs)
        if not gvl_extracted:
            # Look for ST content in the GVL (some CODESYS versions might store it this way)
            st_elem = gvl.find(f".//{{{PLCOPEN_NS}}}ST")
            if st_elem is not None:
                st_text = extract_st_from_xhtml(st_elem)
                if st_text:
                    gvl_content.append(st_text)
                    gvl_extracted = True

        # Write GVL file if we found content
        if gvl_extracted and gvl_content:
            output_file = output_path / f"{gvl_name}.sc"
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(f"(* Global Variable List: {gvl_name} *)\n\n")
                # Check for qualified_only attribute
                for data in gvl.findall(f".//{{{PLCOPEN_NS}}}data"):
                    if (
                        data.get("name")
                        == "http://www.3s-software.com/plcopenxml/attributes"
                    ):
                        attrs = data.find(f".//{{{PLCOPEN_NS}}}Attributes")
                        if attrs is not None:
                            qual_attr = attrs.find(
                                f".//{{{PLCOPEN_NS}}}Attribute[@Name='qualified_only']"
                            )
                            if qual_attr is not None:
                                f.write("{attribute 'qualified_only'}\n")

                # If content looks like ST code (contains VAR_GLOBAL), write as-is
                # Otherwise wrap in VAR_GLOBAL/END_VAR
                content_text = "\n".join(gvl_content)
                if (
                    "VAR_GLOBAL" in content_text.upper()
                    or "VAR" in content_text.upper()
                ):
                    f.write(content_text)
                    f.write("\n")
                else:
                    f.write("VAR_GLOBAL\n\n")
                    f.write(content_text)
                    f.write("\n\nEND_VAR\n")
            extracted_gvls += 1
            print(f"[OK] Extracted Global Variables: {gvl_name}.sc")
        elif not gvl_extracted:
            # Debug: Print what we found in the GVL to help diagnose
            interface = gvl.find(f".//{{{PLCOPEN_NS}}}interface")
            has_st = gvl.find(f".//{{{PLCOPEN_NS}}}ST") is not None
            has_data = len(gvl.findall(f".//{{{PLCOPEN_NS}}}data")) > 0
            print(
                f"[INFO] GVL '{gvl_name}' found but no variables extracted. Has interface: {interface is not None}, Has ST: {has_st}, Has addData: {has_data}"
            )

    total_extracted = extracted_pous + extracted_methods + extracted_gvls
    print(
        f"\n[OK] Extracted {extracted_pous} POUs, {extracted_methods} Methods, and {extracted_gvls} Global Variable Lists to {output_dir}"
    )
    return total_extracted


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python plcopenxmlprocessor.py input.xml output_dir/")
        sys.exit(1)

    xml_file = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.exists(xml_file):
        print(f"Error: File not found: {xml_file}")
        sys.exit(1)

    try:
        parse_plcopen_xml(xml_file, output_dir)
    except Exception as e:
        print(f"Error parsing XML: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
