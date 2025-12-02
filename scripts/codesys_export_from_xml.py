#!/usr/bin/env python3
"""
Export CODESYS XML file to text format (fallback when Scripting API not available).
Extracts POUs and GVLs from XML and exports as .st files.

Usage:
    python codesys_export_from_xml.py <input.xml> <output_dir>
"""

import sys
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import html


def extract_st_from_xhtml(st_element):
    """Extract ST code from <ST><xhtml> structure."""
    if st_element is None:
        return None
    
    XHTML_NS = "http://www.w3.org/1999/xhtml"
    
    # Look for xhtml element inside ST (CODESYS format)
    xhtml = st_element.find(f".//{{{XHTML_NS}}}xhtml")
    if xhtml is not None:
        text = xhtml.text or ""
        if xhtml.tail:
            text += xhtml.tail
        return html.unescape(text.strip())
    
    # Fallback: try direct text content
    if st_element.text:
        return html.unescape(st_element.text.strip())
    
    return None


def extract_variable_declarations(var_list_element, namespace):
    """Extract variable declarations from a variable list element."""
    PLCOPEN_NS = namespace
    
    declarations = []
    variables = var_list_element.findall(f".//{{{PLCOPEN_NS}}}variable")
    
    for var in variables:
        var_name = var.get("name", "")
        var_type = None
        
        type_elem = var.find(f".//{{{PLCOPEN_NS}}}type")
        if type_elem is not None:
            derived = type_elem.find(f".//{{{PLCOPEN_NS}}}derived")
            if derived is not None:
                var_type = derived.get("name", "")
            else:
                # Check for base types
                for base_type in ["BOOL", "INT", "CHAR", "REAL", "STRING", "DINT", "WORD", "BYTE"]:
                    if type_elem.find(f".//{{{PLCOPEN_NS}}}{base_type}") is not None:
                        var_type = base_type
                        break
        
        if var_name and var_type:
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


def export_pou_from_xml(pou_element, output_dir, namespace):
    """Export a POU from XML to a text file, including methods."""
    PLCOPEN_NS = namespace
    
    pou_name = pou_element.get("name", "Unknown")
    pou_type = pou_element.get("pouType", "program")
    
    # Get declaration (interface)
    decl = ""
    interface = pou_element.find(f".//{{{PLCOPEN_NS}}}interface")
    if interface is not None:
        var_lists = interface.findall(f".//{{{PLCOPEN_NS}}}variableList")
        if var_lists:
            decl_parts = []
            for var_list in var_lists:
                vars_text = extract_variable_declarations(var_list, PLCOPEN_NS)
                if vars_text:
                    decl_parts.append(vars_text)
            if decl_parts:
                decl = "\n".join(decl_parts)
    
    # Get implementation (body)
    impl = ""
    body = pou_element.find(f".//{{{PLCOPEN_NS}}}body")
    if body is not None:
        st_element = body.find(f".//{{{PLCOPEN_NS}}}ST")
        if st_element is not None:
            impl = extract_st_from_xhtml(st_element) or ""
    
    # Determine file extension
    if pou_type == "program":
        ext = '.prg.st'
    elif pou_type == "functionBlock":
        ext = '.fb.st'
    elif pou_type == "function":
        ext = '.fun.st'
    else:
        ext = '.st'
    
    filename = os.path.join(output_dir, f"{pou_name}{ext}")
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"(* POU: {pou_name} *)\n")
        f.write(f"(* Type: {pou_type} *)\n\n")
        
        if decl:
            f.write("(* DECLARATION *)\n")
            f.write(decl)
            f.write("\n\n")
        
        if impl:
            f.write("(* IMPLEMENTATION *)\n")
            f.write(impl)
            f.write("\n")
    
    print(f"[OK] Exported POU: {pou_name}")
    
    # Also export methods from the POU's addData sections
    method_count = 0
    for data in pou_element.findall(f".//{{{PLCOPEN_NS}}}data"):
        if data.get("name") == "http://www.3s-software.com/plcopenxml/method":
            method = data.find(f".//{{{PLCOPEN_NS}}}Method")
            if method is not None:
                method_name = method.get("name", "Unknown")
                method_body = method.find(f".//{{{PLCOPEN_NS}}}body")
                if method_body is not None:
                    st_element = method_body.find(f".//{{{PLCOPEN_NS}}}ST")
                    if st_element is not None:
                        method_impl = extract_st_from_xhtml(st_element)
                        if method_impl:
                            # Export method as separate file: POU_METHOD.meth.st
                            method_filename = os.path.join(output_dir, f"{pou_name}_{method_name}.meth.st")
                            with open(method_filename, 'w', encoding='utf-8') as f:
                                f.write(f"(* Method: {method_name} in POU: {pou_name} *)\n\n")
                                f.write("(* IMPLEMENTATION *)\n")
                                f.write(method_impl)
                                f.write("\n")
                            print(f"[OK] Exported Method: {pou_name}_{method_name}")
                            method_count += 1
    
    return True, method_count


def export_gvl_from_xml(gvl_element, output_dir, namespace):
    """Export a GVL from XML to a text file."""
    PLCOPEN_NS = namespace
    
    gvl_name = gvl_element.get("name", "GVL")
    
    # Get variables
    decl = ""
    direct_vars = gvl_element.findall(f"./{{{PLCOPEN_NS}}}variable")
    if direct_vars:
        var_list_wrapper = ET.Element("variableList")
        for var in direct_vars:
            var_list_wrapper.append(var)
        vars_text = extract_variable_declarations(var_list_wrapper, PLCOPEN_NS)
        if vars_text:
            decl = f"VAR_GLOBAL\n\n{vars_text}\n\nEND_VAR"
    
    filename = os.path.join(output_dir, f"{gvl_name}.gvl.st")
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"(* GVL: {gvl_name} *)\n\n")
        if decl:
            f.write(decl)
            f.write("\n")
    
    print(f"[OK] Exported GVL: {gvl_name}")
    return True


def export_xml_to_text(xml_path, output_dir):
    """Export XML file to text format."""
    
    # Parse XML
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    # Detect namespace
    ns_map = root.tag.split("}")[0].strip("{") if "}" in root.tag else ""
    if "tc6_0201" in ns_map:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0201"
    else:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0200"
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    pous_count = 0
    methods_count = 0
    gvls_count = 0
    
    # Extract POUs from addData sections (CODESYS format)
    for data in root.findall(f".//{{{PLCOPEN_NS}}}data"):
        if data.get("name") == "http://www.3s-software.com/plcopenxml/pou":
            pou = data.find(f".//{{{PLCOPEN_NS}}}pou")
            if pou is not None:
                success, method_count = export_pou_from_xml(pou, output_dir, PLCOPEN_NS)
                if success:
                    pous_count += 1
                    methods_count += method_count
    
    # Also check standard location
    types_elem = root.find(f".//{{{PLCOPEN_NS}}}types")
    if types_elem is not None:
        pous_elem = types_elem.find(f".//{{{PLCOPEN_NS}}}pous")
        if pous_elem is not None:
            for pou in pous_elem.findall(f".//{{{PLCOPEN_NS}}}pou"):
                success, method_count = export_pou_from_xml(pou, output_dir, PLCOPEN_NS)
                if success:
                    pous_count += 1
                    methods_count += method_count
    
    # Extract GVLs
    global_vars_elements = root.findall(f".//{{{PLCOPEN_NS}}}globalVars")
    for gvl in global_vars_elements:
        if export_gvl_from_xml(gvl, output_dir, PLCOPEN_NS):
            gvls_count += 1
    
    print(f"\n[OK] Export complete: {pous_count} POUs, {methods_count} Methods, {gvls_count} GVLs")
    print(f"[INFO] Exported to: {output_dir}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python codesys_export_from_xml.py <input.xml> <output_dir>")
        print("\nExample:")
        print('  python codesys_export_from_xml.py "Untitled1.xml" "export"')
        sys.exit(1)
    
    xml_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    if not os.path.exists(xml_file):
        print(f"Error: XML file not found: {xml_file}")
        sys.exit(1)
    
    try:
        export_xml_to_text(xml_file, output_dir)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

