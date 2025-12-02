#!/usr/bin/env python3
"""
Restructure CODESYS-exported PLCopen XML files to be importable.
Moves POUs from addData sections to standard <types><pous> location.

Usage:
    python plcopenxmlrestructure.py input.xml output.xml
"""

import sys
import os
from pathlib import Path
import xml.etree.ElementTree as ET


def restructure_codesys_xml(input_path, output_path):
    """
    Restructure CODESYS XML to move POUs from addData to standard PLCopen location.
    
    Args:
        input_path: Path to CODESYS-exported XML file
        output_path: Path to output restructured XML file
    """
    # Parse XML file
    tree = ET.parse(input_path)
    root = tree.getroot()
    
    # Detect namespace - CODESYS uses tc6_0200
    ns_map = root.tag.split("}")[0].strip("{") if "}" in root.tag else ""
    if "tc6_0201" in ns_map:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0201"
    else:
        PLCOPEN_NS = "http://www.plcopen.org/xml/tc6_0200"
    
    XHTML_NS = "http://www.w3.org/1999/xhtml"
    
    # Register namespaces
    ET.register_namespace("", PLCOPEN_NS)
    
    # Check if root already has xhtml namespace declaration
    has_xhtml_ns = False
    for key in root.attrib:
        if key.startswith("xmlns") and root.attrib[key] == XHTML_NS:
            has_xhtml_ns = True
            break
    
    # If not, add it (but don't register as prefix - xhtml elements use default namespace)
    if not has_xhtml_ns:
        root.attrib["xmlns:xhtml"] = XHTML_NS
    
    # Find or create <types><pous> section
    types_elem = root.find(f".//{{{PLCOPEN_NS}}}types")
    if types_elem is None:
        # Create types element if it doesn't exist
        types_elem = ET.SubElement(root, f"{{{PLCOPEN_NS}}}types")
        ET.SubElement(types_elem, f"{{{PLCOPEN_NS}}}dataTypes")
    
    pous_elem = types_elem.find(f".//{{{PLCOPEN_NS}}}pous")
    if pous_elem is None:
        # Create pous element if it doesn't exist
        pous_elem = ET.SubElement(types_elem, f"{{{PLCOPEN_NS}}}pous")
    
    moved_count = 0
    
    # Find all POUs in addData sections
    # Look for: //addData/data[@name="http://www.3s-software.com/plcopenxml/pou"]/pou
    pou_data_elements = []
    for add_data in root.findall(f".//{{{PLCOPEN_NS}}}addData"):
        for data in add_data.findall(f".//{{{PLCOPEN_NS}}}data"):
            if data.get("name") == "http://www.3s-software.com/plcopenxml/pou":
                pou = data.find(f".//{{{PLCOPEN_NS}}}pou")
                if pou is not None:
                    pou_data_elements.append((data, pou, add_data))
    
    # Move each POU to standard location
    for data_elem, pou_elem, add_data_elem in pou_data_elements:
        pou_name = pou_elem.get("name", "Unknown")
        pou_type = pou_elem.get("pouType", "program")
        
        # Check if POU already exists in standard location (avoid duplicates)
        existing_pou = None
        for existing in pous_elem.findall(f".//{{{PLCOPEN_NS}}}pou"):
            if existing.get("name") == pou_name:
                existing_pou = existing
                break
        
        if existing_pou is None:
            # Create a deep copy of the POU element
            # We need to copy the entire POU including its addData (methods)
            new_pou = ET.Element(pou_elem.tag, pou_elem.attrib)
            
            # Copy all children (interface, body, addData, etc.)
            for child in pou_elem:
                new_pou.append(copy_element(child))
            
            # Add to standard location
            pous_elem.append(new_pou)
            moved_count += 1
            print(f"[OK] Moved POU '{pou_name}' ({pou_type}) to standard location")
        else:
            print(f"[INFO] POU '{pou_name}' already exists in standard location, skipping")
    
    # Optionally remove the original addData entries (commented out for safety)
    # Uncomment if you want to clean up the addData sections
    # for data_elem, pou_elem, add_data_elem in pou_data_elements:
    #     # Remove the data element from addData
    #     if data_elem in list(add_data_elem):
    #         add_data_elem.remove(data_elem)
    #         print(f"[INFO] Removed POU addData entry for '{pou_elem.get('name')}'")
    
    # Write restructured XML
    ET.indent(tree, space="  ", level=0)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    
    # Fix XML declaration format
    with open(output_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Remove duplicate XML declarations
    lines = content.split("\n")
    xml_decl_count = 0
    cleaned_lines = []
    for line in lines:
        if line.strip().startswith("<?xml"):
            xml_decl_count += 1
            if xml_decl_count == 1:
                cleaned_lines.append('<?xml version="1.0" encoding="utf-8"?>')
        else:
            cleaned_lines.append(line)
    
    content = "\n".join(cleaned_lines)
    
    # Ensure proper XML declaration
    if not content.strip().startswith("<?xml"):
        content = '<?xml version="1.0" encoding="utf-8"?>\n' + content
    
    # Fix xhtml namespace declarations
    content = fix_xhtml_namespaces(content)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    print(f"\n[OK] Restructured {moved_count} POUs in XML file: {output_path}")
    print(f"[INFO] POUs are now in standard <types><pous> location")
    print(f"[INFO] File is ready for import into CODESYS")
    
    return moved_count


def copy_element(element):
    """Create a deep copy of an XML element, preserving namespace declarations."""
    # Preserve tag and attributes exactly as they are
    new_elem = ET.Element(element.tag, element.attrib)
    new_elem.text = element.text
    new_elem.tail = element.tail
    
    # Copy all children recursively
    for child in element:
        new_elem.append(copy_element(child))
    
    return new_elem


def fix_xhtml_namespaces(content):
    """
    Fix xhtml namespace declarations - ElementTree writes them with prefixes
    but they should be <xhtml xmlns="http://www.w3.org/1999/xhtml">
    """
    import re
    # Replace any xhtml prefix (xhtml:xhtml, html:xhtml, etc.) with default namespace
    content = re.sub(
        r'<([a-z]+):xhtml>',
        r'<xhtml xmlns="http://www.w3.org/1999/xhtml">',
        content
    )
    content = re.sub(
        r'</([a-z]+):xhtml>',
        '</xhtml>',
        content
    )
    # Also handle self-closing tags
    content = re.sub(
        r'<([a-z]+):xhtml\s*/>',
        '<xhtml xmlns="http://www.w3.org/1999/xhtml" />',
        content
    )
    return content


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python plcopenxmlrestructure.py input.xml output.xml")
        print("\nThis script restructures CODESYS-exported XML files by moving")
        print("POUs from addData sections to the standard <types><pous> location")
        print("so they can be imported back into CODESYS.")
        sys.exit(1)
    
    input_xml = sys.argv[1]
    output_xml = sys.argv[2]
    
    if not os.path.exists(input_xml):
        print(f"Error: Input file not found: {input_xml}")
        sys.exit(1)
    
    try:
        restructure_codesys_xml(input_xml, output_xml)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

