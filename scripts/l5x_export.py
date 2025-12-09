#!/usr/bin/env python3
"""
Export Rockwell/Allen-Bradley L5X files to structured code (.sc) format.
Extracts Add-On Instructions (AOIs), their parameters, local tags, and routines.

Usage:
    python l5x_export.py <input.L5X> <output_dir>
    python l5x_export.py <input_dir> <output_dir>  # Process all L5X files in directory
"""

import sys
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import html


def extract_parameters(aoi_element):
    """Extract parameter declarations from AddOnInstructionDefinition."""
    params = []

    parameters = aoi_element.find("Parameters")
    if parameters is None:
        return ""

    for param in parameters.findall("Parameter"):
        name = param.get("Name", "")
        usage = param.get("Usage", "Input")  # Input, Output, InOut
        data_type = param.get("DataType", "BOOL")
        required = param.get("Required", "false")
        visible = param.get("Visible", "true")

        # Skip system parameters
        if name in ["EnableIn", "EnableOut"]:
            continue

        # Get description if available
        desc_elem = param.find("Description")
        description = ""
        if desc_elem is not None and desc_elem.text:
            description = f"  // {desc_elem.text.strip()}"

        # Format parameter declaration
        param_line = f"\t{name}: {data_type};"
        if description:
            param_line += description

        params.append((usage, param_line))

    # Group by usage type
    input_params = [p[1] for p in params if p[0] == "Input"]
    output_params = [p[1] for p in params if p[0] == "Output"]
    inout_params = [p[1] for p in params if p[0] == "InOut"]

    result = []

    if input_params:
        result.append("VAR_INPUT")
        result.extend(input_params)
        result.append("END_VAR")

    if output_params:
        result.append("\nVAR_OUTPUT")
        result.extend(output_params)
        result.append("END_VAR")

    if inout_params:
        result.append("\nVAR_IN_OUT")
        result.extend(inout_params)
        result.append("END_VAR")

    return "\n".join(result) if result else ""


def extract_local_tags(aoi_element):
    """Extract local tag declarations from AddOnInstructionDefinition."""
    tags = []

    local_tags = aoi_element.find("LocalTags")
    if local_tags is None:
        return ""

    for tag in local_tags.findall("LocalTag"):
        name = tag.get("Name", "")
        data_type = tag.get("DataType", "BOOL")

        # Get description if available
        desc_elem = tag.find("Description")
        description = ""
        if desc_elem is not None and desc_elem.text:
            description = f"  // {desc_elem.text.strip()}"

        # Get default value if available
        default_elem = tag.find("DefaultData")
        default_val = ""
        if default_elem is not None:
            # Try to extract decorated value
            data_value = default_elem.find(".//DataValue")
            if data_value is not None:
                value = data_value.get("Value", "")
                if value:
                    default_val = f" := {value}"

        tag_line = f"\t{name}: {data_type}{default_val};"
        if description:
            tag_line += description

        tags.append(tag_line)

    if tags:
        result = ["VAR"]
        result.extend(tags)
        result.append("END_VAR")
        return "\n".join(result)

    return ""


def extract_routines(aoi_element):
    """Extract routines (ladder logic or structured text) from AddOnInstructionDefinition."""
    routines_text = []

    routines = aoi_element.find("Routines")
    if routines is None:
        return ""

    for routine in routines.findall("Routine"):
        routine_name = routine.get("Name", "Main")
        routine_type = routine.get("Type", "RLL")  # RLL (ladder) or ST (structured text)

        routines_text.append(f"\n(* ROUTINE: {routine_name} [{routine_type}] *)")

        if routine_type == "RLL":
            # Extract ladder logic rungs
            rll_content = routine.find("RLLContent")
            if rll_content is not None:
                for rung in rll_content.findall("Rung"):
                    rung_num = rung.get("Number", "0")
                    rung_type = rung.get("Type", "N")

                    # Get comment
                    comment_elem = rung.find("Comment")
                    if comment_elem is not None and comment_elem.text:
                        comment = comment_elem.text.strip()
                        routines_text.append(f"\n// Rung {rung_num}: {comment}")
                    else:
                        routines_text.append(f"\n// Rung {rung_num}")

                    # Get ladder logic text
                    text_elem = rung.find("Text")
                    if text_elem is not None and text_elem.text:
                        ladder_text = text_elem.text.strip()
                        routines_text.append(f"{ladder_text}")

        elif routine_type == "ST":
            # Extract structured text
            st_content = routine.find("STContent")
            if st_content is not None and st_content.text:
                st_text = st_content.text.strip()
                routines_text.append(f"\n{st_text}")

    return "\n".join(routines_text) if routines_text else ""


def export_aoi_from_l5x(aoi_element, output_dir):
    """Export an Add-On Instruction from L5X to .sc file."""

    aoi_name = aoi_element.get("Name", "Unknown")
    revision = aoi_element.get("Revision", "1.0")
    vendor = aoi_element.get("Vendor", "")

    # Get description
    desc_elem = aoi_element.find("Description")
    description = ""
    if desc_elem is not None and desc_elem.text:
        description = desc_elem.text.strip()

    # Extract components
    parameters = extract_parameters(aoi_element)
    local_tags = extract_local_tags(aoi_element)
    routines = extract_routines(aoi_element)

    # Create output file
    filename = os.path.join(output_dir, f"{aoi_name}.aoi.sc")

    with open(filename, 'w', encoding='utf-8') as f:
        # Header
        f.write(f"(* AOI: {aoi_name} *)\n")
        f.write(f"(* Type: AddOnInstruction *)\n")
        f.write(f"(* Revision: {revision} *)\n")
        if vendor:
            f.write(f"(* Vendor: {vendor} *)\n")
        if description:
            f.write(f"(* Description: {description} *)\n")
        f.write("\n")

        # Parameters
        if parameters:
            f.write("(* PARAMETERS *)\n")
            f.write(parameters)
            f.write("\n\n")

        # Local Tags
        if local_tags:
            f.write("(* LOCAL TAGS *)\n")
            f.write(local_tags)
            f.write("\n\n")

        # Implementation
        if routines:
            f.write("(* IMPLEMENTATION *)\n")
            f.write(routines)
            f.write("\n")

    print(f"[OK] Exported AOI: {aoi_name}")
    return True


def export_datatypes_from_l5x(l5x_root, output_dir):
    """Export custom data types from L5X file."""
    datatypes_exported = 0

    datatypes = l5x_root.find(".//DataTypes")
    if datatypes is None:
        return 0

    for datatype in datatypes.findall("DataType"):
        dt_name = datatype.get("Name", "Unknown")
        dt_family = datatype.get("Family", "NoFamily")
        dt_class = datatype.get("Class", "User")

        members = []
        members_elem = datatype.find("Members")
        if members_elem is not None:
            for member in members_elem.findall("Member"):
                mem_name = member.get("Name", "")
                mem_type = member.get("DataType", "SINT")
                dimension = member.get("Dimension", "0")
                hidden = member.get("Hidden", "false")

                # Skip hidden helper members
                if hidden == "true":
                    continue

                # Handle bit members
                target = member.get("Target")
                bit_num = member.get("BitNumber")
                if target and bit_num:
                    members.append(f"\t{mem_name}: BIT;  // Bit {bit_num} of {target}")
                else:
                    # Handle arrays
                    if dimension != "0":
                        members.append(f"\t{mem_name}: ARRAY[0..{int(dimension)-1}] OF {mem_type};")
                    else:
                        members.append(f"\t{mem_name}: {mem_type};")

        if members:
            filename = os.path.join(output_dir, f"{dt_name}.udt.sc")

            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"(* UDT: {dt_name} *)\n")
                f.write(f"(* Type: UserDefinedType *)\n")
                f.write(f"(* Family: {dt_family} *)\n\n")
                f.write(f"TYPE {dt_name} :\n")
                f.write("STRUCT\n")
                f.write("\n".join(members))
                f.write("\nEND_STRUCT\n")
                f.write("END_TYPE\n")

            print(f"[OK] Exported UDT: {dt_name}")
            datatypes_exported += 1

    return datatypes_exported


def export_l5x_to_sc(l5x_path, output_dir):
    """Export L5X file to structured code (.sc) format."""

    # Parse XML
    try:
        tree = ET.parse(l5x_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"[ERROR] Failed to parse XML: {e}")
        return False

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    aois_count = 0
    datatypes_count = 0

    # Extract Add-On Instructions
    controller = root.find(".//Controller")
    if controller is not None:
        aoi_defs = controller.find("AddOnInstructionDefinitions")
        if aoi_defs is not None:
            for aoi in aoi_defs.findall("AddOnInstructionDefinition"):
                if export_aoi_from_l5x(aoi, output_dir):
                    aois_count += 1

    # Extract custom data types
    datatypes_count = export_datatypes_from_l5x(root, output_dir)

    print(f"\n[OK] Export complete: {aois_count} AOIs, {datatypes_count} UDTs")
    print(f"[INFO] Exported to: {output_dir}")

    return True


def process_directory(input_dir, output_dir):
    """Process all L5X files in a directory."""
    l5x_files = list(Path(input_dir).glob("*.L5X")) + list(Path(input_dir).glob("*.l5x"))

    if not l5x_files:
        print(f"[WARNING] No L5X files found in {input_dir}")
        return False

    print(f"[INFO] Found {len(l5x_files)} L5X file(s)")

    for l5x_file in l5x_files:
        print(f"\n[INFO] Processing: {l5x_file.name}")
        # Create subdirectory for each L5X file
        file_output_dir = os.path.join(output_dir, l5x_file.stem)
        export_l5x_to_sc(str(l5x_file), file_output_dir)

    return True


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python l5x_export.py <input.L5X|input_dir> <output_dir>")
        print("\nExamples:")
        print('  python l5x_export.py "Motor_Control.L5X" "export"')
        print('  python l5x_export.py "PLC" "export"  # Process all L5X in PLC directory')
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.exists(input_path):
        print(f"Error: Input path not found: {input_path}")
        sys.exit(1)

    try:
        if os.path.isdir(input_path):
            success = process_directory(input_path, output_dir)
        else:
            success = export_l5x_to_sc(input_path, output_dir)

        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
