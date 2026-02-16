#!/usr/bin/env python3
"""
Export Rockwell/Allen-Bradley L5X files to structured code (.sc) format.

Handles both component-level L5X exports (single AOI/UDT) and full-project
L5X files that contain Programs, controller-scoped Tags, Tasks, Modules,
and Add-On Instructions.

Usage:
    python l5x_export.py <input.L5X> <output_dir>
    python l5x_export.py <input_dir> <output_dir>  # Process all L5X files in directory
"""

import sys
import os
from pathlib import Path
from typing import List, Optional
import xml.etree.ElementTree as ET
import html

from sc_parser import SCFile, Tag, LogicRung


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


def _extract_tags_from_xml(tags_element) -> List[Tag]:
    """Extract Tag objects from an L5X <Tags> element."""
    tags = []
    if tags_element is None:
        return tags

    for tag_elem in tags_element.findall("Tag"):
        name = tag_elem.get("Name", "")
        data_type = tag_elem.get("DataType", "BOOL")
        usage = tag_elem.get("Usage", "")
        tag_type = tag_elem.get("TagType", "Base")
        dimensions = tag_elem.get("Dimensions", "0")
        external_access = tag_elem.get("ExternalAccess", "Read/Write")

        # Get description
        desc_elem = tag_elem.find("Description")
        description = None
        if desc_elem is not None:
            # L5X descriptions can have CDATA children
            cdata = desc_elem.find(".//{http://www.w3.org/1999/xhtml}span")
            if cdata is not None and cdata.text:
                description = cdata.text.strip()
            elif desc_elem.text:
                description = desc_elem.text.strip()

        # Map usage to direction
        direction_map = {
            'Input': 'INPUT',
            'Output': 'OUTPUT',
            'InOut': 'IN_OUT',
        }
        direction = direction_map.get(usage)

        # Handle arrays
        is_array = False
        array_bounds = None
        if dimensions and dimensions != "0":
            is_array = True
            array_bounds = f"0..{int(dimensions) - 1}"

        tags.append(Tag(
            name=name,
            data_type=data_type,
            direction=direction,
            description=description,
            is_array=is_array,
            array_bounds=array_bounds,
        ))

    return tags


def _extract_routines_as_dicts(routines_element) -> List[dict]:
    """Extract routine dicts from an L5X <Routines> element."""
    routine_list = []
    if routines_element is None:
        return routine_list

    for routine in routines_element.findall("Routine"):
        routine_name = routine.get("Name", "Main")
        routine_type = routine.get("Type", "RLL")

        rungs: List[LogicRung] = []
        raw_content_parts = []

        if routine_type == "RLL":
            rll_content = routine.find("RLLContent")
            if rll_content is not None:
                for rung in rll_content.findall("Rung"):
                    rung_num = int(rung.get("Number", "0"))
                    comment_elem = rung.find("Comment")
                    comment = None
                    if comment_elem is not None:
                        cdata = comment_elem.find(".//{http://www.w3.org/1999/xhtml}span")
                        if cdata is not None and cdata.text:
                            comment = cdata.text.strip()
                        elif comment_elem.text:
                            comment = comment_elem.text.strip()

                    text_elem = rung.find("Text")
                    logic = ""
                    if text_elem is not None:
                        cdata = text_elem.find(".//{http://www.w3.org/1999/xhtml}span")
                        if cdata is not None and cdata.text:
                            logic = cdata.text.strip()
                        elif text_elem.text:
                            logic = text_elem.text.strip()

                    rungs.append(LogicRung(
                        number=rung_num,
                        comment=comment,
                        logic=logic,
                    ))
                    if comment:
                        raw_content_parts.append(f"// Rung {rung_num}: {comment}")
                    else:
                        raw_content_parts.append(f"// Rung {rung_num}")
                    raw_content_parts.append(logic)

        elif routine_type == "ST":
            st_content = routine.find("STContent")
            if st_content is not None:
                for line_elem in st_content.findall("Line"):
                    if line_elem.text:
                        raw_content_parts.append(line_elem.text)
                # Fallback: some L5X files put ST in text directly
                if not raw_content_parts and st_content.text:
                    raw_content_parts.append(st_content.text.strip())

        elif routine_type == "FBD":
            fbd_content = routine.find("FBDContent")
            if fbd_content is not None:
                raw_content_parts.append(f"(* FBD routine — {routine_name} *)")
                # Extract sheet/block names for context
                for sheet in fbd_content.findall("Sheet"):
                    sheet_num = sheet.get("Number", "?")
                    raw_content_parts.append(f"(* Sheet {sheet_num} *)")
                    for block in sheet.findall(".//Block"):
                        block_type = block.get("Type", "?")
                        operand = block.get("Operand", "")
                        raw_content_parts.append(f"  {block_type}({operand})")

        elif routine_type == "SFC":
            sfc_content = routine.find("SFCContent")
            if sfc_content is not None:
                raw_content_parts.append(f"(* SFC routine — {routine_name} *)")
                for step in sfc_content.findall(".//Step"):
                    step_name = step.get("Name", "?")
                    raw_content_parts.append(f"  STEP: {step_name}")
                for trans in sfc_content.findall(".//Transition"):
                    trans_name = trans.get("Name", "?")
                    raw_content_parts.append(f"  TRANSITION: {trans_name}")

        routine_list.append({
            'name': routine_name,
            'type': routine_type,
            'rungs': rungs,
            'raw_content': '\n'.join(raw_content_parts),
        })

    return routine_list


def _parse_aoi_to_scfile(aoi_element, source_file: str) -> SCFile:
    """Parse an L5X AddOnInstructionDefinition element into an SCFile."""
    aoi_name = aoi_element.get("Name", "Unknown")
    revision = aoi_element.get("Revision", "1.0")
    vendor = aoi_element.get("Vendor", "")

    desc_elem = aoi_element.find("Description")
    description = None
    if desc_elem is not None and desc_elem.text:
        description = desc_elem.text.strip()

    sc = SCFile(
        file_path=source_file,
        name=aoi_name,
        type='AOI',
        revision=revision,
        vendor=vendor,
        description=description,
    )

    # Parameters
    parameters = aoi_element.find("Parameters")
    if parameters is not None:
        for param in parameters.findall("Parameter"):
            name = param.get("Name", "")
            usage = param.get("Usage", "Input")
            data_type = param.get("DataType", "BOOL")

            if name in ("EnableIn", "EnableOut"):
                continue

            p_desc_elem = param.find("Description")
            p_desc = None
            if p_desc_elem is not None and p_desc_elem.text:
                p_desc = p_desc_elem.text.strip()

            tag = Tag(name=name, data_type=data_type, description=p_desc)
            if usage == "Input":
                tag.direction = "INPUT"
                sc.input_tags.append(tag)
            elif usage == "Output":
                tag.direction = "OUTPUT"
                sc.output_tags.append(tag)
            elif usage == "InOut":
                tag.direction = "IN_OUT"
                sc.inout_tags.append(tag)

    # Local tags
    local_tags = aoi_element.find("LocalTags")
    if local_tags is not None:
        for lt in local_tags.findall("LocalTag"):
            name = lt.get("Name", "")
            data_type = lt.get("DataType", "BOOL")

            lt_desc_elem = lt.find("Description")
            lt_desc = None
            if lt_desc_elem is not None and lt_desc_elem.text:
                lt_desc = lt_desc_elem.text.strip()

            default_val = None
            default_elem = lt.find("DefaultData")
            if default_elem is not None:
                data_value = default_elem.find(".//DataValue")
                if data_value is not None:
                    value = data_value.get("Value", "")
                    if value:
                        default_val = value

            sc.local_tags.append(Tag(
                name=name,
                data_type=data_type,
                description=lt_desc,
                default_value=default_val,
            ))

    # Routines
    routines_elem = aoi_element.find("Routines")
    sc.routines = _extract_routines_as_dicts(routines_elem)

    # Build raw implementation
    if sc.routines:
        parts = []
        for r in sc.routines:
            parts.append(f"(* ROUTINE: {r['name']} [{r['type']}] *)")
            if r.get('raw_content'):
                parts.append(r['raw_content'])
        sc.raw_implementation = '\n'.join(parts)

    return sc


def _parse_datatype_to_scfile(datatype_elem, source_file: str) -> Optional[SCFile]:
    """Parse an L5X DataType element into an SCFile (UDT)."""
    dt_name = datatype_elem.get("Name", "Unknown")
    dt_family = datatype_elem.get("Family", "NoFamily")

    desc_elem = datatype_elem.find("Description")
    description = None
    if desc_elem is not None and desc_elem.text:
        description = desc_elem.text.strip()

    members = []
    members_elem = datatype_elem.find("Members")
    if members_elem is not None:
        for member in members_elem.findall("Member"):
            mem_name = member.get("Name", "")
            mem_type = member.get("DataType", "SINT")
            dimension = member.get("Dimension", "0")
            hidden = member.get("Hidden", "false")

            if hidden == "true":
                continue

            m_desc_elem = member.find("Description")
            m_desc = None
            if m_desc_elem is not None and m_desc_elem.text:
                m_desc = m_desc_elem.text.strip()

            target = member.get("Target")
            bit_num = member.get("BitNumber")

            is_array = False
            array_bounds = None
            if target and bit_num:
                mem_type = "BIT"
                m_desc = f"Bit {bit_num} of {target}" + (f" - {m_desc}" if m_desc else "")
            elif dimension != "0":
                is_array = True
                array_bounds = f"0..{int(dimension) - 1}"

            members.append(Tag(
                name=mem_name,
                data_type=mem_type,
                description=m_desc,
                is_array=is_array,
                array_bounds=array_bounds,
            ))

    if not members:
        return None

    sc = SCFile(
        file_path=source_file,
        name=dt_name,
        type='UDT',
        description=description,
    )
    sc.local_tags = members
    return sc


def _parse_program_to_scfile(program_elem, source_file: str) -> SCFile:
    """Parse an L5X Program element into an SCFile."""
    prog_name = program_elem.get("Name", "Unknown")

    desc_elem = program_elem.find("Description")
    description = None
    if desc_elem is not None and desc_elem.text:
        description = desc_elem.text.strip()

    sc = SCFile(
        file_path=source_file,
        name=prog_name,
        type='PROGRAM',
        description=description,
    )

    # Program-scoped tags
    tags_elem = program_elem.find("Tags")
    for tag in _extract_tags_from_xml(tags_elem):
        if tag.direction == 'INPUT':
            sc.input_tags.append(tag)
        elif tag.direction == 'OUTPUT':
            sc.output_tags.append(tag)
        elif tag.direction == 'IN_OUT':
            sc.inout_tags.append(tag)
        else:
            sc.local_tags.append(tag)

    # Routines
    routines_elem = program_elem.find("Routines")
    sc.routines = _extract_routines_as_dicts(routines_elem)

    if sc.routines:
        parts = []
        for r in sc.routines:
            parts.append(f"(* ROUTINE: {r['name']} [{r['type']}] *)")
            if r.get('raw_content'):
                parts.append(r['raw_content'])
        sc.raw_implementation = '\n'.join(parts)

    return sc


class L5XParser:
    """Parser for Rockwell L5X (XML) project and component files.

    Parses full L5X projects or component exports and returns SCFile objects
    for each AOI, UDT, and Program found.
    """

    def parse_file(self, file_path: str) -> List[SCFile]:
        """Parse an L5X file and return list of SCFile objects."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
        except ET.ParseError as e:
            print(f"[ERROR] Failed to parse L5X XML: {e}")
            return []

        return self._parse_root(root, source_file=file_path)

    def _parse_root(self, root, source_file: str) -> List[SCFile]:
        """Parse the L5X XML root element."""
        results: List[SCFile] = []

        # Determine export scope
        target_type = root.get("TargetType", "Controller")

        controller = root.find(".//Controller")
        if controller is None:
            # Component-level export (single routine, etc.)
            # Try to extract whatever is in the root
            return results

        controller_name = controller.get("Name", "Unknown")

        # 1. DataTypes → UDTs
        datatypes = controller.find("DataTypes")
        if datatypes is not None:
            for dt in datatypes.findall("DataType"):
                sc = _parse_datatype_to_scfile(dt, source_file)
                if sc:
                    results.append(sc)

        # 2. Add-On Instructions → AOIs
        aoi_defs = controller.find("AddOnInstructionDefinitions")
        if aoi_defs is not None:
            for aoi in aoi_defs.findall("AddOnInstructionDefinition"):
                sc = _parse_aoi_to_scfile(aoi, source_file)
                results.append(sc)

        # 3. Programs
        programs = controller.find("Programs")
        if programs is not None:
            for prog in programs.findall("Program"):
                sc = _parse_program_to_scfile(prog, source_file)
                results.append(sc)

        # 4. Controller-scoped tags
        ctrl_tags_elem = controller.find("Tags")
        ctrl_tags = _extract_tags_from_xml(ctrl_tags_elem)
        if ctrl_tags:
            ctrl_sc = SCFile(
                file_path=source_file,
                name=controller_name,
                type='CONTROLLER',
            )
            for tag in ctrl_tags:
                if tag.direction == 'INPUT':
                    ctrl_sc.input_tags.append(tag)
                elif tag.direction == 'OUTPUT':
                    ctrl_sc.output_tags.append(tag)
                else:
                    ctrl_sc.local_tags.append(tag)
            results.append(ctrl_sc)

        return results


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
    programs_count = 0

    # Extract Add-On Instructions
    controller = root.find(".//Controller")
    if controller is not None:
        aoi_defs = controller.find("AddOnInstructionDefinitions")
        if aoi_defs is not None:
            for aoi in aoi_defs.findall("AddOnInstructionDefinition"):
                if export_aoi_from_l5x(aoi, output_dir):
                    aois_count += 1

        # Extract Programs (new)
        programs = controller.find("Programs")
        if programs is not None:
            for prog in programs.findall("Program"):
                if _export_program_from_l5x(prog, output_dir):
                    programs_count += 1

        # Extract controller-scoped tags (new)
        ctrl_name = controller.get("Name", "Controller")
        ctrl_tags_elem = controller.find("Tags")
        if ctrl_tags_elem is not None and len(ctrl_tags_elem.findall("Tag")) > 0:
            _export_controller_tags(ctrl_name, ctrl_tags_elem, output_dir)

    # Extract custom data types
    datatypes_count = export_datatypes_from_l5x(root, output_dir)

    print(f"\n[OK] Export complete: {aois_count} AOIs, {datatypes_count} UDTs, "
          f"{programs_count} Programs")
    print(f"[INFO] Exported to: {output_dir}")

    return True


def _export_program_from_l5x(program_elem, output_dir):
    """Export a Program element from L5X to .sc file."""
    prog_name = program_elem.get("Name", "Unknown")

    desc_elem = program_elem.find("Description")
    description = ""
    if desc_elem is not None and desc_elem.text:
        description = desc_elem.text.strip()

    # Extract program-scoped tags
    tags_elem = program_elem.find("Tags")
    tag_lines = []
    if tags_elem is not None:
        for tag in tags_elem.findall("Tag"):
            name = tag.get("Name", "")
            data_type = tag.get("DataType", "BOOL")
            t_desc_elem = tag.find("Description")
            t_desc = ""
            if t_desc_elem is not None and t_desc_elem.text:
                t_desc = f"  // {t_desc_elem.text.strip()}"
            tag_lines.append(f"\t{name}: {data_type};{t_desc}")

    # Extract routines
    routines = extract_routines(program_elem)

    filename = os.path.join(output_dir, f"{prog_name}.prog.sc")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"(* POU: {prog_name} *)\n")
        f.write(f"(* Type: Program *)\n")
        if description:
            f.write(f"(* Description: {description} *)\n")
        f.write("\n")

        if tag_lines:
            f.write("(* PROGRAM TAGS *)\n")
            f.write("VAR\n")
            f.write("\n".join(tag_lines))
            f.write("\nEND_VAR\n\n")

        if routines:
            f.write("(* IMPLEMENTATION *)\n")
            f.write(routines)
            f.write("\n")

    print(f"[OK] Exported Program: {prog_name}")
    return True


def _export_controller_tags(ctrl_name, tags_elem, output_dir):
    """Export controller-scoped tags to a .sc file."""
    tag_lines = []
    for tag in tags_elem.findall("Tag"):
        name = tag.get("Name", "")
        data_type = tag.get("DataType", "BOOL")
        dimensions = tag.get("Dimensions", "0")

        desc_elem = tag.find("Description")
        desc = ""
        if desc_elem is not None and desc_elem.text:
            desc = f"  // {desc_elem.text.strip()}"

        if dimensions and dimensions != "0":
            tag_lines.append(f"\t{name}: ARRAY[0..{int(dimensions)-1}] OF {data_type};{desc}")
        else:
            tag_lines.append(f"\t{name}: {data_type};{desc}")

    if not tag_lines:
        return

    filename = os.path.join(output_dir, f"{ctrl_name}.ctrl.sc")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"(* POU: {ctrl_name} *)\n")
        f.write(f"(* Type: Controller *)\n\n")
        f.write("(* CONTROLLER-SCOPED TAGS *)\n")
        f.write("VAR\n")
        f.write("\n".join(tag_lines))
        f.write("\nEND_VAR\n")

    print(f"[OK] Exported Controller Tags: {ctrl_name} ({len(tag_lines)} tags)")


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
