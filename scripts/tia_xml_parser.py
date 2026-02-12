#!/usr/bin/env python3
"""
Parser for Siemens TIA Portal XML exports (Openness format).

Parses the XML files exported from TIA Portal via Openness/PLCOpenXML,
which contain Organization Blocks (OB), Function Blocks (FB), Functions (FC),
and Data Blocks (DB) with Ladder Diagram (LAD), FBD, or SCL networks.

Converts the Parts/Wires graph representation of each network into
human-readable pseudo-structured-text so that the downstream ontology
pipeline (ontology_analyzer.py -> Neo4j) works unchanged.

Output: List[SCFile]  (same dataclasses as sc_parser / siemens_parser)

Supported XML constructs:
    SW.Blocks.OB   – Organization Blocks (ProgramCycle, etc.)
    SW.Blocks.FB   – Function Blocks
    SW.Blocks.FC   – Functions
    SW.Blocks.DB   – Data Blocks (variable extraction only)

Network elements handled:
    Access (GlobalVariable, TypedConstant, LiteralConstant)
    Contact / NContact (NO / NC contacts, Negated)
    Coil / SCoil / RCoil  (output, set, reset)
    O  (OR gate with cardinality)
    TP / TON / TOF  (pulse, on-delay, off-delay timers)
    CTU / CTD / CTUD  (counters)
    Eq / Ne / Lt / Gt / Le / Ge  (comparison)
    PBox / NBox  (edge detection)
    Sr / Rs  (set-reset / reset-set flip-flops)
    Move  (data move)

Usage:
    python tia_xml_parser.py <file.xml>
    python tia_xml_parser.py <directory>   # parse all .xml files recursively
"""

import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from sc_parser import SCFile, Tag, LogicRung


# ---------------------------------------------------------------------------
# XML Namespace helpers
# ---------------------------------------------------------------------------

NS_INTERFACE = "http://www.siemens.com/automation/Openness/SW/Interface/v5"
NS_FLGNET = "http://www.siemens.com/automation/Openness/SW/NetworkSource/FlgNet/v5"

# Block element tag prefixes we recognise
BLOCK_TAGS = {
    "SW.Blocks.OB": "OB",
    "SW.Blocks.FB": "FB",
    "SW.Blocks.FC": "FC",
    "SW.Blocks.DB": "DB",
}


# ---------------------------------------------------------------------------
# Ladder logic instruction descriptors (for readable output)
# ---------------------------------------------------------------------------

# Map Part/@Name to a human-readable description format
INSTRUCTION_DISPLAY = {
    "Contact": "XIC",       # Examine If Closed  (normally open)
    "Coil": "OTE",          # Output Energize
    "SCoil": "OTL",         # Output Latch (Set)
    "RCoil": "OTU",         # Output Unlatch (Reset)
    "TP": "TP",             # Pulse Timer
    "TON": "TON",           # On-Delay Timer
    "TOF": "TOF",           # Off-Delay Timer
    "CTU": "CTU",           # Count Up
    "CTD": "CTD",           # Count Down
    "CTUD": "CTUD",         # Count Up/Down
    "Eq": "EQ",             # Equal
    "Ne": "NE",             # Not Equal
    "Lt": "LT",             # Less Than
    "Gt": "GT",             # Greater Than
    "Le": "LE",             # Less or Equal
    "Ge": "GE",             # Greater or Equal
    "O": "OR",              # OR gate
    "PBox": "P_TRIG",       # Positive edge
    "NBox": "N_TRIG",       # Negative edge
    "Sr": "SR",             # Set-Reset flip-flop
    "Rs": "RS",             # Reset-Set flip-flop
    "Move": "MOVE",         # Data move
}


# ---------------------------------------------------------------------------
# TIA XML Parser
# ---------------------------------------------------------------------------


class TiaXmlParser:
    """
    Parse Siemens TIA Portal XML exports into SCFile objects.

    Each XML file typically contains one block (OB/FB/FC/DB).
    Each block may contain multiple networks (CompileUnits).
    """

    def parse_file(self, file_path: str) -> List[SCFile]:
        """Parse a TIA Portal XML file and return SCFile objects."""
        try:
            tree = ET.parse(file_path)
        except ET.ParseError as e:
            print(f"[WARNING] XML parse error in {file_path}: {e}")
            return []

        root = tree.getroot()
        results: List[SCFile] = []

        # Find all block elements
        for child in root:
            block_type = BLOCK_TAGS.get(child.tag)
            if block_type:
                sc = self._parse_block(child, block_type, file_path)
                if sc:
                    results.append(sc)

        return results

    def parse_directory(self, directory: str) -> List[SCFile]:
        """Parse all .xml files in a directory tree, returning SCFile objects."""
        dir_path = Path(directory)
        xml_files = sorted(dir_path.rglob("*.xml"))

        if not xml_files:
            print(f"[WARNING] No .xml files found in {directory}")
            return []

        results: List[SCFile] = []
        for xml_file in xml_files:
            blocks = self.parse_file(str(xml_file))
            results.extend(blocks)

        return results

    # ------------------------------------------------------------------
    # Block parsing
    # ------------------------------------------------------------------

    def _parse_block(
        self, block_elem: ET.Element, block_type: str, file_path: str
    ) -> Optional[SCFile]:
        """Parse a single block element (OB/FB/FC/DB)."""

        attr_list = block_elem.find("AttributeList")
        if attr_list is None:
            return None

        # --- Metadata ---
        name = self._get_text(attr_list, "Name") or Path(file_path).stem
        number = self._get_text(attr_list, "Number")
        prog_lang = self._get_text(attr_list, "ProgrammingLanguage") or "LAD"
        secondary_type = self._get_text(attr_list, "SecondaryType") or ""
        header_version = self._get_text(attr_list, "HeaderVersion")

        # Derive a meaningful project name from the directory structure
        # e.g. .../TIA_Export/Conveyor and Puncher/PLC_1/Main.xml
        project_name = self._derive_project_name(file_path)

        # Use project name as SCFile name if the block is just "Main" or "OB1"
        if name in ("Main", "OB1") and project_name:
            display_name = project_name
        else:
            display_name = name

        sc = SCFile(
            file_path=file_path,
            name=display_name,
            type=block_type,
            revision=header_version,
            vendor="Siemens",
            description=(
                f"TIA Portal {block_type} (#{number}) — "
                f"{prog_lang} — {secondary_type}"
            ).strip(" —"),
        )

        # --- Interface (variables) ---
        interface_elem = attr_list.find("Interface")
        if interface_elem is not None:
            self._parse_interface(interface_elem, sc)

        # --- Networks (CompileUnits) ---
        obj_list = block_elem.find("ObjectList")
        if obj_list is not None:
            networks = self._parse_networks(obj_list, prog_lang)
            sc.routines = networks

            # Build raw implementation text from all networks
            impl_parts = []
            for net in networks:
                impl_parts.append(f"// --- Network {net['name']} ---")
                if net.get("comment"):
                    impl_parts.append(f"// {net['comment']}")
                if net.get("title"):
                    impl_parts.append(f"// Title: {net['title']}")
                impl_parts.append(net["raw_content"])
                impl_parts.append("")
            if impl_parts:
                sc.raw_implementation = "\n".join(impl_parts)

            # Collect all global variables used across networks as local_tags
            # (since OBs reference global tags, not local ones typically)
            global_vars = self._collect_global_variables(obj_list)
            for var_name, var_info in global_vars.items():
                # Don't add duplicates
                existing = {t.name for t in sc.local_tags}
                if var_name not in existing:
                    sc.local_tags.append(
                        Tag(
                            name=var_name,
                            data_type=var_info.get("data_type", "Bool"),
                            direction=None,
                            description=var_info.get("description"),
                        )
                    )

        return sc

    # ------------------------------------------------------------------
    # Interface (variable) parsing
    # ------------------------------------------------------------------

    def _parse_interface(self, interface_elem: ET.Element, sc: SCFile) -> None:
        """Parse the <Interface><Sections> variable declarations."""
        # The Sections element lives in the interface namespace
        sections = interface_elem.find(f"{{{NS_INTERFACE}}}Sections")
        if sections is None:
            # Try without namespace (some exports)
            sections = interface_elem.find("Sections")
        if sections is None:
            return

        for section in sections.findall(f"{{{NS_INTERFACE}}}Section"):
            section_name = section.get("Name", "")

            for member in section.findall(f"{{{NS_INTERFACE}}}Member"):
                tag = self._parse_member(member, section_name)
                if tag:
                    if section_name == "Input":
                        sc.input_tags.append(tag)
                    elif section_name == "Output":
                        sc.output_tags.append(tag)
                    elif section_name in ("InOut", "In_Out"):
                        sc.inout_tags.append(tag)
                    elif section_name in ("Temp", "Static", "Constant"):
                        sc.local_tags.append(tag)

    def _parse_member(self, member: ET.Element, section_name: str) -> Optional[Tag]:
        """Parse a single <Member> into a Tag."""
        name = member.get("Name")
        data_type = member.get("Datatype", "Unknown")
        if not name:
            return None

        # Check for informative-only members (system params like Initial_Call)
        informative = member.get("Informative", "false").lower() == "true"

        # Extract comment (description)
        description = None
        comment_elem = member.find(f"{{{NS_INTERFACE}}}Comment")
        if comment_elem is None:
            comment_elem = member.find("Comment")
        if comment_elem is not None:
            ml_text = comment_elem.find(f"{{{NS_INTERFACE}}}MultiLanguageText")
            if ml_text is None:
                ml_text = comment_elem.find("MultiLanguageText")
            if ml_text is not None:
                description = ml_text.text

        if informative:
            description = (
                f"[System] {description}" if description else "[System parameter]"
            )

        # Determine direction from section name
        direction_map = {
            "Input": "INPUT",
            "Output": "OUTPUT",
            "InOut": "IN_OUT",
            "In_Out": "IN_OUT",
        }
        direction = direction_map.get(section_name)

        return Tag(
            name=name,
            data_type=data_type,
            direction=direction,
            description=description,
        )

    # ------------------------------------------------------------------
    # Network (CompileUnit) parsing
    # ------------------------------------------------------------------

    def _parse_networks(
        self, obj_list: ET.Element, prog_lang: str
    ) -> List[Dict]:
        """Parse all CompileUnit (network) elements from the ObjectList."""
        networks = []
        network_num = 0

        for cu in obj_list.findall("SW.Blocks.CompileUnit"):
            network_num += 1
            cu_attr = cu.find("AttributeList")
            if cu_attr is None:
                continue

            # Extract network title and comment
            cu_obj = cu.find("ObjectList")
            title = ""
            comment = ""
            if cu_obj is not None:
                title = self._get_multilingual_text(cu_obj, "Title")
                comment = self._get_multilingual_text(cu_obj, "Comment")

            # Parse the FlgNet (ladder network graph)
            network_source = cu_attr.find("NetworkSource")
            if network_source is None or len(network_source) == 0:
                # Empty network
                networks.append({
                    "name": f"Network_{network_num}",
                    "type": prog_lang,
                    "title": title,
                    "comment": comment,
                    "rungs": [
                        LogicRung(
                            number=network_num,
                            comment=comment or title or None,
                            logic="(empty network)",
                        )
                    ] if comment or title else [],
                    "raw_content": f"// Network {network_num}: (empty)"
                    + (f"\n// {comment}" if comment else ""),
                })
                continue

            flgnet = network_source.find(f"{{{NS_FLGNET}}}FlgNet")
            if flgnet is None:
                # Try without namespace
                flgnet = network_source.find("FlgNet")
            if flgnet is None:
                continue

            # Parse the network graph into readable logic
            logic_text = self._parse_flgnet(flgnet)

            rung = LogicRung(
                number=network_num,
                comment=comment or title or None,
                logic=logic_text,
            )

            networks.append({
                "name": f"Network_{network_num}",
                "type": prog_lang,
                "title": title,
                "comment": comment,
                "rungs": [rung],
                "raw_content": logic_text,
            })

        return networks

    # ------------------------------------------------------------------
    # FlgNet (Parts/Wires graph) -> readable logic
    # ------------------------------------------------------------------

    def _parse_flgnet(self, flgnet: ET.Element) -> str:
        """
        Convert a FlgNet (Parts + Wires) graph into human-readable
        pseudo-structured-text / ladder logic description.

        The FlgNet contains:
          <Parts>  — Access elements (variables/constants) and Part elements (instructions)
          <Wires>  — Connections between parts via named ports
        """
        parts_elem = flgnet.find(f"{{{NS_FLGNET}}}Parts")
        wires_elem = flgnet.find(f"{{{NS_FLGNET}}}Wires")

        if parts_elem is None or wires_elem is None:
            return "(no logic)"

        # 1. Build lookup tables
        accesses: Dict[str, Dict] = {}   # UId -> {scope, name, value, data_type}
        parts: Dict[str, Dict] = {}      # UId -> {name, negated, instance, template_values}

        for elem in parts_elem:
            local_tag = elem.tag.replace(f"{{{NS_FLGNET}}}", "")
            uid = elem.get("UId")
            if not uid:
                continue

            if local_tag == "Access":
                accesses[uid] = self._parse_access(elem)
            elif local_tag == "Part":
                parts[uid] = self._parse_part(elem)

        # 2. Build wire graph: target_uid.port -> source_uid.port or Powerrail or Access
        # We map: for each Part port, what feeds it
        wire_graph: Dict[str, List[Dict]] = {}  # "uid:port" -> [{type, uid, port}]
        output_targets: Dict[str, List[Dict]] = {}  # "uid:port" -> [{type, uid, port}]

        for wire in wires_elem:
            connections = []
            for conn in wire:
                conn_tag = conn.tag.replace(f"{{{NS_FLGNET}}}", "")
                if conn_tag == "Powerrail":
                    connections.append({"type": "powerrail"})
                elif conn_tag == "NameCon":
                    connections.append({
                        "type": "namecon",
                        "uid": conn.get("UId"),
                        "port": conn.get("Name"),
                    })
                elif conn_tag == "IdentCon":
                    connections.append({
                        "type": "identcon",
                        "uid": conn.get("UId"),
                    })
                elif conn_tag == "OpenCon":
                    connections.append({"type": "opencon", "uid": conn.get("UId")})

            # First element is source, rest are destinations
            if len(connections) >= 2:
                source = connections[0]
                for dest in connections[1:]:
                    if dest.get("uid") and dest.get("port"):
                        key = f"{dest['uid']}:{dest['port']}"
                        wire_graph.setdefault(key, []).append(source)

                    # Track outputs for tracing
                    if source.get("uid") and source.get("port"):
                        src_key = f"{source['uid']}:{source['port']}"
                        output_targets.setdefault(src_key, []).append(dest)

        # 3. Reconstruct logic by tracing from outputs (coils) backward
        logic_lines = []

        # Find all output elements (coils, timers, counters, etc.)
        output_parts = []
        for uid, part in parts.items():
            pname = part["name"]
            if pname in ("Coil", "SCoil", "RCoil"):
                output_parts.append((uid, part, "coil"))
            elif pname in ("TP", "TON", "TOF"):
                output_parts.append((uid, part, "timer"))
            elif pname in ("CTU", "CTD", "CTUD"):
                output_parts.append((uid, part, "counter"))
            elif pname in ("Sr", "Rs"):
                output_parts.append((uid, part, "flipflop"))
            elif pname == "Move":
                output_parts.append((uid, part, "move"))

        # Also handle comparison blocks that feed into coils (they appear inline)
        # They're handled during trace-back

        if not output_parts:
            # Might be a pure comparison or pass-through network
            # Just list what's there
            for uid, part in parts.items():
                pname = part["name"]
                display = INSTRUCTION_DISPLAY.get(pname, pname)
                logic_lines.append(f"{display}(...)")
            if not logic_lines:
                return "(no outputs)"
            return "\n".join(logic_lines)

        # Process each output
        for uid, part, category in output_parts:
            line = self._trace_output(
                uid, part, category, parts, accesses, wire_graph
            )
            logic_lines.append(line)

        return "\n".join(logic_lines)

    def _trace_output(
        self,
        uid: str,
        part: Dict,
        category: str,
        parts: Dict[str, Dict],
        accesses: Dict[str, Dict],
        wire_graph: Dict[str, List[Dict]],
    ) -> str:
        """Trace backward from an output element to reconstruct the logic."""

        pname = part["name"]

        if category == "coil":
            # Get the operand (what variable is being written)
            operand = self._resolve_input(uid, "operand", parts, accesses, wire_graph)
            # Get the condition chain feeding "in"
            condition = self._trace_condition(uid, "in", parts, accesses, wire_graph)

            coil_type = {
                "Coil": "OTE",
                "SCoil": "SET",
                "RCoil": "RST",
            }.get(pname, "OTE")

            return f"{coil_type}({operand}) := {condition};"

        elif category == "timer":
            timer_type = INSTRUCTION_DISPLAY.get(pname, pname)
            instance = part.get("instance", "?")
            # Get IN condition
            condition = self._trace_condition(uid, "IN", parts, accesses, wire_graph)
            # Get PT value
            pt_val = self._resolve_input(uid, "PT", parts, accesses, wire_graph)
            # Get Q output target
            q_target = self._find_coil_target(uid, "Q", parts, accesses, wire_graph)

            line = f"{timer_type}({instance}, IN := {condition}, PT := {pt_val})"
            if q_target:
                line += f" -> {q_target}"
            return line + ";"

        elif category == "counter":
            ctr_type = INSTRUCTION_DISPLAY.get(pname, pname)
            instance = part.get("instance", "?")
            parts_list = []
            # CU/CD condition
            for port in ("CU", "CD"):
                cond = self._trace_condition(uid, port, parts, accesses, wire_graph)
                if cond and cond != "?":
                    parts_list.append(f"{port} := {cond}")
            # R (reset)
            r_cond = self._trace_condition(uid, "R", parts, accesses, wire_graph)
            if r_cond and r_cond != "?":
                parts_list.append(f"R := {r_cond}")
            # PV (preset value)
            pv = self._resolve_input(uid, "PV", parts, accesses, wire_graph)
            if pv and pv != "?":
                parts_list.append(f"PV := {pv}")
            q_target = self._find_coil_target(uid, "Q", parts, accesses, wire_graph)
            line = f"{ctr_type}({instance}, {', '.join(parts_list)})"
            if q_target:
                line += f" -> {q_target}"
            return line + ";"

        elif category == "flipflop":
            ff_type = INSTRUCTION_DISPLAY.get(pname, pname)
            instance = part.get("instance", "?")
            s_cond = self._trace_condition(uid, "s", parts, accesses, wire_graph)
            r_cond = self._trace_condition(uid, "r1", parts, accesses, wire_graph)
            q_target = self._find_coil_target(uid, "Q", parts, accesses, wire_graph)
            line = f"{ff_type}({instance}, S := {s_cond}, R := {r_cond})"
            if q_target:
                line += f" -> {q_target}"
            return line + ";"

        elif category == "move":
            in_val = self._resolve_input(uid, "in", parts, accesses, wire_graph)
            out_val = self._resolve_input(uid, "out", parts, accesses, wire_graph)
            en_cond = self._trace_condition(uid, "en", parts, accesses, wire_graph)
            return f"MOVE({in_val} -> {out_val}, EN := {en_cond});"

        return f"{pname}(...);"

    def _trace_condition(
        self,
        uid: str,
        port: str,
        parts: Dict[str, Dict],
        accesses: Dict[str, Dict],
        wire_graph: Dict[str, List[Dict]],
        depth: int = 0,
    ) -> str:
        """
        Recursively trace back from a part's input port to build
        a condition expression string.
        """
        if depth > 20:
            return "..."  # Guard against cycles

        key = f"{uid}:{port}"
        sources = wire_graph.get(key, [])

        if not sources:
            return "?"

        terms = []
        for src in sources:
            if src["type"] == "powerrail":
                terms.append("POWER")
            elif src["type"] == "identcon":
                # Direct connection to an Access (variable/constant)
                acc = accesses.get(src["uid"], {})
                terms.append(acc.get("display", f"?[{src['uid']}]"))
            elif src["type"] == "namecon":
                # Connection from another Part's output port
                src_uid = src["uid"]
                src_port = src["port"]
                src_part = parts.get(src_uid)

                if src_part is None:
                    terms.append(f"?[{src_uid}:{src_port}]")
                    continue

                src_name = src_part["name"]

                if src_name == "Contact":
                    # Trace the contact's input condition
                    inner = self._trace_condition(
                        src_uid, "in", parts, accesses, wire_graph, depth + 1
                    )
                    operand = self._resolve_input(
                        src_uid, "operand", parts, accesses, wire_graph
                    )
                    is_negated = src_part.get("negated", False)

                    if inner == "POWER":
                        if is_negated:
                            terms.append(f"NOT {operand}")
                        else:
                            terms.append(operand)
                    else:
                        if is_negated:
                            terms.append(f"{inner} AND NOT {operand}")
                        else:
                            terms.append(f"{inner} AND {operand}")

                elif src_name == "O":
                    # OR gate — collect all inputs
                    or_terms = []
                    card = src_part.get("cardinality", 2)
                    for i in range(1, card + 1):
                        in_port = f"in{i}"
                        branch = self._trace_condition(
                            src_uid, in_port, parts, accesses, wire_graph, depth + 1
                        )
                        or_terms.append(branch)
                    if len(or_terms) == 1:
                        terms.append(or_terms[0])
                    else:
                        terms.append(f"({' OR '.join(or_terms)})")

                elif src_name in ("PBox", "NBox"):
                    inner = self._trace_condition(
                        src_uid, "in", parts, accesses, wire_graph, depth + 1
                    )
                    bit = self._resolve_input(
                        src_uid, "bit", parts, accesses, wire_graph
                    )
                    edge_type = "P_TRIG" if src_name == "PBox" else "N_TRIG"
                    terms.append(f"{edge_type}({inner}, {bit})")

                elif src_name in ("Eq", "Ne", "Lt", "Gt", "Le", "Ge"):
                    # Comparison — get in1 and in2
                    in1 = self._resolve_input(
                        src_uid, "in1", parts, accesses, wire_graph
                    )
                    in2 = self._resolve_input(
                        src_uid, "in2", parts, accesses, wire_graph
                    )
                    op_map = {
                        "Eq": "==", "Ne": "!=", "Lt": "<",
                        "Gt": ">", "Le": "<=", "Ge": ">=",
                    }
                    op = op_map.get(src_name, "?")
                    # Also trace the pre-condition if present
                    pre = self._trace_condition(
                        src_uid, "pre", parts, accesses, wire_graph, depth + 1
                    )
                    cmp_expr = f"({in1} {op} {in2})"
                    if pre and pre != "?" and pre != "POWER":
                        terms.append(f"{pre} AND {cmp_expr}")
                    else:
                        terms.append(cmp_expr)

                elif src_name in ("TP", "TON", "TOF"):
                    # Timer output port (Q or ET)
                    instance = src_part.get("instance", "?")
                    terms.append(f"{src_name}({instance}).{src_port}")

                elif src_name in ("CTU", "CTD", "CTUD"):
                    instance = src_part.get("instance", "?")
                    terms.append(f"{src_name}({instance}).{src_port}")

                elif src_name in ("Sr", "Rs"):
                    instance = src_part.get("instance", "?")
                    terms.append(f"{src_name}({instance}).{src_port}")

                else:
                    # Generic: show as function output
                    terms.append(f"{src_name}({src_uid}).{src_port}")

            elif src["type"] == "opencon":
                terms.append("(open)")

        if len(terms) == 0:
            return "?"
        elif len(terms) == 1:
            return terms[0]
        else:
            return " AND ".join(terms)

    def _resolve_input(
        self,
        uid: str,
        port: str,
        parts: Dict[str, Dict],
        accesses: Dict[str, Dict],
        wire_graph: Dict[str, List[Dict]],
    ) -> str:
        """Resolve a single input port to a variable name or constant value."""
        key = f"{uid}:{port}"
        sources = wire_graph.get(key, [])

        for src in sources:
            if src["type"] == "identcon":
                acc = accesses.get(src["uid"], {})
                return acc.get("display", f"?[{src['uid']}]")
            elif src["type"] == "namecon":
                # It's coming from another part's output
                src_part = parts.get(src["uid"])
                if src_part:
                    return f"{src_part['name']}({src['uid']}).{src['port']}"
                return f"?[{src['uid']}:{src['port']}]"
            elif src["type"] == "powerrail":
                return "POWER"
        return "?"

    def _find_coil_target(
        self,
        uid: str,
        port: str,
        parts: Dict[str, Dict],
        accesses: Dict[str, Dict],
        wire_graph: Dict[str, List[Dict]],
    ) -> Optional[str]:
        """
        From a timer/counter Q output, find what coil it feeds
        and return the coil's operand name.
        """
        # We need to search the wire_graph for any target that references uid:port as source
        # The wire_graph maps target -> source, so we need to search values
        for target_key, source_list in wire_graph.items():
            for src in source_list:
                if (
                    src["type"] == "namecon"
                    and src.get("uid") == uid
                    and src.get("port") == port
                ):
                    # target_key is "dest_uid:dest_port"
                    dest_uid, dest_port = target_key.split(":", 1)
                    dest_part = parts.get(dest_uid)
                    if dest_part and dest_part["name"] in (
                        "Coil", "SCoil", "RCoil"
                    ):
                        operand = self._resolve_input(
                            dest_uid, "operand", parts, accesses, wire_graph
                        )
                        coil_type = {
                            "Coil": "OTE",
                            "SCoil": "SET",
                            "RCoil": "RST",
                        }.get(dest_part["name"], "OTE")
                        return f"{coil_type}({operand})"
        return None

    # ------------------------------------------------------------------
    # Part/Access element parsers
    # ------------------------------------------------------------------

    def _parse_access(self, elem: ET.Element) -> Dict:
        """Parse an <Access> element into a dict."""
        scope = elem.get("Scope", "")
        uid = elem.get("UId", "")
        result = {"scope": scope, "uid": uid}

        if scope == "GlobalVariable":
            # <Symbol><Component Name="..." /></Symbol>
            symbol = elem.find(f"{{{NS_FLGNET}}}Symbol")
            if symbol is None:
                symbol = elem.find("Symbol")
            if symbol is not None:
                components = []
                for comp in symbol.findall(f"{{{NS_FLGNET}}}Component"):
                    components.append(comp.get("Name", ""))
                if not components:
                    for comp in symbol.findall("Component"):
                        components.append(comp.get("Name", ""))
                var_name = ".".join(components)
                result["name"] = var_name
                result["display"] = f'"{var_name}"'
                result["data_type"] = "Bool"  # Default; actual type not in XML
            else:
                result["name"] = f"?global_{uid}"
                result["display"] = f"?global_{uid}"

        elif scope in ("TypedConstant", "LiteralConstant"):
            const_elem = elem.find(f"{{{NS_FLGNET}}}Constant")
            if const_elem is None:
                const_elem = elem.find("Constant")
            if const_elem is not None:
                val_elem = const_elem.find(f"{{{NS_FLGNET}}}ConstantValue")
                if val_elem is None:
                    val_elem = const_elem.find("ConstantValue")
                type_elem = const_elem.find(f"{{{NS_FLGNET}}}ConstantType")
                if type_elem is None:
                    type_elem = const_elem.find("ConstantType")

                value = val_elem.text if val_elem is not None else "?"
                ctype = type_elem.text if type_elem is not None else ""
                result["value"] = value
                result["data_type"] = ctype
                result["display"] = value
            else:
                result["display"] = f"?const_{uid}"

        else:
            result["display"] = f"?[{scope}:{uid}]"

        return result

    def _parse_part(self, elem: ET.Element) -> Dict:
        """Parse a <Part> element into a dict."""
        name = elem.get("Name", "")
        uid = elem.get("UId", "")
        version = elem.get("Version")

        result = {
            "name": name,
            "uid": uid,
            "version": version,
            "negated": False,
            "instance": None,
            "cardinality": 2,
            "template_values": {},
        }

        for child in elem:
            child_tag = child.tag.replace(f"{{{NS_FLGNET}}}", "")

            if child_tag == "Negated":
                result["negated"] = True

            elif child_tag == "Instance":
                # Timer/counter instance DB
                components = []
                for comp in child.findall(f"{{{NS_FLGNET}}}Component"):
                    components.append(comp.get("Name", ""))
                if not components:
                    for comp in child.findall("Component"):
                        components.append(comp.get("Name", ""))
                result["instance"] = ".".join(components) if components else None

            elif child_tag == "TemplateValue":
                tv_name = child.get("Name", "")
                tv_value = child.text or ""
                result["template_values"][tv_name] = tv_value
                if tv_name == "Card":
                    try:
                        result["cardinality"] = int(tv_value)
                    except ValueError:
                        pass

        return result

    # ------------------------------------------------------------------
    # Global variable collector
    # ------------------------------------------------------------------

    def _collect_global_variables(self, obj_list: ET.Element) -> Dict[str, Dict]:
        """
        Walk all networks and collect unique global variable names.
        Returns dict: var_name -> {data_type, description}
        """
        variables: Dict[str, Dict] = {}

        for cu in obj_list.findall("SW.Blocks.CompileUnit"):
            cu_attr = cu.find("AttributeList")
            if cu_attr is None:
                continue

            ns_elem = cu_attr.find("NetworkSource")
            if ns_elem is None or len(ns_elem) == 0:
                continue

            flgnet = ns_elem.find(f"{{{NS_FLGNET}}}FlgNet")
            if flgnet is None:
                flgnet = ns_elem.find("FlgNet")
            if flgnet is None:
                continue

            parts_elem = flgnet.find(f"{{{NS_FLGNET}}}Parts")
            if parts_elem is None:
                continue

            for access in parts_elem.findall(f"{{{NS_FLGNET}}}Access"):
                if access.get("Scope") == "GlobalVariable":
                    parsed = self._parse_access(access)
                    name = parsed.get("name")
                    if name and name not in variables:
                        variables[name] = {
                            "data_type": parsed.get("data_type", "Bool"),
                            "description": f"Global PLC tag referenced in ladder logic",
                        }

        return variables

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _get_text(self, parent: ET.Element, child_tag: str) -> Optional[str]:
        """Get text content of a direct child element."""
        child = parent.find(child_tag)
        if child is not None and child.text:
            return child.text.strip()
        return None

    def _get_multilingual_text(
        self, obj_list: ET.Element, composition_name: str
    ) -> str:
        """Extract text from a MultilingualText element by CompositionName."""
        for ml in obj_list.findall("MultilingualText"):
            if ml.get("CompositionName") == composition_name:
                inner_obj = ml.find("ObjectList")
                if inner_obj is not None:
                    for item in inner_obj.findall("MultilingualTextItem"):
                        attr = item.find("AttributeList")
                        if attr is not None:
                            text_elem = attr.find("Text")
                            if text_elem is not None and text_elem.text:
                                return text_elem.text.strip().strip('"')
        return ""

    def _derive_project_name(self, file_path: str) -> Optional[str]:
        """
        Derive a project name from the file path.

        Expected structure:
            .../TIA_Export/<ProjectName>/<PLC_X>/Main.xml

        Returns the project folder name, or None if pattern doesn't match.
        """
        p = Path(file_path)
        parts = p.parts

        # Look for PLC_N pattern in path
        for i, part in enumerate(parts):
            if part.startswith("PLC_") and i > 0:
                return parts[i - 1]

        # Fallback: use grandparent directory name
        if len(parts) >= 3:
            return parts[-3]

        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_scfile_summary(sc: SCFile) -> None:
    """Print a human-readable summary of a parsed SCFile."""
    print(f"\n{'=' * 70}")
    print(f"  {sc.name}  ({sc.type})")
    print(f"  {sc.description or ''}")
    print(f"  Source: {sc.file_path}")
    print(f"{'=' * 70}")

    if sc.input_tags:
        print(f"\n  --- Input Tags ({len(sc.input_tags)}) ---")
        for t in sc.input_tags:
            desc = f"  // {t.description}" if t.description else ""
            print(f"    {t.name}: {t.data_type}{desc}")

    if sc.output_tags:
        print(f"\n  --- Output Tags ({len(sc.output_tags)}) ---")
        for t in sc.output_tags:
            desc = f"  // {t.description}" if t.description else ""
            print(f"    {t.name}: {t.data_type}{desc}")

    if sc.local_tags:
        print(f"\n  --- Global Variables / Tags ({len(sc.local_tags)}) ---")
        for t in sc.local_tags[:20]:
            desc = f"  // {t.description}" if t.description else ""
            print(f"    {t.name}: {t.data_type}{desc}")
        if len(sc.local_tags) > 20:
            print(f"    ... and {len(sc.local_tags) - 20} more")

    if sc.routines:
        print(f"\n  --- Networks ({len(sc.routines)}) ---")
        for r in sc.routines:
            comment = f": {r['comment']}" if r.get("comment") else ""
            title = f" [{r['title']}]" if r.get("title") else ""
            print(f"    {r['name']}{title}{comment}")
            # Show the logic
            content = r["raw_content"]
            for line in content.split("\n")[:8]:
                print(f"      {line}")
            lines = content.split("\n")
            if len(lines) > 8:
                print(f"      ... ({len(lines)} lines total)")
            print()


def main():
    """CLI: parse Siemens TIA Portal XML files and print summaries."""
    if len(sys.argv) < 2:
        print("Usage: python tia_xml_parser.py <file.xml | directory>")
        sys.exit(1)

    target = Path(sys.argv[1])
    parser = TiaXmlParser()

    total_blocks = 0

    if target.is_dir():
        xml_files = sorted(target.rglob("*.xml"))
        if not xml_files:
            print(f"[WARNING] No .xml files found in {target}")
            sys.exit(1)
        print(f"[INFO] Found {len(xml_files)} XML file(s) in {target}\n")
        for xml_file in xml_files:
            blocks = parser.parse_file(str(xml_file))
            if not blocks:
                print(f"  (no parseable blocks in {xml_file.name})")
            for sc in blocks:
                _print_scfile_summary(sc)
                total_blocks += 1
        print(f"\n[INFO] Total: {total_blocks} blocks from {len(xml_files)} files")

    elif target.is_file():
        blocks = parser.parse_file(str(target))
        if not blocks:
            print("  (no parseable blocks found)")
        for sc in blocks:
            _print_scfile_summary(sc)
            total_blocks += 1

    else:
        print(f"[ERROR] Path not found: {target}")
        sys.exit(1)


if __name__ == "__main__":
    main()
