#!/usr/bin/env python3
"""
Parser for full Siemens TIA Portal project exports (Openness format).

Walks the entire directory structure of a TIA Portal Openness export,
parsing PLC devices (blocks, tag tables, types) and HMI devices
(connections, tags, alarms, scripts, screens, text lists).

This parser goes beyond individual PLC blocks (handled by tia_xml_parser.py)
to capture the full project topology:

    TiaProject
    ├── PLCDevice (PLC_PLC_1, ...)
    │   ├── Blocks/       → OB, FB, FC, DB  (XML, delegated to TiaXmlParser)
    │   ├── TagTables/    → Global PLC tags  (XML)
    │   ├── Types/        → UDTs / Structs   (XML)
    │   └── TechnologyObjects/
    └── HMIDevice (HMI_HMI_RT_1, ...)
        ├── Alarms/       → Analog, Discrete alarms + AlarmClasses (JSON)
        ├── Connections/  → HMI→PLC connections (JSON)
        ├── Screens/      → HMI screen definitions
        ├── Scripts/      → JavaScript HMI scripts (JS + YAML)
        ├── Tags/         → HMI tag tables (JSON)
        └── TextLists/    → Display enumerations (JSON)

Usage:
    python siemens_project_parser.py <project_directory>

    # Parse and ingest into Neo4j:
    python ontology_analyzer.py <project_directory> --tia-project -v
"""

import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sc_parser import SCFile, Tag

# Re-use TIA XML block parser for PLC blocks
from tia_xml_parser import TiaXmlParser, NS_INTERFACE


# ---------------------------------------------------------------------------
# Data classes for the project model
# ---------------------------------------------------------------------------


@dataclass
class HMIConnection:
    """HMI-to-PLC communication link."""
    name: str
    partner: str                    # e.g. "PLC_1"
    station: str                    # e.g. "S71500/ET200MP station_1"
    communication_driver: str       # e.g. "SIMATIC S7 1200/1500"
    node: str                       # e.g. "CPU 1516-3 PN/DP, PROFINET interface"
    address: str                    # raw InitialAddress field
    disabled_at_startup: bool = False
    source_file: str = ""


@dataclass
class HMIAlarmClass:
    """Alarm classification definition."""
    name: str
    alarm_id: str = ""
    priority: str = "0"
    state_machine: str = ""
    is_system: bool = False
    source_file: str = ""


@dataclass
class HMIAlarm:
    """Alarm definition (analog or discrete)."""
    name: str
    alarm_type: str                 # "Analog" or "Discrete"
    alarm_class: str = ""
    origin: str = ""
    priority: str = "0"
    alarm_id: str = ""
    area: str = ""
    raised_state_tag: str = ""
    # Analog-specific
    condition: str = ""             # "LowerLimit", "UpperLimit"
    condition_value: str = ""
    # Discrete-specific
    trigger_bit_address: str = ""
    trigger_mode: str = ""
    source_file: str = ""


@dataclass
class HMITagTable:
    """HMI tag table metadata."""
    name: str
    folder: str = ""
    source_file: str = ""


@dataclass
class HMIScript:
    """HMI JavaScript script module."""
    name: str
    script_file: str = ""
    script_text: str = ""
    functions: List[str] = field(default_factory=list)
    source_file: str = ""


@dataclass
class HMITextList:
    """HMI text list (enumeration display)."""
    name: str
    source_file: str = ""


@dataclass
class HMIScreen:
    """HMI screen definition."""
    name: str
    folder: str = ""
    source_file: str = ""


@dataclass
class PLCTag:
    """PLC global tag from a tag table."""
    name: str
    data_type: str = "Bool"
    logical_address: str = ""       # e.g. "%MW333"
    comment: str = ""
    external_accessible: bool = True
    external_visible: bool = True
    external_writable: bool = True


@dataclass
class PLCTagTable:
    """PLC tag table with its tags."""
    name: str
    tags: List[PLCTag] = field(default_factory=list)
    source_file: str = ""


@dataclass
class PLCType:
    """PLC user-defined type (UDT / Struct)."""
    name: str
    members: List[Tag] = field(default_factory=list)
    source_file: str = ""
    is_failsafe: bool = False


@dataclass
class PLCDevice:
    """A PLC device within the project."""
    name: str                       # e.g. "PLC_1"
    dir_name: str                   # e.g. "PLC_PLC_1"
    blocks: List[SCFile] = field(default_factory=list)
    tag_tables: List[PLCTagTable] = field(default_factory=list)
    types: List[PLCType] = field(default_factory=list)
    block_metadata: List[Dict] = field(default_factory=list)


@dataclass
class HMIDevice:
    """An HMI device within the project."""
    name: str                       # e.g. "HMI_RT_1"
    dir_name: str                   # e.g. "HMI_HMI_RT_1"
    connections: List[HMIConnection] = field(default_factory=list)
    tag_tables: List[HMITagTable] = field(default_factory=list)
    alarms: List[HMIAlarm] = field(default_factory=list)
    alarm_classes: List[HMIAlarmClass] = field(default_factory=list)
    scripts: List[HMIScript] = field(default_factory=list)
    screens: List[HMIScreen] = field(default_factory=list)
    text_lists: List[HMITextList] = field(default_factory=list)


@dataclass
class TiaProject:
    """Top-level TIA Portal project model."""
    name: str
    directory: str
    plc_devices: List[PLCDevice] = field(default_factory=list)
    hmi_devices: List[HMIDevice] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class SiemensProjectParser:
    """
    Parse an entire Siemens TIA Portal Openness export directory.

    Expected directory layout:
        <ProjectName>/
        ├── PLC_<name>/
        │   ├── Blocks/
        │   ├── TagTables/
        │   ├── Types/
        │   └── TechnologyObjects/
        └── HMI_<name>/
            ├── Alarms/
            ├── Connections/
            ├── Screens/
            ├── Scripts/
            ├── Tags/
            └── TextLists/
    """

    def __init__(self):
        self._tia_parser = TiaXmlParser()

    def parse_project(self, project_dir: str) -> TiaProject:
        """Parse the full TIA Portal project directory."""
        root = Path(project_dir)
        if not root.is_dir():
            raise FileNotFoundError(f"Project directory not found: {project_dir}")

        project_name = self._derive_project_name(root)
        project = TiaProject(name=project_name, directory=str(root))

        # Walk top-level subdirectories
        for child in sorted(root.iterdir()):
            if not child.is_dir():
                continue

            dir_name = child.name

            if dir_name.startswith("PLC_"):
                plc_name = dir_name.replace("PLC_", "", 1)
                plc = self._parse_plc_device(child, plc_name, dir_name)
                project.plc_devices.append(plc)

            elif dir_name.startswith("HMI_"):
                hmi_name = dir_name.replace("HMI_", "", 1)
                hmi = self._parse_hmi_device(child, hmi_name, dir_name)
                project.hmi_devices.append(hmi)

        return project

    # ------------------------------------------------------------------
    # Project name derivation
    # ------------------------------------------------------------------

    def _derive_project_name(self, root: Path) -> str:
        """Derive a clean project name from the directory."""
        name = root.name
        # Strip common TIA export suffixes like timestamps
        # e.g. "ECar_Demo_UBP_UCP_PC_Features_VoT_V20_20250510_1818"
        # Keep meaningful part before version/timestamp patterns
        parts = name.split("_")
        # Find where the version or date part starts
        clean_parts = []
        for p in parts:
            # Stop at version markers like "V20" or timestamps like "20250510"
            if len(p) >= 8 and p.isdigit():
                break
            clean_parts.append(p)
        return "_".join(clean_parts) if clean_parts else name

    # ------------------------------------------------------------------
    # PLC Device parsing
    # ------------------------------------------------------------------

    def _parse_plc_device(
        self, plc_dir: Path, plc_name: str, dir_name: str
    ) -> PLCDevice:
        """Parse a PLC device directory."""
        device = PLCDevice(name=plc_name, dir_name=dir_name)

        blocks_dir = plc_dir / "Blocks"
        tag_tables_dir = plc_dir / "TagTables"
        types_dir = plc_dir / "Types"

        # Parse blocks (XML) via TiaXmlParser
        if blocks_dir.is_dir():
            device.blocks, device.block_metadata = self._parse_plc_blocks(blocks_dir)

        # Parse tag tables (XML)
        if tag_tables_dir.is_dir():
            device.tag_tables = self._parse_plc_tag_tables(tag_tables_dir)

        # Parse types / UDTs (XML)
        if types_dir.is_dir():
            device.types = self._parse_plc_types(types_dir)

        return device

    def _parse_plc_blocks(
        self, blocks_dir: Path
    ) -> Tuple[List[SCFile], List[Dict]]:
        """Parse all PLC block files (XML for logic, JSON for metadata)."""
        blocks: List[SCFile] = []
        metadata: List[Dict] = []

        for xml_file in sorted(blocks_dir.rglob("*.xml")):
            try:
                parsed = self._tia_parser.parse_file(str(xml_file))
                blocks.extend(parsed)
            except Exception as e:
                print(f"  [WARNING] Could not parse block {xml_file.name}: {e}")

        for json_file in sorted(blocks_dir.rglob("*.json")):
            try:
                data = self._read_json(json_file)
                if data:
                    data["_source_file"] = str(json_file)
                    data["_relative_path"] = str(
                        json_file.relative_to(blocks_dir.parent.parent)
                    )
                    metadata.append(data)
            except Exception as e:
                print(f"  [WARNING] Could not parse metadata {json_file.name}: {e}")

        return blocks, metadata

    def _parse_plc_tag_tables(self, tag_tables_dir: Path) -> List[PLCTagTable]:
        """Parse PLC tag table XML files."""
        tables: List[PLCTagTable] = []

        for xml_file in sorted(tag_tables_dir.rglob("*.xml")):
            try:
                table = self._parse_plc_tag_table_xml(xml_file)
                if table:
                    tables.append(table)
            except Exception as e:
                print(f"  [WARNING] Could not parse tag table {xml_file.name}: {e}")

        return tables

    def _parse_plc_tag_table_xml(self, xml_file: Path) -> Optional[PLCTagTable]:
        """Parse a single PLC tag table XML file."""
        try:
            tree = ET.parse(str(xml_file))
        except ET.ParseError as e:
            print(f"  [WARNING] XML parse error in {xml_file.name}: {e}")
            return None

        root = tree.getroot()

        # Find SW.Tags.PlcTagTable element
        for child in root:
            if child.tag == "SW.Tags.PlcTagTable":
                attr_list = child.find("AttributeList")
                if attr_list is None:
                    continue

                name_elem = attr_list.find("Name")
                name = name_elem.text.strip() if name_elem is not None and name_elem.text else xml_file.stem

                table = PLCTagTable(name=name, source_file=str(xml_file))

                # Parse individual tags
                obj_list = child.find("ObjectList")
                if obj_list is not None:
                    for tag_elem in obj_list.findall("SW.Tags.PlcTag"):
                        tag = self._parse_plc_tag_elem(tag_elem)
                        if tag:
                            table.tags.append(tag)

                return table

        return None

    def _parse_plc_tag_elem(self, tag_elem: ET.Element) -> Optional[PLCTag]:
        """Parse a single SW.Tags.PlcTag element."""
        attr_list = tag_elem.find("AttributeList")
        if attr_list is None:
            return None

        name_elem = attr_list.find("Name")
        if name_elem is None or not name_elem.text:
            return None

        name = name_elem.text.strip()
        data_type_elem = attr_list.find("DataTypeName")
        data_type = data_type_elem.text.strip() if data_type_elem is not None and data_type_elem.text else "Bool"

        logical_addr_elem = attr_list.find("LogicalAddress")
        logical_address = logical_addr_elem.text.strip() if logical_addr_elem is not None and logical_addr_elem.text else ""

        ext_accessible = self._get_bool_attr(attr_list, "ExternalAccessible", True)
        ext_visible = self._get_bool_attr(attr_list, "ExternalVisible", True)
        ext_writable = self._get_bool_attr(attr_list, "ExternalWritable", True)

        # Extract comment
        comment = ""
        obj_list = tag_elem.find("ObjectList")
        if obj_list is not None:
            comment = self._extract_multilingual_comment(obj_list)

        return PLCTag(
            name=name,
            data_type=data_type,
            logical_address=logical_address,
            comment=comment,
            external_accessible=ext_accessible,
            external_visible=ext_visible,
            external_writable=ext_writable,
        )

    def _parse_plc_types(self, types_dir: Path) -> List[PLCType]:
        """Parse PLC type/UDT XML files."""
        types: List[PLCType] = []

        for xml_file in sorted(types_dir.rglob("*.xml")):
            try:
                plc_type = self._parse_plc_type_xml(xml_file)
                if plc_type:
                    types.append(plc_type)
            except Exception as e:
                print(f"  [WARNING] Could not parse type {xml_file.name}: {e}")

        return types

    def _parse_plc_type_xml(self, xml_file: Path) -> Optional[PLCType]:
        """Parse a single PLC type/UDT XML file."""
        try:
            tree = ET.parse(str(xml_file))
        except ET.ParseError as e:
            print(f"  [WARNING] XML parse error in {xml_file.name}: {e}")
            return None

        root = tree.getroot()

        for child in root:
            if child.tag == "SW.Types.PlcStruct":
                attr_list = child.find("AttributeList")
                if attr_list is None:
                    continue

                name_elem = attr_list.find("Name")
                name = name_elem.text.strip() if name_elem is not None and name_elem.text else xml_file.stem

                is_failsafe_elem = attr_list.find("IsFailsafeCompliant")
                is_failsafe = (
                    is_failsafe_elem.text.strip().lower() == "true"
                    if is_failsafe_elem is not None and is_failsafe_elem.text
                    else False
                )

                plc_type = PLCType(
                    name=name,
                    source_file=str(xml_file),
                    is_failsafe=is_failsafe,
                )

                # Parse interface members
                interface_elem = attr_list.find("Interface")
                if interface_elem is not None:
                    plc_type.members = self._parse_type_members(interface_elem)

                return plc_type

        return None

    def _parse_type_members(self, interface_elem: ET.Element) -> List[Tag]:
        """Parse member definitions from a type's Interface element."""
        members: List[Tag] = []

        sections = interface_elem.find(f"{{{NS_INTERFACE}}}Sections")
        if sections is None:
            sections = interface_elem.find("Sections")
        if sections is None:
            return members

        for section in sections:
            section_tag = section.tag.replace(f"{{{NS_INTERFACE}}}", "")
            if section_tag != "Section":
                continue

            for member in section:
                member_tag = member.tag.replace(f"{{{NS_INTERFACE}}}", "")
                if member_tag != "Member":
                    continue

                name = member.get("Name", "")
                data_type = member.get("Datatype", "Unknown")
                if not name:
                    continue

                # Extract comment from nested elements
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

                members.append(Tag(
                    name=name,
                    data_type=data_type,
                    description=description,
                ))

        return members

    # ------------------------------------------------------------------
    # HMI Device parsing
    # ------------------------------------------------------------------

    def _parse_hmi_device(
        self, hmi_dir: Path, hmi_name: str, dir_name: str
    ) -> HMIDevice:
        """Parse an HMI device directory."""
        device = HMIDevice(name=hmi_name, dir_name=dir_name)

        connections_dir = hmi_dir / "Connections"
        tags_dir = hmi_dir / "Tags"
        alarms_dir = hmi_dir / "Alarms"
        scripts_dir = hmi_dir / "Scripts"
        screens_dir = hmi_dir / "Screens"
        text_lists_dir = hmi_dir / "TextLists"

        # Connections
        if connections_dir.is_dir():
            device.connections = self._parse_hmi_connections(connections_dir)

        # Tags (HMI tag tables)
        if tags_dir.is_dir():
            device.tag_tables = self._parse_hmi_tag_tables(tags_dir)

        # Alarms
        if alarms_dir.is_dir():
            device.alarms, device.alarm_classes = self._parse_hmi_alarms(alarms_dir)

        # Scripts
        if scripts_dir.is_dir():
            device.scripts = self._parse_hmi_scripts(scripts_dir)

        # Screens
        if screens_dir.is_dir():
            device.screens = self._parse_hmi_screens(screens_dir)

        # Text lists
        if text_lists_dir.is_dir():
            device.text_lists = self._parse_hmi_text_lists(text_lists_dir)

        return device

    # -- Connections --

    def _parse_hmi_connections(self, conn_dir: Path) -> List[HMIConnection]:
        """Parse HMI connection JSON files."""
        connections: List[HMIConnection] = []
        for json_file in sorted(conn_dir.rglob("*.json")):
            data = self._read_json(json_file)
            if not data or not data.get("Name"):
                continue
            connections.append(HMIConnection(
                name=data["Name"],
                partner=data.get("Partner", ""),
                station=data.get("Station", ""),
                communication_driver=data.get("CommunicationDriver", ""),
                node=data.get("Node", ""),
                address=data.get("InitialAddress", ""),
                disabled_at_startup=data.get("DisabledAtStartup", "False").lower() == "true",
                source_file=str(json_file),
            ))
        return connections

    # -- Tags --

    def _parse_hmi_tag_tables(self, tags_dir: Path) -> List[HMITagTable]:
        """Parse HMI tag table JSON files."""
        tables: List[HMITagTable] = []
        for json_file in sorted(tags_dir.rglob("*.json")):
            data = self._read_json(json_file)
            if not data or not data.get("Name"):
                continue
            # Determine folder from relative path
            rel = json_file.relative_to(tags_dir)
            folder = str(rel.parent) if str(rel.parent) != "." else ""
            tables.append(HMITagTable(
                name=data["Name"],
                folder=folder,
                source_file=str(json_file),
            ))
        return tables

    # -- Alarms --

    def _parse_hmi_alarms(
        self, alarms_dir: Path
    ) -> Tuple[List[HMIAlarm], List[HMIAlarmClass]]:
        """Parse HMI alarm and alarm class JSON files."""
        alarms: List[HMIAlarm] = []
        alarm_classes: List[HMIAlarmClass] = []

        # Alarm classes
        classes_dir = alarms_dir / "Classes"
        if classes_dir.is_dir():
            for json_file in sorted(classes_dir.rglob("*.json")):
                data = self._read_json(json_file)
                if not data or not data.get("Name"):
                    continue
                alarm_classes.append(HMIAlarmClass(
                    name=data["Name"],
                    alarm_id=data.get("Id", ""),
                    priority=data.get("Priority", "0"),
                    state_machine=data.get("StateMachine", ""),
                    is_system=data.get("IsSystem", "False").lower() == "true",
                    source_file=str(json_file),
                ))

        # Analog alarms
        analog_dir = alarms_dir / "Analog"
        if analog_dir.is_dir():
            for json_file in sorted(analog_dir.rglob("*.json")):
                data = self._read_json(json_file)
                if not data or not data.get("Name"):
                    continue
                alarms.append(HMIAlarm(
                    name=data["Name"],
                    alarm_type="Analog",
                    alarm_class=data.get("AlarmClass", ""),
                    origin=data.get("Origin", ""),
                    priority=data.get("Priority", "0"),
                    alarm_id=data.get("Id", ""),
                    area=data.get("Area", ""),
                    raised_state_tag=data.get("RaisedStateTag", ""),
                    condition=data.get("Condition", ""),
                    condition_value=data.get("ConditionValue", ""),
                    source_file=str(json_file),
                ))

        # Discrete alarms
        discrete_dir = alarms_dir / "Discrete"
        if discrete_dir.is_dir():
            for json_file in sorted(discrete_dir.rglob("*.json")):
                data = self._read_json(json_file)
                if not data or not data.get("Name"):
                    continue
                alarms.append(HMIAlarm(
                    name=data["Name"],
                    alarm_type="Discrete",
                    alarm_class=data.get("AlarmClass", ""),
                    origin=data.get("Origin", ""),
                    priority=data.get("Priority", "0"),
                    alarm_id=data.get("Id", ""),
                    area=data.get("Area", ""),
                    raised_state_tag=data.get("RaisedStateTag", ""),
                    trigger_bit_address=data.get("TriggerBitAddress", ""),
                    trigger_mode=data.get("TriggerMode", ""),
                    source_file=str(json_file),
                ))

        return alarms, alarm_classes

    # -- Scripts --

    def _parse_hmi_scripts(self, scripts_dir: Path) -> List[HMIScript]:
        """Parse HMI JavaScript script files."""
        scripts: List[HMIScript] = []

        for js_file in sorted(scripts_dir.rglob("*.hmi.js")):
            try:
                script_text = js_file.read_text(encoding="utf-8", errors="replace")
            except Exception:
                script_text = ""

            # Extract exported function names
            functions = self._extract_js_function_names(script_text)

            name = js_file.stem  # e.g. "08_ECar.hmi"
            if name.endswith(".hmi"):
                name = name[:-4]

            scripts.append(HMIScript(
                name=name,
                script_file=js_file.name,
                script_text=script_text,
                functions=functions,
                source_file=str(js_file),
            ))

        return scripts

    def _extract_js_function_names(self, script_text: str) -> List[str]:
        """Extract exported function names from HMI JavaScript."""
        import re
        # Match: export function FuncName(...)
        pattern = r"export\s+function\s+(\w+)\s*\("
        return re.findall(pattern, script_text)

    # -- Screens --

    def _parse_hmi_screens(self, screens_dir: Path) -> List[HMIScreen]:
        """Parse HMI screen directory structure."""
        screens: List[HMIScreen] = []

        # Screens may be directories or files
        for item in sorted(screens_dir.rglob("*")):
            if item.is_dir():
                # Directory represents a screen group
                rel = item.relative_to(screens_dir)
                screens.append(HMIScreen(
                    name=item.name,
                    folder=str(rel.parent) if str(rel.parent) != "." else "",
                    source_file=str(item),
                ))
            elif item.suffix.lower() in (".json", ".xml"):
                screens.append(HMIScreen(
                    name=item.stem,
                    folder=str(item.parent.relative_to(screens_dir)),
                    source_file=str(item),
                ))

        return screens

    # -- Text Lists --

    def _parse_hmi_text_lists(self, textlists_dir: Path) -> List[HMITextList]:
        """Parse HMI text list JSON files."""
        text_lists: List[HMITextList] = []

        for json_file in sorted(textlists_dir.rglob("*.json")):
            data = self._read_json(json_file)
            if not data or not data.get("Name"):
                continue
            text_lists.append(HMITextList(
                name=data["Name"],
                source_file=str(json_file),
            ))

        return text_lists

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _read_json(self, path: Path) -> Optional[Dict]:
        """Read and parse a JSON file, returning None on error."""
        try:
            text = path.read_text(encoding="utf-8-sig", errors="replace")
            return json.loads(text)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  [WARNING] JSON read error {path.name}: {e}")
            return None

    def _get_bool_attr(
        self, parent: ET.Element, tag: str, default: bool = False
    ) -> bool:
        """Get a boolean attribute from an XML element."""
        elem = parent.find(tag)
        if elem is not None and elem.text:
            return elem.text.strip().lower() == "true"
        return default

    def _extract_multilingual_comment(self, obj_list: ET.Element) -> str:
        """Extract comment text from a MultilingualText element inside ObjectList."""
        for ml in obj_list.findall("MultilingualText"):
            if ml.get("CompositionName") == "Comment":
                inner = ml.find("ObjectList")
                if inner is not None:
                    for item in inner.findall("MultilingualTextItem"):
                        attr = item.find("AttributeList")
                        if attr is not None:
                            culture = attr.find("Culture")
                            text = attr.find("Text")
                            # Prefer English
                            if (
                                culture is not None
                                and culture.text
                                and "en" in culture.text.lower()
                                and text is not None
                                and text.text
                            ):
                                return text.text.strip()
                    # Fallback: any non-empty text
                    for item in inner.findall("MultilingualTextItem"):
                        attr = item.find("AttributeList")
                        if attr is not None:
                            text = attr.find("Text")
                            if text is not None and text.text and text.text.strip():
                                return text.text.strip()
        return ""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_project_summary(project: TiaProject) -> None:
    """Print a human-readable summary of the parsed project."""
    print(f"\n{'=' * 70}")
    print(f"  TIA Portal Project: {project.name}")
    print(f"  Directory: {project.directory}")
    print(f"{'=' * 70}")

    print(f"\n  PLCs: {len(project.plc_devices)}")
    for plc in project.plc_devices:
        print(f"\n  --- PLC: {plc.name} ({plc.dir_name}) ---")
        print(f"    Blocks:     {len(plc.blocks)}")
        print(f"    Tag Tables: {len(plc.tag_tables)}")
        total_tags = sum(len(t.tags) for t in plc.tag_tables)
        print(f"    PLC Tags:   {total_tags}")
        print(f"    Types/UDTs: {len(plc.types)}")
        print(f"    Block Meta: {len(plc.block_metadata)}")

        if plc.tag_tables:
            for tt in plc.tag_tables:
                print(f"      TagTable '{tt.name}': {len(tt.tags)} tags")
                for tag in tt.tags[:5]:
                    addr = f" @ {tag.logical_address}" if tag.logical_address else ""
                    print(f"        {tag.name}: {tag.data_type}{addr}")
                if len(tt.tags) > 5:
                    print(f"        ... and {len(tt.tags) - 5} more")

        if plc.types:
            for t in plc.types:
                members_str = ", ".join(
                    f"{m.name}:{m.data_type}" for m in t.members[:5]
                )
                if len(t.members) > 5:
                    members_str += f", ... +{len(t.members) - 5}"
                print(f"      Type '{t.name}': {members_str}")

        if plc.blocks:
            by_type: Dict[str, int] = {}
            for b in plc.blocks:
                by_type[b.type] = by_type.get(b.type, 0) + 1
            print(f"    Block types: {dict(by_type)}")

    print(f"\n  HMIs: {len(project.hmi_devices)}")
    for hmi in project.hmi_devices:
        print(f"\n  --- HMI: {hmi.name} ({hmi.dir_name}) ---")
        print(f"    Connections: {len(hmi.connections)}")
        for conn in hmi.connections:
            print(f"      {conn.name} -> {conn.partner} ({conn.communication_driver})")
        print(f"    Tag Tables:  {len(hmi.tag_tables)}")
        print(f"    Alarms:      {len(hmi.alarms)}")
        analog = sum(1 for a in hmi.alarms if a.alarm_type == "Analog")
        discrete = sum(1 for a in hmi.alarms if a.alarm_type == "Discrete")
        print(f"      Analog: {analog}, Discrete: {discrete}")
        print(f"    Alarm Classes: {len(hmi.alarm_classes)}")
        print(f"    Scripts:     {len(hmi.scripts)}")
        for s in hmi.scripts:
            print(f"      {s.name}: {len(s.functions)} functions ({', '.join(s.functions[:5])})")
        print(f"    Screens:     {len(hmi.screens)}")
        print(f"    Text Lists:  {len(hmi.text_lists)}")

    # Interlinks summary
    print(f"\n  --- Interlinks ---")
    all_connections = [
        c for hmi in project.hmi_devices for c in hmi.connections
    ]
    plc_names = {plc.name for plc in project.plc_devices}
    for conn in all_connections:
        partner = conn.partner
        linked = "LINKED" if partner in plc_names else "EXTERNAL"
        hmi_name = next(
            (h.name for h in project.hmi_devices if conn in h.connections),
            "?"
        )
        print(f"    {hmi_name} --[{conn.name}]--> {partner} [{linked}]")

    # Alarm-to-tag references
    alarm_refs = set()
    for hmi in project.hmi_devices:
        for alarm in hmi.alarms:
            if alarm.raised_state_tag:
                alarm_refs.add(alarm.raised_state_tag)
            if alarm.trigger_bit_address:
                alarm_refs.add(alarm.trigger_bit_address)
    if alarm_refs:
        print(f"\n  Alarm tag references: {len(alarm_refs)} unique tags")
        for ref in sorted(alarm_refs)[:10]:
            print(f"    {ref}")
        if len(alarm_refs) > 10:
            print(f"    ... and {len(alarm_refs) - 10} more")


def main():
    """CLI: parse a Siemens TIA Portal project directory."""
    if len(sys.argv) < 2:
        print("Usage: python siemens_project_parser.py <project_directory>")
        sys.exit(1)

    target = Path(sys.argv[1])
    if not target.is_dir():
        print(f"[ERROR] Not a directory: {target}")
        sys.exit(1)

    print(f"[INFO] Parsing TIA Portal project: {target}")
    parser = SiemensProjectParser()
    project = parser.parse_project(str(target))
    _print_project_summary(project)


if __name__ == "__main__":
    main()
