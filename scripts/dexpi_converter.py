#!/usr/bin/env python3
"""
DEXPI Converter - Maps internal PLC/SCADA ontology to DEXPI P&ID standard.

Uses pydexpi for DEXPI data model reference and converts Neo4j ontology
nodes/relationships into DEXPI-classified graph data for visualization.

DEXPI (Data Exchange in the Process Industry) is a vendor-neutral standard
for representing P&ID (Piping and Instrumentation Diagram) information.
"""

import json
import argparse
import sys
import re
import uuid
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date

try:
    from neo4j_ontology import OntologyGraph, get_ontology_graph
except ImportError:
    # Allow running standalone for testing
    OntologyGraph = None
    get_ontology_graph = None

# Optional: import pydexpi for model building and Proteus XML export
try:
    from pydexpi.dexpi_classes.dexpiModel import DexpiModel
    from pydexpi.dexpi_classes.equipment import Equipment as DexpiEquipment
    from pydexpi.dexpi_classes.instrumentation import (
        ProcessInstrumentationFunction as DexpiInstrument,
    )
    from pydexpi.dexpi_classes.piping import PipingNetworkSegment
    from pydexpi.loaders import JsonSerializer as DexpiJsonSerializer
    PYDEXPI_AVAILABLE = True
except ImportError:
    PYDEXPI_AVAILABLE = False


# =============================================================================
# DEXPI Classification Rules
# =============================================================================

# Pattern-based classification of internal ontology nodes into DEXPI categories.
# Each category maps to a DEXPI concept from the P&ID specification.

DEXPI_CATEGORIES = {
    "equipment": {
        "label": "Equipment",
        "description": "Tagged plant equipment (vessels, exchangers, machines)",
        "color": "#1565C0",
        "shape": "round-rectangle",
        "icon": "E",
        "patterns": [
            r"motor", r"pump", r"valve", r"tank", r"vessel", r"compressor",
            r"heat.?exchanger", r"hx", r"heater", r"cooler", r"reactor",
            r"column", r"tower", r"filter", r"mixer", r"agitator",
            r"conveyor", r"blower", r"fan", r"boiler", r"condenser",
            r"evaporator", r"dryer", r"hopper", r"silo", r"bin",
            r"centrifuge", r"separator", r"cyclone", r"screen",
            r"crusher", r"mill", r"grinder", r"feeder",
        ],
    },
    "instrument": {
        "label": "Instrumentation",
        "description": "Process instrumentation functions (sensors, controllers)",
        "color": "#2E7D32",
        "shape": "ellipse",
        "icon": "I",
        "patterns": [
            r"sensor", r"transmitter", r"indicator", r"controller",
            r"pid", r"temperature", r"pressure", r"flow", r"level",
            r"analyzer", r"switch", r"alarm", r"trip",
            r"thermocouple", r"rtd", r"gauge", r"meter",
            r"transducer", r"recorder", r"totalizer",
            r"[TPFLAQ]I[CTRS]?\b",  # ISA tag patterns like TIC, FIT, LT, etc.
        ],
    },
    "actuator": {
        "label": "Actuating System",
        "description": "Actuators, drives, and positioning systems",
        "color": "#E65100",
        "shape": "diamond",
        "icon": "A",
        "patterns": [
            r"actuator", r"drive", r"vfd", r"starter", r"contactor",
            r"solenoid", r"damper", r"positioner", r"servo",
            r"pneumatic", r"hydraulic",
        ],
    },
    "piping": {
        "label": "Piping",
        "description": "Piping network components (pipes, fittings)",
        "color": "#546E7A",
        "shape": "round-rectangle",
        "icon": "P",
        "patterns": [
            r"pipe", r"line", r"header", r"manifold", r"fitting",
            r"elbow", r"tee", r"reducer", r"flange", r"gasket",
            r"expansion.?joint", r"hose", r"tubing",
        ],
    },
    "safety": {
        "label": "Safety System",
        "description": "Safety instrumented systems and protective devices",
        "color": "#C62828",
        "shape": "diamond",
        "icon": "S",
        "patterns": [
            r"safety", r"sif", r"sil", r"emergency", r"shutdown",
            r"e.?stop", r"relief", r"rupture", r"interlock", r"guard",
            r"bursting.?disc", r"safety.?valve", r"psv", r"prv",
            r"protective", r"trip",
        ],
    },
    "process_control": {
        "label": "Process Control",
        "description": "Process control loops and sequencing",
        "color": "#00838F",
        "shape": "round-rectangle",
        "icon": "C",
        "patterns": [
            r"control.?loop", r"sequence", r"phase", r"batch",
            r"recipe", r"state.?machine", r"fsm", r"plc.?program",
            r"function.?block", r"fb_",
        ],
    },
    "nozzle": {
        "label": "Nozzle / Connection",
        "description": "Equipment nozzles and connection points",
        "color": "#6A1B9A",
        "shape": "ellipse",
        "icon": "N",
        "patterns": [
            r"nozzle", r"connection.?point", r"port", r"inlet",
            r"outlet", r"suction", r"discharge",
        ],
    },
    "scada_hmi": {
        "label": "SCADA / HMI",
        "description": "SCADA tags, views, and HMI display components",
        "color": "#7B1FA2",
        "shape": "round-rectangle",
        "icon": "H",
        "patterns": [
            r"scada", r"hmi", r"display", r"screen", r"panel",
            r"faceplate", r"overview", r"opc",
        ],
    },
    "data_interface": {
        "label": "Data Interface",
        "description": "Named queries, scripts, and data access components",
        "color": "#F57F17",
        "shape": "round-rectangle",
        "icon": "D",
        "patterns": [
            r"query", r"report", r"historian", r"datalog",
            r"archive", r"database", r"stored.?proc",
        ],
    },
}

# Default category for unclassified nodes
DEXPI_DEFAULT = {
    "label": "Unclassified",
    "description": "Nodes not yet classified into DEXPI categories",
    "color": "#616161",
    "shape": "round-rectangle",
    "icon": "?",
}

# Map internal relationship types to DEXPI connection types
DEXPI_EDGE_MAP = {
    # Piping connections (physical flow)
    "HAS_FLOW": {"dexpi_type": "piping_connection", "color": "#1565C0", "style": "solid", "label": "Piping"},
    "FEEDS": {"dexpi_type": "piping_connection", "color": "#1565C0", "style": "solid", "label": "Piping"},
    "CONNECTED_TO": {"dexpi_type": "piping_connection", "color": "#1565C0", "style": "solid", "label": "Piping"},
    "PIPED_TO": {"dexpi_type": "piping_connection", "color": "#1565C0", "style": "solid", "label": "Piping"},
    # Signal connections (instrumentation/control)
    "CONTROLS": {"dexpi_type": "signal_line", "color": "#2E7D32", "style": "dashed", "label": "Signal"},
    "MONITORS": {"dexpi_type": "signal_line", "color": "#2E7D32", "style": "dashed", "label": "Signal"},
    "MEASURES": {"dexpi_type": "signal_line", "color": "#2E7D32", "style": "dashed", "label": "Signal"},
    "HAS_TAG": {"dexpi_type": "signal_line", "color": "#2E7D32", "style": "dashed", "label": "Has Tag"},
    "SIGNALS": {"dexpi_type": "signal_line", "color": "#2E7D32", "style": "dashed", "label": "Signal"},
    # Structural / composition
    "HAS_COMPONENT": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Contains"},
    "PART_OF": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Part Of"},
    "BELONGS_TO": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Belongs To"},
    "INSTANTIATES": {"dexpi_type": "structural", "color": "#9E9E9E", "style": "dotted", "label": "Instantiates"},
    "USES_TYPE": {"dexpi_type": "structural", "color": "#9E9E9E", "style": "dotted", "label": "Uses Type"},
    # Safety connections
    "SAFETY_CRITICAL": {"dexpi_type": "safety_connection", "color": "#C62828", "style": "solid", "label": "Safety"},
    "DEMAND_ON": {"dexpi_type": "safety_connection", "color": "#C62828", "style": "solid", "label": "Demand"},
    "TRIPS": {"dexpi_type": "safety_connection", "color": "#C62828", "style": "solid", "label": "Trips"},
    # SCADA mapping (informational)
    "MAPS_TO_SCADA": {"dexpi_type": "information", "color": "#9C27B0", "style": "dotted", "label": "SCADA Map"},
    "BINDS_TO": {"dexpi_type": "information", "color": "#7B1FA2", "style": "dashed", "label": "Binds To"},
    "REFERENCES": {"dexpi_type": "information", "color": "#7B1FA2", "style": "dotted", "label": "References"},
    "DISPLAYS": {"dexpi_type": "information", "color": "#7B1FA2", "style": "dashed", "label": "Displays"},
    "CALLS_QUERY": {"dexpi_type": "information", "color": "#F57F17", "style": "dashed", "label": "Calls Query"},
    "CALLS_SCRIPT": {"dexpi_type": "information", "color": "#F57F17", "style": "dashed", "label": "Calls Script"},
    "BELONGS_TO_BU": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Belongs To BU"},
    # Siemens TIA Portal - structural / composition
    "HAS_DEVICE": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Has Device"},
    "HAS_BLOCK": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Has Block"},
    "HAS_TAG_TABLE": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Has Tag Table"},
    "HAS_TYPE": {"dexpi_type": "structural", "color": "#9E9E9E", "style": "dotted", "label": "Has Type"},
    "HAS_SCRIPT": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Has Script"},
    "HAS_ALARM": {"dexpi_type": "structural", "color": "#C62828", "style": "dotted", "label": "Has Alarm"},
    "HAS_ALARM_CLASS": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Has Alarm Class"},
    "HAS_SCREEN": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Has Screen"},
    "HAS_CONNECTION": {"dexpi_type": "structural", "color": "#757575", "style": "dotted", "label": "Has Connection"},
    # Siemens TIA Portal - signal / monitoring
    "MONITORS_TAG": {"dexpi_type": "signal_line", "color": "#2E7D32", "style": "dashed", "label": "Monitors Tag"},
    "CONNECTS_TO_PLC": {"dexpi_type": "signal_line", "color": "#0288D1", "style": "dashed", "label": "Connects To PLC"},
    # Troubleshooting (not P&ID but useful)
    "HAS_SYMPTOM": {"dexpi_type": "diagnostic", "color": "#FF5722", "style": "dotted", "label": "Symptom"},
    "CAUSED_BY": {"dexpi_type": "diagnostic", "color": "#FF5722", "style": "dotted", "label": "Cause"},
    "HAS_PATTERN": {"dexpi_type": "information", "color": "#795548", "style": "dotted", "label": "Pattern"},
}

# Default edge styling
DEXPI_EDGE_DEFAULT = {
    "dexpi_type": "unknown",
    "color": "#9E9E9E",
    "style": "solid",
    "label": "Related",
}


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        try:
            return str(obj)
        except Exception:
            return super().default(obj)


def output_json(data: Any) -> None:
    """Output JSON to stdout."""
    print(json.dumps(data, cls=DateTimeEncoder))


def output_error(message: str) -> None:
    """Output error as JSON."""
    output_json({"error": message, "success": False})
    sys.exit(1)


# =============================================================================
# DEXPI Classifier
# =============================================================================

class DexpiClassifier:
    """Classify internal ontology nodes into DEXPI P&ID categories."""

    def __init__(self):
        # Pre-compile regex patterns for performance
        self._compiled = {}
        for category, info in DEXPI_CATEGORIES.items():
            self._compiled[category] = [
                re.compile(p, re.IGNORECASE) for p in info["patterns"]
            ]

    def classify_node(self, name: str, node_type: str, properties: Dict) -> str:
        """
        Classify a node into a DEXPI category.

        Uses a priority-based approach:
        1. Explicit node type mapping (e.g., SafetyElement → safety)
        2. Property-based hints (e.g., purpose mentioning 'pump')
        3. Name pattern matching
        4. Default to 'equipment' for PLC types, 'unclassified' for others

        Returns the DEXPI category key.
        """
        # 1. Explicit type mapping
        type_lower = node_type.lower() if node_type else ""

        type_map = {
            "safetyelement": "safety",
            "controlpattern": "process_control",
            "dataflow": "piping",
            "endtoendflow": "piping",
            "faultsymptom": "instrument",  # Diagnostic instruments
            "faultcause": "equipment",     # Equipment failure
            "equipment": "equipment",      # Direct equipment type
            # SCADA / HMI components (Ignition)
            "scadatag": "scada_hmi",
            "view": "scada_hmi",
            "perspectiveview": "scada_hmi",
            "viewcomponent": "scada_hmi",
            # Data interface components (Ignition)
            "namedquery": "data_interface",
            "script": "data_interface",
            "gatewayevent": "data_interface",
            # Siemens TIA Portal - PLC components
            "plcdevice": "process_control",   # PLC controller
            "plctag": "instrument",           # I/O tags map to process instrumentation
            "plctagtable": "process_control", # Tag table (structural container)
            # Siemens TIA Portal - HMI components
            "hmidevice": "scada_hmi",         # HMI panel
            "hmiscreen": "scada_hmi",         # HMI display screen
            "hmitagtable": "scada_hmi",       # HMI tag container
            "hmitextlist": "scada_hmi",       # HMI display text resources
            "hmiconnection": "data_interface", # HMI communication link
            "hmiscript": "data_interface",    # HMI automation script
            "hmialarm": "instrument",         # Alarm monitoring process variable
            "hmialarmclass": "safety",        # Alarm classification (safety-relevant)
        }

        if type_lower in type_map:
            return type_map[type_lower]

        # 2. Combine name + purpose + description for pattern matching
        search_text = " ".join(filter(None, [
            name or "",
            properties.get("purpose", ""),
            properties.get("description", ""),
            properties.get("inferred_purpose", ""),
        ])).lower()

        # 3. Safety first (highest priority in P&ID)
        for pattern in self._compiled.get("safety", []):
            if pattern.search(search_text):
                return "safety"

        # 4. Check other categories in priority order
        priority_order = [
            "instrument", "actuator", "equipment",
            "piping", "nozzle", "process_control",
        ]

        for category in priority_order:
            for pattern in self._compiled.get(category, []):
                if pattern.search(search_text):
                    return category

        # 5. Default based on internal type
        if type_lower in ("aoi", "aoiinstance", "tag"):
            return "equipment"  # Default PLC nodes to equipment
        elif type_lower in ("equipment",):
            return "equipment"
        elif type_lower in ("udt", "udttype"):
            return "process_control"

        return "unclassified"

    def get_category_info(self, category: str) -> Dict:
        """Get display info for a DEXPI category."""
        return DEXPI_CATEGORIES.get(category, DEXPI_DEFAULT)

    def classify_edge(self, rel_type: str) -> Dict:
        """Classify an edge into a DEXPI connection type."""
        return DEXPI_EDGE_MAP.get(rel_type, DEXPI_EDGE_DEFAULT)


# =============================================================================
# DEXPI Converter
# =============================================================================

class DexpiConverter:
    """
    Convert internal ontology graph to DEXPI-classified P&ID representation.

    Reads from Neo4j and produces a Cytoscape.js-compatible graph with
    DEXPI node types, colors, and edge classifications.
    """

    def __init__(self, graph: Optional["OntologyGraph"] = None):
        self._graph = graph
        self._owns_graph = False
        self.classifier = DexpiClassifier()

    def _get_graph(self) -> "OntologyGraph":
        """Get or create Neo4j connection."""
        if self._graph is None:
            if get_ontology_graph is None:
                raise RuntimeError("neo4j_ontology module not available")
            self._graph = get_ontology_graph()
            self._owns_graph = True
        return self._graph

    def close(self):
        """Close Neo4j connection if we own it."""
        if self._owns_graph and self._graph:
            self._graph.close()
            self._graph = None

    def convert(
        self,
        node_types: Optional[List[str]] = None,
        limit: int = 500,
        include_scada: bool = False,
        include_troubleshooting: bool = False,
    ) -> Dict:
        """
        Convert internal ontology to DEXPI-classified graph.

        Args:
            node_types: Optional list of internal node types to include
            limit: Maximum number of nodes
            include_scada: Include SCADA-specific nodes (Views, Scripts, etc.)
            include_troubleshooting: Include troubleshooting nodes

        Returns:
            Dict with DEXPI-classified nodes, edges, and legend info
        """
        graph = self._get_graph()
        nodes = []
        edges = []
        dexpi_stats = {}  # Count per DEXPI category

        # Build exclusion list for non-P&ID nodes
        exclude_types = set()
        exclude_types.add("Project")  # Always exclude project metadata
        exclude_types.add("TiaProject")  # Siemens project metadata
        if not include_scada:
            exclude_types.update([
                # Ignition SCADA
                "View", "PerspectiveView",
                "GatewayEvent", "ViewComponent",
                # Siemens HMI (SCADA-level, not P&ID-relevant by default)
                "HMIDevice", "HMIScreen", "HMIScript",
                "HMITagTable", "HMITextList", "HMIConnection",
            ])
        if not include_troubleshooting:
            exclude_types.update([
                "FaultSymptom", "FaultCause", "OperatorPhrase",
                "CommonPhrase", "Intent",
            ])

        with graph.session() as session:
            # Build node query
            if node_types:
                labels = " OR ".join([f"n:{t}" for t in node_types])
                node_query = f"""
                    MATCH (n) WHERE {labels}
                    RETURN elementId(n) as id,
                           labels(n)[0] as type,
                           coalesce(n.name, n.symptom, n.phrase, n.key,
                                    n.pattern_name, 'unknown') as label,
                           properties(n) as props
                    LIMIT $limit
                """
            else:
                node_query = """
                    MATCH (n)
                    RETURN elementId(n) as id,
                           labels(n)[0] as type,
                           coalesce(n.name, n.symptom, n.phrase, n.key,
                                    n.pattern_name, 'unknown') as label,
                           properties(n) as props
                    LIMIT $limit
                """

            result = session.run(node_query, limit=limit)
            node_ids = set()

            for record in result:
                rec = dict(record)
                node_type = rec.get("type", "unknown")

                # Skip excluded types
                if node_type in exclude_types:
                    continue

                name = rec.get("label", "Unknown")
                props = rec.get("props", {})

                # Classify into DEXPI category
                dexpi_category = self.classifier.classify_node(
                    name, node_type, props
                )
                cat_info = self.classifier.get_category_info(dexpi_category)

                # Track stats
                dexpi_stats[dexpi_category] = dexpi_stats.get(dexpi_category, 0) + 1

                node = {
                    "id": str(rec["id"]),
                    "label": name,
                    "type": node_type,
                    "dexpiCategory": dexpi_category,
                    "dexpiLabel": cat_info.get("label", "Unknown"),
                    "color": cat_info.get("color", "#616161"),
                    "shape": cat_info.get("shape", "round-rectangle"),
                    "icon": cat_info.get("icon", "?"),
                    "group": "dexpi",
                    "properties": props,
                }
                nodes.append(node)
                node_ids.add(node["id"])

            # Get edges between loaded nodes
            if node_ids:
                edge_query = """
                    MATCH (a)-[r]->(b)
                    WHERE elementId(a) IN $node_ids
                      AND elementId(b) IN $node_ids
                    RETURN elementId(a) as source,
                           elementId(b) as target,
                           type(r) as type,
                           properties(r) as props
                """
                result = session.run(edge_query, node_ids=list(node_ids))

                for record in result:
                    rec = dict(record)
                    rel_type = rec.get("type", "RELATED")
                    edge_info = self.classifier.classify_edge(rel_type)

                    edge = {
                        "id": f"{rec['source']}-{rel_type}-{rec['target']}",
                        "source": str(rec["source"]),
                        "target": str(rec["target"]),
                        "type": rel_type,
                        "dexpiType": edge_info["dexpi_type"],
                        "color": edge_info["color"],
                        "lineStyle": edge_info["style"],
                        "label": edge_info["label"],
                        "properties": rec.get("props", {}),
                    }
                    edges.append(edge)

        # Build legend info
        legend = self._build_legend(dexpi_stats)

        return {
            "success": True,
            "nodes": nodes,
            "edges": edges,
            "nodeCount": len(nodes),
            "edgeCount": len(edges),
            "legend": legend,
            "stats": dexpi_stats,
        }

    def _build_legend(self, stats: Dict) -> List[Dict]:
        """Build legend entries based on categories present in the data."""
        legend = []

        # Node categories
        for category, info in DEXPI_CATEGORIES.items():
            count = stats.get(category, 0)
            legend.append({
                "category": category,
                "label": info["label"],
                "description": info["description"],
                "color": info["color"],
                "shape": info["shape"],
                "icon": info["icon"],
                "count": count,
                "type": "node",
            })

        # Add unclassified if present
        if stats.get("unclassified", 0) > 0:
            legend.append({
                "category": "unclassified",
                "label": DEXPI_DEFAULT["label"],
                "description": DEXPI_DEFAULT["description"],
                "color": DEXPI_DEFAULT["color"],
                "shape": DEXPI_DEFAULT["shape"],
                "icon": DEXPI_DEFAULT["icon"],
                "count": stats.get("unclassified", 0),
                "type": "node",
            })

        # Edge types
        seen_edge_types = set()
        edge_legend = [
            {"dexpiType": "piping_connection", "label": "Piping Connection",
             "color": "#1565C0", "style": "solid", "description": "Physical piping flow path"},
            {"dexpiType": "signal_line", "label": "Signal Line",
             "color": "#2E7D32", "style": "dashed", "description": "Instrument signal / control connection"},
            {"dexpiType": "structural", "label": "Structural",
             "color": "#757575", "style": "dotted", "description": "Containment / composition relationship"},
            {"dexpiType": "safety_connection", "label": "Safety Connection",
             "color": "#C62828", "style": "solid", "description": "Safety interlock / trip connection"},
            {"dexpiType": "information", "label": "Information Link",
             "color": "#9C27B0", "style": "dotted", "description": "Informational / mapping link"},
        ]

        for entry in edge_legend:
            entry["type"] = "edge"
            legend.append(entry)

        return legend

    def export_dexpi_json(
        self,
        node_types: Optional[List[str]] = None,
        limit: int = 500,
    ) -> Dict:
        """
        Export ontology as a DEXPI-compatible JSON structure.

        This creates a simplified DEXPI model representation that can be
        used for data exchange or further processing with pydexpi.
        """
        result = self.convert(node_types=node_types, limit=limit)
        if not result["success"]:
            return result

        # Build DEXPI-structured output
        dexpi_model = {
            "dexpiVersion": "1.3",
            "generatedBy": "axilon-dexpi-converter",
            "generatedAt": datetime.now().isoformat(),
            "plantItems": [],
            "connections": [],
        }

        # Map nodes to DEXPI plant items
        for node in result["nodes"]:
            plant_item = {
                "id": node["id"],
                "tagName": node["label"],
                "dexpiClass": node["dexpiLabel"],
                "dexpiCategory": node["dexpiCategory"],
                "internalType": node["type"],
                "attributes": {
                    k: v for k, v in node.get("properties", {}).items()
                    if isinstance(v, (str, int, float, bool))
                },
            }
            dexpi_model["plantItems"].append(plant_item)

        # Map edges to DEXPI connections
        for edge in result["edges"]:
            connection = {
                "id": edge["id"],
                "sourceId": edge["source"],
                "targetId": edge["target"],
                "connectionType": edge["dexpiType"],
                "internalType": edge["type"],
                "label": edge["label"],
            }
            dexpi_model["connections"].append(connection)

        return {
            "success": True,
            "dexpiModel": dexpi_model,
            "stats": result["stats"],
        }

    def build_pydexpi_model(
        self,
        node_types: Optional[List[str]] = None,
        limit: int = 500,
    ) -> Dict:
        """
        Build a pydexpi PlantModel from the ontology data.

        Requires pydexpi to be installed.
        Returns success status and file path if exported.
        """
        if not PYDEXPI_AVAILABLE:
            return {
                "success": False,
                "error": "pydexpi is not installed. Install with: pip install pydexpi",
            }

        result = self.convert(node_types=node_types, limit=limit)
        if not result["success"]:
            return result

        try:
            # Create a DexpiModel
            dexpi_model = DexpiModel(
                id=str(uuid.uuid4()),
            )

            # Note: Full pydexpi model construction requires detailed attribute
            # mapping which depends on the specific DEXPI version and data
            # available. This is a foundation that can be extended.

            return {
                "success": True,
                "message": "pydexpi model created successfully",
                "pydexpiAvailable": True,
                "nodeCount": len(result["nodes"]),
                "edgeCount": len(result["edges"]),
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to build pydexpi model: {str(e)}",
            }


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """CLI interface for DEXPI converter."""
    parser = argparse.ArgumentParser(description="DEXPI P&ID Converter")
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # Convert: get DEXPI-classified graph for visualization
    convert_parser = subparsers.add_parser(
        "convert", help="Convert ontology to DEXPI graph"
    )
    convert_parser.add_argument("--types", nargs="*", help="Node types to include")
    convert_parser.add_argument("--limit", type=int, default=500, help="Max nodes")
    convert_parser.add_argument(
        "--include-scada", action="store_true", help="Include SCADA-specific nodes"
    )
    convert_parser.add_argument(
        "--include-troubleshooting", action="store_true",
        help="Include troubleshooting nodes"
    )

    # Export: export as DEXPI JSON
    export_parser = subparsers.add_parser(
        "export", help="Export ontology as DEXPI JSON"
    )
    export_parser.add_argument("--types", nargs="*", help="Node types to include")
    export_parser.add_argument("--limit", type=int, default=500, help="Max nodes")
    export_parser.add_argument("-o", "--output", help="Output file path")

    # Legend: get DEXPI legend/schema info
    subparsers.add_parser("legend", help="Get DEXPI legend information")

    # Check: verify pydexpi availability
    subparsers.add_parser("check", help="Check pydexpi installation")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "check":
        output_json({
            "success": True,
            "pydexpiAvailable": PYDEXPI_AVAILABLE,
            "pydexpiVersion": "1.1.0" if PYDEXPI_AVAILABLE else None,
        })
        return

    if args.command == "legend":
        # Return legend without Neo4j connection
        converter = DexpiConverter.__new__(DexpiConverter)
        converter.classifier = DexpiClassifier()
        legend = converter._build_legend({
            cat: 0 for cat in DEXPI_CATEGORIES
        })
        output_json({"success": True, "legend": legend})
        return

    converter = DexpiConverter()

    try:
        if args.command == "convert":
            result = converter.convert(
                node_types=args.types,
                limit=args.limit,
                include_scada=args.include_scada,
                include_troubleshooting=args.include_troubleshooting,
            )
        elif args.command == "export":
            result = converter.export_dexpi_json(
                node_types=args.types,
                limit=args.limit,
            )
            # Write to file if output specified
            if result["success"] and args.output:
                with open(args.output, "w") as f:
                    json.dump(result["dexpiModel"], f, indent=2, cls=DateTimeEncoder)
                result["outputFile"] = args.output
        else:
            output_error(f"Unknown command: {args.command}")
            return

        output_json(result)

    except Exception as e:
        output_error(str(e))
    finally:
        converter.close()


if __name__ == "__main__":
    main()
