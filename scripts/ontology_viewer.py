#!/usr/bin/env python3
"""
Interactive HTML graph visualization for PLC/SCADA ontologies.
Generates an expandable, zoomable graph using D3.js.
Reads data from Neo4j graph database.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, date

from neo4j_ontology import OntologyGraph, get_ontology_graph


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles DateTime and other non-serializable types."""
    
    def default(self, obj):
        # Handle datetime objects
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # Handle neo4j DateTime objects (they have isoformat method)
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        # Handle neo4j DateTime objects that might have to_native method
        if hasattr(obj, 'to_native'):
            return str(obj.to_native())
        # Fallback for any other non-serializable objects
        try:
            return str(obj)
        except Exception:
            return super().default(obj)


class OntologyType(Enum):
    L5X = "l5x"
    IGNITION = "ignition"
    UNIFIED = "unified"


@dataclass
class Node:
    id: str
    label: str
    type: str  # aoi, udt, equipment, view, flow, safety, etc.
    group: str  # plc, scada, flows, safety, etc.
    details: Dict[str, Any]
    expanded: bool = False


@dataclass
class Edge:
    source: str
    target: str
    label: str
    type: str  # mapping, instance, flow, etc.


class OntologyViewer:
    """Generate interactive HTML visualization from Neo4j ontology."""

    COLORS = {
        # Groups
        "plc": "#1976D2",
        "scada": "#388E3C",
        "flows": "#C2185B",
        "troubleshooting": "#FF5722",
        "patterns": "#795548",
        "overview": "#607D8B",
        "other": "#9E9E9E",
        # Node types
        "aoi": "#F57C00",
        "aoiinstance": "#F57C00",
        "tag": "#009688",
        "udt": "#7B1FA2",
        "udttype": "#7B1FA2",
        "equipment": "#FBC02D",
        "view": "#00ACC1",
        "perspectiveview": "#00ACC1",
        "project": "#3F51B5",
        "endtoendflow": "#E91E63",
        "dataflow": "#E91E63",
        "faultsymptom": "#FF5722",
        "operatorphrase": "#FF7043",
        "commonphrase": "#FFAB91",
        "controlpattern": "#795548",
        "safetyelement": "#D32F2F",
        "systemoverview": "#607D8B",
    }

    def __init__(self, graph: Optional[OntologyGraph] = None):
        self.nodes: List[Node] = []
        self.edges: List[Edge] = []
        self.node_counter = 0
        self._graph = graph
        self._owns_graph = False

    def _get_graph(self) -> OntologyGraph:
        """Get or create Neo4j connection."""
        if self._graph is None:
            self._graph = get_ontology_graph()
            self._owns_graph = True
        return self._graph

    def close(self):
        """Close Neo4j connection if we own it."""
        if self._owns_graph and self._graph:
            self._graph.close()
            self._graph = None

    def _node_id(self, prefix: str = "n") -> str:
        self.node_counter += 1
        return f"{prefix}_{self.node_counter}"

    def load_from_neo4j(self, include_details: bool = True) -> None:
        """Load graph data from Neo4j - grabs ALL nodes and relationships."""
        graph = self._get_graph()

        with graph.session() as session:
            # Get ALL nodes from the database
            nodes_result = session.run(
                """
                MATCH (n)
                RETURN elementId(n) as id, 
                       labels(n)[0] as type, 
                       coalesce(n.name, n.symptom, n.phrase, n.key, n.pattern_name, 'unknown') as label,
                       properties(n) as props
            """
            )

            for record in nodes_result:
                node_type = record["type"].lower() if record["type"] else "unknown"

                # Determine group based on node type
                if node_type in ("aoi", "aoiinstance", "tag"):
                    group = "plc"
                elif node_type in (
                    "udt",
                    "udtype",
                    "equipment",
                    "view",
                    "perspectiveview",
                    "project",
                ):
                    group = "scada"
                elif node_type in ("faultsymptom", "operatorphrase", "commonphrase"):
                    group = "troubleshooting"
                elif node_type in ("controlpattern", "safetyelement"):
                    group = "patterns"
                elif node_type in ("endtoendflow", "dataflow"):
                    group = "flows"
                elif node_type in ("systemoverview",):
                    group = "overview"
                else:
                    group = "other"

                self.nodes.append(
                    Node(
                        id=str(record["id"]),
                        label=record["label"],
                        type=node_type,
                        group=group,
                        details=dict(record["props"]) if include_details else {},
                    )
                )

            # Get ALL relationships
            edges_result = session.run(
                """
                MATCH (a)-[r]->(b)
                RETURN elementId(a) as source, elementId(b) as target, type(r) as type,
                       properties(r) as props
            """
            )

            for record in edges_result:
                self.edges.append(
                    Edge(
                        source=str(record["source"]),
                        target=str(record["target"]),
                        label=(
                            record["props"].get("mapping_type", record["type"])
                            if record["props"]
                            else record["type"]
                        ),
                        type=record["type"],
                    )
                )

    def _load_extended_data(self) -> None:
        """Load extended data like tags, patterns, etc."""
        graph = self._get_graph()

        with graph.session() as session:
            # Load fault symptoms for troubleshooting view
            symptoms_result = session.run(
                """
                MATCH (a:AOI)-[:HAS_SYMPTOM]->(s:FaultSymptom)
                RETURN elementId(a) as aoi_id, elementId(s) as symptom_id, s.symptom as symptom,
                       s.resolution_steps as steps
            """
            )

            for record in symptoms_result:
                self.nodes.append(
                    Node(
                        id=str(record["symptom_id"]),
                        label=(
                            record["symptom"][:40] + "..."
                            if len(record["symptom"]) > 40
                            else record["symptom"]
                        ),
                        type="faultsymptom",
                        group="troubleshooting",
                        details={
                            "symptom": record["symptom"],
                            "resolution_steps": record["steps"],
                        },
                    )
                )
                self.edges.append(
                    Edge(
                        source=str(record["aoi_id"]),
                        target=str(record["symptom_id"]),
                        label="HAS_SYMPTOM",
                        type="troubleshooting",
                    )
                )

            # Load control patterns
            patterns_result = session.run(
                """
                MATCH (a:AOI)-[:HAS_PATTERN]->(p:ControlPattern)
                RETURN elementId(a) as aoi_id, elementId(p) as pattern_id, 
                       p.name as name, p.description as description
            """
            )

            for record in patterns_result:
                self.nodes.append(
                    Node(
                        id=str(record["pattern_id"]),
                        label=record["name"],
                        type="controlpattern",
                        group="patterns",
                        details={"description": record["description"]},
                    )
                )
                self.edges.append(
                    Edge(
                        source=str(record["aoi_id"]),
                        target=str(record["pattern_id"]),
                        label="HAS_PATTERN",
                        type="pattern",
                    )
                )

    def detect_type(self, data: Any) -> OntologyType:
        """Legacy method for JSON compatibility."""
        if isinstance(data, list):
            return OntologyType.L5X
        if data.get("type") == "unified_system_ontology":
            return OntologyType.UNIFIED
        if data.get("source") == "ignition":
            return OntologyType.IGNITION
        return OntologyType.L5X

    def parse(self, data: Dict) -> None:
        """Legacy: Parse ontology data from JSON into nodes and edges."""
        ont_type = self.detect_type(data)

        if ont_type == OntologyType.UNIFIED:
            self._parse_unified(data)
        elif ont_type == OntologyType.IGNITION:
            self._parse_ignition(data)
        else:
            self._parse_l5x(data)

    def _parse_l5x(self, data: Any) -> None:
        """Parse L5X ontology from JSON."""
        aois = data if isinstance(data, list) else [data]

        for aoi in aois:
            name = aoi.get("name", "Unknown")
            analysis = aoi.get("analysis", {})

            node = Node(
                id=self._node_id("aoi"),
                label=name,
                type="aoi",
                group="plc",
                details={
                    "purpose": analysis.get("purpose", ""),
                    "tags": analysis.get("tags", {}),
                    "relationships": analysis.get("relationships", []),
                    "category": analysis.get("category", ""),
                },
            )
            self.nodes.append(node)

    def _parse_ignition(self, data: Dict) -> None:
        """Parse Ignition ontology from JSON."""
        analysis = data.get("analysis", {})

        # UDTs
        for udt_name, udt_purpose in analysis.get("udt_semantics", {}).items():
            node = Node(
                id=self._node_id("udt"),
                label=udt_name,
                type="udt",
                group="scada",
                details={"purpose": udt_purpose},
            )
            self.nodes.append(node)

        # Equipment instances
        for equip in analysis.get("equipment_instances", []):
            node = Node(
                id=self._node_id("equip"),
                label=equip.get("name", "Unknown"),
                type="equipment",
                group="scada",
                details={
                    "type": equip.get("type", ""),
                    "purpose": equip.get("purpose", ""),
                },
            )
            self.nodes.append(node)

        # Views
        for view_name, view_purpose in analysis.get("view_purposes", {}).items():
            node = Node(
                id=self._node_id("view"),
                label=view_name,
                type="view",
                group="scada",
                details={"purpose": view_purpose},
            )
            self.nodes.append(node)

    def _parse_unified(self, data: Dict) -> None:
        """Parse unified ontology from JSON."""
        ua = data.get("unified_analysis", {})

        # System overview
        self.nodes.append(
            Node(
                id="overview",
                label="System Overview",
                type="overview",
                group="overview",
                details={"description": ua.get("system_overview", "")},
            )
        )

        # PLC components
        plc_ontology = data.get("component_ontologies", {}).get("plc", [])
        plc_node_map = {}
        if isinstance(plc_ontology, list):
            for aoi in plc_ontology:
                name = aoi.get("name", "Unknown")
                analysis = aoi.get("analysis", {})
                node_id = self._node_id("plc")
                plc_node_map[name] = node_id

                self.nodes.append(
                    Node(
                        id=node_id,
                        label=name,
                        type="aoi",
                        group="plc",
                        details={
                            "purpose": analysis.get("purpose", ""),
                            "tags": analysis.get("tags", {}),
                            "relationships": analysis.get("relationships", []),
                        },
                    )
                )

        # SCADA components
        scada_ontology = data.get("component_ontologies", {}).get("scada", {})
        scada_analysis = scada_ontology.get("analysis", {})
        scada_node_map = {}

        for udt_name, udt_purpose in scada_analysis.get("udt_semantics", {}).items():
            node_id = self._node_id("scada")
            scada_node_map[udt_name] = node_id
            self.nodes.append(
                Node(
                    id=node_id,
                    label=udt_name,
                    type="udt",
                    group="scada",
                    details={"purpose": udt_purpose},
                )
            )

        for equip in scada_analysis.get("equipment_instances", []):
            name = equip.get("name", "")
            node_id = self._node_id("equip")
            scada_node_map[name] = node_id
            self.nodes.append(
                Node(
                    id=node_id,
                    label=name,
                    type="equipment",
                    group="scada",
                    details={
                        "type": equip.get("type", ""),
                        "purpose": equip.get("purpose", ""),
                    },
                )
            )

        # PLC-to-SCADA mappings
        for mapping in ua.get("plc_to_scada_mappings", []):
            plc_comp = mapping.get("plc_component", "")
            scada_comp = mapping.get("scada_component", "")

            # Find matching nodes
            plc_node = None
            scada_node = None

            for name, node_id in plc_node_map.items():
                if plc_comp in name or name in plc_comp:
                    plc_node = node_id
                    break

            for name, node_id in scada_node_map.items():
                if any(s in name or name in s for s in scada_comp.split()):
                    scada_node = node_id
                    break

            if plc_node and scada_node:
                self.edges.append(
                    Edge(
                        source=plc_node,
                        target=scada_node,
                        label=mapping.get("mapping_type", ""),
                        type="mapping",
                    )
                )

        # End-to-end flows
        for i, flow in enumerate(ua.get("end_to_end_flows", [])):
            flow_name = flow.get("flow_name", flow.get("name", f"Flow_{i}"))
            self.nodes.append(
                Node(
                    id=self._node_id("flow"),
                    label=flow_name,
                    type="flow",
                    group="flows",
                    details=flow,
                )
            )

        # Safety architecture
        safety = ua.get("safety_architecture", {})
        for layer_name, layer_details in safety.items():
            self.nodes.append(
                Node(
                    id=self._node_id("safety"),
                    label=layer_name.replace("_", " ").title(),
                    type="safety",
                    group="safety",
                    details=(
                        layer_details
                        if isinstance(layer_details, dict)
                        else {"description": str(layer_details)}
                    ),
                )
            )

        # Control responsibilities
        ctrl = ua.get("control_responsibilities", {})
        for layer, funcs in ctrl.items():
            self.nodes.append(
                Node(
                    id=self._node_id("ctrl"),
                    label=layer.replace("_", " ").title(),
                    type="responsibility",
                    group="responsibilities",
                    details=funcs if isinstance(funcs, dict) else {"functions": funcs},
                )
            )

        # Recommendations
        for i, rec in enumerate(ua.get("recommendations", [])):
            if isinstance(rec, dict):
                label = rec.get("category", rec.get("title", f"Recommendation {i+1}"))
                details = rec
            else:
                label = f"Recommendation {i+1}"
                details = {"text": str(rec)}

            self.nodes.append(
                Node(
                    id=self._node_id("rec"),
                    label=label,
                    type="recommendation",
                    group="recommendations",
                    details=details,
                )
            )

    def generate_html(self, title: str = "Ontology Viewer") -> str:
        """Generate interactive HTML visualization."""

        # Convert nodes and edges to JSON (using custom encoder for DateTime objects)
        nodes_json = json.dumps(
            [
                {
                    "id": n.id,
                    "label": n.label,
                    "type": n.type,
                    "group": n.group,
                    "details": n.details,
                }
                for n in self.nodes
            ],
            cls=DateTimeEncoder
        )

        edges_json = json.dumps(
            [
                {
                    "source": e.source,
                    "target": e.target,
                    "label": e.label,
                    "type": e.type,
                }
                for e in self.edges
            ],
            cls=DateTimeEncoder
        )

        colors_json = json.dumps(self.COLORS, cls=DateTimeEncoder)

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #eee;
            overflow: hidden;
        }}
        #container {{
            display: flex;
            height: 100vh;
        }}
        #graph {{
            flex: 1;
            position: relative;
        }}
        #sidebar {{
            width: 400px;
            background: #16213e;
            border-left: 1px solid #0f3460;
            overflow-y: auto;
            padding: 20px;
            transition: width 0.3s;
        }}
        #sidebar.collapsed {{
            width: 0;
            padding: 0;
            overflow: hidden;
        }}
        #toggle-sidebar {{
            position: absolute;
            right: 410px;
            top: 10px;
            background: #0f3460;
            border: none;
            color: #eee;
            padding: 8px 12px;
            cursor: pointer;
            border-radius: 4px;
            z-index: 100;
        }}
        #sidebar.collapsed + #toggle-sidebar,
        #toggle-sidebar.shifted {{
            right: 10px;
        }}
        .node {{
            cursor: pointer;
            transition: all 0.2s;
        }}
        .node:hover {{
            filter: brightness(1.2);
        }}
        .node-label {{
            font-size: 11px;
            font-weight: 500;
            pointer-events: none;
            text-anchor: middle;
        }}
        .link {{
            stroke-opacity: 0.6;
            fill: none;
        }}
        .link-label {{
            font-size: 9px;
            fill: #888;
        }}
        #details h2 {{
            color: #e94560;
            margin-bottom: 15px;
            font-size: 18px;
        }}
        #details h3 {{
            color: #0f3460;
            background: #e94560;
            padding: 8px 12px;
            margin: 15px -20px;
            font-size: 14px;
        }}
        .detail-section {{
            margin-bottom: 15px;
        }}
        .detail-section h4 {{
            color: #00d9ff;
            font-size: 12px;
            margin-bottom: 5px;
            text-transform: uppercase;
        }}
        .detail-section p, .detail-section pre {{
            color: #ccc;
            font-size: 13px;
            line-height: 1.5;
            white-space: pre-wrap;
            word-break: break-word;
        }}
        .detail-section pre {{
            background: #0f3460;
            padding: 10px;
            border-radius: 4px;
            overflow-x: auto;
            font-family: 'Fira Code', monospace;
            font-size: 11px;
        }}
        .tag-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 5px;
        }}
        .tag {{
            background: #0f3460;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 11px;
            cursor: pointer;
            transition: background 0.2s;
        }}
        .tag:hover {{
            background: #e94560;
        }}
        .tag-expanded {{
            display: block;
            width: 100%;
            background: #0a1628;
            margin-top: 5px;
            padding: 8px;
            border-radius: 4px;
        }}
        #legend {{
            position: absolute;
            bottom: 20px;
            left: 20px;
            background: rgba(22, 33, 62, 0.95);
            padding: 15px 20px;
            border-radius: 8px;
            font-size: 12px;
            max-height: calc(100vh - 100px);
            overflow-y: auto;
            min-width: 180px;
        }}
        #legend h4 {{
            color: #e94560;
            font-size: 14px;
            margin-bottom: 12px;
            padding-bottom: 8px;
            border-bottom: 1px solid #0f3460;
        }}
        .legend-group {{
            margin-bottom: 12px;
        }}
        .legend-group-title {{
            color: #00d9ff;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 6px;
            font-weight: 600;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 4px 0;
            padding: 2px 0;
        }}
        .legend-color {{
            width: 14px;
            height: 14px;
            border-radius: 50%;
            margin-right: 10px;
            flex-shrink: 0;
            border: 1px solid rgba(255,255,255,0.2);
        }}
        .legend-label {{
            color: #ccc;
            font-size: 11px;
        }}
        #controls {{
            position: absolute;
            top: 10px;
            left: 10px;
            display: flex;
            gap: 10px;
        }}
        #controls button, #controls select {{
            background: #0f3460;
            border: none;
            color: #eee;
            padding: 8px 12px;
            cursor: pointer;
            border-radius: 4px;
            font-size: 12px;
        }}
        #controls button:hover {{
            background: #e94560;
        }}
        #search {{
            background: #0f3460;
            border: 1px solid #1a1a2e;
            color: #eee;
            padding: 8px 12px;
            border-radius: 4px;
            width: 200px;
        }}
        .highlight {{
            stroke: #e94560 !important;
            stroke-width: 3px !important;
        }}
        .dimmed {{
            opacity: 0.2;
        }}
        #neo4j-info {{
            position: absolute;
            bottom: 20px;
            right: 420px;
            background: rgba(22, 33, 62, 0.9);
            padding: 10px 15px;
            border-radius: 8px;
            font-size: 11px;
            color: #888;
        }}
    </style>
</head>
<body>
    <div id="container">
        <div id="graph">
            <div id="controls">
                <input type="text" id="search" placeholder="Search nodes...">
                <select id="filter-group">
                    <option value="all">All Groups</option>
                    <option value="plc">PLC Layer</option>
                    <option value="scada">SCADA Layer</option>
                    <option value="flows">Data Flows</option>
                    <option value="troubleshooting">Troubleshooting</option>
                    <option value="patterns">Patterns & Safety</option>
                    <option value="overview">Overview</option>
                    <option value="other">Other</option>
                </select>
                <button id="reset-zoom">Reset Zoom</button>
                <button id="expand-all">Expand All</button>
                <button id="collapse-all">Collapse All</button>
            </div>
            <div id="legend"></div>
            <div id="neo4j-info">Data source: Neo4j Graph Database</div>
        </div>
        <div id="sidebar">
            <div id="details">
                <h2>Select a node</h2>
                <p style="color: #888;">Click on any node in the graph to view its details here.</p>
            </div>
        </div>
        <button id="toggle-sidebar">◀</button>
    </div>

    <script>
        const nodes = {nodes_json};
        const links = {edges_json};
        const colors = {colors_json};

        // Setup
        const container = document.getElementById('graph');
        const width = container.clientWidth;
        const height = container.clientHeight;

        const svg = d3.select('#graph')
            .append('svg')
            .attr('width', '100%')
            .attr('height', '100%')
            .attr('viewBox', [0, 0, width, height]);

        // Zoom
        const g = svg.append('g');
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => g.attr('transform', event.transform));
        svg.call(zoom);

        // Arrow marker
        svg.append('defs').append('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 20)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('fill', '#888')
            .attr('d', 'M0,-5L10,0L0,5');

        // Force simulation
        const simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(150))
            .force('charge', d3.forceManyBody().strength(-400))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(50));

        // Group clusters
        const groups = [...new Set(nodes.map(n => n.group))];
        const groupCenters = {{}};
        groups.forEach((group, i) => {{
            const angle = (2 * Math.PI * i) / groups.length;
            groupCenters[group] = {{
                x: width/2 + Math.cos(angle) * 250,
                y: height/2 + Math.sin(angle) * 250
            }};
        }});

        simulation.force('group', d3.forceX(d => groupCenters[d.group]?.x || width/2).strength(0.1))
                  .force('groupY', d3.forceY(d => groupCenters[d.group]?.y || height/2).strength(0.1));

        // Links
        const link = g.append('g')
            .selectAll('line')
            .data(links)
            .join('line')
            .attr('class', 'link')
            .attr('stroke', '#444')
            .attr('stroke-width', 2)
            .attr('marker-end', 'url(#arrow)');

        // Link labels
        const linkLabel = g.append('g')
            .selectAll('text')
            .data(links)
            .join('text')
            .attr('class', 'link-label')
            .text(d => d.label);

        // Nodes
        const node = g.append('g')
            .selectAll('g')
            .data(nodes)
            .join('g')
            .attr('class', 'node')
            .call(d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended));

        // Node circles
        node.append('circle')
            .attr('r', d => d.type === 'overview' ? 30 : 20)
            .attr('fill', d => colors[d.type] || colors[d.group] || '#666')
            .attr('stroke', '#fff')
            .attr('stroke-width', 2);

        // Node labels
        node.append('text')
            .attr('class', 'node-label')
            .attr('dy', 35)
            .attr('fill', '#fff')
            .text(d => d.label.length > 20 ? d.label.substring(0, 18) + '...' : d.label);

        // Node click handler
        node.on('click', (event, d) => {{
            showDetails(d);
            highlightNode(d);
        }});

        // Simulation tick
        simulation.on('tick', () => {{
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);

            linkLabel
                .attr('x', d => (d.source.x + d.target.x) / 2)
                .attr('y', d => (d.source.y + d.target.y) / 2);

            node.attr('transform', d => `translate(${{d.x}},${{d.y}})`);
        }});

        // Drag functions
        function dragstarted(event) {{
            if (!event.active) simulation.alphaTarget(0.3).restart();
            event.subject.fx = event.subject.x;
            event.subject.fy = event.subject.y;
        }}

        function dragged(event) {{
            event.subject.fx = event.x;
            event.subject.fy = event.y;
        }}

        function dragended(event) {{
            if (!event.active) simulation.alphaTarget(0);
            event.subject.fx = null;
            event.subject.fy = null;
        }}

        // Show details in sidebar
        function showDetails(d) {{
            const details = document.getElementById('details');
            let html = `<h2>${{d.label}}</h2>`;
            html += `<div class="detail-section"><h4>Type</h4><p>${{d.type}} (${{d.group}})</p></div>`;

            // Render details based on type
            for (const [key, value] of Object.entries(d.details)) {{
                if (value === null || value === undefined || value === '') continue;

                html += `<div class="detail-section"><h4>${{key.replace(/_/g, ' ')}}</h4>`;

                if (key === 'tags' && typeof value === 'object') {{
                    html += '<div class="tag-list">';
                    for (const [tagName, tagDesc] of Object.entries(value)) {{
                        const descStr = typeof tagDesc === 'string' ? tagDesc : JSON.stringify(tagDesc, null, 2);
                        html += `<div class="tag" onclick="this.classList.toggle('expanded'); this.nextElementSibling.style.display = this.classList.contains('expanded') ? 'block' : 'none';">${{tagName}}</div>`;
                        html += `<div class="tag-expanded" style="display:none;"><pre>${{escapeHtml(descStr)}}</pre></div>`;
                    }}
                    html += '</div>';
                }} else if (Array.isArray(value)) {{
                    html += '<pre>' + escapeHtml(JSON.stringify(value, null, 2)) + '</pre>';
                }} else if (typeof value === 'object') {{
                    html += '<pre>' + escapeHtml(JSON.stringify(value, null, 2)) + '</pre>';
                }} else {{
                    html += `<p>${{escapeHtml(String(value))}}</p>`;
                }}

                html += '</div>';
            }}

            details.innerHTML = html;
        }}

        function escapeHtml(text) {{
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }}

        function highlightNode(d) {{
            // Reset all
            node.classed('dimmed', false).classed('highlight', false);
            link.classed('dimmed', false).classed('highlight', false);

            // Find connected nodes
            const connectedIds = new Set([d.id]);
            links.forEach(l => {{
                if (l.source.id === d.id) connectedIds.add(l.target.id);
                if (l.target.id === d.id) connectedIds.add(l.source.id);
            }});

            // Dim non-connected
            node.classed('dimmed', n => !connectedIds.has(n.id));
            link.classed('dimmed', l => l.source.id !== d.id && l.target.id !== d.id);
            link.classed('highlight', l => l.source.id === d.id || l.target.id === d.id);
        }}

        // Controls
        document.getElementById('reset-zoom').onclick = () => {{
            svg.transition().duration(750).call(zoom.transform, d3.zoomIdentity);
        }};

        document.getElementById('toggle-sidebar').onclick = function() {{
            const sidebar = document.getElementById('sidebar');
            sidebar.classList.toggle('collapsed');
            this.textContent = sidebar.classList.contains('collapsed') ? '▶' : '◀';
            this.classList.toggle('shifted');
        }};

        document.getElementById('search').oninput = function() {{
            const query = this.value.toLowerCase();
            if (!query) {{
                node.classed('dimmed', false);
                link.classed('dimmed', false);
                return;
            }}
            node.classed('dimmed', d => !d.label.toLowerCase().includes(query));
            link.classed('dimmed', true);
        }};

        document.getElementById('filter-group').onchange = function() {{
            const group = this.value;
            if (group === 'all') {{
                node.classed('dimmed', false);
                link.classed('dimmed', false);
                return;
            }}
            node.classed('dimmed', d => d.group !== group);
            link.classed('dimmed', l => l.source.group !== group && l.target.group !== group);
        }};

        // Legend - organized by group with human-readable labels
        const legendLabels = {{
            // Node types with friendly names
            'aoi': 'Add-On Instruction',
            'aoiinstance': 'AOI Instance',
            'tag': 'Tag',
            'udt': 'User-Defined Type',
            'udttype': 'UDT Type',
            'equipment': 'Equipment',
            'view': 'View',
            'perspectiveview': 'Perspective View',
            'project': 'Project',
            'endtoendflow': 'End-to-End Flow',
            'dataflow': 'Data Flow',
            'faultsymptom': 'Fault Symptom',
            'operatorphrase': 'Operator Phrase',
            'commonphrase': 'Common Phrase',
            'controlpattern': 'Control Pattern',
            'safetyelement': 'Safety Element',
            'systemoverview': 'System Overview',
            'flow': 'Flow',
            'safety': 'Safety',
            'responsibility': 'Responsibility',
            'recommendation': 'Recommendation',
            'overview': 'Overview'
        }};

        const groupLabels = {{
            'plc': 'PLC Layer',
            'scada': 'SCADA / HMI',
            'flows': 'Data Flows',
            'troubleshooting': 'Troubleshooting',
            'patterns': 'Patterns & Safety',
            'overview': 'System Overview',
            'other': 'Other'
        }};

        // Get unique types present in the graph, organized by group
        const presentTypes = [...new Set(nodes.map(n => n.type))];
        const presentGroups = [...new Set(nodes.map(n => n.group))];
        
        // Build legend HTML organized by group
        const legend = document.getElementById('legend');
        let legendHtml = '<h4>Legend</h4>';
        
        // Define group order
        const groupOrder = ['plc', 'scada', 'flows', 'troubleshooting', 'patterns', 'overview', 'other'];
        
        groupOrder.forEach(group => {{
            if (!presentGroups.includes(group)) return;
            
            // Get types in this group that are present
            const typesInGroup = presentTypes.filter(type => {{
                const nodesOfType = nodes.filter(n => n.type === type);
                return nodesOfType.some(n => n.group === group);
            }});
            
            if (typesInGroup.length === 0) return;
            
            legendHtml += `<div class="legend-group">`;
            legendHtml += `<div class="legend-group-title">${{groupLabels[group] || group}}</div>`;
            
            typesInGroup.forEach(type => {{
                const label = legendLabels[type] || type.charAt(0).toUpperCase() + type.slice(1);
                const color = colors[type] || colors[group] || '#666';
                legendHtml += `
                    <div class="legend-item">
                        <div class="legend-color" style="background: ${{color}}"></div>
                        <span class="legend-label">${{label}}</span>
                    </div>`;
            }});
            
            legendHtml += `</div>`;
        }});
        
        legend.innerHTML = legendHtml;

        // Double-click to reset highlight
        svg.on('dblclick', () => {{
            node.classed('dimmed', false).classed('highlight', false);
            link.classed('dimmed', false).classed('highlight', false);
        }});
    </script>
</body>
</html>"""
        return html

    def save(self, output_path: str, title: str = "Ontology Viewer") -> None:
        """Save HTML visualization to file."""
        html = self.generate_html(title)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)


def main():
    parser = argparse.ArgumentParser(
        description="Generate interactive HTML visualization from Neo4j ontology"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="ontologies/neo4j_viewer.html",
        help="Output HTML file path",
    )
    parser.add_argument(
        "--title", "-t", default="PLC/SCADA Ontology Viewer", help="Page title"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--no-details", action="store_true", help="Exclude extended details (faster)"
    )

    # Legacy JSON support
    parser.add_argument(
        "--json", metavar="FILE", help="Load from JSON file instead of Neo4j (legacy)"
    )

    args = parser.parse_args()

    viewer = OntologyViewer()

    try:
        if args.json:
            # Legacy JSON mode
            with open(args.json, "r", encoding="utf-8") as f:
                data = json.load(f)
            viewer.parse(data)
            if args.verbose:
                print(
                    f"[INFO] Loaded from JSON: {len(viewer.nodes)} nodes, {len(viewer.edges)} edges"
                )
        else:
            # Neo4j mode
            if args.verbose:
                print("[INFO] Loading from Neo4j...")
            viewer.load_from_neo4j(include_details=not args.no_details)
            if args.verbose:
                print(
                    f"[INFO] Loaded from Neo4j: {len(viewer.nodes)} nodes, {len(viewer.edges)} edges"
                )

        viewer.save(args.output, args.title)
        print(f"[OK] Generated interactive viewer: {args.output}")
        print(f"[INFO] Open in browser to view")

    finally:
        viewer.close()


if __name__ == "__main__":
    main()
