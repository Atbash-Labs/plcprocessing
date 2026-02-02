#!/usr/bin/env python3
"""
Graph API for Electron UI - CRUD operations for ontology graph visualization and editing.
Provides JSON-based interface for graph queries and mutations.
"""

import json
import argparse
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime, date

from neo4j_ontology import OntologyGraph, get_ontology_graph


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        if hasattr(obj, "to_native"):
            return str(obj.to_native())
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


class GraphAPI:
    """Graph API for Electron UI interactions."""

    # Node type to group mapping for visualization
    NODE_GROUPS = {
        "aoi": "plc",
        "aoiinstance": "plc",
        "tag": "plc",
        "udt": "scada",
        "udttype": "scada",
        "equipment": "scada",
        "view": "scada",
        "perspectiveview": "scada",
        "project": "scada",
        "script": "scada",
        "namedquery": "scada",
        "gatewayevent": "scada",
        "viewcomponent": "scada",
        "scadatag": "scada",
        "faultsymptom": "troubleshooting",
        "faultcause": "troubleshooting",
        "operatorphrase": "troubleshooting",
        "commonphrase": "troubleshooting",
        "intent": "troubleshooting",
        "controlpattern": "patterns",
        "safetyelement": "safety",
        "dataflow": "flows",
        "endtoendflow": "flows",
        "systemoverview": "overview",
        "material": "mes",
        "batch": "mes",
        "productionorder": "mes",
        "operation": "mes",
        "ccp": "mes",
        "processdeviation": "mes",
        "functionallocation": "mes",
        "vendor": "mes",
    }

    # Color palette for node types
    NODE_COLORS = {
        "plc": "#F57C00",
        "scada": "#7B1FA2",
        "troubleshooting": "#FF5722",
        "patterns": "#795548",
        "safety": "#D32F2F",
        "flows": "#E91E63",
        "overview": "#607D8B",
        "mes": "#00897B",
        "other": "#9E9E9E",
    }

    def __init__(self, graph: Optional[OntologyGraph] = None):
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

    def _get_node_group(self, node_type: str) -> str:
        """Get group for a node type."""
        return self.NODE_GROUPS.get(node_type.lower(), "other")

    def _format_node(self, record: Dict) -> Dict:
        """Format a node record for Cytoscape.js."""
        node_type = record.get("type", "unknown")
        if node_type:
            node_type = node_type.lower()
        group = self._get_node_group(node_type)

        return {
            "id": str(record["id"]),
            "label": record.get("label", record.get("name", "Unknown")),
            "type": node_type,
            "group": group,
            "color": self.NODE_COLORS.get(group, self.NODE_COLORS["other"]),
            "properties": record.get("props", {}),
        }

    def _format_edge(self, record: Dict) -> Dict:
        """Format an edge record for Cytoscape.js."""
        return {
            "id": f"{record['source']}-{record['type']}-{record['target']}",
            "source": str(record["source"]),
            "target": str(record["target"]),
            "type": record.get("type", "RELATED"),
            "label": record.get("label", record.get("type", "")),
            "properties": record.get("props", {}),
        }

    # =========================================================================
    # Read Operations
    # =========================================================================

    def load_graph(
        self, node_types: Optional[List[str]] = None, limit: int = 500
    ) -> Dict:
        """
        Load graph data for visualization.

        Args:
            node_types: Optional list of node types to include
            limit: Maximum number of nodes to return

        Returns:
            Dict with nodes and edges arrays
        """
        graph = self._get_graph()
        nodes = []
        edges = []

        with graph.session() as session:
            # Build node query
            if node_types:
                labels = " OR ".join([f"n:{t}" for t in node_types])
                node_query = f"""
                    MATCH (n) WHERE {labels}
                    RETURN elementId(n) as id,
                           labels(n)[0] as type,
                           coalesce(n.name, n.symptom, n.phrase, n.key, n.pattern_name, 'unknown') as label,
                           properties(n) as props
                    LIMIT $limit
                """
            else:
                node_query = """
                    MATCH (n)
                    RETURN elementId(n) as id,
                           labels(n)[0] as type,
                           coalesce(n.name, n.symptom, n.phrase, n.key, n.pattern_name, 'unknown') as label,
                           properties(n) as props
                    LIMIT $limit
                """

            result = session.run(node_query, limit=limit)
            node_ids = set()

            for record in result:
                node = self._format_node(dict(record))
                nodes.append(node)
                node_ids.add(node["id"])

            # Get edges between loaded nodes
            if node_ids:
                edge_query = """
                    MATCH (a)-[r]->(b)
                    WHERE elementId(a) IN $node_ids AND elementId(b) IN $node_ids
                    RETURN elementId(a) as source, 
                           elementId(b) as target, 
                           type(r) as type,
                           properties(r) as props
                """
                result = session.run(edge_query, node_ids=list(node_ids))

                for record in result:
                    edges.append(self._format_edge(dict(record)))

        return {
            "success": True,
            "nodes": nodes,
            "edges": edges,
            "nodeCount": len(nodes),
            "edgeCount": len(edges),
        }

    def get_neighbors(
        self,
        node_id: str,
        node_type: str = None,
        hops: int = 1,
        max_nodes: int = 50,
        include_types: Optional[List[str]] = None,
    ) -> Dict:
        """
        Get neighbors of a node up to N hops away.

        Args:
            node_id: Node name or element ID
            node_type: Node label (e.g., 'Equipment', 'AOI')
            hops: Number of relationship hops (1-3)
            max_nodes: Maximum nodes to return
            include_types: Optional list of node types to include

        Returns:
            Dict with center node, neighbor nodes, and edges
        """
        graph = self._get_graph()
        hops = min(max(1, hops), 3)  # Clamp to 1-3

        nodes = []
        edges = []

        with graph.session() as session:
            # Find the center node - try exact match first, then partial match
            if node_type:
                # Try exact match first
                center_query = f"""
                    MATCH (center:{node_type})
                    WHERE center.name = $node_id 
                       OR center.name ENDS WITH $node_id
                       OR center.name CONTAINS $node_id
                    RETURN elementId(center) as id,
                           labels(center)[0] as type,
                           center.name as label,
                           properties(center) as props
                    LIMIT 1
                """
            else:
                center_query = """
                    MATCH (center)
                    WHERE center.name = $node_id 
                       OR center.name ENDS WITH $node_id
                       OR center.name CONTAINS $node_id
                    RETURN elementId(center) as id,
                           labels(center)[0] as type,
                           center.name as label,
                           properties(center) as props
                    LIMIT 1
                """

            result = session.run(center_query, node_id=node_id)
            record = result.single()

            if not record:
                return {"success": False, "error": f"Node not found: {node_id}"}

            center_node = self._format_node(dict(record))
            center_node["isCenter"] = True
            nodes.append(center_node)
            node_ids = {center_node["id"]}
            center_element_id = center_node["id"]

            # Get neighbors up to N hops
            type_filter = ""
            if include_types:
                type_labels = " OR ".join([f"neighbor:{t}" for t in include_types])
                type_filter = f"AND ({type_labels})"

            neighbor_query = f"""
                MATCH path = (center)-[*1..{hops}]-(neighbor)
                WHERE elementId(center) = $center_id {type_filter}
                WITH neighbor, min(length(path)) as distance
                RETURN DISTINCT elementId(neighbor) as id,
                       labels(neighbor)[0] as type,
                       coalesce(neighbor.name, neighbor.symptom, neighbor.phrase, 'unknown') as label,
                       properties(neighbor) as props,
                       distance
                ORDER BY distance
                LIMIT $limit
            """

            result = session.run(
                neighbor_query, center_id=center_element_id, limit=max_nodes
            )

            for record in result:
                node = self._format_node(dict(record))
                node["distance"] = record.get("distance", 1)
                nodes.append(node)
                node_ids.add(node["id"])

            # Get edges between all loaded nodes
            if len(node_ids) > 1:
                edge_query = """
                    MATCH (a)-[r]->(b)
                    WHERE elementId(a) IN $node_ids AND elementId(b) IN $node_ids
                    RETURN elementId(a) as source,
                           elementId(b) as target,
                           type(r) as type,
                           properties(r) as props
                """
                result = session.run(edge_query, node_ids=list(node_ids))

                for record in result:
                    edges.append(self._format_edge(dict(record)))

        return {
            "success": True,
            "centerNode": center_node,
            "nodes": nodes,
            "edges": edges,
            "nodeCount": len(nodes),
            "edgeCount": len(edges),
        }

    def get_node_details(self, node_id: str, node_type: str = None) -> Dict:
        """
        Get full details for a specific node.

        Args:
            node_id: Node name
            node_type: Node label

        Returns:
            Dict with node details and relationships
        """
        graph = self._get_graph()

        with graph.session() as session:
            if node_type:
                query = f"""
                    MATCH (n:{node_type} {{name: $node_id}})
                    OPTIONAL MATCH (n)-[r_out]->(target)
                    OPTIONAL MATCH (source)-[r_in]->(n)
                    RETURN n,
                           collect(DISTINCT {{
                               type: type(r_out),
                               target: target.name,
                               targetType: labels(target)[0]
                           }}) as outgoing,
                           collect(DISTINCT {{
                               type: type(r_in),
                               source: source.name,
                               sourceType: labels(source)[0]
                           }}) as incoming
                """
            else:
                query = """
                    MATCH (n {name: $node_id})
                    OPTIONAL MATCH (n)-[r_out]->(target)
                    OPTIONAL MATCH (source)-[r_in]->(n)
                    RETURN n,
                           collect(DISTINCT {
                               type: type(r_out),
                               target: target.name,
                               targetType: labels(target)[0]
                           }) as outgoing,
                           collect(DISTINCT {
                               type: type(r_in),
                               source: source.name,
                               sourceType: labels(source)[0]
                           }) as incoming
                """

            result = session.run(query, node_id=node_id)
            record = result.single()

            if not record:
                return {"success": False, "error": f"Node not found: {node_id}"}

            node = record["n"]
            outgoing = [r for r in record["outgoing"] if r["target"]]
            incoming = [r for r in record["incoming"] if r["source"]]

            return {
                "success": True,
                "node": {
                    "name": node.get("name", node_id),
                    "type": list(node.labels)[0] if node.labels else "Unknown",
                    "properties": dict(node),
                },
                "relationships": {"outgoing": outgoing, "incoming": incoming},
            }

    def search_nodes(
        self, query: str, node_types: Optional[List[str]] = None, limit: int = 20
    ) -> Dict:
        """
        Search nodes by name or properties.

        Args:
            query: Search string (case-insensitive partial match)
            node_types: Optional list of node types to search
            limit: Maximum results

        Returns:
            Dict with matching nodes
        """
        graph = self._get_graph()
        nodes = []

        with graph.session() as session:
            if node_types:
                labels = " OR ".join([f"n:{t}" for t in node_types])
                search_query = f"""
                    MATCH (n) WHERE ({labels})
                    AND (toLower(n.name) CONTAINS toLower($search_term)
                         OR toLower(n.purpose) CONTAINS toLower($search_term)
                         OR toLower(n.description) CONTAINS toLower($search_term))
                    RETURN elementId(n) as id,
                           labels(n)[0] as type,
                           n.name as label,
                           properties(n) as props
                    LIMIT $limit
                """
            else:
                search_query = """
                    MATCH (n)
                    WHERE toLower(coalesce(n.name, '')) CONTAINS toLower($search_term)
                       OR toLower(coalesce(n.purpose, '')) CONTAINS toLower($search_term)
                       OR toLower(coalesce(n.description, '')) CONTAINS toLower($search_term)
                    RETURN elementId(n) as id,
                           labels(n)[0] as type,
                           coalesce(n.name, n.symptom, n.phrase, 'unknown') as label,
                           properties(n) as props
                    LIMIT $limit
                """

            result = session.run(search_query, search_term=query, limit=limit)

            for record in result:
                nodes.append(self._format_node(dict(record)))

        return {"success": True, "query": query, "nodes": nodes, "count": len(nodes)}

    # =========================================================================
    # Write Operations (Mutations)
    # =========================================================================

    def create_node(
        self, node_type: str, name: str, properties: Optional[Dict] = None
    ) -> Dict:
        """
        Create a new node.

        Args:
            node_type: Node label (e.g., 'Equipment', 'AOI')
            name: Node name (must be unique for type)
            properties: Additional properties

        Returns:
            Dict with created node info
        """
        graph = self._get_graph()
        props = properties or {}
        props["name"] = name
        props["created_at"] = datetime.now().isoformat()

        with graph.session() as session:
            # Check if node already exists
            check_query = f"MATCH (n:{node_type} {{name: $name}}) RETURN n"
            result = session.run(check_query, name=name)
            if result.single():
                return {
                    "success": False,
                    "error": f'{node_type} with name "{name}" already exists',
                }

            # Create the node
            create_query = f"""
                CREATE (n:{node_type} $props)
                RETURN elementId(n) as id, labels(n)[0] as type, n.name as label, properties(n) as props
            """
            result = session.run(create_query, props=props)
            record = result.single()

            if record:
                return {"success": True, "node": self._format_node(dict(record))}
            else:
                return {"success": False, "error": "Failed to create node"}

    def update_node(self, node_type: str, name: str, properties: Dict) -> Dict:
        """
        Update node properties.

        Args:
            node_type: Node label
            name: Node name
            properties: Properties to update (merged with existing)

        Returns:
            Dict with updated node info
        """
        graph = self._get_graph()
        properties["updated_at"] = datetime.now().isoformat()

        with graph.session() as session:
            update_query = f"""
                MATCH (n:{node_type} {{name: $name}})
                SET n += $props
                RETURN elementId(n) as id, labels(n)[0] as type, n.name as label, properties(n) as props
            """
            result = session.run(update_query, name=name, props=properties)
            record = result.single()

            if record:
                return {"success": True, "node": self._format_node(dict(record))}
            else:
                return {
                    "success": False,
                    "error": f'Node not found: {node_type} "{name}"',
                }

    def delete_node(self, node_type: str, name: str) -> Dict:
        """
        Delete a node and its relationships.

        Args:
            node_type: Node label
            name: Node name

        Returns:
            Dict with deletion result
        """
        graph = self._get_graph()

        with graph.session() as session:
            delete_query = f"""
                MATCH (n:{node_type} {{name: $name}})
                DETACH DELETE n
                RETURN count(n) as deleted
            """
            result = session.run(delete_query, name=name)
            record = result.single()

            if record and record["deleted"] > 0:
                return {
                    "success": True,
                    "deleted": True,
                    "nodeType": node_type,
                    "name": name,
                }
            else:
                return {
                    "success": False,
                    "error": f'Node not found: {node_type} "{name}"',
                }

    def create_edge(
        self,
        source_type: str,
        source_name: str,
        target_type: str,
        target_name: str,
        relationship_type: str,
        properties: Optional[Dict] = None,
    ) -> Dict:
        """
        Create a relationship between two nodes.

        Args:
            source_type: Source node label
            source_name: Source node name
            target_type: Target node label
            target_name: Target node name
            relationship_type: Relationship type (e.g., 'CONTROLLED_BY')
            properties: Optional relationship properties

        Returns:
            Dict with created edge info
        """
        graph = self._get_graph()
        props = properties or {}
        props["created_at"] = datetime.now().isoformat()

        with graph.session() as session:
            create_query = f"""
                MATCH (source:{source_type} {{name: $source_name}})
                MATCH (target:{target_type} {{name: $target_name}})
                MERGE (source)-[r:{relationship_type}]->(target)
                SET r += $props
                RETURN elementId(source) as source,
                       elementId(target) as target,
                       type(r) as type,
                       properties(r) as props
            """
            result = session.run(
                create_query,
                source_name=source_name,
                target_name=target_name,
                props=props,
            )
            record = result.single()

            if record:
                return {"success": True, "edge": self._format_edge(dict(record))}
            else:
                return {
                    "success": False,
                    "error": f"Could not create edge - check that both nodes exist",
                }

    def delete_edge(
        self,
        source_type: str,
        source_name: str,
        target_type: str,
        target_name: str,
        relationship_type: str,
    ) -> Dict:
        """
        Delete a relationship between two nodes.

        Args:
            source_type: Source node label
            source_name: Source node name
            target_type: Target node label
            target_name: Target node name
            relationship_type: Relationship type

        Returns:
            Dict with deletion result
        """
        graph = self._get_graph()

        with graph.session() as session:
            delete_query = f"""
                MATCH (source:{source_type} {{name: $source_name}})
                      -[r:{relationship_type}]->
                      (target:{target_type} {{name: $target_name}})
                DELETE r
                RETURN count(r) as deleted
            """
            result = session.run(
                delete_query, source_name=source_name, target_name=target_name
            )
            record = result.single()

            if record and record["deleted"] > 0:
                return {"success": True, "deleted": True}
            else:
                return {"success": False, "error": "Relationship not found"}

    def apply_batch(self, changes: Dict) -> Dict:
        """
        Apply a batch of changes atomically.

        Args:
            changes: Dict with structure:
                {
                    'nodes': {
                        'create': [{type, name, properties}, ...],
                        'update': [{type, name, properties}, ...],
                        'delete': [{type, name}, ...]
                    },
                    'edges': {
                        'create': [{sourceType, sourceName, targetType, targetName, type, properties}, ...],
                        'delete': [{sourceType, sourceName, targetType, targetName, type}, ...]
                    }
                }

        Returns:
            Dict with results for each operation
        """
        results = {
            "success": True,
            "nodes": {"created": 0, "updated": 0, "deleted": 0},
            "edges": {"created": 0, "deleted": 0},
            "errors": [],
        }

        nodes = changes.get("nodes", {})
        edges = changes.get("edges", {})

        # Create nodes
        for node in nodes.get("create", []):
            result = self.create_node(
                node["type"], node["name"], node.get("properties")
            )
            if result["success"]:
                results["nodes"]["created"] += 1
            else:
                results["errors"].append(result["error"])

        # Update nodes
        for node in nodes.get("update", []):
            result = self.update_node(
                node["type"], node["name"], node.get("properties", {})
            )
            if result["success"]:
                results["nodes"]["updated"] += 1
            else:
                results["errors"].append(result["error"])

        # Delete nodes
        for node in nodes.get("delete", []):
            result = self.delete_node(node["type"], node["name"])
            if result["success"]:
                results["nodes"]["deleted"] += 1
            else:
                results["errors"].append(result["error"])

        # Create edges
        for edge in edges.get("create", []):
            result = self.create_edge(
                edge["sourceType"],
                edge["sourceName"],
                edge["targetType"],
                edge["targetName"],
                edge["type"],
                edge.get("properties"),
            )
            if result["success"]:
                results["edges"]["created"] += 1
            else:
                results["errors"].append(result["error"])

        # Delete edges
        for edge in edges.get("delete", []):
            result = self.delete_edge(
                edge["sourceType"],
                edge["sourceName"],
                edge["targetType"],
                edge["targetName"],
                edge["type"],
            )
            if result["success"]:
                results["edges"]["deleted"] += 1
            else:
                results["errors"].append(result["error"])

        results["success"] = len(results["errors"]) == 0
        return results

    # =========================================================================
    # Schema Information
    # =========================================================================

    def get_schema(self) -> Dict:
        """
        Get graph schema information (node types, relationship types).

        Returns:
            Dict with schema info
        """
        graph = self._get_graph()

        with graph.session() as session:
            # Get node labels with counts
            labels_query = """
                CALL db.labels() YIELD label
                CALL {
                    WITH label
                    MATCH (n) WHERE label IN labels(n)
                    RETURN count(n) as count
                }
                RETURN label, count
                ORDER BY count DESC
            """

            labels = []
            try:
                result = session.run(labels_query)
                for record in result:
                    labels.append(
                        {
                            "label": record["label"],
                            "count": record["count"],
                            "group": self._get_node_group(record["label"]),
                        }
                    )
            except Exception:
                # Fallback for older Neo4j versions
                result = session.run("CALL db.labels() YIELD label RETURN label")
                for record in result:
                    labels.append(
                        {
                            "label": record["label"],
                            "group": self._get_node_group(record["label"]),
                        }
                    )

            # Get relationship types
            rels_query = "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
            relationships = []
            result = session.run(rels_query)
            for record in result:
                relationships.append(record["relationshipType"])

        return {
            "success": True,
            "nodeTypes": labels,
            "relationshipTypes": sorted(relationships),
            "groups": list(set(self.NODE_GROUPS.values())),
        }


def main():
    """CLI interface for graph API."""
    parser = argparse.ArgumentParser(description="Graph API for Electron UI")
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # Load graph
    load_parser = subparsers.add_parser("load", help="Load graph data")
    load_parser.add_argument("--types", nargs="*", help="Node types to include")
    load_parser.add_argument("--limit", type=int, default=500, help="Max nodes")

    # Get neighbors
    neighbors_parser = subparsers.add_parser("neighbors", help="Get node neighbors")
    neighbors_parser.add_argument("node_id", help="Node name")
    neighbors_parser.add_argument("--type", help="Node type")
    neighbors_parser.add_argument(
        "--hops", type=int, default=1, help="Hop distance (1-3)"
    )
    neighbors_parser.add_argument("--max", type=int, default=50, help="Max nodes")
    neighbors_parser.add_argument(
        "--include", nargs="*", help="Include only these types"
    )

    # Get node details
    details_parser = subparsers.add_parser("details", help="Get node details")
    details_parser.add_argument("node_id", help="Node name")
    details_parser.add_argument("--type", help="Node type")

    # Search nodes
    search_parser = subparsers.add_parser("search", help="Search nodes")
    search_parser.add_argument("query", help="Search string")
    search_parser.add_argument("--types", nargs="*", help="Node types to search")
    search_parser.add_argument("--limit", type=int, default=20, help="Max results")

    # Create node
    create_node_parser = subparsers.add_parser("create-node", help="Create a node")
    create_node_parser.add_argument("node_type", help="Node type")
    create_node_parser.add_argument("name", help="Node name")
    create_node_parser.add_argument(
        "--props", type=json.loads, default={}, help="Properties (JSON)"
    )

    # Update node
    update_node_parser = subparsers.add_parser("update-node", help="Update a node")
    update_node_parser.add_argument("node_type", help="Node type")
    update_node_parser.add_argument("name", help="Node name")
    update_node_parser.add_argument(
        "props", type=json.loads, help="Properties to update (JSON)"
    )

    # Delete node
    delete_node_parser = subparsers.add_parser("delete-node", help="Delete a node")
    delete_node_parser.add_argument("node_type", help="Node type")
    delete_node_parser.add_argument("name", help="Node name")

    # Create edge
    create_edge_parser = subparsers.add_parser(
        "create-edge", help="Create a relationship"
    )
    create_edge_parser.add_argument("source_type", help="Source node type")
    create_edge_parser.add_argument("source_name", help="Source node name")
    create_edge_parser.add_argument("target_type", help="Target node type")
    create_edge_parser.add_argument("target_name", help="Target node name")
    create_edge_parser.add_argument("rel_type", help="Relationship type")
    create_edge_parser.add_argument(
        "--props", type=json.loads, default={}, help="Properties (JSON)"
    )

    # Delete edge
    delete_edge_parser = subparsers.add_parser(
        "delete-edge", help="Delete a relationship"
    )
    delete_edge_parser.add_argument("source_type", help="Source node type")
    delete_edge_parser.add_argument("source_name", help="Source node name")
    delete_edge_parser.add_argument("target_type", help="Target node type")
    delete_edge_parser.add_argument("target_name", help="Target node name")
    delete_edge_parser.add_argument("rel_type", help="Relationship type")

    # Apply batch
    batch_parser = subparsers.add_parser(
        "batch", help="Apply batch changes (JSON from stdin)"
    )

    # Schema
    subparsers.add_parser("schema", help="Get graph schema")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    api = GraphAPI()

    try:
        if args.command == "load":
            result = api.load_graph(args.types, args.limit)
        elif args.command == "neighbors":
            result = api.get_neighbors(
                args.node_id, args.type, args.hops, args.max, args.include
            )
        elif args.command == "details":
            result = api.get_node_details(args.node_id, args.type)
        elif args.command == "search":
            result = api.search_nodes(args.query, args.types, args.limit)
        elif args.command == "create-node":
            result = api.create_node(args.node_type, args.name, args.props)
        elif args.command == "update-node":
            result = api.update_node(args.node_type, args.name, args.props)
        elif args.command == "delete-node":
            result = api.delete_node(args.node_type, args.name)
        elif args.command == "create-edge":
            result = api.create_edge(
                args.source_type,
                args.source_name,
                args.target_type,
                args.target_name,
                args.rel_type,
                args.props,
            )
        elif args.command == "delete-edge":
            result = api.delete_edge(
                args.source_type,
                args.source_name,
                args.target_type,
                args.target_name,
                args.rel_type,
            )
        elif args.command == "batch":
            changes = json.load(sys.stdin)
            result = api.apply_batch(changes)
        elif args.command == "schema":
            result = api.get_schema()
        else:
            output_error(f"Unknown command: {args.command}")
            return

        output_json(result)

    except Exception as e:
        output_error(str(e))
    finally:
        api.close()


if __name__ == "__main__":
    main()
