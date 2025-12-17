#!/usr/bin/env python3
"""
Neo4j-based ontology storage for PLC/SCADA semantic knowledge graphs.
Replaces JSON file storage with a proper graph database.
"""

import os
import json
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from contextlib import contextmanager
from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver, Session


# Load environment variables
load_dotenv()

# Default connection settings
DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "leortest1!!!")


@dataclass
class Neo4jConfig:
    """Neo4j connection configuration."""

    uri: str = DEFAULT_URI
    user: str = DEFAULT_USER
    password: str = DEFAULT_PASSWORD


class OntologyGraph:
    """
    Neo4j graph database interface for PLC/SCADA ontologies.

    Node Types:
    - AOI: Add-On Instruction (PLC component)
    - Tag: Individual tag with semantic description
    - UDT: User Defined Type (SCADA)
    - Equipment: Equipment instance
    - View: SCADA view/window
    - FaultSymptom: Troubleshooting symptom
    - FaultCause: Root cause of a fault
    - OperatorPhrase: Natural language mapping
    - ControlPattern: Identified control pattern
    - DataFlow: Data flow path
    - SafetyElement: Safety-critical element

    Relationship Types:
    - HAS_TAG: AOI contains tag
    - INHIBITS, TRIGGERS, CONTROLS, MODE_CONTROL, PERSISTENCE_CONTROL, AUTO_CLEAR, DIRECT_MAPPING: Tag relationships
    - MAPS_TO_SCADA: PLC to SCADA mapping
    - INSTANCE_OF: Equipment is instance of UDT
    - HAS_SYMPTOM: AOI has fault symptom
    - CAUSED_BY: Symptom caused by cause
    - HAS_PATTERN: AOI uses control pattern
    - HAS_FLOW: AOI has data flow
    - SAFETY_CRITICAL: AOI has safety element
    - PHRASE_MAPS_TO: Operator phrase maps to technical meaning
    """

    def __init__(self, config: Optional[Neo4jConfig] = None):
        """Initialize Neo4j connection."""
        self.config = config or Neo4jConfig()
        self._driver: Optional[Driver] = None

    def connect(self) -> None:
        """Establish connection to Neo4j."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.config.uri, auth=(self.config.user, self.config.password)
            )
            # Verify connectivity
            self._driver.verify_connectivity()

    def close(self) -> None:
        """Close Neo4j connection."""
        if self._driver:
            self._driver.close()
            self._driver = None

    @contextmanager
    def session(self):
        """Context manager for Neo4j sessions."""
        if self._driver is None:
            self.connect()
        session = self._driver.session()
        try:
            yield session
        finally:
            session.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # =========================================================================
    # Schema Management
    # =========================================================================

    def create_indexes(self) -> None:
        """Create indexes and constraints for optimal query performance."""
        with self.session() as session:
            # Unique constraints (also create indexes)
            constraints = [
                "CREATE CONSTRAINT aoi_name IF NOT EXISTS FOR (a:AOI) REQUIRE a.name IS UNIQUE",
                "CREATE CONSTRAINT udt_name IF NOT EXISTS FOR (u:UDT) REQUIRE u.name IS UNIQUE",
                "CREATE CONSTRAINT equipment_name IF NOT EXISTS FOR (e:Equipment) REQUIRE e.name IS UNIQUE",
                "CREATE CONSTRAINT view_name IF NOT EXISTS FOR (v:View) REQUIRE v.name IS UNIQUE",
            ]

            # Regular indexes
            indexes = [
                "CREATE INDEX tag_name IF NOT EXISTS FOR (t:Tag) ON (t.name)",
                "CREATE INDEX tag_aoi IF NOT EXISTS FOR (t:Tag) ON (t.aoi_name)",
                "CREATE INDEX symptom_text IF NOT EXISTS FOR (s:FaultSymptom) ON (s.symptom)",
                "CREATE INDEX phrase_text IF NOT EXISTS FOR (p:OperatorPhrase) ON (p.phrase)",
                # Semantic status indexes for incremental analysis
                "CREATE INDEX aoi_semantic_status IF NOT EXISTS FOR (a:AOI) ON (a.semantic_status)",
                "CREATE INDEX udt_semantic_status IF NOT EXISTS FOR (u:UDT) ON (u.semantic_status)",
                "CREATE INDEX view_semantic_status IF NOT EXISTS FOR (v:View) ON (v.semantic_status)",
                "CREATE INDEX equipment_semantic_status IF NOT EXISTS FOR (e:Equipment) ON (e.semantic_status)",
                "CREATE INDEX viewcomponent_semantic_status IF NOT EXISTS FOR (c:ViewComponent) ON (c.semantic_status)",
                "CREATE INDEX scadatag_semantic_status IF NOT EXISTS FOR (t:ScadaTag) ON (t.semantic_status)",
                # Soft delete indexes
                "CREATE INDEX aoi_deleted IF NOT EXISTS FOR (a:AOI) ON (a.deleted)",
                "CREATE INDEX udt_deleted IF NOT EXISTS FOR (u:UDT) ON (u.deleted)",
                "CREATE INDEX view_deleted IF NOT EXISTS FOR (v:View) ON (v.deleted)",
                "CREATE INDEX equipment_deleted IF NOT EXISTS FOR (e:Equipment) ON (e.deleted)",
                "CREATE INDEX viewcomponent_deleted IF NOT EXISTS FOR (c:ViewComponent) ON (c.deleted)",
            ]

            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"[WARNING] Constraint error: {e}")

            for index in indexes:
                try:
                    session.run(index)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"[WARNING] Index error: {e}")

    def clear_all(self) -> None:
        """Clear all nodes and relationships. USE WITH CAUTION."""
        with self.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def clear_ignition(self) -> Dict[str, int]:
        """Clear all Ignition/SCADA-related nodes and cross-system mappings.
        
        Deletes: UDT, Equipment, View, ViewComponent nodes and any MAPS_TO_SCADA relationships.
        
        Returns:
            Dict with counts of deleted nodes by type
        """
        with self.session() as session:
            counts = {}
            
            # Delete MAPS_TO_SCADA relationships first (cross-system links)
            result = session.run(
                "MATCH ()-[r:MAPS_TO_SCADA]->() DELETE r RETURN count(r) as count"
            )
            counts["MAPS_TO_SCADA_relationships"] = result.single()["count"]
            
            # Delete ViewComponents and their relationships
            result = session.run(
                "MATCH (c:ViewComponent) DETACH DELETE c RETURN count(c) as count"
            )
            counts["ViewComponent"] = result.single()["count"]
            
            # Delete Views and their relationships
            result = session.run(
                "MATCH (v:View) DETACH DELETE v RETURN count(v) as count"
            )
            counts["View"] = result.single()["count"]
            
            # Delete Equipment and their relationships
            result = session.run(
                "MATCH (e:Equipment) DETACH DELETE e RETURN count(e) as count"
            )
            counts["Equipment"] = result.single()["count"]
            
            # Delete UDTs and their member tags
            result = session.run(
                """
                MATCH (u:UDT)
                OPTIONAL MATCH (u)-[:HAS_MEMBER]->(t:Tag)
                DETACH DELETE u, t
                RETURN count(DISTINCT u) as count
                """
            )
            counts["UDT"] = result.single()["count"]
            
            # Delete ScadaTags (standalone SCADA tags)
            result = session.run(
                "MATCH (t:ScadaTag) DETACH DELETE t RETURN count(t) as count"
            )
            counts["ScadaTag"] = result.single()["count"]
            
            # Delete EndToEndFlow nodes (cross-system)
            result = session.run(
                "MATCH (f:EndToEndFlow) DETACH DELETE f RETURN count(f) as count"
            )
            counts["EndToEndFlow"] = result.single()["count"]
            
            # Delete SystemOverview (cross-system)
            result = session.run(
                "MATCH (s:SystemOverview) DETACH DELETE s RETURN count(s) as count"
            )
            counts["SystemOverview"] = result.single()["count"]
            
            return counts

    def clear_plc(self) -> Dict[str, int]:
        """Clear all PLC-related nodes and cross-system mappings.
        
        Deletes: AOI, Tag (AOI-related), ControlPattern, DataFlow, SafetyElement,
        FaultSymptom, FaultCause, Intent, OperatorPhrase nodes and MAPS_TO_SCADA relationships.
        
        Returns:
            Dict with counts of deleted nodes by type
        """
        with self.session() as session:
            counts = {}
            
            # Delete MAPS_TO_SCADA relationships first (cross-system links)
            result = session.run(
                "MATCH ()-[r:MAPS_TO_SCADA]->() DELETE r RETURN count(r) as count"
            )
            counts["MAPS_TO_SCADA_relationships"] = result.single()["count"]
            
            # Delete EndToEndFlow nodes (cross-system)
            result = session.run(
                "MATCH (f:EndToEndFlow) DETACH DELETE f RETURN count(f) as count"
            )
            counts["EndToEndFlow"] = result.single()["count"]
            
            # Delete SystemOverview (cross-system)
            result = session.run(
                "MATCH (s:SystemOverview) DETACH DELETE s RETURN count(s) as count"
            )
            counts["SystemOverview"] = result.single()["count"]
            
            # Delete AOIs and all their related nodes
            result = session.run(
                """
                MATCH (a:AOI)
                OPTIONAL MATCH (a)-[:HAS_TAG]->(t:Tag)
                OPTIONAL MATCH (a)-[:HAS_PATTERN]->(p:ControlPattern)
                OPTIONAL MATCH (a)-[:HAS_FLOW]->(f:DataFlow)
                OPTIONAL MATCH (a)-[:SAFETY_CRITICAL]->(s:SafetyElement)
                OPTIONAL MATCH (a)-[:HAS_SYMPTOM]->(sym:FaultSymptom)
                OPTIONAL MATCH (sym)-[:CAUSED_BY]->(c:FaultCause)
                OPTIONAL MATCH (a)-[:HAS_INTENT]->(i:Intent)
                OPTIONAL MATCH (a)-[:HAS_PHRASE]->(op:OperatorPhrase)
                DETACH DELETE a, t, p, f, s, sym, c, i, op
                RETURN count(DISTINCT a) as count
                """
            )
            counts["AOI"] = result.single()["count"]
            
            # Delete CommonPhrases (operator dictionary)
            result = session.run(
                "MATCH (p:CommonPhrase) DETACH DELETE p RETURN count(p) as count"
            )
            counts["CommonPhrase"] = result.single()["count"]
            
            return counts

    def clear_unification(self) -> Dict[str, int]:
        """Clear all unification/cross-system data without touching PLC or Ignition data.
        
        Deletes: MAPS_TO_SCADA relationships, EndToEndFlow, SystemOverview, CommonPhrase nodes.
        Preserves: AOIs, UDTs, Views, Equipment, ViewComponents.
        
        Returns:
            Dict with counts of deleted items
        """
        with self.session() as session:
            counts = {}
            
            # Delete MAPS_TO_SCADA relationships
            result = session.run(
                "MATCH ()-[r:MAPS_TO_SCADA]->() DELETE r RETURN count(r) as count"
            )
            counts["MAPS_TO_SCADA_relationships"] = result.single()["count"]
            
            # Delete EndToEndFlow nodes
            result = session.run(
                "MATCH (f:EndToEndFlow) DETACH DELETE f RETURN count(f) as count"
            )
            counts["EndToEndFlow"] = result.single()["count"]
            
            # Delete SystemOverview
            result = session.run(
                "MATCH (s:SystemOverview) DETACH DELETE s RETURN count(s) as count"
            )
            counts["SystemOverview"] = result.single()["count"]
            
            # Delete CommonPhrases (operator dictionary)
            result = session.run(
                "MATCH (p:CommonPhrase) DETACH DELETE p RETURN count(p) as count"
            )
            counts["CommonPhrase"] = result.single()["count"]
            
            return counts

    # =========================================================================
    # AOI Operations
    # =========================================================================

    def create_aoi(
        self,
        name: str,
        aoi_type: str,
        source_file: str,
        metadata: Optional[Dict] = None,
        analysis: Optional[Dict] = None,
        semantic_status: str = "pending",
    ) -> str:
        """
        Create an AOI node with all its related data.
        
        Args:
            name: AOI name
            aoi_type: Type of AOI (AOI, UDT, etc.)
            source_file: Source file path
            metadata: Metadata dict (revision, vendor, description)
            analysis: Analysis dict (purpose, tags, patterns, etc.)
            semantic_status: One of 'pending', 'in_progress', 'complete', 'review'
        
        Returns:
            The AOI name.
        """
        purpose = (analysis or {}).get("purpose", "")
        
        with self.session() as session:
            # Create main AOI node with semantic_status tracking
            result = session.run(
                """
                MERGE (a:AOI {name: $name})
                SET a.type = $type,
                    a.source_file = $source_file,
                    a.revision = $revision,
                    a.vendor = $vendor,
                    a.description = $description
                WITH a
                // Set semantic_status to 'pending' only if not already set
                SET a.semantic_status = COALESCE(a.semantic_status, $semantic_status)
                WITH a
                // Update purpose and mark complete if purpose is provided
                FOREACH (_ IN CASE WHEN $purpose <> '' THEN [1] ELSE [] END |
                    SET a.purpose = $purpose,
                        a.semantic_status = 'complete',
                        a.analyzed_at = datetime()
                )
                RETURN a.name as name
            """,
                {
                    "name": name,
                    "type": aoi_type,
                    "source_file": source_file,
                    "revision": (metadata or {}).get("revision", ""),
                    "vendor": (metadata or {}).get("vendor", ""),
                    "description": (metadata or {}).get("description", ""),
                    "purpose": purpose,
                    "semantic_status": semantic_status,
                },
            )

            # Create tags
            tags = (analysis or {}).get("tags", {})
            for tag_name, tag_desc in tags.items():
                self._create_tag(session, name, tag_name, tag_desc)

            # Create tag relationships
            relationships = (analysis or {}).get("relationships", [])
            for rel in relationships:
                self._create_tag_relationship(session, name, rel)

            # Create control patterns
            patterns = (analysis or {}).get("control_patterns", [])
            for pattern in patterns:
                self._create_control_pattern(session, name, pattern)

            # Create data flows
            flows = (analysis or {}).get("data_flows", [])
            for flow in flows:
                self._create_data_flow(session, name, flow)

            # Create safety elements
            safety = (analysis or {}).get("safety_critical", [])
            for element in safety:
                self._create_safety_element(session, name, element)

            return name

    def _create_tag(
        self, session: Session, aoi_name: str, tag_name: str, description: str
    ) -> None:
        """Create a tag node linked to an AOI."""
        session.run(
            """
            MATCH (a:AOI {name: $aoi_name})
            MERGE (t:Tag {name: $tag_name, aoi_name: $aoi_name})
            SET t.description = $description
            MERGE (a)-[:HAS_TAG]->(t)
        """,
            {
                "aoi_name": aoi_name,
                "tag_name": tag_name,
                "description": description,
            },
        )

    def _create_tag_relationship(
        self, session: Session, aoi_name: str, rel: Dict
    ) -> None:
        """Create a relationship between tags."""
        from_tag = rel.get("from", "")
        to_tag = rel.get("to", "")
        rel_type = rel.get("relationship_type", "RELATES_TO").upper().replace(" ", "_")
        description = rel.get("description", "")

        # Sanitize relationship type for Neo4j
        rel_type = "".join(c if c.isalnum() or c == "_" else "_" for c in rel_type)

        # Use APOC or dynamic relationship - fallback to property-based approach
        session.run(
            f"""
            MATCH (a:AOI {{name: $aoi_name}})
            MERGE (from:Tag {{name: $from_tag, aoi_name: $aoi_name}})
            MERGE (to:Tag {{name: $to_tag, aoi_name: $aoi_name}})
            MERGE (a)-[:HAS_TAG]->(from)
            MERGE (a)-[:HAS_TAG]->(to)
            MERGE (from)-[r:{rel_type}]->(to)
            SET r.description = $description
        """,
            {
                "aoi_name": aoi_name,
                "from_tag": from_tag,
                "to_tag": to_tag,
                "description": description,
            },
        )

    def _create_control_pattern(
        self, session: Session, aoi_name: str, pattern: Union[Dict, str]
    ) -> None:
        """Create a control pattern node."""
        if isinstance(pattern, dict):
            pattern_name = pattern.get("pattern", pattern.get("name", str(pattern)))
            description = pattern.get("description", "")
        else:
            pattern_name = str(pattern)
            description = ""

        session.run(
            """
            MATCH (a:AOI {name: $aoi_name})
            MERGE (p:ControlPattern {name: $pattern_name, aoi_name: $aoi_name})
            SET p.description = $description
            MERGE (a)-[:HAS_PATTERN]->(p)
        """,
            {
                "aoi_name": aoi_name,
                "pattern_name": pattern_name,
                "description": description,
            },
        )

    def _create_data_flow(
        self, session: Session, aoi_name: str, flow: Union[Dict, str]
    ) -> None:
        """Create a data flow node."""
        if isinstance(flow, dict):
            path = flow.get("path", "")
            description = flow.get("description", "")
        else:
            path = str(flow)
            description = ""

        session.run(
            """
            MATCH (a:AOI {name: $aoi_name})
            MERGE (f:DataFlow {path: $path, aoi_name: $aoi_name})
            SET f.description = $description
            MERGE (a)-[:HAS_FLOW]->(f)
        """,
            {
                "aoi_name": aoi_name,
                "path": path,
                "description": description,
            },
        )

    def _create_safety_element(
        self, session: Session, aoi_name: str, element: Union[Dict, str]
    ) -> None:
        """Create a safety-critical element node."""
        if isinstance(element, dict):
            elem_name = element.get("element", element.get("name", str(element)))
            criticality = element.get("criticality", "unknown")
            reason = element.get("reason", "")
        else:
            elem_name = str(element)
            criticality = "unknown"
            reason = ""

        session.run(
            """
            MATCH (a:AOI {name: $aoi_name})
            MERGE (s:SafetyElement {name: $elem_name, aoi_name: $aoi_name})
            SET s.criticality = $criticality, s.reason = $reason
            MERGE (a)-[:SAFETY_CRITICAL]->(s)
        """,
            {
                "aoi_name": aoi_name,
                "elem_name": elem_name,
                "criticality": criticality,
                "reason": reason,
            },
        )

    def get_aoi(self, name: str) -> Optional[Dict]:
        """Get an AOI with all its related data."""
        with self.session() as session:
            # Get main AOI
            result = session.run(
                """
                MATCH (a:AOI {name: $name})
                RETURN a
            """,
                {"name": name},
            )
            record = result.single()
            if not record:
                return None

            aoi_node = dict(record["a"])

            # Get tags
            tags_result = session.run(
                """
                MATCH (a:AOI {name: $name})-[:HAS_TAG]->(t:Tag)
                RETURN t.name as name, t.description as description
            """,
                {"name": name},
            )
            tags = {}
            for r in tags_result:
                tag_name = r["name"]
                # Handle case where name might be a list (shouldn't happen but be defensive)
                if isinstance(tag_name, list):
                    tag_name = tag_name[0] if tag_name else "unknown"
                if tag_name:
                    tags[tag_name] = r["description"] or ""

            # Get tag relationships
            rels_result = session.run(
                """
                MATCH (a:AOI {name: $name})-[:HAS_TAG]->(from:Tag)-[r]->(to:Tag)
                WHERE type(r) <> 'HAS_TAG'
                RETURN from.name as from_tag, to.name as to_tag, 
                       type(r) as rel_type, r.description as description
            """,
                {"name": name},
            )
            relationships = [
                {
                    "from": r["from_tag"],
                    "to": r["to_tag"],
                    "relationship_type": r["rel_type"],
                    "description": r["description"],
                }
                for r in rels_result
            ]

            # Get patterns
            patterns_result = session.run(
                """
                MATCH (a:AOI {name: $name})-[:HAS_PATTERN]->(p:ControlPattern)
                RETURN p.name as name, p.description as description
            """,
                {"name": name},
            )
            patterns = [
                {"pattern": r["name"], "description": r["description"]}
                for r in patterns_result
            ]

            # Get data flows
            flows_result = session.run(
                """
                MATCH (a:AOI {name: $name})-[:HAS_FLOW]->(f:DataFlow)
                RETURN f.path as path, f.description as description
            """,
                {"name": name},
            )
            flows = [
                {"path": r["path"], "description": r["description"]}
                for r in flows_result
            ]

            # Get safety elements
            safety_result = session.run(
                """
                MATCH (a:AOI {name: $name})-[:SAFETY_CRITICAL]->(s:SafetyElement)
                RETURN s.name as element, s.criticality as criticality, s.reason as reason
            """,
                {"name": name},
            )
            safety = [dict(r) for r in safety_result]

            return {
                "name": aoi_node.get("name"),
                "type": aoi_node.get("type"),
                "source_file": aoi_node.get("source_file"),
                "metadata": {
                    "revision": aoi_node.get("revision"),
                    "vendor": aoi_node.get("vendor"),
                    "description": aoi_node.get("description"),
                },
                "analysis": {
                    "purpose": aoi_node.get("purpose"),
                    "tags": tags,
                    "relationships": relationships,
                    "control_patterns": patterns,
                    "data_flows": flows,
                    "safety_critical": safety,
                },
            }

    def get_all_aois(self) -> List[Dict]:
        """Get all AOIs with their data."""
        with self.session() as session:
            result = session.run("MATCH (a:AOI) RETURN a.name as name")
            names = [r["name"] for r in result]

        return [self.get_aoi(name) for name in names]

    def delete_aoi(self, name: str) -> bool:
        """Delete an AOI and all its related nodes."""
        with self.session() as session:
            result = session.run(
                """
                MATCH (a:AOI {name: $name})
                OPTIONAL MATCH (a)-[:HAS_TAG]->(t:Tag)
                OPTIONAL MATCH (a)-[:HAS_PATTERN]->(p:ControlPattern)
                OPTIONAL MATCH (a)-[:HAS_FLOW]->(f:DataFlow)
                OPTIONAL MATCH (a)-[:SAFETY_CRITICAL]->(s:SafetyElement)
                OPTIONAL MATCH (a)-[:HAS_SYMPTOM]->(sym:FaultSymptom)
                OPTIONAL MATCH (sym)-[:CAUSED_BY]->(c:FaultCause)
                DETACH DELETE a, t, p, f, s, sym, c
                RETURN count(a) as deleted
            """,
                {"name": name},
            )
            record = result.single()
            return record["deleted"] > 0 if record else False

    # =========================================================================
    # Troubleshooting Operations
    # =========================================================================

    def add_troubleshooting(self, aoi_name: str, troubleshooting: Dict) -> None:
        """Add troubleshooting data to an AOI and mark it as enriched."""
        with self.session() as session:
            # Mark AOI as enriched
            session.run(
                """
                MATCH (a:AOI {name: $aoi_name})
                SET a.troubleshooting_enriched = true,
                    a.enriched_at = datetime()
                """,
                {"aoi_name": aoi_name},
            )
            
            # Fault tree
            fault_tree = troubleshooting.get("fault_tree", [])
            for fault in fault_tree:
                self._create_fault_symptom(session, aoi_name, fault)

            # Intents
            intents = troubleshooting.get("intents", {})
            for intent_name, intent_data in intents.items():
                self._create_intent(session, aoi_name, intent_name, intent_data)

            # Operator phrases
            phrases = troubleshooting.get("operator_phrases", [])
            for phrase in phrases:
                self._create_operator_phrase(session, aoi_name, phrase)

            # Expected states
            expected_states = troubleshooting.get("expected_states", {})
            if expected_states:
                session.run(
                    """
                    MATCH (a:AOI {name: $aoi_name})
                    SET a.expected_states = $states
                """,
                    {
                        "aoi_name": aoi_name,
                        "states": str(expected_states),
                    },
                )

            # Diagnostic tags
            diagnostic_tags = troubleshooting.get("diagnostic_tags", [])
            for diag in diagnostic_tags:
                self._create_diagnostic_tag(session, aoi_name, diag)

    def _create_fault_symptom(
        self, session: Session, aoi_name: str, fault: Dict
    ) -> None:
        """Create fault symptom and its causes."""
        symptom = fault.get("symptom", "")
        plc_indicators = fault.get("plc_indicators", [])
        scada_indicators = fault.get("scada_indicators", [])
        resolution_steps = fault.get("resolution_steps", [])

        # Create symptom node
        session.run(
            """
            MATCH (a:AOI {name: $aoi_name})
            MERGE (s:FaultSymptom {symptom: $symptom, aoi_name: $aoi_name})
            SET s.plc_indicators = $plc_indicators,
                s.scada_indicators = $scada_indicators,
                s.resolution_steps = $resolution_steps
            MERGE (a)-[:HAS_SYMPTOM]->(s)
        """,
            {
                "aoi_name": aoi_name,
                "symptom": symptom,
                "plc_indicators": plc_indicators,
                "scada_indicators": scada_indicators,
                "resolution_steps": resolution_steps,
            },
        )

        # Create causes
        for cause_data in fault.get("possible_causes", []):
            cause = cause_data.get("cause", "")
            likelihood = cause_data.get("likelihood", "unknown")
            check = cause_data.get("check", "")

            session.run(
                """
                MATCH (s:FaultSymptom {symptom: $symptom, aoi_name: $aoi_name})
                MERGE (c:FaultCause {cause: $cause, aoi_name: $aoi_name})
                SET c.likelihood = $likelihood, c.check = $check
                MERGE (s)-[:CAUSED_BY {likelihood: $likelihood}]->(c)
            """,
                {
                    "aoi_name": aoi_name,
                    "symptom": symptom,
                    "cause": cause,
                    "likelihood": likelihood,
                    "check": check,
                },
            )

    def _create_intent(
        self, session: Session, aoi_name: str, intent_name: str, intent_data: Dict
    ) -> None:
        """Create an intent node."""
        session.run(
            """
            MATCH (a:AOI {name: $aoi_name})
            MERGE (i:Intent {name: $intent_name, aoi_name: $aoi_name})
            SET i.what = $what,
                i.why = $why,
                i.consequence_if_missing = $consequence,
                i.failure_symptom = $failure
            MERGE (a)-[:HAS_INTENT]->(i)
        """,
            {
                "aoi_name": aoi_name,
                "intent_name": intent_name,
                "what": intent_data.get("what", ""),
                "why": intent_data.get("why", ""),
                "consequence": intent_data.get("consequence_if_missing", ""),
                "failure": intent_data.get("failure_symptom", ""),
            },
        )

    def _create_operator_phrase(
        self, session: Session, aoi_name: str, phrase_data: Dict
    ) -> None:
        """Create an operator phrase mapping."""
        session.run(
            """
            MATCH (a:AOI {name: $aoi_name})
            MERGE (p:OperatorPhrase {phrase: $phrase, aoi_name: $aoi_name})
            SET p.means = $means,
                p.check_first = $check_first,
                p.related_tags = $related_tags
            MERGE (a)-[:HAS_PHRASE]->(p)
        """,
            {
                "aoi_name": aoi_name,
                "phrase": phrase_data.get("phrase", ""),
                "means": phrase_data.get("means", ""),
                "check_first": phrase_data.get("check_first", ""),
                "related_tags": phrase_data.get("related_tags", []),
            },
        )

    def _create_diagnostic_tag(
        self, session: Session, aoi_name: str, diag: Dict
    ) -> None:
        """Create a diagnostic tag entry."""
        tag_name = diag.get("tag", "")
        session.run(
            """
            MATCH (a:AOI {name: $aoi_name})
            MERGE (t:Tag {name: $tag_name, aoi_name: $aoi_name})
            SET t.normal_value = $normal_value,
                t.meaning_if_abnormal = $meaning
            MERGE (a)-[:HAS_TAG]->(t)
        """,
            {
                "aoi_name": aoi_name,
                "tag_name": tag_name,
                "normal_value": diag.get("normal_value", ""),
                "meaning": diag.get("meaning_if_abnormal", ""),
            },
        )

    def get_troubleshooting(self, aoi_name: str) -> Dict:
        """Get troubleshooting data for an AOI."""
        with self.session() as session:
            # Fault tree
            fault_result = session.run(
                """
                MATCH (a:AOI {name: $aoi_name})-[:HAS_SYMPTOM]->(s:FaultSymptom)
                OPTIONAL MATCH (s)-[:CAUSED_BY]->(c:FaultCause)
                RETURN s, collect(c) as causes
            """,
                {"aoi_name": aoi_name},
            )

            fault_tree = []
            for record in fault_result:
                symptom = dict(record["s"])
                causes = [dict(c) for c in record["causes"] if c]
                fault_tree.append(
                    {
                        "symptom": symptom.get("symptom"),
                        "plc_indicators": symptom.get("plc_indicators", []),
                        "scada_indicators": symptom.get("scada_indicators", []),
                        "resolution_steps": symptom.get("resolution_steps", []),
                        "possible_causes": [
                            {
                                "cause": c.get("cause"),
                                "likelihood": c.get("likelihood"),
                                "check": c.get("check"),
                            }
                            for c in causes
                        ],
                    }
                )

            # Intents
            intent_result = session.run(
                """
                MATCH (a:AOI {name: $aoi_name})-[:HAS_INTENT]->(i:Intent)
                RETURN i
            """,
                {"aoi_name": aoi_name},
            )
            intents = {
                r["i"]["name"]: {
                    "what": r["i"].get("what"),
                    "why": r["i"].get("why"),
                    "consequence_if_missing": r["i"].get("consequence_if_missing"),
                    "failure_symptom": r["i"].get("failure_symptom"),
                }
                for r in intent_result
            }

            # Operator phrases
            phrase_result = session.run(
                """
                MATCH (a:AOI {name: $aoi_name})-[:HAS_PHRASE]->(p:OperatorPhrase)
                RETURN p
            """,
                {"aoi_name": aoi_name},
            )
            phrases = [
                {
                    "phrase": r["p"].get("phrase"),
                    "means": r["p"].get("means"),
                    "check_first": r["p"].get("check_first"),
                    "related_tags": r["p"].get("related_tags", []),
                }
                for r in phrase_result
            ]

            # Diagnostic tags
            diag_result = session.run(
                """
                MATCH (a:AOI {name: $aoi_name})-[:HAS_TAG]->(t:Tag)
                WHERE t.normal_value IS NOT NULL
                RETURN t.name as tag, t.normal_value as normal_value, 
                       t.meaning_if_abnormal as meaning_if_abnormal
            """,
                {"aoi_name": aoi_name},
            )
            diagnostic_tags = [dict(r) for r in diag_result]

            return {
                "fault_tree": fault_tree,
                "intents": intents,
                "operator_phrases": phrases,
                "diagnostic_tags": diagnostic_tags,
            }

    # =========================================================================
    # SCADA/Ignition Operations
    # =========================================================================

    def create_udt(
        self,
        name: str,
        purpose: str,
        source_file: str = "",
        members: Optional[List[Dict]] = None,
        semantic_status: str = "pending",
    ) -> str:
        """Create a UDT node.
        
        Args:
            name: UDT name
            purpose: Semantic description (empty if not yet analyzed)
            source_file: Source file path
            members: List of member tag definitions
            semantic_status: One of 'pending', 'in_progress', 'complete', 'review'
        """
        with self.session() as session:
            # Only update semantic_status if purpose is being set (analysis complete)
            # or if this is a new node
            session.run(
                """
                MERGE (u:UDT {name: $name})
                SET u.source_file = $source_file
                WITH u
                // Set semantic_status to 'pending' only if not already set
                SET u.semantic_status = COALESCE(u.semantic_status, $semantic_status)
                WITH u
                // Update purpose and mark complete if purpose is provided
                FOREACH (_ IN CASE WHEN $purpose <> '' THEN [1] ELSE [] END |
                    SET u.purpose = $purpose,
                        u.semantic_status = 'complete',
                        u.analyzed_at = datetime()
                )
            """,
                {
                    "name": name,
                    "purpose": purpose,
                    "source_file": source_file,
                    "semantic_status": semantic_status,
                },
            )

            # Create member tags
            for member in members or []:
                session.run(
                    """
                    MATCH (u:UDT {name: $udt_name})
                    MERGE (t:Tag {name: $tag_name, udt_name: $udt_name})
                    SET t.data_type = $data_type, t.tag_type = $tag_type
                    MERGE (u)-[:HAS_MEMBER]->(t)
                """,
                    {
                        "udt_name": name,
                        "tag_name": member.get("name", ""),
                        "data_type": member.get("data_type", ""),
                        "tag_type": member.get("tag_type", ""),
                    },
                )
        return name

    def create_equipment(
        self,
        name: str,
        equipment_type: str,
        purpose: str,
        udt_name: Optional[str] = None,
        semantic_status: str = "pending",
    ) -> str:
        """Create an equipment instance node.
        
        Args:
            name: Equipment instance name
            equipment_type: Type of equipment
            purpose: Semantic description (empty if not yet analyzed)
            udt_name: Name of the UDT this equipment instantiates
            semantic_status: One of 'pending', 'in_progress', 'complete', 'review'
        """
        with self.session() as session:
            session.run(
                """
                MERGE (e:Equipment {name: $name})
                SET e.type = $type
                WITH e
                SET e.semantic_status = COALESCE(e.semantic_status, $semantic_status)
                WITH e
                FOREACH (_ IN CASE WHEN $purpose <> '' THEN [1] ELSE [] END |
                    SET e.purpose = $purpose,
                        e.semantic_status = 'complete',
                        e.analyzed_at = datetime()
                )
            """,
                {
                    "name": name,
                    "type": equipment_type,
                    "purpose": purpose,
                    "semantic_status": semantic_status,
                },
            )

            if udt_name:
                session.run(
                    """
                    MATCH (e:Equipment {name: $equip_name})
                    MATCH (u:UDT {name: $udt_name})
                    MERGE (e)-[:INSTANCE_OF]->(u)
                """,
                    {
                        "equip_name": name,
                        "udt_name": udt_name,
                    },
                )
        return name

    def create_view(
        self,
        name: str,
        path: str,
        purpose: str,
        semantic_status: str = "pending",
    ) -> str:
        """Create a SCADA view node.
        
        Args:
            name: View name
            path: View path in Ignition
            purpose: Semantic description (empty if not yet analyzed)
            semantic_status: One of 'pending', 'in_progress', 'complete', 'review'
        """
        with self.session() as session:
            session.run(
                """
                MERGE (v:View {name: $name})
                SET v.path = $path
                WITH v
                SET v.semantic_status = COALESCE(v.semantic_status, $semantic_status)
                WITH v
                FOREACH (_ IN CASE WHEN $purpose <> '' THEN [1] ELSE [] END |
                    SET v.purpose = $purpose,
                        v.semantic_status = 'complete',
                        v.analyzed_at = datetime()
                )
            """,
                {
                    "name": name,
                    "path": path,
                    "purpose": purpose,
                    "semantic_status": semantic_status,
                },
            )
        return name

    def create_view_udt_mapping(
        self, view_name: str, udt_name: str, binding_type: str = "displays"
    ) -> bool:
        """Create a DISPLAYS relationship between a View and a UDT.

        Args:
            view_name: Name of the view
            udt_name: Name of the UDT the view displays/controls
            binding_type: Type of binding (displays, controls, monitors)

        Returns:
            True if relationship was created, False if nodes not found
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (v:View {name: $view_name})
                MATCH (u:UDT {name: $udt_name})
                MERGE (v)-[r:DISPLAYS]->(u)
                SET r.binding_type = $binding_type
                RETURN v.name as view, u.name as udt
            """,
                {
                    "view_name": view_name,
                    "udt_name": udt_name,
                    "binding_type": binding_type,
                },
            )
            return result.single() is not None

    def create_view_equipment_mapping(
        self, view_name: str, equipment_name: str, binding_type: str = "displays"
    ) -> bool:
        """Create a DISPLAYS relationship between a View and Equipment.

        Args:
            view_name: Name of the view
            equipment_name: Name of the equipment the view displays/controls
            binding_type: Type of binding (displays, controls, monitors)

        Returns:
            True if relationship was created, False if nodes not found
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (v:View {name: $view_name})
                MATCH (e:Equipment {name: $equipment_name})
                MERGE (v)-[r:DISPLAYS]->(e)
                SET r.binding_type = $binding_type
                RETURN v.name as view, e.name as equipment
            """,
                {
                    "view_name": view_name,
                    "equipment_name": equipment_name,
                    "binding_type": binding_type,
                },
            )
            return result.single() is not None

    def create_scada_tag(
        self,
        name: str,
        tag_type: str,
        folder_name: str = "",
        data_type: str = "",
        datasource: str = "",
        query: str = "",
        opc_item_path: str = "",
        expression: str = "",
        initial_value: str = "",
        semantic_status: str = "pending",
    ) -> str:
        """Create a standalone SCADA tag node.

        Args:
            name: Tag name
            tag_type: Type of tag (query, memory, opc, expression)
            folder_name: Folder path in Ignition
            data_type: Data type (DataSet, Int, Float, Boolean, etc.)
            datasource: Database datasource for query tags
            query: SQL query for query tags
            opc_item_path: OPC path for opc tags
            expression: Expression for expression tags
            initial_value: Initial value for memory tags
            semantic_status: One of 'pending', 'in_progress', 'complete', 'review'

        Returns:
            Tag name
        """
        with self.session() as session:
            session.run(
                """
                MERGE (t:ScadaTag {name: $name})
                SET t.tag_type = $tag_type,
                    t.folder_name = $folder_name,
                    t.data_type = $data_type,
                    t.datasource = $datasource,
                    t.query = $query,
                    t.opc_item_path = $opc_item_path,
                    t.expression = $expression,
                    t.initial_value = $initial_value
                WITH t
                SET t.semantic_status = COALESCE(t.semantic_status, $semantic_status)
                """,
                {
                    "name": name,
                    "tag_type": tag_type,
                    "folder_name": folder_name or "",
                    "data_type": data_type or "",
                    "datasource": datasource or "",
                    "query": query or "",
                    "opc_item_path": opc_item_path or "",
                    "expression": expression or "",
                    "initial_value": str(initial_value) if initial_value else "",
                    "semantic_status": semantic_status,
                },
            )
        return name

    def create_view_component(
        self,
        view_name: str,
        component_name: str,
        component_type: str,
        component_path: str = "",
        inferred_purpose: str = "",
        props: dict = None,
        semantic_status: str = "pending",
    ) -> bool:
        """Create a ViewComponent node and link it to a View.

        Args:
            view_name: Name of the parent view
            component_name: Name/label of the component
            component_type: Type (Button, Label, LED, Input, etc.)
            component_path: Hierarchical path within the view
            inferred_purpose: Type-based inferred purpose (deterministic, not AI)
            props: Additional properties (text, style, etc.)
            semantic_status: One of 'pending', 'in_progress', 'complete', 'review'

        Returns:
            True if component was created and linked
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (v:View {name: $view_name})
                MERGE (c:ViewComponent {view: $view_name, path: $component_path})
                SET c.name = $component_name,
                    c.type = $component_type,
                    c.inferred_purpose = $inferred_purpose,
                    c.props = $props
                WITH c
                SET c.semantic_status = COALESCE(c.semantic_status, $semantic_status)
                WITH c
                MATCH (v:View {name: $view_name})
                MERGE (v)-[:HAS_COMPONENT]->(c)
                RETURN c.path as created
            """,
                {
                    "view_name": view_name,
                    "component_name": component_name,
                    "component_type": component_type,
                    "component_path": component_path,
                    "inferred_purpose": inferred_purpose,
                    "props": json.dumps(props or {}),
                    "semantic_status": semantic_status,
                },
            )
            return result.single() is not None

    def create_component_udt_binding(
        self,
        view_name: str,
        component_path: str,
        udt_name: str,
        binding_property: str,
        tag_path: str = "",
    ) -> bool:
        """Create a BINDS_TO relationship between a ViewComponent and a UDT.

        Args:
            view_name: Name of the parent view
            component_path: Path of the component within the view
            udt_name: Name of the UDT being bound
            binding_property: Which property is bound (e.g., 'value', 'text', 'visible')
            tag_path: Full tag path of the binding

        Returns:
            True if relationship was created
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (c:ViewComponent {view: $view_name, path: $component_path})
                MATCH (u:UDT {name: $udt_name})
                MERGE (c)-[r:BINDS_TO]->(u)
                SET r.property = $binding_property,
                    r.tag_path = $tag_path
                RETURN c.path as component, u.name as udt
            """,
                {
                    "view_name": view_name,
                    "component_path": component_path,
                    "udt_name": udt_name,
                    "binding_property": binding_property,
                    "tag_path": tag_path,
                },
            )
            return result.single() is not None

    def create_component_tag_binding(
        self,
        view_name: str,
        component_path: str,
        tag_name: str,
        binding_property: str,
        tag_path: str = "",
    ) -> bool:
        """Create a BINDS_TO relationship between a ViewComponent and a ScadaTag.

        Args:
            view_name: Name of the parent view
            component_path: Path of the component within the view
            tag_name: Name of the ScadaTag being bound
            binding_property: Which property is bound (e.g., 'value', 'text', 'visible')
            tag_path: Full tag path of the binding

        Returns:
            True if relationship was created
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (c:ViewComponent {view: $view_name, path: $component_path})
                MATCH (t:ScadaTag {name: $tag_name})
                MERGE (c)-[r:BINDS_TO]->(t)
                SET r.property = $binding_property,
                    r.tag_path = $tag_path
                RETURN c.path as component, t.name as tag
            """,
                {
                    "view_name": view_name,
                    "component_path": component_path,
                    "tag_name": tag_name,
                    "binding_property": binding_property,
                    "tag_path": tag_path,
                },
            )
            return result.single() is not None

    def create_tag_reference(
        self,
        source_tag: str,
        target_tag: str,
        reference_type: str = "expression",
    ) -> bool:
        """Create a REFERENCES relationship between two ScadaTags.
        
        Used when one tag references another (e.g., expression tags).

        Args:
            source_tag: Name of the tag that contains the reference
            target_tag: Name of the tag being referenced
            reference_type: Type of reference (expression, derived, etc.)

        Returns:
            True if relationship was created
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (s:ScadaTag {name: $source})
                MATCH (t:ScadaTag {name: $target})
                MERGE (s)-[r:REFERENCES]->(t)
                SET r.type = $ref_type
                RETURN s.name as source, t.name as target
            """,
                {
                    "source": source_tag,
                    "target": target_tag,
                    "ref_type": reference_type,
                },
            )
            return result.single() is not None

    def create_udt_nested_type(
        self,
        parent_udt: str,
        member_name: str,
        child_udt: str,
    ) -> bool:
        """Create a CONTAINS_TYPE relationship when a UDT member is another UDT.

        Args:
            parent_udt: Name of the parent UDT
            member_name: Name of the member that uses the nested UDT
            child_udt: Name of the nested UDT type

        Returns:
            True if relationship was created
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (p:UDT {name: $parent})
                MATCH (c:UDT {name: $child})
                MERGE (p)-[r:CONTAINS_TYPE]->(c)
                SET r.member_name = $member
                RETURN p.name as parent, c.name as child
            """,
                {
                    "parent": parent_udt,
                    "member": member_name,
                    "child": child_udt,
                },
            )
            return result.single() is not None

    def create_udt_tag_reference(
        self,
        udt_name: str,
        member_name: str,
        tag_name: str,
    ) -> bool:
        """Create a REFERENCES relationship when a UDT member references a ScadaTag.

        Args:
            udt_name: Name of the UDT
            member_name: Name of the member that references the tag
            tag_name: Name of the ScadaTag being referenced

        Returns:
            True if relationship was created
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (u:UDT {name: $udt})
                MATCH (t:ScadaTag {name: $tag})
                MERGE (u)-[r:REFERENCES]->(t)
                SET r.member_name = $member
                RETURN u.name as udt, t.name as tag
            """,
                {
                    "udt": udt_name,
                    "member": member_name,
                    "tag": tag_name,
                },
            )
            return result.single() is not None

    # =========================================================================
    # Unified Ontology / Cross-System Mappings
    # =========================================================================

    def create_plc_scada_mapping(
        self,
        plc_component: str,
        scada_component: str,
        mapping_type: str,
        description: str,
    ) -> None:
        """Create a mapping between PLC and SCADA components.

        Uses fuzzy matching: tries exact match first, then falls back to
        substring matching to handle naming differences like:
        - Valve_Solenoid (AOI) -> ValveSolenoidControl (UDT)
        - Motor_Reversing (AOI) -> MotorReversingControl (UDT)
        """
        with self.session() as session:
            # First try exact match
            result = session.run(
                """
                OPTIONAL MATCH (plc:AOI {name: $plc})
                OPTIONAL MATCH (scada:UDT {name: $scada})
                OPTIONAL MATCH (scada2:Equipment {name: $scada})
                WITH plc, COALESCE(scada, scada2) as scada_node
                WHERE plc IS NOT NULL AND scada_node IS NOT NULL
                MERGE (plc)-[r:MAPS_TO_SCADA]->(scada_node)
                SET r.mapping_type = $mapping_type, r.description = $description
                RETURN plc.name as plc_name
            """,
                {
                    "plc": plc_component,
                    "scada": scada_component,
                    "mapping_type": mapping_type,
                    "description": description,
                },
            )

            # If exact match found, we're done
            if result.single():
                return

            # Try fuzzy matching: extract key words from component names
            # e.g., "Valve_Solenoid" -> ["valve", "solenoid"]
            plc_words = [w.lower() for w in plc_component.replace("_", " ").split()]
            scada_words = [w.lower() for w in scada_component.replace("_", " ").split()]

            # Find AOI where name contains key words
            session.run(
                """
                MATCH (plc:AOI)
                WHERE any(word IN $plc_words WHERE toLower(plc.name) CONTAINS word)
                WITH plc
                MATCH (scada)
                WHERE (scada:UDT OR scada:Equipment)
                  AND any(word IN $scada_words WHERE toLower(scada.name) CONTAINS word)
                MERGE (plc)-[r:MAPS_TO_SCADA]->(scada)
                SET r.mapping_type = $mapping_type, 
                    r.description = $description,
                    r.fuzzy_match = true
            """,
                {
                    "plc_words": plc_words,
                    "scada_words": scada_words,
                    "mapping_type": mapping_type,
                    "description": description,
                },
            )

    def create_system_overview(
        self,
        overview: str,
        safety_architecture: Dict = None,
        control_responsibilities: Dict = None,
    ) -> None:
        """Create/update the system overview node."""
        with self.session() as session:
            session.run(
                """
                MERGE (s:SystemOverview {id: 'main'})
                SET s.overview = $overview,
                    s.safety_architecture = $safety,
                    s.control_responsibilities = $control
            """,
                {
                    "overview": overview,
                    "safety": str(safety_architecture) if safety_architecture else "",
                    "control": (
                        str(control_responsibilities)
                        if control_responsibilities
                        else ""
                    ),
                },
            )

    def create_end_to_end_flow(self, flow_name: str, flow_data: Dict) -> None:
        """Create an end-to-end data flow."""
        with self.session() as session:
            session.run(
                """
                MERGE (f:EndToEndFlow {name: $name})
                SET f.description = $description,
                    f.path = $path,
                    f.data = $data
            """,
                {
                    "name": flow_name,
                    "description": flow_data.get("description", ""),
                    "path": str(flow_data.get("path", flow_data.get("stages", []))),
                    "data": str(flow_data),
                },
            )

    # =========================================================================
    # Operator Dictionary
    # =========================================================================

    def create_common_phrase(self, phrase_key: str, phrase_data: Dict) -> None:
        """Create a common operator phrase in the dictionary."""
        with self.session() as session:
            session.run(
                """
                MERGE (p:CommonPhrase {key: $key})
                SET p.variations = $variations,
                    p.means = $means,
                    p.scada_check = $scada_check,
                    p.plc_check = $plc_check,
                    p.follow_up_questions = $follow_up
            """,
                {
                    "key": phrase_key,
                    "variations": phrase_data.get("variations", []),
                    "means": phrase_data.get("means", ""),
                    "scada_check": phrase_data.get("scada_check", []),
                    "plc_check": phrase_data.get("plc_check", []),
                    "follow_up": phrase_data.get("follow_up_questions", []),
                },
            )

    # =========================================================================
    # Semantic Status Operations (for incremental analysis)
    # =========================================================================

    def get_pending_items(
        self, item_type: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get items that haven't been semantically analyzed yet.
        
        Args:
            item_type: One of 'AOI', 'UDT', 'View', 'Equipment', 'ViewComponent', 'ScadaTag'
            limit: Maximum number of items to return
            
        Returns:
            List of dicts with 'name' and other relevant properties
        """
        valid_types = {"AOI", "UDT", "View", "Equipment", "ViewComponent", "ScadaTag"}
        if item_type not in valid_types:
            raise ValueError(f"item_type must be one of {valid_types}")
        
        with self.session() as session:
            if item_type == "ViewComponent":
                result = session.run(
                    f"""
                    MATCH (n:{item_type})
                    WHERE (n.semantic_status = 'pending' OR n.semantic_status IS NULL)
                      AND (n.deleted IS NULL OR n.deleted = false)
                    RETURN n.view as view, n.path as path, n.name as name, 
                           n.type as type, n.props as props,
                           n.inferred_purpose as inferred_purpose
                    LIMIT $limit
                    """,
                    {"limit": limit},
                )
                return [dict(r) for r in result]
            elif item_type == "ScadaTag":
                result = session.run(
                    """
                    MATCH (n:ScadaTag)
                    WHERE (n.semantic_status = 'pending' OR n.semantic_status IS NULL)
                      AND (n.deleted IS NULL OR n.deleted = false)
                    RETURN n.name as name, n.tag_type as tag_type, 
                           n.data_type as data_type, n.folder_name as folder_name,
                           n.query as query, n.datasource as datasource,
                           n.opc_item_path as opc_item_path, n.expression as expression
                    LIMIT $limit
                    """,
                    {"limit": limit},
                )
                return [dict(r) for r in result]
            else:
                result = session.run(
                    f"""
                    MATCH (n:{item_type})
                    WHERE (n.semantic_status = 'pending' OR n.semantic_status IS NULL)
                      AND (n.deleted IS NULL OR n.deleted = false)
                    RETURN n.name as name, n.source_file as source_file, 
                           n.type as type, n.path as path
                    LIMIT $limit
                    """,
                    {"limit": limit},
                )
                return [dict(r) for r in result]

    def set_semantic_status(
        self, item_type: str, name: str, status: str, purpose: str = None
    ) -> bool:
        """Update the semantic status of an item.
        
        Args:
            item_type: One of 'AOI', 'UDT', 'View', 'Equipment', 'ViewComponent', 'ScadaTag'
            name: Name of the item (or path for ViewComponent)
            status: One of 'pending', 'in_progress', 'complete', 'review'
            purpose: Semantic description to set (only used when status='complete')
            
        Returns:
            True if item was found and updated
        """
        valid_types = {"AOI", "UDT", "View", "Equipment", "ViewComponent", "ScadaTag"}
        valid_statuses = {"pending", "in_progress", "complete", "review"}
        
        if item_type not in valid_types:
            raise ValueError(f"item_type must be one of {valid_types}")
        if status not in valid_statuses:
            raise ValueError(f"status must be one of {valid_statuses}")
        
        with self.session() as session:
            if item_type == "ViewComponent":
                # ViewComponent uses path as identifier
                result = session.run(
                    f"""
                    MATCH (n:{item_type} {{path: $name}})
                    SET n.semantic_status = $status
                    WITH n
                    FOREACH (_ IN CASE WHEN $purpose IS NOT NULL THEN [1] ELSE [] END |
                        SET n.purpose = $purpose, n.analyzed_at = datetime()
                    )
                    RETURN n.path as name
                    """,
                    {"name": name, "status": status, "purpose": purpose},
                )
            else:
                result = session.run(
                    f"""
                    MATCH (n:{item_type} {{name: $name}})
                    SET n.semantic_status = $status
                    WITH n
                    FOREACH (_ IN CASE WHEN $purpose IS NOT NULL THEN [1] ELSE [] END |
                        SET n.purpose = $purpose, n.analyzed_at = datetime()
                    )
                    RETURN n.name as name
                    """,
                    {"name": name, "status": status, "purpose": purpose},
                )
            return result.single() is not None

    def get_semantic_status_counts(self, include_deleted: bool = False) -> Dict[str, Dict[str, int]]:
        """Get counts of items by semantic status for each type.
        
        Args:
            include_deleted: If True, include deleted items in counts
        
        Returns:
            Dict like {'UDT': {'pending': 5, 'complete': 3, 'deleted': 1}, ...}
        """
        with self.session() as session:
            result = {}
            for item_type in ["AOI", "UDT", "View", "Equipment", "ViewComponent", "ScadaTag"]:
                if include_deleted:
                    counts_result = session.run(
                        f"""
                        MATCH (n:{item_type})
                        WITH COALESCE(n.semantic_status, 'pending') as status
                        RETURN status, count(*) as count
                        """
                    )
                else:
                    counts_result = session.run(
                        f"""
                        MATCH (n:{item_type})
                        WHERE n.deleted IS NULL OR n.deleted = false
                        WITH COALESCE(n.semantic_status, 'pending') as status
                        RETURN status, count(*) as count
                        """
                    )
                result[item_type] = {r["status"]: r["count"] for r in counts_result}
            return result

    def get_enrichment_status_counts(self) -> Dict[str, Dict[str, int]]:
        """Get counts of items by troubleshooting enrichment status.
        
        Returns:
            Dict like {'AOI': {'enriched': 5, 'pending': 3}, 'View': {'enriched': 2, 'pending': 4}}
        """
        with self.session() as session:
            result = {}
            
            # AOI enrichment status
            aoi_result = session.run("""
                MATCH (a:AOI)
                RETURN 
                    sum(CASE WHEN a.troubleshooting_enriched = true THEN 1 ELSE 0 END) as enriched,
                    sum(CASE WHEN a.troubleshooting_enriched IS NULL OR a.troubleshooting_enriched = false THEN 1 ELSE 0 END) as pending
            """)
            aoi_record = aoi_result.single()
            result["AOI"] = {
                "enriched": aoi_record["enriched"] if aoi_record else 0,
                "pending": aoi_record["pending"] if aoi_record else 0
            }
            
            # View enrichment status
            view_result = session.run("""
                MATCH (v:View)
                RETURN 
                    sum(CASE WHEN v.troubleshooting_enriched = true THEN 1 ELSE 0 END) as enriched,
                    sum(CASE WHEN v.troubleshooting_enriched IS NULL OR v.troubleshooting_enriched = false THEN 1 ELSE 0 END) as pending
            """)
            view_record = view_result.single()
            result["View"] = {
                "enriched": view_record["enriched"] if view_record else 0,
                "pending": view_record["pending"] if view_record else 0
            }
            
            return result

    def get_item_with_context(
        self, item_type: str, name: str
    ) -> Optional[Dict[str, Any]]:
        """Get an item with its related context for semantic analysis.
        
        For AOI: includes tags, patterns, flows, SCADA mappings
        For UDT: includes member tags, related views, any AOI mappings
        For View: includes components, bound UDTs
        For Equipment: includes UDT type, related views
        
        Args:
            item_type: One of 'AOI', 'UDT', 'View', 'Equipment', 'ViewComponent'
            name: Name of the item
            
        Returns:
            Dict with item data and context, or None if not found
        """
        with self.session() as session:
            if item_type == "AOI":
                result = session.run(
                    """
                    MATCH (a:AOI {name: $name})
                    OPTIONAL MATCH (a)-[:HAS_TAG]->(t:Tag)
                    OPTIONAL MATCH (a)-[:HAS_PATTERN]->(p:ControlPattern)
                    OPTIONAL MATCH (a)-[:MAPS_TO_SCADA]->(s)
                    RETURN a as item,
                           collect(DISTINCT {name: t.name, description: t.description}) as tags,
                           collect(DISTINCT p.name) as patterns,
                           collect(DISTINCT s.name) as scada_mappings
                    """,
                    {"name": name},
                )
            elif item_type == "UDT":
                result = session.run(
                    """
                    MATCH (u:UDT {name: $name})
                    OPTIONAL MATCH (u)<-[:DISPLAYS]-(v:View)
                    OPTIONAL MATCH (u)<-[:INSTANCE_OF]-(e:Equipment)
                    OPTIONAL MATCH (u)<-[:MAPS_TO_SCADA]-(a:AOI)
                    OPTIONAL MATCH (u)-[:HAS_MEMBER]->(t:Tag)
                    RETURN u as item,
                           collect(DISTINCT v.name) as views,
                           collect(DISTINCT e.name) as equipment,
                           collect(DISTINCT a.name) as aois,
                           collect(DISTINCT {name: t.name, data_type: t.data_type}) as members
                    """,
                    {"name": name},
                )
            elif item_type == "View":
                result = session.run(
                    """
                    MATCH (v:View {name: $name})
                    OPTIONAL MATCH (v)-[:DISPLAYS]->(u:UDT)
                    OPTIONAL MATCH (v)-[:HAS_COMPONENT]->(c:ViewComponent)
                    RETURN v as item,
                           collect(DISTINCT u.name) as udts,
                           collect(DISTINCT {name: c.name, type: c.type, path: c.path}) as components
                    """,
                    {"name": name},
                )
            elif item_type == "Equipment":
                result = session.run(
                    """
                    MATCH (e:Equipment {name: $name})
                    OPTIONAL MATCH (e)-[:INSTANCE_OF]->(u:UDT)
                    OPTIONAL MATCH (e)<-[:DISPLAYS]-(v:View)
                    RETURN e as item,
                           u.name as udt_type,
                           collect(DISTINCT v.name) as views
                    """,
                    {"name": name},
                )
            elif item_type == "ViewComponent":
                # For ViewComponent, name is actually the path
                result = session.run(
                    """
                    MATCH (c:ViewComponent {path: $name})
                    OPTIONAL MATCH (c)-[:BINDS_TO]->(u:UDT)
                    OPTIONAL MATCH (c)-[:BINDS_TO]->(t:ScadaTag)
                    OPTIONAL MATCH (v:View)-[:HAS_COMPONENT]->(c)
                    RETURN c as item,
                           v.name as parent_view,
                           collect(DISTINCT u.name) as bound_udts,
                           collect(DISTINCT t.name) as bound_tags
                    """,
                    {"name": name},
                )
            elif item_type == "ScadaTag":
                result = session.run(
                    """
                    MATCH (t:ScadaTag {name: $name})
                    OPTIONAL MATCH (t)-[:REFERENCES]->(ref:ScadaTag)
                    OPTIONAL MATCH (t)<-[:BINDS_TO]-(c:ViewComponent)
                    OPTIONAL MATCH (v:View)-[:HAS_COMPONENT]->(c)
                    RETURN t as item,
                           collect(DISTINCT ref.name) as referenced_tags,
                           collect(DISTINCT c.path) as bound_components,
                           collect(DISTINCT v.name) as used_in_views
                    """,
                    {"name": name},
                )
            else:
                return None
            
            record = result.single()
            if not record:
                return None
            
            item_data = dict(record["item"])
            context = {k: v for k, v in dict(record).items() if k != "item"}
            return {"item": item_data, "context": context}

    # =========================================================================
    # Query Operations
    # =========================================================================

    def find_by_symptom(self, symptom_text: str) -> List[Dict]:
        """Find AOIs by fault symptom text (fuzzy match)."""
        with self.session() as session:
            result = session.run(
                """
                MATCH (a:AOI)-[:HAS_SYMPTOM]->(s:FaultSymptom)
                WHERE toLower(s.symptom) CONTAINS toLower($text)
                RETURN a.name as aoi, s.symptom as symptom, 
                       s.resolution_steps as steps
            """,
                {"text": symptom_text},
            )
            return [dict(r) for r in result]

    def find_by_operator_phrase(self, phrase: str) -> List[Dict]:
        """Find matches for operator language."""
        with self.session() as session:
            # Check common phrases
            common_result = session.run(
                """
                MATCH (p:CommonPhrase)
                WHERE any(v IN p.variations WHERE toLower(v) CONTAINS toLower($phrase))
                RETURN p.key as key, p.means as means, p.scada_check as scada_check,
                       p.plc_check as plc_check, p.follow_up_questions as follow_up
            """,
                {"phrase": phrase},
            )
            common = [dict(r) for r in common_result]

            # Check AOI-specific phrases
            aoi_result = session.run(
                """
                MATCH (a:AOI)-[:HAS_PHRASE]->(p:OperatorPhrase)
                WHERE toLower(p.phrase) CONTAINS toLower($phrase)
                RETURN a.name as aoi, p.phrase as phrase, p.means as means,
                       p.check_first as check_first, p.related_tags as related_tags
            """,
                {"phrase": phrase},
            )
            aoi_phrases = [dict(r) for r in aoi_result]

            return {"common_phrases": common, "aoi_phrases": aoi_phrases}

    def trace_tag_influence(
        self, aoi_name: str, tag_name: str, depth: int = 3
    ) -> List[Dict]:
        """Trace what a tag influences (downstream effects)."""
        with self.session() as session:
            result = session.run(
                """
                MATCH path = (start:Tag {name: $tag_name, aoi_name: $aoi_name})-[*1..$depth]->(end:Tag)
                RETURN [n IN nodes(path) | n.name] as path,
                       [r IN relationships(path) | type(r)] as relationships
            """,
                {
                    "tag_name": tag_name,
                    "aoi_name": aoi_name,
                    "depth": depth,
                },
            )
            return [dict(r) for r in result]

    def trace_tag_dependencies(
        self, aoi_name: str, tag_name: str, depth: int = 3
    ) -> List[Dict]:
        """Trace what influences a tag (upstream dependencies)."""
        with self.session() as session:
            result = session.run(
                """
                MATCH path = (start:Tag)-[*1..$depth]->(end:Tag {name: $tag_name, aoi_name: $aoi_name})
                RETURN [n IN nodes(path) | n.name] as path,
                       [r IN relationships(path) | type(r)] as relationships
            """,
                {
                    "tag_name": tag_name,
                    "aoi_name": aoi_name,
                    "depth": depth,
                },
            )
            return [dict(r) for r in result]

    def get_graph_for_visualization(self) -> Dict:
        """Get nodes and edges for visualization."""
        with self.session() as session:
            # Get all nodes
            nodes_result = session.run(
                """
                MATCH (n)
                WHERE n:AOI OR n:UDT OR n:Equipment OR n:View OR n:EndToEndFlow
                RETURN elementId(n) as id, labels(n)[0] as type, 
                       coalesce(n.name, n.key, 'unknown') as label,
                       properties(n) as props
            """
            )
            nodes = [
                {
                    "id": str(r["id"]),
                    "type": r["type"].lower(),
                    "label": r["label"],
                    "details": dict(r["props"]),
                }
                for r in nodes_result
            ]

            # Get relationships between main nodes
            edges_result = session.run(
                """
                MATCH (a)-[r]->(b)
                WHERE (a:AOI OR a:UDT OR a:Equipment OR a:View)
                  AND (b:AOI OR b:UDT OR b:Equipment OR b:View)
                RETURN elementId(a) as source, elementId(b) as target, type(r) as type,
                       properties(r) as props
            """
            )
            edges = [
                {
                    "source": str(r["source"]),
                    "target": str(r["target"]),
                    "type": r["type"],
                    "label": r["props"].get("mapping_type", r["type"]),
                }
                for r in edges_result
            ]

            return {"nodes": nodes, "edges": edges}


# =========================================================================
# Convenience Functions
# =========================================================================


def get_ontology_graph(config: Optional[Neo4jConfig] = None) -> OntologyGraph:
    """Get a connected OntologyGraph instance."""
    graph = OntologyGraph(config)
    graph.connect()
    graph.create_indexes()
    return graph


def import_json_ontology(json_path: str, graph: Optional[OntologyGraph] = None) -> None:
    """Import an existing JSON ontology into Neo4j."""
    import json
    from pathlib import Path

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    close_after = False
    if graph is None:
        graph = get_ontology_graph()
        close_after = True

    try:
        # Detect ontology type
        if isinstance(data, list):
            # L5X ontology (list of AOIs)
            for aoi in data:
                _import_aoi(graph, aoi)
        elif data.get("type") == "troubleshooting_ontology":
            # Troubleshooting ontology
            for aoi in data.get("aois", []):
                _import_aoi(graph, aoi)
                if "troubleshooting" in aoi:
                    graph.add_troubleshooting(aoi["name"], aoi["troubleshooting"])
            # Operator dictionary
            op_dict = data.get("operator_dictionary", {})
            for key, phrase_data in op_dict.get("common_phrases", {}).items():
                graph.create_common_phrase(key, phrase_data)
        elif data.get("type") == "unified_system_ontology":
            # Unified ontology
            _import_unified(graph, data)
        elif data.get("source") == "ignition":
            # Ignition ontology
            _import_ignition(graph, data)
        else:
            # Single AOI or unknown
            _import_aoi(graph, data)

        print(f"[OK] Imported {json_path} into Neo4j")
    finally:
        if close_after:
            graph.close()


def _import_aoi(graph: OntologyGraph, aoi: Dict) -> None:
    """Import a single AOI into the graph."""
    graph.create_aoi(
        name=aoi.get("name", "Unknown"),
        aoi_type=aoi.get("type", "AOI"),
        source_file=aoi.get("source_file", ""),
        metadata=aoi.get("metadata", {}),
        analysis=aoi.get("analysis", {}),
    )


def _import_unified(graph: OntologyGraph, data: Dict) -> None:
    """Import a unified ontology."""
    ua = data.get("unified_analysis", {})

    # System overview
    graph.create_system_overview(
        overview=ua.get("system_overview", ""),
        safety_architecture=ua.get("safety_architecture"),
        control_responsibilities=ua.get("control_responsibilities"),
    )

    # PLC components
    plc_ontology = data.get("component_ontologies", {}).get("plc", [])
    if isinstance(plc_ontology, list):
        for aoi in plc_ontology:
            _import_aoi(graph, aoi)

    # SCADA components
    scada_ontology = data.get("component_ontologies", {}).get("scada", {})
    scada_analysis = scada_ontology.get("analysis", {})

    for udt_name, udt_purpose in scada_analysis.get("udt_semantics", {}).items():
        graph.create_udt(udt_name, udt_purpose, scada_ontology.get("source_file", ""))

    for equip in scada_analysis.get("equipment_instances", []):
        graph.create_equipment(
            equip.get("name", ""),
            equip.get("type", ""),
            equip.get("purpose", ""),
        )

    for view_name, view_purpose in scada_analysis.get("view_purposes", {}).items():
        graph.create_view(view_name, "", view_purpose)

    # PLC-to-SCADA mappings
    for mapping in ua.get("plc_to_scada_mappings", []):
        graph.create_plc_scada_mapping(
            mapping.get("plc_component", ""),
            mapping.get("scada_component", ""),
            mapping.get("mapping_type", ""),
            mapping.get("description", ""),
        )

    # End-to-end flows
    for flow in ua.get("end_to_end_flows", []):
        flow_name = flow.get("flow_name", flow.get("name", "Unknown"))
        graph.create_end_to_end_flow(flow_name, flow)


def _import_ignition(graph: OntologyGraph, data: Dict) -> None:
    """Import an Ignition ontology."""
    analysis = data.get("analysis", {})

    for udt_name, udt_purpose in analysis.get("udt_semantics", {}).items():
        graph.create_udt(udt_name, udt_purpose, data.get("source_file", ""))

    for equip in analysis.get("equipment_instances", []):
        graph.create_equipment(
            equip.get("name", ""),
            equip.get("type", ""),
            equip.get("purpose", ""),
        )

    for view_name, view_purpose in analysis.get("view_purposes", {}).items():
        graph.create_view(view_name, "", view_purpose)


# =========================================================================
# CLI
# =========================================================================


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Neo4j ontology management")
    parser.add_argument(
        "command",
        choices=["init", "clear", "clear-ignition", "clear-plc", "clear-unification", "import", "export", "query"],
        help="Command to execute",
    )
    parser.add_argument("--file", "-f", help="JSON file for import/export")
    parser.add_argument("--query", "-q", help="Query string for search")
    parser.add_argument("--enrichment-status", action="store_true", help="Show troubleshooting enrichment status")
    parser.add_argument("--uri", default=DEFAULT_URI, help="Neo4j URI")
    parser.add_argument("--user", default=DEFAULT_USER, help="Neo4j user")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Neo4j password")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompts")

    args = parser.parse_args()

    config = Neo4jConfig(uri=args.uri, user=args.user, password=args.password)

    with OntologyGraph(config) as graph:
        if args.command == "init":
            graph.create_indexes()
            print("[OK] Initialized Neo4j schema with indexes and constraints")

        elif args.command == "clear":
            if not args.yes:
                confirm = input("This will delete ALL data. Type 'yes' to confirm: ")
                if confirm.lower() != "yes":
                    print("[CANCELLED]")
                    return
            graph.clear_all()
            print("[OK] Cleared all data from Neo4j")

        elif args.command == "clear-ignition":
            if not args.yes:
                confirm = input("This will delete all Ignition/SCADA data and cross-system mappings. Type 'yes' to confirm: ")
                if confirm.lower() != "yes":
                    print("[CANCELLED]")
                    return
            counts = graph.clear_ignition()
            print("[OK] Cleared Ignition data from Neo4j:")
            for node_type, count in counts.items():
                if count > 0:
                    print(f"  - {node_type}: {count}")

        elif args.command == "clear-plc":
            if not args.yes:
                confirm = input("This will delete all PLC data and cross-system mappings. Type 'yes' to confirm: ")
                if confirm.lower() != "yes":
                    print("[CANCELLED]")
                    return
            counts = graph.clear_plc()
            print("[OK] Cleared PLC data from Neo4j:")
            for node_type, count in counts.items():
                if count > 0:
                    print(f"  - {node_type}: {count}")

        elif args.command == "clear-unification":
            if not args.yes:
                confirm = input("This will delete all unification data (mappings, flows, overview). Type 'yes' to confirm: ")
                if confirm.lower() != "yes":
                    print("[CANCELLED]")
                    return
            counts = graph.clear_unification()
            print("[OK] Cleared unification data from Neo4j:")
            for node_type, count in counts.items():
                if count > 0:
                    print(f"  - {node_type}: {count}")

        elif args.command == "import":
            if not args.file:
                print("[ERROR] --file required for import")
                return
            import_json_ontology(args.file, graph)

        elif args.command == "export":
            import json

            aois = graph.get_all_aois()
            if args.file:
                with open(args.file, "w", encoding="utf-8") as f:
                    json.dump(aois, f, indent=2)
                print(f"[OK] Exported {len(aois)} AOIs to {args.file}")
            else:
                print(json.dumps(aois, indent=2))

        elif args.command == "query":
            if args.enrichment_status:
                # Show troubleshooting enrichment status
                counts = graph.get_enrichment_status_counts()
                print("Troubleshooting Enrichment Status:")
                print("-" * 40)
                for item_type, status in counts.items():
                    total = status["enriched"] + status["pending"]
                    pct = (status["enriched"] / total * 100) if total > 0 else 0
                    print(f"  {item_type:15} {status['enriched']}/{total} enriched ({pct:.0f}%)")
            elif args.query:
                results = graph.find_by_symptom(args.query)
                if results:
                    for r in results:
                        print(f"\nAOI: {r['aoi']}")
                        print(f"Symptom: {r['symptom']}")
                        print(f"Steps: {r['steps']}")
                else:
                    print("No results found")


if __name__ == "__main__":
    main()
