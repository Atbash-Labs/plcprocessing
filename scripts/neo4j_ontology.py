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
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional fallback for minimal envs
    def load_dotenv(*_args, **_kwargs):
        return False
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
    - AOI: Add-On Instruction / PLC block (Rockwell AOI, Siemens OB/FB/FC/DB)
    - Tag: Individual tag with semantic description
    - UDT: User Defined Type (SCADA or PLC struct)
    - Equipment: Equipment instance
    - View: SCADA view/window
    - FaultSymptom: Troubleshooting symptom
    - FaultCause: Root cause of a fault
    - OperatorPhrase: Natural language mapping
    - ControlPattern: Identified control pattern
    - DataFlow: Data flow path
    - SafetyElement: Safety-critical element

    Siemens TIA Portal Node Types:
    - TiaProject: Top-level Siemens TIA Portal project
    - PLCDevice: PLC hardware device within a TIA project
    - HMIDevice: HMI panel within a TIA project
    - HMIConnection: HMI-to-PLC communication link
    - HMIAlarm: Analog or discrete alarm definition
    - HMIAlarmClass: Alarm classification / severity
    - HMIScript: HMI JavaScript automation script
    - HMITagTable: HMI tag table
    - HMITextList: Display enumeration / text list
    - HMIScreen: HMI screen definition
    - PLCTagTable: PLC global tag table
    - PLCTag: PLC global tag with address and type

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
    - INSTANTIATES: AOI/FB declares a variable whose type is another AOI/FB
    - USES_TYPE: AOI/FB declares a variable whose type is a UDT

    Siemens TIA Relationship Types:
    - HAS_DEVICE: TiaProject -> PLCDevice/HMIDevice
    - HAS_BLOCK: PLCDevice -> AOI
    - HAS_TYPE: PLCDevice -> UDT
    - HAS_TAG_TABLE: PLCDevice/HMIDevice -> PLCTagTable/HMITagTable
    - HAS_TAG (PLCTagTable): PLCTagTable -> PLCTag
    - HAS_CONNECTION: HMIDevice -> HMIConnection
    - CONNECTS_TO: HMIConnection -> PLCDevice
    - HAS_ALARM: HMIDevice -> HMIAlarm
    - HAS_ALARM_CLASS: HMIDevice -> HMIAlarmClass
    - CLASSIFIED_AS: HMIAlarm -> HMIAlarmClass
    - HAS_SCRIPT: HMIDevice -> HMIScript
    - HAS_SCREEN: HMIDevice -> HMIScreen
    - HAS_TEXT_LIST: HMIDevice -> HMITextList
    - MONITORS_TAG: HMIAlarm -> PLCTag

    Process-Semantic Node Types:
    - ProcessMedium: A material/utility stream (water, steam, product, etc.)
    - UnitOperation: A canonical plant operation (pumping, heating, mixing, etc.)
    - OperatingEnvelope: Normal ranges, alarm bands, trip windows for a parameter
    - PhysicalPrinciple: A measurable physical quantity (temperature, pressure, flow)
    - ChemicalSpecies: A chemical substance involved in plant processes
    - Reaction: A chemical or physical transformation step

    Process-Semantic Relationship Types:
    - HANDLES_MEDIUM: Equipment -> ProcessMedium
    - PERFORMS_OPERATION: Equipment -> UnitOperation
    - HAS_OPERATING_ENVELOPE: Equipment -> OperatingEnvelope
    - MEASURES: ScadaTag -> PhysicalPrinciple
    - MONITORS_ENVELOPE: ScadaTag -> OperatingEnvelope
    - IMPLEMENTS_CONTROL_OF: AOI -> UnitOperation
    - USES_PRINCIPLE: UnitOperation -> PhysicalPrinciple
    - INVOLVES_SPECIES: Reaction -> ChemicalSpecies
    - PROCESSES_SPECIES: UnitOperation -> ChemicalSpecies
    - HAS_REACTION: UnitOperation -> Reaction
    - MEDIUM_CONTAINS: ProcessMedium -> ChemicalSpecies
    - ENVELOPE_FOR_PRINCIPLE: OperatingEnvelope -> PhysicalPrinciple
    - VISUALIZES: ViewComponent -> Equipment
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
                "CREATE CONSTRAINT project_name IF NOT EXISTS FOR (p:Project) REQUIRE p.name IS UNIQUE",
                "CREATE CONSTRAINT script_name IF NOT EXISTS FOR (s:Script) REQUIRE s.name IS UNIQUE",
                "CREATE CONSTRAINT namedquery_name IF NOT EXISTS FOR (q:NamedQuery) REQUIRE q.name IS UNIQUE",
                "CREATE CONSTRAINT agentrun_id IF NOT EXISTS FOR (r:AgentRun) REQUIRE r.run_id IS UNIQUE",
                "CREATE CONSTRAINT anomalyevent_id IF NOT EXISTS FOR (e:AnomalyEvent) REQUIRE e.event_id IS UNIQUE",
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
                "CREATE INDEX script_semantic_status IF NOT EXISTS FOR (s:Script) ON (s.semantic_status)",
                "CREATE INDEX namedquery_semantic_status IF NOT EXISTS FOR (q:NamedQuery) ON (q.semantic_status)",
                # Soft delete indexes
                "CREATE INDEX aoi_deleted IF NOT EXISTS FOR (a:AOI) ON (a.deleted)",
                "CREATE INDEX udt_deleted IF NOT EXISTS FOR (u:UDT) ON (u.deleted)",
                "CREATE INDEX view_deleted IF NOT EXISTS FOR (v:View) ON (v.deleted)",
                "CREATE INDEX equipment_deleted IF NOT EXISTS FOR (e:Equipment) ON (e.deleted)",
                "CREATE INDEX viewcomponent_deleted IF NOT EXISTS FOR (c:ViewComponent) ON (c.deleted)",
                # Project-related indexes
                "CREATE INDEX view_project IF NOT EXISTS FOR (v:View) ON (v.project)",
                "CREATE INDEX script_project IF NOT EXISTS FOR (s:Script) ON (s.project)",
                "CREATE INDEX namedquery_project IF NOT EXISTS FOR (q:NamedQuery) ON (q.project)",
                # Siemens TIA Portal project indexes
                "CREATE INDEX tiaproject_name IF NOT EXISTS FOR (tp:TiaProject) ON (tp.name)",
                "CREATE INDEX plcdevice_name IF NOT EXISTS FOR (pd:PLCDevice) ON (pd.name)",
                "CREATE INDEX hmidevice_name IF NOT EXISTS FOR (hd:HMIDevice) ON (hd.name)",
                "CREATE INDEX hmiconnection_name IF NOT EXISTS FOR (hc:HMIConnection) ON (hc.name)",
                "CREATE INDEX hmialarm_name IF NOT EXISTS FOR (ha:HMIAlarm) ON (ha.name)",
                "CREATE INDEX hmialarmclass_name IF NOT EXISTS FOR (hac:HMIAlarmClass) ON (hac.name)",
                "CREATE INDEX hmiscript_name IF NOT EXISTS FOR (hs:HMIScript) ON (hs.name)",
                "CREATE INDEX hmitagtable_name IF NOT EXISTS FOR (ht:HMITagTable) ON (ht.name)",
                "CREATE INDEX hmitextlist_name IF NOT EXISTS FOR (htl:HMITextList) ON (htl.name)",
                "CREATE INDEX plctagtable_name IF NOT EXISTS FOR (pt:PLCTagTable) ON (pt.name)",
                "CREATE INDEX plctag_name IF NOT EXISTS FOR (ptg:PLCTag) ON (ptg.name)",
                # ScadaTag lookup indexes (used by agent persist queries)
                "CREATE INDEX scadatag_name IF NOT EXISTS FOR (t:ScadaTag) ON (t.name)",
                "CREATE INDEX scadatag_opc_item_path IF NOT EXISTS FOR (t:ScadaTag) ON (t.opc_item_path)",
                # Agent monitoring indexes
                "CREATE INDEX anomalyevent_created IF NOT EXISTS FOR (e:AnomalyEvent) ON (e.created_at)",
                "CREATE INDEX anomalyevent_state IF NOT EXISTS FOR (e:AnomalyEvent) ON (e.state)",
                "CREATE INDEX anomalyevent_severity IF NOT EXISTS FOR (e:AnomalyEvent) ON (e.severity)",
                "CREATE INDEX anomalyevent_dedup_key IF NOT EXISTS FOR (e:AnomalyEvent) ON (e.dedup_key)",
                # Process-semantic layer indexes
                "CREATE INDEX processmedium_name IF NOT EXISTS FOR (pm:ProcessMedium) ON (pm.name)",
                "CREATE INDEX unitoperation_name IF NOT EXISTS FOR (uo:UnitOperation) ON (uo.name)",
                "CREATE INDEX operatingenvelope_name IF NOT EXISTS FOR (oe:OperatingEnvelope) ON (oe.name)",
                "CREATE INDEX physicalprinciple_name IF NOT EXISTS FOR (pp:PhysicalPrinciple) ON (pp.name)",
                "CREATE INDEX chemicalspecies_name IF NOT EXISTS FOR (cs:ChemicalSpecies) ON (cs.name)",
                "CREATE INDEX reaction_name IF NOT EXISTS FOR (rx:Reaction) ON (rx.name)",
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

    def init_agent_monitoring_schema(self) -> None:
        """Ensure agent monitoring labels and indexes exist."""
        self.create_indexes()

    def list_anomaly_events(
        self,
        limit: int = 100,
        state: Optional[str] = None,
        severity: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List persisted anomaly events for UI feeds."""
        with self.session() as session:
            clauses = []
            params: Dict[str, Any] = {"limit": max(1, min(limit, 500))}
            if state:
                clauses.append("e.state = $state")
                params["state"] = state
            if severity:
                clauses.append("e.severity = $severity")
                params["severity"] = severity
            if run_id:
                clauses.append("e.run_id = $run_id")
                params["run_id"] = run_id
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            query = f"""
                MATCH (e:AnomalyEvent)
                {where}
                OPTIONAL MATCH (e)-[:OBSERVED_ON]->(t:ScadaTag)
                OPTIONAL MATCH (e)-[:AFFECTS]->(eq:Equipment)
                RETURN e, collect(DISTINCT t.name) AS tags, collect(DISTINCT eq.name) AS equipment
                ORDER BY e.created_at DESC
                LIMIT $limit
            """
            result = session.run(query, **params)
            events: List[Dict[str, Any]] = []
            for record in result:
                node = record["e"]
                props = dict(node)
                props["tags"] = [x for x in record["tags"] if x]
                props["equipment"] = [x for x in record["equipment"] if x]
                events.append(props)
            return events

    def get_anomaly_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get one anomaly event with linked context labels."""
        with self.session() as session:
            result = session.run(
                """
                MATCH (e:AnomalyEvent {event_id: $event_id})
                OPTIONAL MATCH (e)-[:OBSERVED_ON]->(t:ScadaTag)
                OPTIONAL MATCH (e)-[:AFFECTS]->(eq:Equipment)
                OPTIONAL MATCH (e)-[r:RELATED_TO]->(n)
                RETURN e,
                       collect(DISTINCT t.name) AS tags,
                       collect(DISTINCT eq.name) AS equipment,
                       collect(DISTINCT {type: type(r), label: labels(n)[0], name: coalesce(n.name, n.symptom, n.phrase)}) AS related
                LIMIT 1
                """,
                event_id=event_id,
            )
            record = result.single()
            if not record:
                return None
            data = dict(record["e"])
            data["tags"] = [x for x in record["tags"] if x]
            data["equipment"] = [x for x in record["equipment"] if x]
            data["related"] = [
                x for x in record["related"] if x and x.get("name")
            ]
            return data

    def cleanup_anomaly_events(self, retention_days: int = 14) -> int:
        """Delete old anomaly events outside retention window."""
        with self.session() as session:
            result = session.run(
                """
                MATCH (e:AnomalyEvent)
                WHERE e.created_at IS NOT NULL
                  AND datetime(e.created_at) < datetime() - duration({days: $days})
                WITH collect(e) AS old_events
                FOREACH (n IN old_events | DETACH DELETE n)
                RETURN size(old_events) AS deleted
                """,
                days=max(1, retention_days),
            )
            record = result.single()
            return int(record["deleted"]) if record else 0

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

    # -------------------------------------------------------------------
    # AOI / FB cross-reference (dependency) relationships
    # -------------------------------------------------------------------

    def create_aoi_dependency(
        self,
        from_aoi: str,
        to_aoi: str,
        rel_type: str = "INSTANTIATES",
        via_tag: str = "",
        description: str = "",
    ) -> bool:
        """
        Create a dependency relationship between two AOI/FB nodes.

        Args:
            from_aoi:  Name of the AOI that *uses* the other.
            to_aoi:    Name of the AOI/UDT that is *used*.
            rel_type:  'INSTANTIATES' (AOI→AOI/FB) or 'USES_TYPE' (AOI→UDT).
            via_tag:   The variable name that caused the dependency
                       (e.g. 'valve1' whose type is 'ValveStatus').
            description: Optional human-readable note.

        Returns:
            True if the relationship was created, False if either node is missing.
        """
        if rel_type not in ("INSTANTIATES", "USES_TYPE"):
            rel_type = "INSTANTIATES"

        with self.session() as session:
            result = session.run(
                f"""
                MATCH (a:AOI {{name: $from_aoi}})
                MATCH (b:AOI {{name: $to_aoi}})
                MERGE (a)-[r:{rel_type}]->(b)
                SET r.via_tag = $via_tag,
                    r.description = $description
                RETURN a.name AS src, b.name AS tgt
                """,
                {
                    "from_aoi": from_aoi,
                    "to_aoi": to_aoi,
                    "via_tag": via_tag,
                    "description": description,
                },
            )
            return result.single() is not None

    def create_aoi_dependencies_batch(
        self, dependencies: List[Dict]
    ) -> int:
        """
        Batch-create AOI dependency relationships.

        Each dict in *dependencies* must have:
            from_aoi, to_aoi, rel_type, via_tag (opt), description (opt)

        Returns:
            Number of relationships created.
        """
        if not dependencies:
            return 0

        with self.session() as session:
            result = session.run(
                """
                UNWIND $deps AS d
                MATCH (a:AOI {name: d.from_aoi})
                MATCH (b:AOI {name: d.to_aoi})
                WITH a, b, d
                // Two-branch FOREACH to pick INSTANTIATES vs USES_TYPE
                FOREACH (_ IN CASE WHEN d.rel_type = 'INSTANTIATES' THEN [1] ELSE [] END |
                    MERGE (a)-[r:INSTANTIATES]->(b)
                    SET r.via_tag = d.via_tag, r.description = d.description
                )
                FOREACH (_ IN CASE WHEN d.rel_type = 'USES_TYPE' THEN [1] ELSE [] END |
                    MERGE (a)-[r:USES_TYPE]->(b)
                    SET r.via_tag = d.via_tag, r.description = d.description
                )
                RETURN count(*) AS cnt
                """,
                {
                    "deps": [
                        {
                            "from_aoi": d["from_aoi"],
                            "to_aoi": d["to_aoi"],
                            "rel_type": d.get("rel_type", "INSTANTIATES"),
                            "via_tag": d.get("via_tag", ""),
                            "description": d.get("description", ""),
                        }
                        for d in dependencies
                    ]
                },
            )
            record = result.single()
            return record["cnt"] if record else 0

    def get_aoi_dependencies(
        self, name: Optional[str] = None
    ) -> List[Dict]:
        """
        Get AOI dependency relationships.

        Args:
            name: If provided, get dependencies for a specific AOI.
                  If None, get all dependencies.

        Returns:
            List of dicts with from_aoi, to_aoi, rel_type, via_tag, description.
        """
        with self.session() as session:
            if name:
                result = session.run(
                    """
                    MATCH (a:AOI {name: $name})-[r:INSTANTIATES|USES_TYPE]->(b:AOI)
                    RETURN a.name AS from_aoi, b.name AS to_aoi,
                           type(r) AS rel_type, r.via_tag AS via_tag,
                           r.description AS description
                    ORDER BY type(r), b.name
                    """,
                    {"name": name},
                )
            else:
                result = session.run(
                    """
                    MATCH (a:AOI)-[r:INSTANTIATES|USES_TYPE]->(b:AOI)
                    RETURN a.name AS from_aoi, b.name AS to_aoi,
                           type(r) AS rel_type, r.via_tag AS via_tag,
                           r.description AS description
                    ORDER BY a.name, type(r), b.name
                    """
                )

            return [dict(record) for record in result]

    def get_all_aoi_names(self) -> List[Dict[str, str]]:
        """
        Return name and type for every AOI node.
        Useful for building a lookup table for cross-referencing.
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (a:AOI)
                RETURN a.name AS name, a.type AS type
                ORDER BY a.name
                """
            )
            return [dict(r) for r in result]

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

    # -------------------------------------------------------------------------
    # Project Management
    # -------------------------------------------------------------------------

    def create_project(
        self,
        name: str,
        title: str = "",
        description: str = "",
        parent: Optional[str] = None,
        enabled: bool = True,
        inheritable: bool = False,
    ) -> str:
        """Create a Project node with optional inheritance relationship.

        Args:
            name: Project name (unique identifier)
            title: Display title
            description: Project description
            parent: Name of parent project for inheritance
            enabled: Whether project is enabled
            inheritable: Whether project can be inherited from

        Returns:
            Project name
        """
        with self.session() as session:
            # Create or update the project node
            session.run(
                """
                MERGE (p:Project {name: $name})
                SET p.title = $title,
                    p.description = $description,
                    p.enabled = $enabled,
                    p.inheritable = $inheritable
            """,
                {
                    "name": name,
                    "title": title,
                    "description": description,
                    "enabled": enabled,
                    "inheritable": inheritable,
                },
            )

            # Create INHERITS_FROM relationship if parent specified
            if parent:
                session.run(
                    """
                    MATCH (child:Project {name: $child_name})
                    MATCH (parent:Project {name: $parent_name})
                    MERGE (child)-[:INHERITS_FROM]->(parent)
                """,
                    {"child_name": name, "parent_name": parent},
                )

        return name

    def get_all_projects(self) -> List[Dict]:
        """Get all projects with their inheritance info.

        Returns:
            List of project dicts with name, title, parent, inheritable, enabled
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (p:Project)
                OPTIONAL MATCH (p)-[:INHERITS_FROM]->(parent:Project)
                RETURN p.name as name,
                       p.title as title,
                       p.description as description,
                       p.enabled as enabled,
                       p.inheritable as inheritable,
                       parent.name as parent
                ORDER BY p.name
            """
            )
            return [dict(r) for r in result]

    def get_project_inheritance_chain(self, project_name: str) -> List[str]:
        """Get the inheritance chain for a project (current -> parent -> grandparent).

        Args:
            project_name: Starting project name

        Returns:
            List of project names from current to root ancestor
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH path = (p:Project {name: $name})-[:INHERITS_FROM*0..10]->(ancestor:Project)
                WITH DISTINCT ancestor, length(path) as depth
                ORDER BY depth
                RETURN ancestor.name as name
            """,
                {"name": project_name},
            )
            return [r["name"] for r in result]

    def get_project_resources(self, project_name: str) -> Dict[str, List[Dict]]:
        """Get all resources belonging to a project.

        Args:
            project_name: Project name

        Returns:
            Dict with views, scripts, queries, events, components lists
        """
        with self.session() as session:
            resources = {
                "views": [],
                "scripts": [],
                "queries": [],
                "events": [],
                "components": [],
            }

            # Get views with component counts
            result = session.run(
                """
                MATCH (v:View)-[:BELONGS_TO]->(p:Project {name: $project})
                OPTIONAL MATCH (v)-[:HAS_COMPONENT]->(c:ViewComponent)
                WHERE c.deleted IS NULL OR c.deleted = false
                WITH v, 
                     count(c) as component_count,
                     count(CASE WHEN c.semantic_status = 'complete' THEN 1 END) as enriched_count
                RETURN v.name as name, v.path as path, v.purpose as purpose,
                       v.semantic_status as status,
                       component_count, enriched_count
                ORDER BY v.name
            """,
                {"project": project_name},
            )
            resources["views"] = [dict(r) for r in result]

            # Get scripts
            result = session.run(
                """
                MATCH (s:Script)-[:BELONGS_TO]->(p:Project {name: $project})
                RETURN s.name as name, s.path as path, s.scope as scope,
                       s.semantic_status as status
                ORDER BY s.path
            """,
                {"project": project_name},
            )
            resources["scripts"] = [dict(r) for r in result]

            # Get named queries
            result = session.run(
                """
                MATCH (q:NamedQuery)-[:BELONGS_TO]->(p:Project {name: $project})
                RETURN q.name as name, q.folder_path as folder_path,
                       q.semantic_status as status
                ORDER BY q.name
            """,
                {"project": project_name},
            )
            resources["queries"] = [dict(r) for r in result]

            # Get gateway events
            result = session.run(
                """
                MATCH (e:GatewayEvent)-[:BELONGS_TO]->(p:Project {name: $project})
                RETURN e.name as name, e.script_type as script_type,
                       e.delay as delay
                ORDER BY e.script_type, e.name
            """,
                {"project": project_name},
            )
            resources["events"] = [dict(r) for r in result]

            # Get view components (through views that belong to project)
            result = session.run(
                """
                MATCH (v:View)-[:BELONGS_TO]->(p:Project {name: $project})
                MATCH (v)-[:HAS_COMPONENT]->(c:ViewComponent)
                WHERE c.deleted IS NULL OR c.deleted = false
                RETURN c.name as name, c.path as path, c.component_type as component_type,
                       c.semantic_status as status, v.name as view_name
                ORDER BY v.name, c.path
            """,
                {"project": project_name},
            )
            resources["components"] = [dict(r) for r in result]

            return resources

    def get_gateway_resources(self) -> Dict[str, List[Dict]]:
        """Get all gateway-wide resources (Tags, UDTs, AOIs).

        Returns:
            Dict with tags, udts, aois lists
        """
        with self.session() as session:
            resources = {"tags": [], "udts": [], "aois": []}

            # Get ScadaTags (gateway-wide)
            result = session.run(
                """
                MATCH (t:ScadaTag)
                WHERE t.deleted IS NULL OR t.deleted = false
                RETURN t.name as name, t.tag_type as tag_type,
                       t.folder_name as folder, t.semantic_status as status
                ORDER BY t.folder_name, t.name
                LIMIT 500
            """
            )
            resources["tags"] = [dict(r) for r in result]

            # Get UDTs (gateway-wide, exclude Siemens TIA UDTs)
            result = session.run(
                """
                MATCH (u:UDT)
                WHERE (u.deleted IS NULL OR u.deleted = false)
                  AND NOT (:PLCDevice)-[:HAS_TYPE]->(u)
                RETURN u.name as name, u.purpose as purpose,
                       u.semantic_status as status
                ORDER BY u.name
            """
            )
            resources["udts"] = [dict(r) for r in result]

            # Get AOIs (gateway-wide, exclude Siemens TIA AOIs)
            result = session.run(
                """
                MATCH (a:AOI)
                WHERE (a.deleted IS NULL OR a.deleted = false)
                  AND NOT (:PLCDevice)-[:HAS_BLOCK]->(a)
                RETURN a.name as name, a.type as type, a.purpose as purpose,
                       a.semantic_status as status
                ORDER BY a.name
            """
            )
            resources["aois"] = [dict(r) for r in result]

            return resources

    def create_script(
        self,
        name: str,
        path: str,
        project: str,
        scope: str = "A",
        script_text: str = "",
        semantic_status: str = "pending",
    ) -> str:
        """Create a Script node and link to project.

        Args:
            name: Qualified script name (project/path)
            path: Script path within project
            project: Project name
            scope: Script scope (A=All, G=Gateway, C=Client, D=Designer)
            script_text: Full script code from code.py file
            semantic_status: Analysis status

        Returns:
            Script name
        """
        with self.session() as session:
            session.run(
                """
                MERGE (s:Script {name: $name})
                SET s.path = $path,
                    s.project = $project,
                    s.scope = $scope,
                    s.script_text = $script_text,
                    s.semantic_status = COALESCE(s.semantic_status, $semantic_status)
                WITH s
                MATCH (p:Project {name: $project})
                MERGE (s)-[:BELONGS_TO]->(p)
            """,
                {
                    "name": name,
                    "path": path,
                    "project": project,
                    "scope": scope,
                    "script_text": script_text,
                    "semantic_status": semantic_status,
                },
            )
        return name

    def create_gateway_event(
        self,
        name: str,
        project: str,
        script_type: str,
        event_name: Optional[str] = None,
        script_preview: str = "",
        delay: Optional[int] = None,
    ) -> str:
        """Create a GatewayEvent node and link to project.

        Args:
            name: Qualified event name (project/script_type/event_name)
            project: Project name
            script_type: Event type (startup, shutdown, timer, message_handler)
            event_name: Name for timer/message handler
            script_preview: Preview of script code
            delay: Delay in ms for timer scripts

        Returns:
            Event name
        """
        with self.session() as session:
            session.run(
                """
                MERGE (e:GatewayEvent {name: $name})
                SET e.project = $project,
                    e.script_type = $script_type,
                    e.event_name = $event_name,
                    e.script_preview = $script_preview,
                    e.delay = $delay
                WITH e
                MATCH (p:Project {name: $project})
                MERGE (e)-[:BELONGS_TO]->(p)
            """,
                {
                    "name": name,
                    "project": project,
                    "script_type": script_type,
                    "event_name": event_name,
                    "script_preview": script_preview,
                    "delay": delay,
                },
            )
        return name

    def create_named_query(
        self,
        name: str,
        project: str,
        folder_path: str = "",
        query_id: str = "",
        query_text: str = "",
        database: str = "",
        semantic_status: str = "pending",
    ) -> str:
        """Create a NamedQuery node and link to project.

        Args:
            name: Qualified query name (project/folder/query_name)
            project: Project name
            folder_path: Folder path within project
            query_id: Query identifier
            query_text: Full SQL from query.sql file
            database: DB connection name this query targets
            semantic_status: Analysis status

        Returns:
            Query name
        """
        with self.session() as session:
            session.run(
                """
                MERGE (q:NamedQuery {name: $name})
                SET q.project = $project,
                    q.folder_path = $folder_path,
                    q.query_id = $query_id,
                    q.query_text = $query_text,
                    q.database = $database,
                    q.semantic_status = COALESCE(q.semantic_status, $semantic_status)
                WITH q
                MATCH (p:Project {name: $project})
                MERGE (q)-[:BELONGS_TO]->(p)
            """,
                {
                    "name": name,
                    "project": project,
                    "folder_path": folder_path,
                    "query_id": query_id,
                    "query_text": query_text,
                    "database": database or "",
                    "semantic_status": semantic_status,
                },
            )
        return name

    # -------------------------------------------------------------------------
    # Database Connection Operations
    # -------------------------------------------------------------------------

    def create_database_connection(
        self,
        name: str,
        database_type: str,
        url: str,
        username: str = "",
        enabled: bool = True,
        description: str = "",
        translator: str = "",
        max_active: int = 8,
        validation_query: str = "SELECT 1",
    ) -> str:
        """Create a DatabaseConnection node.

        Args:
            name: Connection name (e.g. 'ProveITDBMESLite')
            database_type: MYSQL, MSSQL, POSTGRESQL
            url: Connection URL (host:port/database)
            username: Default username from project config
            enabled: Whether the connection is enabled
            description: Optional description
            translator: SQL translator type
            max_active: Max active connections in pool
            validation_query: Query used to validate connections

        Returns:
            Connection name
        """
        with self.session() as session:
            session.run(
                """
                MERGE (d:DatabaseConnection {name: $name})
                SET d.database_type = $database_type,
                    d.url = $url,
                    d.username = $username,
                    d.enabled = $enabled,
                    d.description = $description,
                    d.translator = $translator,
                    d.max_active = $max_active,
                    d.validation_query = $validation_query
            """,
                {
                    "name": name,
                    "database_type": database_type,
                    "url": url,
                    "username": username,
                    "enabled": enabled,
                    "description": description,
                    "translator": translator,
                    "max_active": max_active,
                    "validation_query": validation_query,
                },
            )
        return name

    def link_uses_database(self) -> int:
        """Create USES_DATABASE edges from NamedQueries and query ScadaTags
        to their corresponding DatabaseConnection nodes. Run after all nodes
        are created.

        Returns:
            Number of relationships created.
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (q:NamedQuery)
                WHERE q.database IS NOT NULL AND q.database <> ''
                MATCH (d:DatabaseConnection {name: q.database})
                MERGE (q)-[:USES_DATABASE]->(d)
                RETURN count(*) AS cnt
            """
            )
            nq_count = result.single()["cnt"]

            result = session.run(
                """
                MATCH (t:ScadaTag)
                WHERE t.tag_type = 'query'
                  AND t.datasource IS NOT NULL AND t.datasource <> ''
                MATCH (d:DatabaseConnection {name: t.datasource})
                MERGE (t)-[:USES_DATABASE]->(d)
                RETURN count(*) AS cnt
            """
            )
            tag_count = result.single()["cnt"]

        return nq_count + tag_count

    # -------------------------------------------------------------------------
    # UDT Operations
    # -------------------------------------------------------------------------

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
        project: Optional[str] = None,
        semantic_status: str = "pending",
    ) -> str:
        """Create a SCADA view node with optional project association.

        Args:
            name: View name (project-qualified if project specified)
            path: View path in Ignition
            purpose: Semantic description (empty if not yet analyzed)
            project: Project name (creates BELONGS_TO relationship)
            semantic_status: One of 'pending', 'in_progress', 'complete', 'review'
        """
        with self.session() as session:
            session.run(
                """
                MERGE (v:View {name: $name})
                SET v.path = $path,
                    v.project = $project
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
                    "project": project,
                    "semantic_status": semantic_status,
                },
            )

            # Create BELONGS_TO relationship if project specified
            if project:
                session.run(
                    """
                    MATCH (v:View {name: $name})
                    MATCH (p:Project {name: $project})
                    MERGE (v)-[:BELONGS_TO]->(p)
                """,
                    {"name": name, "project": project},
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
        unresolved_bindings: list = None,
        event_scripts: list = None,
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
            unresolved_bindings: Bindings that couldn't resolve to a known entity
            event_scripts: Event script text extracted from this component

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
                    c.props = $props,
                    c.unresolved_bindings = $unresolved_bindings,
                    c.event_scripts = $event_scripts
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
                    "unresolved_bindings": json.dumps(unresolved_bindings) if unresolved_bindings else None,
                    "event_scripts": json.dumps(event_scripts) if event_scripts else None,
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
        binding_type: str = "",
        target_text: str = "",
        bidirectional: bool = False,
    ) -> bool:
        """Create a BINDS_TO relationship between a ViewComponent and a UDT.

        Args:
            view_name: Name of the parent view
            component_path: Path of the component within the view
            udt_name: Name of the UDT being bound
            binding_property: Which property is bound (e.g., 'value', 'text', 'visible')
            tag_path: Full tag path of the binding
            binding_type: Type of binding (tag, expression, query, property)
            target_text: Full binding target as-written
            bidirectional: Whether the binding is bidirectional

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
                    r.tag_path = $tag_path,
                    r.binding_type = $binding_type,
                    r.target_text = $target_text,
                    r.bidirectional = $bidirectional
                RETURN c.path as component, u.name as udt
            """,
                {
                    "view_name": view_name,
                    "component_path": component_path,
                    "udt_name": udt_name,
                    "binding_property": binding_property,
                    "tag_path": tag_path,
                    "binding_type": binding_type,
                    "target_text": target_text,
                    "bidirectional": bidirectional,
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
        binding_type: str = "",
        target_text: str = "",
        bidirectional: bool = False,
    ) -> bool:
        """Create a BINDS_TO relationship between a ViewComponent and a ScadaTag.

        Args:
            view_name: Name of the parent view
            component_path: Path of the component within the view
            tag_name: Name of the ScadaTag being bound
            binding_property: Which property is bound (e.g., 'value', 'text', 'visible')
            tag_path: Full tag path of the binding
            binding_type: Type of binding (tag, expression, query, property)
            target_text: Full binding target as-written
            bidirectional: Whether the binding is bidirectional

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
                    r.tag_path = $tag_path,
                    r.binding_type = $binding_type,
                    r.target_text = $target_text,
                    r.bidirectional = $bidirectional
                RETURN c.path as component, t.name as tag
            """,
                {
                    "view_name": view_name,
                    "component_path": component_path,
                    "tag_name": tag_name,
                    "binding_property": binding_property,
                    "tag_path": tag_path,
                    "binding_type": binding_type,
                    "target_text": target_text,
                    "bidirectional": bidirectional,
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
    # Cross-Reference Relationships (Script/Query/View calls)
    # =========================================================================

    def create_script_call(
        self,
        source_type: str,
        source_name: str,
        target_script: str,
        function_name: str = "",
        source_project: str = "",
    ) -> bool:
        """Create a CALLS_SCRIPT relationship between entities.

        Args:
            source_type: Type of caller (Script, View, GatewayEvent)
            source_name: Name of the calling entity
            target_script: Name/path of the target script module
            function_name: Optional function being called
            source_project: Project context for the source (needed for Views)

        Returns:
            True if relationship was created
        """
        with self.session() as session:
            # Build source match based on type
            if source_type == "View":
                # Views use project-qualified names
                source_match = "MATCH (s:View {name: $source})"
            elif source_type == "GatewayEvent":
                source_match = "MATCH (s:GatewayEvent {name: $source})"
            else:
                source_match = "MATCH (s:Script {name: $source})"

            # Match scripts where the path starts with the module name
            # e.g., target="pss" should match path="pss/assets" or name containing "pss"
            result = session.run(
                f"""
                {source_match}
                MATCH (t:Script)
                WHERE t.name = $target 
                   OR t.path = $target 
                   OR t.path STARTS WITH $target + '/'
                   OR t.name STARTS WITH $target + '/'
                   OR t.path CONTAINS '/' + $target + '/'
                MERGE (s)-[r:CALLS_SCRIPT]->(t)
                SET r.function = $function,
                    r.source_project = $source_project
                RETURN s.name as source, t.name as target
            """,
                {
                    "source": source_name,
                    "target": target_script,
                    "function": function_name,
                    "source_project": source_project,
                },
            )
            return result.single() is not None

    def create_query_usage(
        self,
        source_type: str,
        source_name: str,
        query_path: str,
        source_project: str = "",
    ) -> bool:
        """Create a USES_QUERY relationship between an entity and a NamedQuery.

        Args:
            source_type: Type of caller (Script, View, GatewayEvent, ViewComponent)
            source_name: Name of the calling entity. For ViewComponent, use format "view_name/component_path"
            query_path: Path of the named query (e.g., "GIS/GetAreaById")
            source_project: Project context for the source

        Returns:
            True if relationship was created
        """
        with self.session() as session:
            # Build source match based on type
            if source_type == "View":
                source_match = "MATCH (s:View {name: $source})"
                params = {"source": source_name}
            elif source_type == "GatewayEvent":
                source_match = "MATCH (s:GatewayEvent {name: $source})"
                params = {"source": source_name}
            elif source_type == "ViewComponent":
                # source_name format: "view_name/component_path" e.g. "ProveIT/Run Data/root/areaLineDropdown"
                # ViewComponent is matched by view + path
                # Split to get view_name and component_path
                parts = source_name.split("/", 2)  # Split into at most 3 parts
                if len(parts) >= 3:
                    # Format: project/viewname/component_path
                    view_name = f"{parts[0]}/{parts[1]}"
                    comp_path = parts[2] if len(parts) > 2 else ""
                elif len(parts) == 2:
                    view_name = parts[0]
                    comp_path = parts[1]
                else:
                    view_name = source_name
                    comp_path = ""
                source_match = (
                    "MATCH (s:ViewComponent {view: $view_name, path: $comp_path})"
                )
                params = {"view_name": view_name, "comp_path": comp_path}
            else:
                source_match = "MATCH (s:Script {name: $source})"
                params = {"source": source_name}

            # Try to match query by path or name
            # Query names in Neo4j are project-qualified: "ProveIT/Charts/GetEquipmentRunTimeByID"
            # Query path from code can be: "Charts/GetEquipmentRunTimeByID" or just "GetEquipmentRunTimeByID"
            params["query_path"] = query_path
            params["source_project"] = source_project

            result = session.run(
                f"""
                {source_match}
                MATCH (q:NamedQuery)
                WHERE q.name = $query_path 
                   OR q.name ENDS WITH '/' + $query_path
                   OR q.name ENDS WITH $query_path
                   OR $query_path ENDS WITH '/' + q.name
                   OR q.name CONTAINS $query_path
                WITH s, q LIMIT 1
                MERGE (s)-[r:USES_QUERY]->(q)
                SET r.source_project = $source_project,
                    r.query_path = $query_path
                RETURN s.name as source, q.name as query
            """,
                params,
            )
            return result.single() is not None

    def create_view_script_event(
        self,
        view_name: str,
        component_path: str,
        event_type: str,
        script_text: str,
    ) -> bool:
        """Create a HAS_EVENT_SCRIPT relationship for view components with scripts.

        This stores the event script text on the relationship for later analysis.

        Args:
            view_name: Name of the view
            component_path: Path to the component within the view
            event_type: Type of event (onClick, onChange, etc.)
            script_text: The script code

        Returns:
            True if relationship was created
        """
        with self.session() as session:
            # Create or update the component node and relationship
            result = session.run(
                """
                MATCH (v:View {name: $view})
                MERGE (c:ViewComponent {path: $component_path})
                SET c.view = $view
                MERGE (v)-[:HAS_COMPONENT]->(c)
                MERGE (c)-[r:HAS_EVENT_SCRIPT]->(v)
                SET r.event_type = $event_type,
                    r.script_preview = left($script, 500)
                RETURN v.name as view, c.path as component
            """,
                {
                    "view": view_name,
                    "component_path": component_path,
                    "event_type": event_type,
                    "script": script_text,
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
            item_type: One of 'AOI', 'UDT', 'View', 'Equipment', 'ViewComponent', 'ScadaTag',
                       'Script', 'NamedQuery', 'GatewayEvent'
            limit: Maximum number of items to return

        Returns:
            List of dicts with 'name' and other relevant properties
        """
        valid_types = {
            "AOI",
            "UDT",
            "View",
            "Equipment",
            "ViewComponent",
            "ScadaTag",
            "Script",
            "NamedQuery",
            "GatewayEvent",
            # Siemens TIA Portal types
            "HMIScript",
            "HMIAlarm",
            "HMIScreen",
            "PLCTag",
        }
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
                           n.inferred_purpose as inferred_purpose,
                           n.unresolved_bindings as unresolved_bindings,
                           n.event_scripts as event_scripts
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
            elif item_type == "HMIScript":
                result = session.run(
                    """
                    MATCH (n:HMIScript)
                    WHERE (n.semantic_status = 'pending' OR n.semantic_status IS NULL)
                    RETURN n.name as name, n.hmi as hmi, n.project as project,
                           n.script_file as script_file,
                           n.functions as functions,
                           n.script_text as script_text
                    LIMIT $limit
                    """,
                    {"limit": limit},
                )
                return [dict(r) for r in result]
            elif item_type == "HMIAlarm":
                result = session.run(
                    """
                    MATCH (n:HMIAlarm)
                    WHERE (n.semantic_status = 'pending' OR n.semantic_status IS NULL)
                    RETURN n.name as name, n.hmi as hmi, n.project as project,
                           n.alarm_type as alarm_type,
                           n.alarm_class as alarm_class,
                           n.origin as origin, n.priority as priority,
                           n.raised_state_tag as raised_state_tag,
                           n.trigger_bit_address as trigger_bit_address,
                           n.trigger_mode as trigger_mode,
                           n.condition as condition,
                           n.condition_value as condition_value
                    LIMIT $limit
                    """,
                    {"limit": limit},
                )
                return [dict(r) for r in result]
            elif item_type == "HMIScreen":
                result = session.run(
                    """
                    MATCH (n:HMIScreen)
                    WHERE (n.semantic_status = 'pending' OR n.semantic_status IS NULL)
                    RETURN n.name as name, n.hmi as hmi, n.project as project,
                           n.folder as folder
                    LIMIT $limit
                    """,
                    {"limit": limit},
                )
                return [dict(r) for r in result]
            elif item_type == "PLCTag":
                result = session.run(
                    """
                    MATCH (n:PLCTag)
                    WHERE (n.semantic_status = 'pending' OR n.semantic_status IS NULL)
                    RETURN n.name as name, n.table as table_name,
                           n.plc as plc, n.project as project,
                           n.data_type as data_type,
                           n.logical_address as logical_address,
                           n.comment as comment
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
            item_type: One of 'AOI', 'UDT', 'View', 'Equipment', 'ViewComponent', 'ScadaTag',
                       'Script', 'NamedQuery', 'GatewayEvent'
            name: Name of the item (or path for ViewComponent)
            status: One of 'pending', 'in_progress', 'complete', 'review'
            purpose: Semantic description to set (only used when status='complete')

        Returns:
            True if item was found and updated
        """
        valid_types = {
            "AOI",
            "UDT",
            "View",
            "Equipment",
            "ViewComponent",
            "ScadaTag",
            "Script",
            "NamedQuery",
            "GatewayEvent",
            # Siemens TIA Portal types
            "HMIScript",
            "HMIAlarm",
            "HMIScreen",
            "PLCTag",
        }
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

    def get_semantic_status_counts(
        self, include_deleted: bool = False
    ) -> Dict[str, Dict[str, int]]:
        """Get counts of items by semantic status for each type.

        Args:
            include_deleted: If True, include deleted items in counts

        Returns:
            Dict like {'UDT': {'pending': 5, 'complete': 3, 'deleted': 1}, ...}
        """
        with self.session() as session:
            result = {}
            for item_type in [
                "AOI",
                "UDT",
                "View",
                "Equipment",
                "ViewComponent",
                "ScadaTag",
                # Siemens TIA Portal types
                "HMIScript",
                "HMIAlarm",
                "HMIScreen",
                "PLCTag",
            ]:
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
            aoi_result = session.run(
                """
                MATCH (a:AOI)
                RETURN 
                    sum(CASE WHEN a.troubleshooting_enriched = true THEN 1 ELSE 0 END) as enriched,
                    sum(CASE WHEN a.troubleshooting_enriched IS NULL OR a.troubleshooting_enriched = false THEN 1 ELSE 0 END) as pending
            """
            )
            aoi_record = aoi_result.single()
            result["AOI"] = {
                "enriched": aoi_record["enriched"] if aoi_record else 0,
                "pending": aoi_record["pending"] if aoi_record else 0,
            }

            # View enrichment status
            view_result = session.run(
                """
                MATCH (v:View)
                RETURN 
                    sum(CASE WHEN v.troubleshooting_enriched = true THEN 1 ELSE 0 END) as enriched,
                    sum(CASE WHEN v.troubleshooting_enriched IS NULL OR v.troubleshooting_enriched = false THEN 1 ELSE 0 END) as pending
            """
            )
            view_record = view_result.single()
            result["View"] = {
                "enriched": view_record["enriched"] if view_record else 0,
                "pending": view_record["pending"] if view_record else 0,
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
    # Process-Semantic Layer Write Helpers
    # =========================================================================

    def create_process_medium(
        self, name: str, category: str = "", phase: str = "",
        description: str = "", purpose: str = "",
        evidence_json: str = "",
    ) -> str:
        """Create or merge a ProcessMedium node with provenance."""
        with self.session() as session:
            session.run(
                """
                MERGE (n:ProcessMedium {name: $name})
                SET n.category = COALESCE(n.category, $category),
                    n.phase = COALESCE(n.phase, $phase),
                    n.description = COALESCE(n.description, $description),
                    n.purpose = COALESCE(n.purpose, $purpose),
                    n.evidence_items = CASE
                        WHEN $ev = '' THEN n.evidence_items
                        WHEN n.evidence_items IS NULL THEN $ev
                        ELSE n.evidence_items + $ev
                    END,
                    n.last_evidence_at = datetime()
                """,
                {"name": name, "category": category, "phase": phase,
                 "description": description, "purpose": purpose, "ev": evidence_json},
            )
        return name

    def create_unit_operation(
        self, name: str, category: str = "",
        description: str = "", purpose: str = "",
        evidence_json: str = "",
    ) -> str:
        """Create or merge a UnitOperation node with provenance."""
        with self.session() as session:
            session.run(
                """
                MERGE (n:UnitOperation {name: $name})
                SET n.category = COALESCE(n.category, $category),
                    n.description = COALESCE(n.description, $description),
                    n.purpose = COALESCE(n.purpose, $purpose),
                    n.evidence_items = CASE
                        WHEN $ev = '' THEN n.evidence_items
                        WHEN n.evidence_items IS NULL THEN $ev
                        ELSE n.evidence_items + $ev
                    END,
                    n.last_evidence_at = datetime()
                """,
                {"name": name, "category": category,
                 "description": description, "purpose": purpose, "ev": evidence_json},
            )
        return name

    def create_operating_envelope(
        self, name: str, parameter: str = "", unit: str = "",
        low_limit: float = None, low_warning: float = None,
        normal_low: float = None, normal_high: float = None,
        high_warning: float = None, high_limit: float = None,
        trip_low: float = None, trip_high: float = None,
        description: str = "", evidence_json: str = "",
    ) -> str:
        """Create or merge an OperatingEnvelope node with provenance."""
        with self.session() as session:
            session.run(
                """
                MERGE (n:OperatingEnvelope {name: $name})
                SET n.parameter = COALESCE(n.parameter, $parameter),
                    n.unit = COALESCE(n.unit, $unit),
                    n.description = COALESCE(n.description, $description),
                    n.evidence_items = CASE
                        WHEN $ev = '' THEN n.evidence_items
                        WHEN n.evidence_items IS NULL THEN $ev
                        ELSE n.evidence_items + $ev
                    END,
                    n.last_evidence_at = datetime()
                FOREACH (_ IN CASE WHEN $low_limit IS NOT NULL THEN [1] ELSE [] END |
                    SET n.low_limit = $low_limit)
                FOREACH (_ IN CASE WHEN $low_warning IS NOT NULL THEN [1] ELSE [] END |
                    SET n.low_warning = $low_warning)
                FOREACH (_ IN CASE WHEN $normal_low IS NOT NULL THEN [1] ELSE [] END |
                    SET n.normal_low = $normal_low)
                FOREACH (_ IN CASE WHEN $normal_high IS NOT NULL THEN [1] ELSE [] END |
                    SET n.normal_high = $normal_high)
                FOREACH (_ IN CASE WHEN $high_warning IS NOT NULL THEN [1] ELSE [] END |
                    SET n.high_warning = $high_warning)
                FOREACH (_ IN CASE WHEN $high_limit IS NOT NULL THEN [1] ELSE [] END |
                    SET n.high_limit = $high_limit)
                FOREACH (_ IN CASE WHEN $trip_low IS NOT NULL THEN [1] ELSE [] END |
                    SET n.trip_low = $trip_low)
                FOREACH (_ IN CASE WHEN $trip_high IS NOT NULL THEN [1] ELSE [] END |
                    SET n.trip_high = $trip_high)
                """,
                {"name": name, "parameter": parameter, "unit": unit,
                 "description": description, "ev": evidence_json,
                 "low_limit": low_limit, "low_warning": low_warning,
                 "normal_low": normal_low, "normal_high": normal_high,
                 "high_warning": high_warning, "high_limit": high_limit,
                 "trip_low": trip_low, "trip_high": trip_high},
            )
        return name

    def create_physical_principle(
        self, name: str, category: str = "", unit_family: str = "",
        description: str = "", evidence_json: str = "",
    ) -> str:
        """Create or merge a PhysicalPrinciple node with provenance."""
        with self.session() as session:
            session.run(
                """
                MERGE (n:PhysicalPrinciple {name: $name})
                SET n.category = COALESCE(n.category, $category),
                    n.unit_family = COALESCE(n.unit_family, $unit_family),
                    n.description = COALESCE(n.description, $description),
                    n.evidence_items = CASE
                        WHEN $ev = '' THEN n.evidence_items
                        WHEN n.evidence_items IS NULL THEN $ev
                        ELSE n.evidence_items + $ev
                    END,
                    n.last_evidence_at = datetime()
                """,
                {"name": name, "category": category,
                 "unit_family": unit_family, "description": description,
                 "ev": evidence_json},
            )
        return name

    def create_chemical_species(
        self, name: str, category: str = "", cas_number: str = "",
        molecular_formula: str = "", description: str = "",
        evidence_json: str = "",
    ) -> str:
        """Create or merge a ChemicalSpecies node with provenance."""
        with self.session() as session:
            session.run(
                """
                MERGE (n:ChemicalSpecies {name: $name})
                SET n.category = COALESCE(n.category, $category),
                    n.cas_number = COALESCE(n.cas_number, $cas_number),
                    n.molecular_formula = COALESCE(n.molecular_formula, $molecular_formula),
                    n.description = COALESCE(n.description, $description),
                    n.evidence_items = CASE
                        WHEN $ev = '' THEN n.evidence_items
                        WHEN n.evidence_items IS NULL THEN $ev
                        ELSE n.evidence_items + $ev
                    END,
                    n.last_evidence_at = datetime()
                """,
                {"name": name, "category": category,
                 "cas_number": cas_number, "molecular_formula": molecular_formula,
                 "description": description, "ev": evidence_json},
            )
        return name

    def create_reaction(
        self, name: str, category: str = "", description: str = "",
        conditions: str = "", evidence_json: str = "",
    ) -> str:
        """Create or merge a Reaction node with provenance."""
        with self.session() as session:
            session.run(
                """
                MERGE (n:Reaction {name: $name})
                SET n.category = COALESCE(n.category, $category),
                    n.description = COALESCE(n.description, $description),
                    n.conditions = COALESCE(n.conditions, $conditions),
                    n.evidence_items = CASE
                        WHEN $ev = '' THEN n.evidence_items
                        WHEN n.evidence_items IS NULL THEN $ev
                        ELSE n.evidence_items + $ev
                    END,
                    n.last_evidence_at = datetime()
                """,
                {"name": name, "category": category,
                 "description": description, "conditions": conditions,
                 "ev": evidence_json},
            )
        return name

    def create_process_relationship(
        self, source_label: str, source_name: str,
        target_label: str, target_name: str,
        rel_type: str, evidence_json: str = "",
        properties: dict = None,
    ) -> bool:
        """Create a process-semantic relationship with provenance.

        Only allows relationship types defined in PROCESS_RELATIONSHIPS.
        Returns True if the relationship was created/updated.
        """
        from process_semantics import PROCESS_RELATIONSHIPS
        if rel_type not in PROCESS_RELATIONSHIPS:
            return False

        prop_sets = ""
        params = {
            "src_name": source_name,
            "tgt_name": target_name,
            "ev": evidence_json,
        }
        if properties:
            for k, v in properties.items():
                param_key = f"prop_{k}"
                prop_sets += f", r.{k} = ${param_key}"
                params[param_key] = v

        with self.session() as session:
            session.run(
                f"""
                MATCH (src:{source_label} {{name: $src_name}})
                MATCH (tgt:{target_label} {{name: $tgt_name}})
                MERGE (src)-[r:{rel_type}]->(tgt)
                SET r.evidence_items = CASE
                        WHEN $ev = '' THEN r.evidence_items
                        WHEN r.evidence_items IS NULL THEN $ev
                        ELSE r.evidence_items + $ev
                    END,
                    r.last_evidence_at = datetime(){prop_sets}
                """,
                params,
            )
        return True

    def get_process_context_for_equipment(self, equipment_name: str) -> Dict:
        """Get process-semantic context for an equipment node.

        Returns media handled, operations performed, operating envelopes,
        and connected tags with their physical principles.
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (e:Equipment {name: $name})
                OPTIONAL MATCH (e)-[:HANDLES_MEDIUM]->(pm:ProcessMedium)
                OPTIONAL MATCH (e)-[:PERFORMS_OPERATION]->(uo:UnitOperation)
                OPTIONAL MATCH (e)-[:HAS_OPERATING_ENVELOPE]->(oe:OperatingEnvelope)
                OPTIONAL MATCH (e)<-[:MAPS_TO_SCADA]-(a:AOI)-[:IMPLEMENTS_CONTROL_OF]->(uo2:UnitOperation)
                RETURN e.name AS name,
                       collect(DISTINCT pm.name) AS media,
                       collect(DISTINCT uo.name) AS operations,
                       collect(DISTINCT {name: oe.name, parameter: oe.parameter,
                                         normal_low: oe.normal_low, normal_high: oe.normal_high,
                                         unit: oe.unit}) AS envelopes,
                       collect(DISTINCT uo2.name) AS controlled_operations
                """,
                {"name": equipment_name},
            )
            record = result.single()
            if not record:
                return {}
            return dict(record)

    def get_process_context_for_tag(self, tag_name: str) -> Dict:
        """Get process-semantic context for a SCADA tag."""
        with self.session() as session:
            result = session.run(
                """
                MATCH (t:ScadaTag {name: $name})
                OPTIONAL MATCH (t)-[:MEASURES]->(pp:PhysicalPrinciple)
                OPTIONAL MATCH (t)-[:MONITORS_ENVELOPE]->(oe:OperatingEnvelope)
                RETURN t.name AS name,
                       collect(DISTINCT pp.name) AS measures,
                       collect(DISTINCT {name: oe.name, parameter: oe.parameter,
                                         normal_low: oe.normal_low, normal_high: oe.normal_high,
                                         unit: oe.unit}) AS envelopes
                """,
                {"name": tag_name},
            )
            record = result.single()
            if not record:
                return {}
            return dict(record)

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

    def export_full_database(self) -> Dict:
        """Export the entire database to a serializable dict for backup.

        Returns:
            Dict with nodes, relationships, and metadata
        """
        from datetime import datetime

        with self.session() as session:
            # Export all nodes with their labels and properties
            nodes_result = session.run(
                """
                MATCH (n)
                RETURN elementId(n) as id, labels(n) as labels, properties(n) as props
                """
            )
            nodes = []
            node_id_map = {}  # Map internal IDs to export IDs
            for idx, record in enumerate(nodes_result):
                export_id = str(idx)
                node_id_map[record["id"]] = export_id
                nodes.append(
                    {
                        "id": export_id,
                        "labels": list(record["labels"]),
                        "properties": self._serialize_properties(dict(record["props"])),
                    }
                )

            # Export all relationships
            rels_result = session.run(
                """
                MATCH (a)-[r]->(b)
                RETURN elementId(a) as source, elementId(b) as target, 
                       type(r) as type, properties(r) as props
                """
            )
            relationships = []
            for record in rels_result:
                source_id = node_id_map.get(record["source"])
                target_id = node_id_map.get(record["target"])
                if source_id is not None and target_id is not None:
                    relationships.append(
                        {
                            "source": source_id,
                            "target": target_id,
                            "type": record["type"],
                            "properties": self._serialize_properties(
                                dict(record["props"])
                            ),
                        }
                    )

            return {
                "version": "1.0",
                "type": "neo4j_backup",
                "metadata": {
                    "exported_at": datetime.now().isoformat(),
                    "node_count": len(nodes),
                    "relationship_count": len(relationships),
                },
                "nodes": nodes,
                "relationships": relationships,
            }

    def _serialize_properties(self, props: Dict) -> Dict:
        """Convert Neo4j properties to JSON-serializable format."""
        from datetime import datetime as dt

        result = {}
        for key, value in props.items():
            if hasattr(value, "isoformat"):  # datetime objects
                result[key] = value.isoformat()
            elif isinstance(value, (list, tuple)):
                result[key] = [
                    v.isoformat() if hasattr(v, "isoformat") else v for v in value
                ]
            else:
                result[key] = value
        return result

    def import_full_database(self, data: Dict, clear_first: bool = True) -> Dict:
        """Import a full database backup.

        Args:
            data: The backup data from export_full_database
            clear_first: If True, clear the database before importing

        Returns:
            Dict with import statistics
        """
        if data.get("type") != "neo4j_backup":
            raise ValueError("Invalid backup format: expected 'neo4j_backup' type")

        with self.session() as session:
            if clear_first:
                session.run("MATCH (n) DETACH DELETE n")

            # Create nodes - build a map from export ID to new Neo4j ID
            id_map = {}
            nodes_created = 0

            for node in data.get("nodes", []):
                export_id = node["id"]
                labels = ":".join(node["labels"])
                props = node.get("properties", {})

                # Create node with labels and properties
                result = session.run(
                    f"""
                    CREATE (n:{labels})
                    SET n = $props
                    RETURN elementId(n) as id
                    """,
                    {"props": props},
                )
                record = result.single()
                if record:
                    id_map[export_id] = record["id"]
                    nodes_created += 1

            # Create relationships
            rels_created = 0
            for rel in data.get("relationships", []):
                source_id = id_map.get(rel["source"])
                target_id = id_map.get(rel["target"])

                if source_id and target_id:
                    rel_type = rel["type"]
                    props = rel.get("properties", {})

                    session.run(
                        f"""
                        MATCH (a), (b)
                        WHERE elementId(a) = $source AND elementId(b) = $target
                        CREATE (a)-[r:{rel_type}]->(b)
                        SET r = $props
                        """,
                        {"source": source_id, "target": target_id, "props": props},
                    )
                    rels_created += 1

            # Recreate indexes
            self.create_indexes()

            return {
                "nodes_created": nodes_created,
                "relationships_created": rels_created,
            }

    # =========================================================================
    # Siemens TIA Portal Project Operations
    # =========================================================================

    def create_tia_project(
        self,
        name: str,
        directory: str,
    ) -> str:
        """Create a TiaProject node.

        Args:
            name: Project name (e.g. "ECar_Demo")
            directory: Source directory path

        Returns:
            The project name.
        """
        with self.session() as session:
            session.run(
                """
                MERGE (tp:TiaProject {name: $name})
                SET tp.directory = $directory,
                    tp.platform = 'Siemens',
                    tp.imported_at = datetime()
                """,
                {"name": name, "directory": directory},
            )
        return name

    def create_plc_device(
        self,
        name: str,
        project_name: str,
        dir_name: str = "",
    ) -> str:
        """Create a PLCDevice node and link to its TiaProject.

        Args:
            name: PLC name (e.g. "PLC_1")
            project_name: Parent TiaProject name
            dir_name: Directory name (e.g. "PLC_PLC_1")

        Returns:
            The device name.
        """
        with self.session() as session:
            session.run(
                """
                MERGE (pd:PLCDevice {name: $name, project: $project})
                SET pd.dir_name = $dir_name,
                    pd.platform = 'Siemens'
                WITH pd
                MATCH (tp:TiaProject {name: $project})
                MERGE (tp)-[:HAS_DEVICE]->(pd)
                """,
                {"name": name, "project": project_name, "dir_name": dir_name},
            )
        return name

    def create_hmi_device(
        self,
        name: str,
        project_name: str,
        dir_name: str = "",
    ) -> str:
        """Create an HMIDevice node and link to its TiaProject.

        Args:
            name: HMI name (e.g. "HMI_RT_1")
            project_name: Parent TiaProject name
            dir_name: Directory name (e.g. "HMI_HMI_RT_1")

        Returns:
            The device name.
        """
        with self.session() as session:
            session.run(
                """
                MERGE (hd:HMIDevice {name: $name, project: $project})
                SET hd.dir_name = $dir_name,
                    hd.platform = 'Siemens'
                WITH hd
                MATCH (tp:TiaProject {name: $project})
                MERGE (tp)-[:HAS_DEVICE]->(hd)
                """,
                {"name": name, "project": project_name, "dir_name": dir_name},
            )
        return name

    def create_hmi_connection(
        self,
        name: str,
        hmi_name: str,
        project_name: str,
        partner: str = "",
        station: str = "",
        communication_driver: str = "",
        node: str = "",
        address: str = "",
    ) -> str:
        """Create an HMIConnection node and link HMI->PLC.

        Args:
            name: Connection name (e.g. "HMI_Connection_2")
            hmi_name: Parent HMIDevice name
            project_name: Parent TiaProject name
            partner: PLC partner name (e.g. "PLC_1")
            station: Station string
            communication_driver: Driver string
            node: Node/CPU description
            address: Raw address string

        Returns:
            Connection name.
        """
        with self.session() as session:
            session.run(
                """
                MERGE (hc:HMIConnection {name: $name, hmi: $hmi, project: $project})
                SET hc.partner = $partner,
                    hc.station = $station,
                    hc.communication_driver = $driver,
                    hc.node = $node,
                    hc.address = $address
                WITH hc
                MATCH (hd:HMIDevice {name: $hmi, project: $project})
                MERGE (hd)-[:HAS_CONNECTION]->(hc)
                """,
                {
                    "name": name,
                    "hmi": hmi_name,
                    "project": project_name,
                    "partner": partner,
                    "station": station,
                    "driver": communication_driver,
                    "node": node,
                    "address": address,
                },
            )

            # Link to PLC device if it exists
            if partner:
                session.run(
                    """
                    MATCH (hc:HMIConnection {name: $name, hmi: $hmi, project: $project})
                    MATCH (pd:PLCDevice {name: $partner, project: $project})
                    MERGE (hc)-[:CONNECTS_TO]->(pd)
                    """,
                    {
                        "name": name,
                        "hmi": hmi_name,
                        "project": project_name,
                        "partner": partner,
                    },
                )

        return name

    def create_hmi_alarm_class(
        self,
        name: str,
        hmi_name: str,
        project_name: str,
        priority: str = "0",
        state_machine: str = "",
        is_system: bool = False,
    ) -> str:
        """Create an HMIAlarmClass node."""
        with self.session() as session:
            session.run(
                """
                MERGE (hac:HMIAlarmClass {name: $name, hmi: $hmi, project: $project})
                SET hac.priority = $priority,
                    hac.state_machine = $state_machine,
                    hac.is_system = $is_system
                WITH hac
                MATCH (hd:HMIDevice {name: $hmi, project: $project})
                MERGE (hd)-[:HAS_ALARM_CLASS]->(hac)
                """,
                {
                    "name": name,
                    "hmi": hmi_name,
                    "project": project_name,
                    "priority": priority,
                    "state_machine": state_machine,
                    "is_system": is_system,
                },
            )
        return name

    def create_hmi_alarm(
        self,
        name: str,
        hmi_name: str,
        project_name: str,
        alarm_type: str = "Discrete",
        alarm_class: str = "",
        origin: str = "",
        priority: str = "0",
        raised_state_tag: str = "",
        trigger_bit_address: str = "",
        trigger_mode: str = "",
        condition: str = "",
        condition_value: str = "",
    ) -> str:
        """Create an HMIAlarm node and link to HMI device and alarm class."""
        with self.session() as session:
            session.run(
                """
                MERGE (ha:HMIAlarm {name: $name, hmi: $hmi, project: $project})
                SET ha.alarm_type = $alarm_type,
                    ha.alarm_class = $alarm_class,
                    ha.origin = $origin,
                    ha.priority = $priority,
                    ha.raised_state_tag = $raised_state_tag,
                    ha.trigger_bit_address = $trigger_bit_address,
                    ha.trigger_mode = $trigger_mode,
                    ha.condition = $condition,
                    ha.condition_value = $condition_value,
                    ha.semantic_status = COALESCE(ha.semantic_status, 'pending')
                WITH ha
                MATCH (hd:HMIDevice {name: $hmi, project: $project})
                MERGE (hd)-[:HAS_ALARM]->(ha)
                """,
                {
                    "name": name,
                    "hmi": hmi_name,
                    "project": project_name,
                    "alarm_type": alarm_type,
                    "alarm_class": alarm_class,
                    "origin": origin,
                    "priority": priority,
                    "raised_state_tag": raised_state_tag,
                    "trigger_bit_address": trigger_bit_address,
                    "trigger_mode": trigger_mode,
                    "condition": condition,
                    "condition_value": condition_value,
                },
            )

            # Link to alarm class if it exists
            if alarm_class:
                session.run(
                    """
                    MATCH (ha:HMIAlarm {name: $name, hmi: $hmi, project: $project})
                    MATCH (hac:HMIAlarmClass {name: $alarm_class, hmi: $hmi, project: $project})
                    MERGE (ha)-[:CLASSIFIED_AS]->(hac)
                    """,
                    {
                        "name": name,
                        "hmi": hmi_name,
                        "project": project_name,
                        "alarm_class": alarm_class,
                    },
                )

        return name

    def create_hmi_tag_table(
        self,
        name: str,
        hmi_name: str,
        project_name: str,
        folder: str = "",
    ) -> str:
        """Create an HMITagTable node."""
        with self.session() as session:
            session.run(
                """
                MERGE (ht:HMITagTable {name: $name, hmi: $hmi, project: $project})
                SET ht.folder = $folder
                WITH ht
                MATCH (hd:HMIDevice {name: $hmi, project: $project})
                MERGE (hd)-[:HAS_TAG_TABLE]->(ht)
                """,
                {
                    "name": name,
                    "hmi": hmi_name,
                    "project": project_name,
                    "folder": folder,
                },
            )
        return name

    def create_hmi_script(
        self,
        name: str,
        hmi_name: str,
        project_name: str,
        script_file: str = "",
        functions: Optional[List[str]] = None,
        script_text: str = "",
    ) -> str:
        """Create an HMIScript node."""
        with self.session() as session:
            session.run(
                """
                MERGE (hs:HMIScript {name: $name, hmi: $hmi, project: $project})
                SET hs.script_file = $script_file,
                    hs.functions = $functions,
                    hs.script_text = $script_text,
                    hs.semantic_status = COALESCE(hs.semantic_status, 'pending')
                WITH hs
                MATCH (hd:HMIDevice {name: $hmi, project: $project})
                MERGE (hd)-[:HAS_SCRIPT]->(hs)
                """,
                {
                    "name": name,
                    "hmi": hmi_name,
                    "project": project_name,
                    "script_file": script_file,
                    "functions": functions or [],
                    "script_text": script_text[:5000] if script_text else "",
                },
            )
        return name

    def create_hmi_text_list(
        self,
        name: str,
        hmi_name: str,
        project_name: str,
    ) -> str:
        """Create an HMITextList node."""
        with self.session() as session:
            session.run(
                """
                MERGE (htl:HMITextList {name: $name, hmi: $hmi, project: $project})
                WITH htl
                MATCH (hd:HMIDevice {name: $hmi, project: $project})
                MERGE (hd)-[:HAS_TEXT_LIST]->(htl)
                """,
                {
                    "name": name,
                    "hmi": hmi_name,
                    "project": project_name,
                },
            )
        return name

    def create_hmi_screen(
        self,
        name: str,
        hmi_name: str,
        project_name: str,
        folder: str = "",
    ) -> str:
        """Create an HMIScreen node."""
        with self.session() as session:
            session.run(
                """
                MERGE (hsc:HMIScreen {name: $name, hmi: $hmi, project: $project})
                SET hsc.folder = $folder,
                    hsc.semantic_status = COALESCE(hsc.semantic_status, 'pending')
                WITH hsc
                MATCH (hd:HMIDevice {name: $hmi, project: $project})
                MERGE (hd)-[:HAS_SCREEN]->(hsc)
                """,
                {
                    "name": name,
                    "hmi": hmi_name,
                    "project": project_name,
                    "folder": folder,
                },
            )
        return name

    def create_plc_tag_table(
        self,
        name: str,
        plc_name: str,
        project_name: str,
    ) -> str:
        """Create a PLCTagTable node."""
        with self.session() as session:
            session.run(
                """
                MERGE (pt:PLCTagTable {name: $name, plc: $plc, project: $project})
                WITH pt
                MATCH (pd:PLCDevice {name: $plc, project: $project})
                MERGE (pd)-[:HAS_TAG_TABLE]->(pt)
                """,
                {
                    "name": name,
                    "plc": plc_name,
                    "project": project_name,
                },
            )
        return name

    def create_plc_tag(
        self,
        name: str,
        table_name: str,
        plc_name: str,
        project_name: str,
        data_type: str = "Bool",
        logical_address: str = "",
        comment: str = "",
    ) -> str:
        """Create a PLCTag node and link to its tag table."""
        with self.session() as session:
            session.run(
                """
                MERGE (ptg:PLCTag {name: $name, table: $table, plc: $plc, project: $project})
                SET ptg.data_type = $data_type,
                    ptg.logical_address = $logical_address,
                    ptg.comment = $comment,
                    ptg.semantic_status = COALESCE(ptg.semantic_status, 'pending')
                WITH ptg
                MATCH (pt:PLCTagTable {name: $table, plc: $plc, project: $project})
                MERGE (pt)-[:HAS_TAG]->(ptg)
                """,
                {
                    "name": name,
                    "table": table_name,
                    "plc": plc_name,
                    "project": project_name,
                    "data_type": data_type,
                    "logical_address": logical_address,
                    "comment": comment,
                },
            )
        return name

    def create_plc_type(
        self,
        name: str,
        plc_name: str,
        project_name: str,
        members: Optional[List[Dict]] = None,
        is_failsafe: bool = False,
    ) -> str:
        """Create a PLC UDT/struct type node and link to PLC device.

        Also creates a UDT node for cross-referencing compatibility.
        """
        with self.session() as session:
            # Create or merge the UDT node (for cross-ref compatibility)
            session.run(
                """
                MERGE (u:UDT {name: $name})
                SET u.platform = 'Siemens',
                    u.plc = $plc,
                    u.project = $project,
                    u.is_failsafe = $is_failsafe,
                    u.semantic_status = COALESCE(u.semantic_status, 'pending')
                WITH u
                MATCH (pd:PLCDevice {name: $plc, project: $project})
                MERGE (pd)-[:HAS_TYPE]->(u)
                """,
                {
                    "name": name,
                    "plc": plc_name,
                    "project": project_name,
                    "is_failsafe": is_failsafe,
                },
            )

            # Create member tags
            for member in (members or []):
                member_name = member.get("name", "")
                if not member_name:
                    continue
                session.run(
                    """
                    MATCH (u:UDT {name: $type_name})
                    MERGE (t:Tag {name: $member_name, aoi_name: $type_name})
                    SET t.data_type = $data_type,
                        t.description = $description
                    MERGE (u)-[:HAS_MEMBER]->(t)
                    """,
                    {
                        "type_name": name,
                        "member_name": member_name,
                        "data_type": member.get("data_type", "Unknown"),
                        "description": member.get("description", ""),
                    },
                )

        return name

    def link_aoi_to_plc_device(
        self,
        aoi_name: str,
        plc_name: str,
        project_name: str,
    ) -> None:
        """Link an AOI (block) node to a PLCDevice."""
        with self.session() as session:
            session.run(
                """
                MATCH (a:AOI {name: $aoi_name})
                MATCH (pd:PLCDevice {name: $plc, project: $project})
                MERGE (pd)-[:HAS_BLOCK]->(a)
                """,
                {
                    "aoi_name": aoi_name,
                    "plc": plc_name,
                    "project": project_name,
                },
            )

    def link_alarm_to_plc_tag(
        self,
        alarm_name: str,
        hmi_name: str,
        project_name: str,
        tag_reference: str,
    ) -> None:
        """Link an HMI alarm to a PLC tag by matching the trigger/raised_state reference.

        Attempts to find the tag by name in PLCTag nodes or AOI Tag nodes.
        """
        with self.session() as session:
            # Try to link to a PLCTag first
            session.run(
                """
                MATCH (ha:HMIAlarm {name: $alarm_name, hmi: $hmi, project: $project})
                MATCH (ptg:PLCTag {project: $project})
                WHERE ptg.name = $tag_ref OR ptg.name CONTAINS $tag_ref
                MERGE (ha)-[:MONITORS_TAG]->(ptg)
                """,
                {
                    "alarm_name": alarm_name,
                    "hmi": hmi_name,
                    "project": project_name,
                    "tag_ref": tag_reference,
                },
            )

    def get_tia_project(self, name: str) -> Optional[Dict]:
        """Get a TIA project with all its devices and summary counts."""
        with self.session() as session:
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $name})
                OPTIONAL MATCH (tp)-[:HAS_DEVICE]->(pd:PLCDevice)
                OPTIONAL MATCH (tp)-[:HAS_DEVICE]->(hd:HMIDevice)
                WITH tp,
                     collect(DISTINCT pd.name) as plcs,
                     collect(DISTINCT hd.name) as hmis
                RETURN tp.name as name,
                       tp.directory as directory,
                       tp.platform as platform,
                       plcs, hmis
                """,
                {"name": name},
            )
            record = result.single()
            return dict(record) if record else None

    def get_tia_project_full(self, name: str) -> Dict:
        """Get full TIA project topology with all devices, connections, and counts."""
        with self.session() as session:
            project = {"name": name, "plc_devices": [], "hmi_devices": []}

            # PLC devices with block/type counts
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $name})-[:HAS_DEVICE]->(pd:PLCDevice)
                OPTIONAL MATCH (pd)-[:HAS_BLOCK]->(a:AOI)
                OPTIONAL MATCH (pd)-[:HAS_TYPE]->(u:UDT)
                OPTIONAL MATCH (pd)-[:HAS_TAG_TABLE]->(pt:PLCTagTable)
                WITH pd,
                     count(DISTINCT a) as block_count,
                     count(DISTINCT u) as type_count,
                     count(DISTINCT pt) as tag_table_count
                RETURN pd.name as name, pd.dir_name as dir_name,
                       block_count, type_count, tag_table_count
                ORDER BY pd.name
                """,
                {"name": name},
            )
            for r in result:
                project["plc_devices"].append(dict(r))

            # HMI devices with connection and alarm counts
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $name})-[:HAS_DEVICE]->(hd:HMIDevice)
                OPTIONAL MATCH (hd)-[:HAS_CONNECTION]->(hc:HMIConnection)
                OPTIONAL MATCH (hd)-[:HAS_ALARM]->(ha:HMIAlarm)
                OPTIONAL MATCH (hd)-[:HAS_SCRIPT]->(hs:HMIScript)
                OPTIONAL MATCH (hd)-[:HAS_TAG_TABLE]->(ht:HMITagTable)
                OPTIONAL MATCH (hd)-[:HAS_SCREEN]->(hsc:HMIScreen)
                WITH hd,
                     count(DISTINCT hc) as connection_count,
                     count(DISTINCT ha) as alarm_count,
                     count(DISTINCT hs) as script_count,
                     count(DISTINCT ht) as tag_table_count,
                     count(DISTINCT hsc) as screen_count
                RETURN hd.name as name, hd.dir_name as dir_name,
                       connection_count, alarm_count, script_count,
                       tag_table_count, screen_count
                ORDER BY hd.name
                """,
                {"name": name},
            )
            for r in result:
                project["hmi_devices"].append(dict(r))

            # Connections (interlinks)
            result = session.run(
                """
                MATCH (hd:HMIDevice {project: $name})-[:HAS_CONNECTION]->(hc:HMIConnection)
                OPTIONAL MATCH (hc)-[:CONNECTS_TO]->(pd:PLCDevice)
                RETURN hd.name as hmi, hc.name as connection,
                       hc.partner as partner, hc.communication_driver as driver,
                       pd.name as linked_plc
                ORDER BY hd.name
                """,
                {"name": name},
            )
            project["connections"] = [dict(r) for r in result]

            return project

    def clear_tia_project(self, name: str) -> Dict[str, int]:
        """Clear all nodes belonging to a TIA project.

        Args:
            name: TiaProject name

        Returns:
            Dict with counts of deleted nodes by type
        """
        with self.session() as session:
            counts = {}

            # Delete HMI artifacts
            for label in [
                "HMIAlarm", "HMIAlarmClass", "HMIScript",
                "HMITagTable", "HMITextList", "HMIScreen", "HMIConnection",
            ]:
                result = session.run(
                    f"MATCH (n:{label} {{project: $name}}) DETACH DELETE n RETURN count(n) as count",
                    {"name": name},
                )
                counts[label] = result.single()["count"]

            # Delete PLC tags and tag tables
            result = session.run(
                "MATCH (n:PLCTag {project: $name}) DETACH DELETE n RETURN count(n) as count",
                {"name": name},
            )
            counts["PLCTag"] = result.single()["count"]

            result = session.run(
                "MATCH (n:PLCTagTable {project: $name}) DETACH DELETE n RETURN count(n) as count",
                {"name": name},
            )
            counts["PLCTagTable"] = result.single()["count"]

            # Delete devices
            result = session.run(
                "MATCH (n:HMIDevice {project: $name}) DETACH DELETE n RETURN count(n) as count",
                {"name": name},
            )
            counts["HMIDevice"] = result.single()["count"]

            result = session.run(
                "MATCH (n:PLCDevice {project: $name}) DETACH DELETE n RETURN count(n) as count",
                {"name": name},
            )
            counts["PLCDevice"] = result.single()["count"]

            # Delete project node
            result = session.run(
                "MATCH (n:TiaProject {name: $name}) DETACH DELETE n RETURN count(n) as count",
                {"name": name},
            )
            counts["TiaProject"] = result.single()["count"]

            return counts

    # ------------------------------------------------------------------
    # TIA Portal Browse / Enrichment queries
    # ------------------------------------------------------------------

    def get_tia_projects(self) -> List[Dict]:
        """Get all TIA Portal projects for the Browse tab.

        Returns:
            List of project dicts with name, directory, plc/hmi counts.
        """
        with self.session() as session:
            result = session.run(
                """
                MATCH (tp:TiaProject)
                OPTIONAL MATCH (tp)-[:HAS_DEVICE]->(pd:PLCDevice)
                OPTIONAL MATCH (tp)-[:HAS_DEVICE]->(hd:HMIDevice)
                WITH tp,
                     count(DISTINCT pd) as plc_count,
                     count(DISTINCT hd) as hmi_count
                RETURN tp.name as name,
                       tp.directory as directory,
                       tp.platform as platform,
                       plc_count,
                       hmi_count
                ORDER BY tp.name
                """
            )
            return [dict(r) for r in result]

    def get_tia_project_resources(self, project_name: str) -> Dict[str, List[Dict]]:
        """Get all resources belonging to a TIA project for the Browse tab.

        Args:
            project_name: TiaProject name

        Returns:
            Dict with plc_blocks, plc_tags, plc_types, hmi_scripts,
            hmi_alarms, hmi_screens, hmi_connections, hmi_tag_tables,
            hmi_text_lists lists.
        """
        with self.session() as session:
            resources: Dict[str, List[Dict]] = {
                "plc_blocks": [],
                "plc_tags": [],
                "plc_types": [],
                "hmi_scripts": [],
                "hmi_alarms": [],
                "hmi_screens": [],
                "hmi_connections": [],
                "hmi_tag_tables": [],
                "hmi_text_lists": [],
            }

            # PLC blocks (AOIs linked to PLCDevice)
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(pd:PLCDevice)
                MATCH (pd)-[:HAS_BLOCK]->(a:AOI)
                RETURN a.name as name, a.type as type, a.purpose as purpose,
                       a.semantic_status as status, pd.name as device
                ORDER BY pd.name, a.name
                """,
                {"project": project_name},
            )
            resources["plc_blocks"] = [dict(r) for r in result]

            # PLC tags
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(pd:PLCDevice)
                MATCH (pd)-[:HAS_TAG_TABLE]->(pt:PLCTagTable)-[:HAS_TAG]->(ptg:PLCTag)
                RETURN ptg.name as name, ptg.data_type as data_type,
                       ptg.logical_address as logical_address,
                       ptg.comment as comment,
                       ptg.semantic_status as status,
                       pt.name as table_name, pd.name as device
                ORDER BY pd.name, pt.name, ptg.name
                LIMIT 500
                """,
                {"project": project_name},
            )
            resources["plc_tags"] = [dict(r) for r in result]

            # PLC types (UDTs)
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(pd:PLCDevice)
                MATCH (pd)-[:HAS_TYPE]->(u:UDT)
                RETURN u.name as name, u.purpose as purpose,
                       u.semantic_status as status, pd.name as device
                ORDER BY pd.name, u.name
                """,
                {"project": project_name},
            )
            resources["plc_types"] = [dict(r) for r in result]

            # HMI scripts
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(hd:HMIDevice)
                MATCH (hd)-[:HAS_SCRIPT]->(hs:HMIScript)
                RETURN hs.name as name, hs.script_file as script_file,
                       hs.functions as functions,
                       hs.semantic_status as status, hd.name as device
                ORDER BY hd.name, hs.name
                """,
                {"project": project_name},
            )
            resources["hmi_scripts"] = [dict(r) for r in result]

            # HMI alarms
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(hd:HMIDevice)
                MATCH (hd)-[:HAS_ALARM]->(ha:HMIAlarm)
                RETURN ha.name as name, ha.alarm_type as alarm_type,
                       ha.alarm_class as alarm_class,
                       ha.semantic_status as status, hd.name as device
                ORDER BY hd.name, ha.name
                LIMIT 500
                """,
                {"project": project_name},
            )
            resources["hmi_alarms"] = [dict(r) for r in result]

            # HMI screens
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(hd:HMIDevice)
                MATCH (hd)-[:HAS_SCREEN]->(hsc:HMIScreen)
                RETURN hsc.name as name, hsc.folder as folder,
                       hsc.semantic_status as status, hd.name as device
                ORDER BY hd.name, hsc.folder, hsc.name
                """,
                {"project": project_name},
            )
            resources["hmi_screens"] = [dict(r) for r in result]

            # HMI connections
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(hd:HMIDevice)
                MATCH (hd)-[:HAS_CONNECTION]->(hc:HMIConnection)
                RETURN hc.name as name, hc.partner as partner,
                       hc.communication_driver as driver, hd.name as device
                ORDER BY hd.name, hc.name
                """,
                {"project": project_name},
            )
            resources["hmi_connections"] = [dict(r) for r in result]

            # HMI tag tables
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(hd:HMIDevice)
                MATCH (hd)-[:HAS_TAG_TABLE]->(ht:HMITagTable)
                RETURN ht.name as name, ht.folder as folder, hd.name as device
                ORDER BY hd.name, ht.name
                """,
                {"project": project_name},
            )
            resources["hmi_tag_tables"] = [dict(r) for r in result]

            # HMI text lists
            result = session.run(
                """
                MATCH (tp:TiaProject {name: $project})-[:HAS_DEVICE]->(hd:HMIDevice)
                MATCH (hd)-[:HAS_TEXT_LIST]->(htl:HMITextList)
                RETURN htl.name as name, hd.name as device
                ORDER BY hd.name, htl.name
                """,
                {"project": project_name},
            )
            resources["hmi_text_lists"] = [dict(r) for r in result]

            return resources

    def get_graph_for_visualization(self) -> Dict:
        """Get nodes and edges for visualization."""
        with self.session() as session:
            # Get all nodes (including Siemens TIA project nodes)
            nodes_result = session.run(
                """
                MATCH (n)
                WHERE n:AOI OR n:UDT OR n:Equipment OR n:View OR n:EndToEndFlow
                   OR n:TiaProject OR n:PLCDevice OR n:HMIDevice
                   OR n:HMIConnection OR n:HMIAlarm OR n:HMIScript
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
                WHERE (a:AOI OR a:UDT OR a:Equipment OR a:View
                       OR a:TiaProject OR a:PLCDevice OR a:HMIDevice
                       OR a:HMIConnection OR a:HMIAlarm OR a:HMIScript)
                  AND (b:AOI OR b:UDT OR b:Equipment OR b:View
                       OR b:TiaProject OR b:PLCDevice OR b:HMIDevice
                       OR b:HMIConnection OR b:HMIAlarm OR b:HMIScript)
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
    import json as json_module

    parser = argparse.ArgumentParser(description="Neo4j ontology management")
    parser.add_argument(
        "command",
        choices=[
            "init",
            "clear",
            "clear-ignition",
            "clear-plc",
            "clear-unification",
            "import",
            "export",
            "load",
            "query",
            "projects",
            "gateway-resources",
            "project-resources",
            "tia-projects",
            "tia-project-resources",
            "db-connections",
            "init-agent-schema",
            "list-anomaly-events",
            "get-anomaly-event",
            "cleanup-anomaly-events",
        ],
        help="Command to execute",
    )
    parser.add_argument("--file", "-f", help="JSON file for import/export")
    parser.add_argument("--query", "-q", help="Query string for search")
    parser.add_argument("--project", "-p", help="Project name for project-resources")
    parser.add_argument("--event-id", help="Event ID for get-anomaly-event")
    parser.add_argument("--state", help="Filter anomaly events by state")
    parser.add_argument("--severity", help="Filter anomaly events by severity")
    parser.add_argument("--run-id", help="Filter anomaly events by run_id")
    parser.add_argument("--limit", type=int, default=100, help="Limit results for list commands")
    parser.add_argument("--retention-days", type=int, default=14, help="Retention window in days")
    parser.add_argument("--json", action="store_true", help="Output in JSON format")
    parser.add_argument(
        "--enrichment-status",
        action="store_true",
        help="Show troubleshooting enrichment status",
    )
    parser.add_argument("--uri", default=DEFAULT_URI, help="Neo4j URI")
    parser.add_argument("--user", default=DEFAULT_USER, help="Neo4j user")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Neo4j password")
    parser.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )

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
                confirm = input(
                    "This will delete all Ignition/SCADA data and cross-system mappings. Type 'yes' to confirm: "
                )
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
                confirm = input(
                    "This will delete all PLC data and cross-system mappings. Type 'yes' to confirm: "
                )
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
                confirm = input(
                    "This will delete all unification data (mappings, flows, overview). Type 'yes' to confirm: "
                )
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

            data = graph.export_full_database()
            if args.file:
                with open(args.file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                print(f"[OK] Exported database to {args.file}")
                print(f"  - Nodes: {data['metadata']['node_count']}")
                print(f"  - Relationships: {data['metadata']['relationship_count']}")
            else:
                print(json.dumps(data, indent=2))

        elif args.command == "load":
            if not args.file:
                print("[ERROR] --file required for load")
                return
            import json
            from pathlib import Path

            if not Path(args.file).exists():
                print(f"[ERROR] File not found: {args.file}")
                return

            with open(args.file, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not args.yes:
                confirm = input("This will REPLACE all data. Type 'yes' to confirm: ")
                if confirm.lower() != "yes":
                    print("[CANCELLED]")
                    return

            stats = graph.import_full_database(data, clear_first=True)
            print(f"[OK] Loaded database from {args.file}")
            print(f"  - Nodes created: {stats['nodes_created']}")
            print(f"  - Relationships created: {stats['relationships_created']}")

        elif args.command == "query":
            if args.enrichment_status:
                # Show troubleshooting enrichment status
                counts = graph.get_enrichment_status_counts()
                print("Troubleshooting Enrichment Status:")
                print("-" * 40)
                for item_type, status in counts.items():
                    total = status["enriched"] + status["pending"]
                    pct = (status["enriched"] / total * 100) if total > 0 else 0
                    print(
                        f"  {item_type:15} {status['enriched']}/{total} enriched ({pct:.0f}%)"
                    )
            elif args.query:
                results = graph.find_by_symptom(args.query)
                if results:
                    for r in results:
                        print(f"\nAOI: {r['aoi']}")
                        print(f"Symptom: {r['symptom']}")
                        print(f"Steps: {r['steps']}")
                else:
                    print("No results found")

        elif args.command == "projects":
            # Get all projects with inheritance info
            projects = graph.get_all_projects()
            if args.json:
                print(json_module.dumps(projects))
            else:
                print("\nProjects:")
                print("-" * 40)
                for p in projects:
                    parent_info = (
                        f" (inherits from {p['parent']})" if p.get("parent") else ""
                    )
                    inheritable = " [inheritable]" if p.get("inheritable") else ""
                    print(f"  {p['name']}{parent_info}{inheritable}")

        elif args.command == "gateway-resources":
            # Get gateway-wide resources (Tags, UDTs, AOIs)
            resources = graph.get_gateway_resources()
            if args.json:
                print(json_module.dumps(resources))
            else:
                print("\nGateway Resources:")
                print("-" * 40)
                print(f"  Tags: {len(resources.get('tags', []))}")
                print(f"  UDTs: {len(resources.get('udts', []))}")
                print(f"  AOIs: {len(resources.get('aois', []))}")

        elif args.command == "project-resources":
            # Get project-specific resources
            if not args.project:
                print("[ERROR] --project required for project-resources command")
                return
            resources = graph.get_project_resources(args.project)
            if args.json:
                print(json_module.dumps(resources))
            else:
                print(f"\nResources for Project: {args.project}")
                print("-" * 40)
                print(f"  Views: {len(resources.get('views', []))}")
                print(f"  Scripts: {len(resources.get('scripts', []))}")
                print(f"  Named Queries: {len(resources.get('queries', []))}")
                print(f"  Gateway Events: {len(resources.get('events', []))}")

        elif args.command == "tia-projects":
            # Get all TIA Portal projects
            projects = graph.get_tia_projects()
            if args.json:
                print(json_module.dumps(projects))
            else:
                print("\nTIA Portal Projects:")
                print("-" * 40)
                for p in projects:
                    print(
                        f"  {p['name']} ({p.get('plc_count', 0)} PLCs, "
                        f"{p.get('hmi_count', 0)} HMIs)"
                    )

        elif args.command == "tia-project-resources":
            # Get TIA project-specific resources
            if not args.project:
                print("[ERROR] --project required for tia-project-resources command")
                return
            resources = graph.get_tia_project_resources(args.project)
            if args.json:
                print(json_module.dumps(resources))
            else:
                print(f"\nTIA Resources for Project: {args.project}")
                print("-" * 40)
                print(f"  PLC Blocks:      {len(resources.get('plc_blocks', []))}")
                print(f"  PLC Tags:        {len(resources.get('plc_tags', []))}")
                print(f"  PLC Types:       {len(resources.get('plc_types', []))}")
                print(f"  HMI Scripts:     {len(resources.get('hmi_scripts', []))}")
                print(f"  HMI Alarms:      {len(resources.get('hmi_alarms', []))}")
                print(f"  HMI Screens:     {len(resources.get('hmi_screens', []))}")
                print(f"  HMI Connections: {len(resources.get('hmi_connections', []))}")

        elif args.command == "db-connections":
            with graph.session() as session:
                result = session.run(
                    """
                    MATCH (d:DatabaseConnection)
                    RETURN d.name AS name, d.database_type AS database_type,
                           d.url AS url, d.username AS username,
                           d.enabled AS enabled, d.description AS description,
                           d.translator AS translator
                    ORDER BY d.name
                """
                )
                connections = [dict(record) for record in result]
            if args.json:
                print(json_module.dumps(connections))
            else:
                print(f"\nDatabase Connections ({len(connections)}):")
                print("-" * 40)
                for c in connections:
                    enabled = "enabled" if c.get("enabled") else "disabled"
                    print(
                        f"  {c['name']} ({c['database_type']}) "
                        f"- {c['url']} [{enabled}]"
                    )
        elif args.command == "init-agent-schema":
            graph.init_agent_monitoring_schema()
            print("[OK] Initialized agent monitoring schema")

        elif args.command == "list-anomaly-events":
            events = graph.list_anomaly_events(
                limit=args.limit,
                state=args.state,
                severity=args.severity,
                run_id=args.run_id,
            )
            if args.json:
                print(json_module.dumps(events))
            else:
                print(f"Anomaly events: {len(events)}")
                for event in events:
                    print(
                        f"- {event.get('event_id')} {event.get('severity')} "
                        f"{event.get('summary', '')[:80]}"
                    )

        elif args.command == "get-anomaly-event":
            if not args.event_id:
                print("[ERROR] --event-id required for get-anomaly-event")
                return
            event = graph.get_anomaly_event(args.event_id)
            if args.json:
                print(json_module.dumps(event or {}))
            else:
                if not event:
                    print(f"[ERROR] Event not found: {args.event_id}")
                    return
                print(json_module.dumps(event, indent=2))

        elif args.command == "cleanup-anomaly-events":
            deleted = graph.cleanup_anomaly_events(args.retention_days)
            print(f"[OK] Deleted {deleted} anomaly events older than {args.retention_days} days")

if __name__ == "__main__":
    main()
