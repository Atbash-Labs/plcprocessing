#!/usr/bin/env python3
"""
Diff processor for Ignition SCADA configurations.
Applies diff files to update Neo4j ontology and marks affected entities for re-semanticization.

Supports:
- Views (windows): added, modified, deleted
- ViewComponents: added, modified, deleted within views
- UDT definitions: added, modified, deleted
- Tags: added, modified, deleted
- Equipment (UDT instances): added, modified, deleted

Cascade behavior:
- When a UDT is modified/deleted, all Equipment using it and ViewComponents binding to it
  are marked as pending for re-analysis.
"""

import os
import json
import argparse
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from neo4j_ontology import OntologyGraph, get_ontology_graph
from ignition_parser import IgnitionParser, IgnitionBackup


@dataclass
class DiffStats:
    """Statistics about diff processing."""
    views_added: int = 0
    views_modified: int = 0
    views_deleted: int = 0
    components_added: int = 0
    components_modified: int = 0
    components_deleted: int = 0
    udts_added: int = 0
    udts_modified: int = 0
    udts_deleted: int = 0
    tags_added: int = 0
    tags_modified: int = 0
    tags_deleted: int = 0
    equipment_added: int = 0
    equipment_modified: int = 0
    equipment_deleted: int = 0
    cascade_marked: int = 0
    
    def total_changes(self) -> int:
        return (
            self.views_added + self.views_modified + self.views_deleted +
            self.components_added + self.components_modified + self.components_deleted +
            self.udts_added + self.udts_modified + self.udts_deleted +
            self.tags_added + self.tags_modified + self.tags_deleted +
            self.equipment_added + self.equipment_modified + self.equipment_deleted
        )
    
    def __str__(self) -> str:
        lines = ["=== Diff Processing Stats ==="]
        
        if self.views_added or self.views_modified or self.views_deleted:
            lines.append(f"  Views:      +{self.views_added} ~{self.views_modified} -{self.views_deleted}")
        if self.components_added or self.components_modified or self.components_deleted:
            lines.append(f"  Components: +{self.components_added} ~{self.components_modified} -{self.components_deleted}")
        if self.udts_added or self.udts_modified or self.udts_deleted:
            lines.append(f"  UDTs:       +{self.udts_added} ~{self.udts_modified} -{self.udts_deleted}")
        if self.tags_added or self.tags_modified or self.tags_deleted:
            lines.append(f"  Tags:       +{self.tags_added} ~{self.tags_modified} -{self.tags_deleted}")
        if self.equipment_added or self.equipment_modified or self.equipment_deleted:
            lines.append(f"  Equipment:  +{self.equipment_added} ~{self.equipment_modified} -{self.equipment_deleted}")
        if self.cascade_marked:
            lines.append(f"  Cascade marked: {self.cascade_marked} related items")
        
        lines.append(f"\n  Total changes: {self.total_changes()}")
        return "\n".join(lines)


class DiffProcessor:
    """Processes diff files and applies changes to Neo4j ontology."""
    
    def __init__(
        self,
        graph: Optional[OntologyGraph] = None,
        backup: Optional[IgnitionBackup] = None,
    ):
        """Initialize the processor.
        
        Args:
            graph: Neo4j graph connection (created if not provided)
            backup: Parsed backup for full entity context
        """
        self._graph = graph
        self._owns_graph = False
        self._backup = backup
        
        if self._graph is None:
            self._graph = get_ontology_graph()
            self._owns_graph = True
    
    def close(self):
        """Close resources if we own them."""
        if self._owns_graph and self._graph:
            self._graph.close()
            self._graph = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def load_diff(self, diff_path: str) -> Dict[str, Any]:
        """Load a diff JSON file."""
        with open(diff_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def preview(self, diff: Dict[str, Any], verbose: bool = False) -> DiffStats:
        """Preview what changes would be made without applying them.
        
        Args:
            diff: Parsed diff JSON
            verbose: Print detailed changes
            
        Returns:
            DiffStats with counts of what would change
        """
        stats = DiffStats()
        
        for project_name, project_diff in diff.get("diffs", {}).items():
            if verbose:
                print(f"\n=== Project: {project_name} ===")
            
            # Windows (Views)
            windows = project_diff.get("windows", {})
            stats.views_added += len(windows.get("added", []))
            stats.views_modified += len(windows.get("modified", []))
            stats.views_deleted += len(windows.get("deleted", []))
            
            # Count component changes within modified windows
            for mod in windows.get("modified", []):
                config_diff = mod.get("config_diff", {})
                components = config_diff.get("components", {})
                stats.components_added += len(components.get("added", []))
                stats.components_modified += len(components.get("modified", []))
                stats.components_deleted += len(components.get("deleted", []))
            
            # Count components in added windows
            for added in windows.get("added", []):
                config = added.get("config", {})
                root = config.get("root_container", {})
                stats.components_added += self._count_components(root)
            
            # UDT definitions
            udts = project_diff.get("udt_definitions", {})
            stats.udts_added += len(udts.get("added", []))
            stats.udts_modified += len(udts.get("modified", []))
            stats.udts_deleted += len(udts.get("deleted", []))
            
            # Tags
            tags = project_diff.get("tags", {})
            stats.tags_added += len(tags.get("added", []))
            stats.tags_modified += len(tags.get("modified", []))
            stats.tags_deleted += len(tags.get("deleted", []))
            
            # UDT instances (Equipment)
            instances = project_diff.get("udt_instances", {})
            stats.equipment_added += len(instances.get("added", []))
            stats.equipment_modified += len(instances.get("modified", []))
            stats.equipment_deleted += len(instances.get("deleted", []))
            
            if verbose:
                self._print_preview_details(project_diff)
        
        return stats
    
    def _count_components(self, container: Dict) -> int:
        """Recursively count components in a container."""
        count = 0
        for child in container.get("children", []):
            count += 1
            count += self._count_components(child)
        return count
    
    def _print_preview_details(self, project_diff: Dict):
        """Print detailed preview of changes."""
        # Windows
        windows = project_diff.get("windows", {})
        for added in windows.get("added", []):
            print(f"  + View: {added.get('path', added.get('id'))}")
        for modified in windows.get("modified", []):
            print(f"  ~ View: {modified.get('id')}")
            config_diff = modified.get("config_diff", {})
            components = config_diff.get("components", {})
            for comp in components.get("added", []):
                print(f"    + Component: {comp.get('path', comp.get('name'))}")
            for comp in components.get("deleted", []):
                print(f"    - Component: {comp.get('path', comp.get('name'))}")
        for deleted in windows.get("deleted", []):
            print(f"  - View: {deleted.get('path', deleted.get('id'))}")
        
        # UDTs
        udts = project_diff.get("udt_definitions", {})
        for added in udts.get("added", []):
            print(f"  + UDT: {added.get('id')}")
        for modified in udts.get("modified", []):
            print(f"  ~ UDT: {modified.get('id')}")
        for deleted in udts.get("deleted", []):
            print(f"  - UDT: {deleted.get('id')}")
        
        # Tags
        tags = project_diff.get("tags", {})
        for added in tags.get("added", []):
            print(f"  + Tag: {added.get('path')}")
        for modified in tags.get("modified", []):
            print(f"  ~ Tag: {modified.get('id')}")
        for deleted in tags.get("deleted", []):
            print(f"  - Tag: {deleted.get('path')}")
    
    def apply(self, diff: Dict[str, Any], verbose: bool = False) -> DiffStats:
        """Apply diff changes to Neo4j.
        
        Args:
            diff: Parsed diff JSON
            verbose: Print detailed progress
            
        Returns:
            DiffStats with counts of changes made
        """
        stats = DiffStats()
        
        for project_name, project_diff in diff.get("diffs", {}).items():
            if verbose:
                print(f"\n=== Processing project: {project_name} ===")
            
            # Process UDTs first (other entities may depend on them)
            udt_stats = self._process_udts(project_diff.get("udt_definitions", {}), verbose)
            stats.udts_added += udt_stats[0]
            stats.udts_modified += udt_stats[1]
            stats.udts_deleted += udt_stats[2]
            
            # Process Equipment (UDT instances)
            equip_stats = self._process_equipment(project_diff.get("udt_instances", {}), verbose)
            stats.equipment_added += equip_stats[0]
            stats.equipment_modified += equip_stats[1]
            stats.equipment_deleted += equip_stats[2]
            
            # Process Views
            view_stats = self._process_views(project_diff.get("windows", {}), verbose)
            stats.views_added += view_stats[0]
            stats.views_modified += view_stats[1]
            stats.views_deleted += view_stats[2]
            stats.components_added += view_stats[3]
            stats.components_modified += view_stats[4]
            stats.components_deleted += view_stats[5]
            
            # Process Tags (for bindings)
            tag_stats = self._process_tags(project_diff.get("tags", {}), verbose)
            stats.tags_added += tag_stats[0]
            stats.tags_modified += tag_stats[1]
            stats.tags_deleted += tag_stats[2]
        
        # Cascade marking
        stats.cascade_marked = self._cascade_mark_related(verbose)
        
        return stats
    
    def _process_udts(self, udts_diff: Dict, verbose: bool) -> Tuple[int, int, int]:
        """Process UDT definition changes."""
        added, modified, deleted = 0, 0, 0
        
        # Added UDTs
        for udt in udts_diff.get("added", []):
            udt_id = udt.get("id")
            config = udt.get("config", {})
            name = config.get("name", udt_id)
            members = config.get("members", [])
            
            self._graph.create_udt(
                name=name,
                purpose="",  # Will be set by semantic analysis
                source_file="diff",
                members=members,
                semantic_status="pending",
            )
            if verbose:
                print(f"  + Created UDT: {name}")
            added += 1
        
        # Modified UDTs
        for udt in udts_diff.get("modified", []):
            udt_id = udt.get("id")
            config_diff = udt.get("config_diff", {})
            
            # Update UDT and mark as pending
            self._update_udt(udt_id, config_diff)
            self._graph.set_semantic_status("UDT", udt_id, "pending")
            if verbose:
                print(f"  ~ Modified UDT: {udt_id} (marked pending)")
            modified += 1
        
        # Deleted UDTs (soft delete)
        for udt in udts_diff.get("deleted", []):
            udt_id = udt.get("id") if isinstance(udt, dict) else udt
            self._soft_delete_udt(udt_id)
            if verbose:
                print(f"  - Soft-deleted UDT: {udt_id}")
            deleted += 1
        
        return added, modified, deleted
    
    def _update_udt(self, udt_name: str, config_diff: Dict):
        """Update a UDT based on diff."""
        with self._graph.session() as session:
            # Update members if changed
            if "members" in config_diff:
                new_members = config_diff["members"].get("new", [])
                # Clear existing members and recreate
                session.run(
                    """
                    MATCH (u:UDT {name: $name})-[r:HAS_MEMBER]->(t:Tag)
                    DELETE r, t
                    """,
                    {"name": udt_name}
                )
                # Add new members
                for member in new_members:
                    session.run(
                        """
                        MATCH (u:UDT {name: $udt_name})
                        MERGE (t:Tag {name: $tag_name, udt_name: $udt_name})
                        SET t.data_type = $data_type, t.tag_type = $tag_type
                        MERGE (u)-[:HAS_MEMBER]->(t)
                        """,
                        {
                            "udt_name": udt_name,
                            "tag_name": member.get("name", ""),
                            "data_type": member.get("data_type", ""),
                            "tag_type": member.get("type", ""),
                        }
                    )
    
    def _soft_delete_udt(self, udt_name: str):
        """Soft-delete a UDT (mark as deleted)."""
        with self._graph.session() as session:
            session.run(
                """
                MATCH (u:UDT {name: $name})
                SET u.deleted = true,
                    u.deleted_at = datetime(),
                    u.semantic_status = 'deleted'
                """,
                {"name": udt_name}
            )
    
    def _process_equipment(self, instances_diff: Dict, verbose: bool) -> Tuple[int, int, int]:
        """Process Equipment (UDT instance) changes."""
        added, modified, deleted = 0, 0, 0
        
        # Added equipment
        for equip in instances_diff.get("added", []):
            equip_id = equip.get("id")
            config = equip.get("config", {})
            name = config.get("name", equip_id)
            udt_type = config.get("type_id", "")
            
            self._graph.create_equipment(
                name=name,
                equipment_type=udt_type,
                purpose="",
                udt_name=udt_type,
                semantic_status="pending",
            )
            if verbose:
                print(f"  + Created Equipment: {name}")
            added += 1
        
        # Modified equipment
        for equip in instances_diff.get("modified", []):
            equip_id = equip.get("id")
            self._graph.set_semantic_status("Equipment", equip_id, "pending")
            if verbose:
                print(f"  ~ Modified Equipment: {equip_id} (marked pending)")
            modified += 1
        
        # Deleted equipment (soft delete)
        for equip in instances_diff.get("deleted", []):
            equip_id = equip.get("id") if isinstance(equip, dict) else equip
            self._soft_delete_equipment(equip_id)
            if verbose:
                print(f"  - Soft-deleted Equipment: {equip_id}")
            deleted += 1
        
        return added, modified, deleted
    
    def _soft_delete_equipment(self, equip_name: str):
        """Soft-delete Equipment."""
        with self._graph.session() as session:
            session.run(
                """
                MATCH (e:Equipment {name: $name})
                SET e.deleted = true,
                    e.deleted_at = datetime(),
                    e.semantic_status = 'deleted'
                """,
                {"name": equip_name}
            )
    
    def _process_views(self, windows_diff: Dict, verbose: bool) -> Tuple[int, int, int, int, int, int]:
        """Process View and ViewComponent changes."""
        views_added, views_modified, views_deleted = 0, 0, 0
        comps_added, comps_modified, comps_deleted = 0, 0, 0
        
        # Added views
        for window in windows_diff.get("added", []):
            view_path = window.get("path", window.get("id"))
            view_name = Path(view_path).name if view_path else "Unknown"
            config = window.get("config", {})
            
            self._graph.create_view(
                name=view_name,
                path=view_path,
                purpose="",
                semantic_status="pending",
            )
            
            # Create components from root_container
            root = config.get("root_container", {})
            comp_count = self._create_components_from_container(view_name, root, "root")
            comps_added += comp_count
            
            if verbose:
                print(f"  + Created View: {view_path} ({comp_count} components)")
            views_added += 1
        
        # Modified views
        for window in windows_diff.get("modified", []):
            view_id = window.get("id")
            view_name = Path(view_id).name if view_id else view_id
            config_diff = window.get("config_diff", {})
            
            # Mark view as pending and reset enrichment
            self._graph.set_semantic_status("View", view_name, "pending")
            self._reset_enrichment("View", view_name)
            
            # Process component changes
            components = config_diff.get("components", {})
            
            # Added components
            for comp in components.get("added", []):
                comp_path = comp.get("path", "")
                comp_name = comp.get("name", "")
                content = comp.get("content", {})
                comp_type = content.get("type", "unknown")
                props = content.get("props", {})
                
                self._graph.create_view_component(
                    view_name=view_name,
                    component_name=comp_name,
                    component_type=comp_type,
                    component_path=comp_path,
                    inferred_purpose="",
                    props=props,
                    semantic_status="pending",
                )
                if verbose:
                    print(f"    + Added component: {comp_path}")
                comps_added += 1
            
            # Modified components
            for comp in components.get("modified", []):
                comp_path = comp.get("path", comp.get("name", ""))
                full_path = f"{view_name}/{comp_path}"
                self._graph.set_semantic_status("ViewComponent", full_path, "pending")
                if verbose:
                    print(f"    ~ Modified component: {comp_path}")
                comps_modified += 1
            
            # Deleted components (soft delete)
            for comp in components.get("deleted", []):
                comp_path = comp.get("path", comp.get("name", ""))
                full_path = f"{view_name}/{comp_path}"
                self._soft_delete_component(full_path)
                if verbose:
                    print(f"    - Soft-deleted component: {comp_path}")
                comps_deleted += 1
            
            if verbose:
                print(f"  ~ Modified View: {view_id}")
            views_modified += 1
        
        # Deleted views (soft delete)
        for window in windows_diff.get("deleted", []):
            view_id = window.get("id") if isinstance(window, dict) else window
            view_name = Path(view_id).name if view_id else view_id
            self._soft_delete_view(view_name)
            if verbose:
                print(f"  - Soft-deleted View: {view_id}")
            views_deleted += 1
        
        return views_added, views_modified, views_deleted, comps_added, comps_modified, comps_deleted
    
    def _create_components_from_container(
        self, view_name: str, container: Dict, parent_path: str
    ) -> int:
        """Recursively create ViewComponent nodes from a container."""
        count = 0
        
        for child in container.get("children", []):
            meta = child.get("meta", {})
            comp_name = meta.get("name", "unnamed")
            comp_type = child.get("type", "unknown")
            comp_path = f"{parent_path}.{comp_name}"
            props = child.get("props", {})
            
            # Infer purpose from type
            inferred_purpose = self._infer_component_purpose(comp_type)
            
            self._graph.create_view_component(
                view_name=view_name,
                component_name=comp_name,
                component_type=comp_type,
                component_path=f"{view_name}/{comp_path}",
                inferred_purpose=inferred_purpose,
                props=props,
                semantic_status="pending",
            )
            count += 1
            
            # Recurse for nested components
            count += self._create_components_from_container(view_name, child, comp_path)
        
        return count
    
    def _infer_component_purpose(self, comp_type: str) -> str:
        """Infer component purpose from type."""
        type_purposes = {
            "ia.display.label": "text display",
            "ia.input.text-field": "text input",
            "ia.input.button": "user action trigger",
            "ia.display.led": "status indicator",
            "ia.chart.xy": "data visualization",
            "ia.display.linear-scale": "linear value display",
            "ia.container.coord": "layout container",
            "ia.container.flex": "flex layout container",
        }
        return type_purposes.get(comp_type, "")
    
    def _soft_delete_view(self, view_name: str):
        """Soft-delete a View and its components."""
        with self._graph.session() as session:
            # Soft-delete view
            session.run(
                """
                MATCH (v:View {name: $name})
                SET v.deleted = true,
                    v.deleted_at = datetime(),
                    v.semantic_status = 'deleted'
                """,
                {"name": view_name}
            )
            # Soft-delete all components
            session.run(
                """
                MATCH (v:View {name: $name})-[:HAS_COMPONENT]->(c:ViewComponent)
                SET c.deleted = true,
                    c.deleted_at = datetime(),
                    c.semantic_status = 'deleted'
                """,
                {"name": view_name}
            )
    
    def _soft_delete_component(self, comp_path: str):
        """Soft-delete a ViewComponent."""
        with self._graph.session() as session:
            session.run(
                """
                MATCH (c:ViewComponent {path: $path})
                SET c.deleted = true,
                    c.deleted_at = datetime(),
                    c.semantic_status = 'deleted'
                """,
                {"path": comp_path}
            )
    
    def _reset_enrichment(self, item_type: str, name: str):
        """Reset troubleshooting enrichment status for an item.
        
        This marks the item as needing re-enrichment after modification.
        """
        with self._graph.session() as session:
            if item_type == "View":
                session.run(
                    """
                    MATCH (v:View {name: $name})
                    SET v.troubleshooting_enriched = false,
                        v.enriched_at = null
                    """,
                    {"name": name}
                )
            elif item_type == "AOI":
                session.run(
                    """
                    MATCH (a:AOI {name: $name})
                    SET a.troubleshooting_enriched = false,
                        a.enriched_at = null
                    """,
                    {"name": name}
                )
    
    def _process_tags(self, tags_diff: Dict, verbose: bool) -> Tuple[int, int, int]:
        """Process standalone SCADA Tag changes."""
        added, modified, deleted = 0, 0, 0
        
        # Added tags
        for tag in tags_diff.get("added", []):
            tag_name = tag.get("name", "")
            tag_type = tag.get("type", "memory")
            
            self._graph.create_scada_tag(
                name=tag_name,
                tag_type=tag_type,
                folder_name=tag.get("folder_name", ""),
                data_type=tag.get("data_type", ""),
                datasource=tag.get("datasource", ""),
                query=tag.get("query", ""),
                opc_item_path=tag.get("opc_item_path", ""),
                expression=tag.get("expression", ""),
                initial_value=tag.get("initial_value", ""),
                semantic_status="pending",
            )
            if verbose:
                print(f"  + Created Tag: {tag_name} ({tag_type})")
            added += 1
        
        # Modified tags
        for tag in tags_diff.get("modified", []):
            tag_id = tag.get("id", tag.get("name", ""))
            config_diff = tag.get("config_diff", {})
            self._update_scada_tag(tag_id, config_diff)
            if verbose:
                print(f"  ~ Modified Tag: {tag_id}")
            modified += 1
        
        # Deleted tags (soft delete)
        for tag in tags_diff.get("deleted", []):
            tag_name = tag.get("name", "") if isinstance(tag, dict) else tag
            self._soft_delete_scada_tag(tag_name)
            if verbose:
                print(f"  - Soft-deleted Tag: {tag_name}")
            deleted += 1
        
        return added, modified, deleted
    
    def _update_scada_tag(self, tag_name: str, config_diff: Dict):
        """Update a SCADA tag based on diff."""
        with self._graph.session() as session:
            # Build SET clause for changed properties
            updates = []
            params = {"name": tag_name}
            
            for key in ["query", "datasource", "opc_item_path", "expression", "initial_value", "data_type"]:
                if key in config_diff:
                    new_val = config_diff[key].get("new", "")
                    updates.append(f"t.{key} = ${key}")
                    params[key] = str(new_val) if new_val else ""
            
            if updates:
                updates.append("t.semantic_status = 'pending'")
                query = f"""
                    MATCH (t:ScadaTag {{name: $name}})
                    SET {', '.join(updates)}
                """
                session.run(query, params)
    
    def _soft_delete_scada_tag(self, tag_name: str):
        """Soft-delete a SCADA tag."""
        with self._graph.session() as session:
            session.run(
                """
                MATCH (t:ScadaTag {name: $name})
                SET t.deleted = true,
                    t.deleted_at = datetime(),
                    t.semantic_status = 'deleted'
                """,
                {"name": tag_name}
            )
    
    def _cascade_mark_related(self, verbose: bool) -> int:
        """Mark related entities as pending when their dependencies change."""
        count = 0
        
        with self._graph.session() as session:
            # Mark Equipment using modified/deleted UDTs as pending
            result = session.run(
                """
                MATCH (u:UDT)
                WHERE u.semantic_status IN ['pending', 'deleted']
                MATCH (e:Equipment)-[:INSTANCE_OF]->(u)
                WHERE NOT e.semantic_status IN ['pending', 'deleted']
                SET e.semantic_status = 'pending'
                RETURN count(e) as count
                """
            )
            equip_count = result.single()["count"]
            count += equip_count
            if verbose and equip_count:
                print(f"  Cascade: marked {equip_count} Equipment as pending (UDT changed)")
            
            # Mark ViewComponents binding to modified/deleted UDTs as pending
            result = session.run(
                """
                MATCH (u:UDT)
                WHERE u.semantic_status IN ['pending', 'deleted']
                MATCH (c:ViewComponent)-[:BINDS_TO]->(u)
                WHERE NOT c.semantic_status IN ['pending', 'deleted']
                SET c.semantic_status = 'pending'
                RETURN count(c) as count
                """
            )
            comp_count = result.single()["count"]
            count += comp_count
            if verbose and comp_count:
                print(f"  Cascade: marked {comp_count} ViewComponents as pending (UDT changed)")
            
            # Mark Views containing modified components as pending and reset enrichment
            result = session.run(
                """
                MATCH (c:ViewComponent)
                WHERE c.semantic_status = 'pending'
                MATCH (v:View)-[:HAS_COMPONENT]->(c)
                WHERE NOT v.semantic_status IN ['pending', 'deleted']
                SET v.semantic_status = 'pending',
                    v.troubleshooting_enriched = false,
                    v.enriched_at = null
                RETURN count(DISTINCT v) as count
                """
            )
            view_count = result.single()["count"]
            count += view_count
            if verbose and view_count:
                print(f"  Cascade: marked {view_count} Views as pending (component changed)")
        
        return count


# =========================================================================
# CLI
# =========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Process Ignition diff files and update Neo4j ontology"
    )
    parser.add_argument(
        "command",
        choices=["apply", "preview"],
        help="Command: 'apply' to apply changes, 'preview' to show what would change"
    )
    parser.add_argument(
        "diff_file",
        help="Path to the diff JSON file"
    )
    parser.add_argument(
        "--backup", "-b",
        help="Path to the backup JSON file for full entity context"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed progress"
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompt for apply"
    )
    
    args = parser.parse_args()
    
    # Load diff file
    if not os.path.exists(args.diff_file):
        print(f"[ERROR] Diff file not found: {args.diff_file}")
        return 1
    
    # Load backup if provided
    backup = None
    if args.backup:
        if not os.path.exists(args.backup):
            print(f"[ERROR] Backup file not found: {args.backup}")
            return 1
        parser_obj = IgnitionParser()
        backup = parser_obj.parse_file(args.backup)
        if args.verbose:
            print(f"[INFO] Loaded backup: {args.backup}")
    
    with DiffProcessor(backup=backup) as processor:
        diff = processor.load_diff(args.diff_file)
        
        if args.command == "preview":
            print(f"\n[PREVIEW] Changes in {args.diff_file}:\n")
            stats = processor.preview(diff, verbose=args.verbose)
            print(f"\n{stats}")
            print("\n[INFO] Run 'apply' to apply these changes")
        
        elif args.command == "apply":
            # Preview first
            stats = processor.preview(diff, verbose=False)
            
            if stats.total_changes() == 0:
                print("[INFO] No changes to apply")
                return 0
            
            print(f"\n[APPLY] About to apply changes from {args.diff_file}:")
            print(stats)
            
            if not args.yes:
                confirm = input("\nApply these changes? [y/N]: ")
                if confirm.lower() != "y":
                    print("[CANCELLED]")
                    return 0
            
            print("\n[INFO] Applying changes...")
            stats = processor.apply(diff, verbose=args.verbose)
            print(f"\n[OK] Applied changes:")
            print(stats)
    
    return 0


if __name__ == "__main__":
    exit(main())

