#!/usr/bin/env python3
"""
LLM-based analyzer for Ignition SCADA configurations using Anthropic's Claude API.
Generates semantic understanding of tags, UDTs, views, and data flows.
Stores results in Neo4j graph database.

Uses tool calls to query existing ontology data, enabling Claude to build
on existing knowledge rather than starting from scratch.
"""

import os
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from dotenv import load_dotenv

from ignition_parser import IgnitionParser, IgnitionBackup
from neo4j_ontology import OntologyGraph, get_ontology_graph
from claude_client import ClaudeClient, get_claude_client


class IgnitionOntologyAnalyzer:
    """Analyzes Ignition configurations using Claude to generate semantic ontologies."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-5-20250929",
        graph: Optional[OntologyGraph] = None,
        client: Optional[ClaudeClient] = None,
    ):
        """Initialize the analyzer with Anthropic API and Neo4j connection."""
        load_dotenv()

        # Use provided client or create one
        if client:
            self._client = client
            self._owns_client = False
        else:
            self._client = ClaudeClient(
                api_key=api_key, model=model, graph=graph, enable_tools=True
            )
            self._owns_client = True

    @property
    def graph(self) -> OntologyGraph:
        """Access the Neo4j graph."""
        return self._client.graph

    def close(self):
        """Close resources if we own them."""
        if self._owns_client and self._client:
            self._client.close()
            self._client = None

    def analyze_backup(
        self, backup: IgnitionBackup, verbose: bool = False, skip_ai: bool = False
    ) -> Dict[str, Any]:
        """Analyze an Ignition backup and store ontology in Neo4j.

        Strategy: Create all entities from parsed data first (deterministic),
        then optionally use Claude's analysis to enrich with semantic descriptions.

        Args:
            backup: Parsed Ignition backup
            verbose: Print detailed progress
            skip_ai: If True, skip Phase 3 AI analysis (use incremental analyzer later)

        Returns:
            Dict with ontology summary and analysis results
        """

        if verbose:
            print(f"[INFO] Analyzing Ignition backup...")

        # === PHASE 0: Create Projects first (needed for relationships) ===
        if backup.projects:
            if verbose:
                print(f"[INFO] Creating {len(backup.projects)} projects...")
            self._create_projects(backup, verbose)

        # === PHASE 1: Create entities from parsed data (deterministic) ===
        if verbose:
            print(f"[INFO] Creating entities from parsed data...")

        # Create all UDTs from parsed definitions (gateway-wide, no project)
        for udt_def in backup.udt_definitions:
            self.graph.create_udt(udt_def.name, "", backup.file_path)

        # Create all Views from parsed windows with project-qualified names
        if verbose and len(backup.windows) > 100:
            print(f"[INFO] Creating {len(backup.windows)} views...", flush=True)
        views_created = 0
        for i, window in enumerate(backup.windows):
            # Skip windows without names
            if not window.name:
                continue
            view_name = self._qualify_name(window.name, window.project)
            self.graph.create_view(view_name, window.path, "", project=window.project)
            views_created += 1
            # Progress indicator for large sets
            if verbose and len(backup.windows) > 200 and (i + 1) % 200 == 0:
                print(f"  ... {i + 1}/{len(backup.windows)} views", flush=True)

        # Create UDT instances (equipment) - gateway-wide, no project prefix
        if verbose and len(backup.udt_instances) > 100:
            print(
                f"[INFO] Creating {len(backup.udt_instances)} equipment instances...",
                flush=True,
            )
        for i, inst in enumerate(backup.udt_instances):
            udt_type = self._normalize_udt_name(inst.type_id)
            self.graph.create_equipment(inst.name, udt_type, "", udt_type)
            # Progress indicator for large sets
            if verbose and len(backup.udt_instances) > 500 and (i + 1) % 500 == 0:
                print(
                    f"  ... {i + 1}/{len(backup.udt_instances)} equipment", flush=True
                )

        # Create standalone SCADA tags (gateway-wide, no project prefix)
        tags_created = 0
        for tag in backup.tags:
            # Skip tags with empty or null names
            if not tag.name:
                continue
            # Convert complex values to strings (Neo4j only accepts primitives)
            self.graph.create_scada_tag(
                name=tag.name,
                tag_type=tag.tag_type,
                folder_name=self._to_string(tag.folder_name),
                data_type=self._to_string(tag.data_type),
                datasource=self._to_string(tag.datasource),
                query=self._to_string(tag.query),
                opc_item_path=self._to_string(tag.opc_item_path),
                expression=self._to_string(tag.expression),
                initial_value=self._to_string(tag.initial_value),
            )
            tags_created += 1

        # Create scripts with project-qualified names
        scripts_created = 0
        scripts_with_text = 0
        for script in backup.scripts:
            # Skip scripts without path or project
            if not script.path or not script.project:
                continue
            script_name = self._qualify_name(script.path, script.project)
            self.graph.create_script(
                name=script_name,
                path=script.path,
                project=script.project,
                scope=script.scope,
                script_text=script.script_text,
            )
            scripts_created += 1
            if script.script_text:
                scripts_with_text += 1

        # Create named queries with project-qualified names
        queries_created = 0
        queries_with_text = 0
        for query in backup.named_queries:
            # Skip queries without name or id
            query_id = query.id or query.name
            if not query_id:
                continue
            query_name = self._qualify_name(query_id, query.project)
            self.graph.create_named_query(
                name=query_name,
                project=query.project or "",
                folder_path=query.folder_path or "",
                query_id=query.id,
                query_text=query.query_text,
            )
            queries_created += 1
            if query.query_text:
                queries_with_text += 1

        # Create gateway events with project-qualified names
        events_created = 0
        for event in backup.gateway_events:
            # Skip events without project
            if not event.project:
                continue
            event_name = self._build_event_name(event)
            self.graph.create_gateway_event(
                name=event_name,
                project=event.project,
                script_type=event.script_type,
                event_name=event.name,
                script_preview=event.script[:200] if event.script else "",
                delay=event.delay,
            )
            events_created += 1

        if verbose:
            print(
                f"[OK] Created {len(backup.udt_definitions)} UDTs, {views_created} views, "
                f"{len(backup.udt_instances)} equipment, {tags_created} standalone tags",
                flush=True,
            )
            print(
                f"[OK] Created {scripts_created} scripts ({scripts_with_text} with code), "
                f"{queries_created} queries ({queries_with_text} with SQL), "
                f"{events_created} gateway events",
                flush=True,
            )

        # Create inter-entity relationships
        if verbose:
            print(f"[INFO] Creating inter-entity relationships...", flush=True)
        relationship_count = self._create_entity_relationships(backup, verbose)
        if verbose and relationship_count > 0:
            print(
                f"[OK] Created {relationship_count} inter-entity relationships",
                flush=True,
            )

        # Create cross-reference relationships (script→script, script→query, view→query)
        if verbose:
            print(f"[INFO] Creating cross-reference relationships...", flush=True)
            # Debug: show what we're working with
            scripts_with_code = sum(1 for s in backup.scripts if s.script_text)
            print(
                f"[DEBUG] Analyzing {len(backup.scripts)} scripts ({scripts_with_code} with code)",
                flush=True,
            )
            print(
                f"[DEBUG] Analyzing {len(backup.gateway_events)} gateway events",
                flush=True,
            )
            print(
                f"[DEBUG] Analyzing {len(backup.windows)} views for event scripts",
                flush=True,
            )
        xref_counts = self._create_cross_references(backup, verbose)
        total_xrefs = sum(xref_counts.values())
        if verbose:
            print(
                f"[OK] Created {total_xrefs} cross-references: "
                f"{xref_counts['script_to_script']} script→script, "
                f"{xref_counts['script_to_query']} script→query, "
                f"{xref_counts['view_to_query']} view→query, "
                f"{xref_counts['view_to_script']} view→script, "
                f"{xref_counts['event_to_query']} event→query, "
                f"{xref_counts['event_to_script']} event→script",
                flush=True,
            )

        # Create ViewComponents from parsed windows
        # This can be slow with many views - skip if there are too many
        if len(backup.windows) > 200:
            if verbose:
                print(
                    f"[INFO] Skipping ViewComponent creation for {len(backup.windows)} views (too many - use incremental analyzer)",
                    flush=True,
                )
            component_count, binding_count = 0, 0
        else:
            if verbose:
                print(
                    f"[INFO] Creating ViewComponents from {len(backup.windows)} views...",
                    flush=True,
                )
            component_count, binding_count = self._create_view_components(
                backup, verbose
            )
            if verbose:
                print(
                    f"[OK] Created {component_count} components with {binding_count} bindings",
                    flush=True,
                )

        # === PHASE 2: Extract view-to-UDT mappings (deterministic) ===
        view_udt_mappings = self._extract_view_udt_mappings(backup, verbose)
        mappings_created = 0
        for view_name, udt_names in view_udt_mappings.items():
            for udt_name in udt_names:
                success = self.graph.create_view_udt_mapping(
                    view_name, udt_name, "displays"
                )
                if verbose:
                    if success:
                        print(f"[OK] Mapped View '{view_name}' -> UDT '{udt_name}'")
                        mappings_created += 1
                    else:
                        print(
                            f"[WARN] Failed to map View '{view_name}' -> UDT '{udt_name}' (nodes not found)"
                        )

        if verbose:
            print(f"[INFO] Created {mappings_created} view-to-UDT mappings")

        # === PHASE 3: Enrich with AI analysis (semantic) ===
        analysis = {}

        if skip_ai:
            if verbose:
                print(
                    f"[INFO] Skipping AI analysis (use incremental_analyzer.py to analyze items)"
                )
                # Show status
                status = self.graph.get_semantic_status_counts()
                pending_total = sum(
                    counts.get("pending", 0) for counts in status.values()
                )
                print(f"[INFO] {pending_total} items pending semantic analysis")
        else:
            if verbose:
                print(f"[INFO] Enriching with AI analysis...")

            # Build context for LLM
            context = self._build_analysis_context(backup)

            # Generate analysis with tool support
            analysis = self._query_llm(context, verbose)

            # Enrich UDTs with semantic descriptions
            for udt_name, udt_data in analysis.get("udt_semantics", {}).items():
                # Handle both string and dict formats from Claude
                if isinstance(udt_data, dict):
                    udt_purpose = udt_data.get("purpose", "")
                else:
                    udt_purpose = str(udt_data) if udt_data else ""
                self.graph.create_udt(udt_name, udt_purpose, backup.file_path)

            # Enrich views with semantic descriptions
            for view_name, view_data in analysis.get("view_purposes", {}).items():
                # Handle both string and dict formats from Claude
                if isinstance(view_data, dict):
                    view_purpose = view_data.get("purpose", "")
                else:
                    view_purpose = str(view_data) if view_data else ""
                self.graph.create_view(view_name, "", view_purpose)

            # Add any equipment Claude discovered that we didn't parse
            for equip in analysis.get("equipment_instances", []):
                self.graph.create_equipment(
                    equip.get("name", ""),
                    equip.get("type", ""),
                    equip.get("purpose", ""),
                    equip.get("udt_name"),
                )

            if verbose:
                print(f"[OK] Stored Ignition ontology in Neo4j")

        ontology = {
            "source": "ignition",
            "source_file": backup.file_path,
            "version": backup.version,
            "summary": {
                "udt_definitions": len(backup.udt_definitions),
                "udt_instances": len(backup.udt_instances),
                "tags": len(backup.tags),
                "windows": len(backup.windows),
                "named_queries": len(backup.named_queries),
            },
            "analysis": analysis,
        }

        return ontology

    def _build_analysis_context(self, backup: IgnitionBackup) -> str:
        """Build context string for LLM analysis."""
        parts = []

        parts.append("# Ignition SCADA Configuration Analysis")
        parts.append("")

        # UDT Definitions
        if backup.udt_definitions:
            parts.append("## UDT (User Defined Type) Definitions")
            for udt in backup.udt_definitions:
                parent = f" extends {udt.parent_name}" if udt.parent_name else ""
                parts.append(f"\n### {udt.name}{parent}")

                if udt.parameters:
                    parts.append("Parameters:")
                    for pname, param in udt.parameters.items():
                        parts.append(f"  - {pname}: {param.data_type}")

                if udt.members:
                    parts.append("Members:")
                    for member in udt.members:
                        parts.append(
                            f"  - {member.name}: {member.data_type} [{member.tag_type}]"
                        )
            parts.append("")

        # UDT Instances
        if backup.udt_instances:
            parts.append("## UDT Instances (Tag Configurations)")
            for inst in backup.udt_instances:
                parts.append(f"- {inst.name}: {inst.type_id}")
                if inst.parameters:
                    for pname, pval in inst.parameters.items():
                        if pval:
                            parts.append(f"    {pname} = {pval}")
            parts.append("")

        # Standalone Tags
        if backup.tags:
            parts.append("## Standalone Tags")
            for tag in backup.tags:
                parts.append(f"- {tag.name}: {tag.tag_type}")
                if tag.query:
                    query_str = self._to_string(tag.query)
                    parts.append(f"    Query: {query_str[:200]}...")
                if tag.datasource:
                    parts.append(f"    Datasource: {self._to_string(tag.datasource)}")
            parts.append("")

        # Windows/Views
        if backup.windows:
            parts.append("## Views/Windows")
            for window in backup.windows:
                parts.append(f"\n### {window.name} ({window.path})")
                self._describe_components(window.components, parts, indent=0)
            parts.append("")

        # Named Queries
        if backup.named_queries:
            parts.append("## Named Queries")
            for query in backup.named_queries:
                folder = f" ({query.folder_path})" if query.folder_path else ""
                parts.append(f"- {query.name}{folder}")
            parts.append("")

        # Tag references from UI
        parser = IgnitionParser()
        tag_refs = parser.get_all_tag_references(backup)
        if tag_refs:
            parts.append("## Tag References in UI Bindings")
            for ref in sorted(tag_refs)[:30]:  # Limit
                parts.append(f"- {ref}")
            parts.append("")

        return "\n".join(parts)

    def _describe_components(self, components: List, parts: List[str], indent: int):
        """Recursively describe UI components."""
        for comp in components:
            prefix = "  " * indent
            parts.append(f"{prefix}- {comp.component_type}: {comp.name}")

            for binding in comp.bindings:
                parts.append(
                    f"{prefix}    binding: {binding.property_path} <- [{binding.binding_type}] {binding.target}"
                )

            if comp.children:
                self._describe_components(comp.children, parts, indent + 1)

    def _create_view_components(
        self, backup: IgnitionBackup, verbose: bool = False
    ) -> tuple:
        """Create ViewComponent nodes from parsed windows and link to UDTs/Tags.

        Returns:
            Tuple of (component_count, binding_count)
        """
        # Build UDT lookup maps for binding resolution
        udt_names = {udt.name for udt in backup.udt_definitions}
        member_to_udt = {}
        for udt_def in backup.udt_definitions:
            for member in udt_def.members:
                member_to_udt[member.name] = udt_def.name

        # Build ScadaTag lookup for direct tag bindings
        tag_names = {tag.name for tag in backup.tags}

        component_count = 0
        binding_count = 0

        for window in backup.windows:
            # Use project-qualified view name
            view_name = self._qualify_name(window.name, window.project)
            # Process all components in this view
            components_created, bindings_created = self._process_components(
                view_name,
                window.components,
                "",
                udt_names,
                member_to_udt,
                tag_names,
                verbose,
            )
            component_count += components_created
            binding_count += bindings_created

        return component_count, binding_count

    def _process_components(
        self,
        view_name: str,
        components: list,
        parent_path: str,
        udt_names: set,
        member_to_udt: dict,
        tag_names: set,
        verbose: bool = False,
    ) -> tuple:
        """Recursively process UI components and create nodes.

        Returns:
            Tuple of (components_created, bindings_created)
        """
        component_count = 0
        binding_count = 0

        for comp in components:
            # Build component path
            comp_path = f"{parent_path}/{comp.name}" if parent_path else comp.name

            # Determine component purpose from type
            comp_purpose = self._infer_component_purpose(comp)

            # Extract relevant props for troubleshooting
            relevant_props = self._extract_relevant_props(comp)

            # Create component node
            success = self.graph.create_view_component(
                view_name=view_name,
                component_name=comp.name,
                component_type=comp.component_type,
                component_path=comp_path,
                inferred_purpose=comp_purpose,
                props=relevant_props,
            )
            if success:
                component_count += 1

            # Process bindings to link component to UDTs or Tags
            for binding in comp.bindings:
                # First try to resolve to a UDT
                udt_name = self._resolve_binding_to_udt(
                    binding.target, udt_names, member_to_udt
                )
                if udt_name:
                    bind_success = self.graph.create_component_udt_binding(
                        view_name=view_name,
                        component_path=comp_path,
                        udt_name=udt_name,
                        binding_property=binding.property_path,
                        tag_path=binding.target,
                    )
                    if bind_success:
                        binding_count += 1
                        if verbose:
                            print(
                                f"[DEBUG] Component '{comp_path}' binds to UDT '{udt_name}' via {binding.property_path}"
                            )
                else:
                    # Try to resolve to a standalone ScadaTag
                    tag_name = self._resolve_binding_to_tag(binding.target, tag_names)
                    if tag_name:
                        bind_success = self.graph.create_component_tag_binding(
                            view_name=view_name,
                            component_path=comp_path,
                            tag_name=tag_name,
                            binding_property=binding.property_path,
                            tag_path=binding.target,
                        )
                        if bind_success:
                            binding_count += 1
                            if verbose:
                                print(
                                    f"[DEBUG] Component '{comp_path}' binds to Tag '{tag_name}' via {binding.property_path}"
                                )

            # Recurse into children
            if comp.children:
                child_comps, child_binds = self._process_components(
                    view_name,
                    comp.children,
                    comp_path,
                    udt_names,
                    member_to_udt,
                    tag_names,
                    verbose,
                )
                component_count += child_comps
                binding_count += child_binds

        return component_count, binding_count

    def _resolve_binding_to_tag(self, tag_path: str, tag_names: set) -> str:
        """Try to resolve a binding target to a standalone ScadaTag.

        Args:
            tag_path: Full tag path from the binding (e.g., "[default]GetEquipmentOrders")
            tag_names: Set of known ScadaTag names

        Returns:
            Tag name if found, empty string otherwise
        """
        if not tag_path:
            return ""

        # Remove provider prefix like [default] or [System]
        clean_path = tag_path
        if clean_path.startswith("["):
            bracket_end = clean_path.find("]")
            if bracket_end != -1:
                clean_path = clean_path[bracket_end + 1 :]

        # Check for exact match
        if clean_path in tag_names:
            return clean_path

        # Try the last segment (after last /)
        if "/" in clean_path:
            last_segment = clean_path.split("/")[-1]
            if last_segment in tag_names:
                return last_segment

        return ""

    def _create_entity_relationships(
        self, backup: IgnitionBackup, verbose: bool = False
    ) -> int:
        """Create relationships between entities.

        - Tag → Tag references (expression tags referencing other tags)
        - UDT → UDT nested types (UDT members that are other UDT types)
        - UDT → Tag references (UDT members referencing specific tags)

        Returns:
            Count of relationships created
        """
        import re

        count = 0
        udt_names = {udt.name for udt in backup.udt_definitions}
        tag_names = {tag.name for tag in backup.tags}

        # 1. Tag-to-tag references from expression tags
        for tag in backup.tags:
            if tag.tag_type == "expression" and tag.expression:
                # Convert expression to string if it's a dict
                expr_str = self._to_string(tag.expression)
                # Parse expression for tag references like {[default]TagName} or {TagName}
                refs = re.findall(r"\{(?:\[[^\]]+\])?([^}]+)\}", expr_str)
                for ref in refs:
                    # Clean up the reference (might have .value or /path suffixes)
                    ref_name = ref.split("/")[0].split(".")[0]
                    if ref_name in tag_names and ref_name != tag.name:
                        if self.graph.create_tag_reference(
                            tag.name, ref_name, "expression"
                        ):
                            count += 1
                            if verbose:
                                print(
                                    f"[DEBUG] Tag '{tag.name}' references Tag '{ref_name}'"
                                )

        # 2. UDT-to-UDT nested types (when a member's data_type is another UDT)
        for udt_def in backup.udt_definitions:
            for member in udt_def.members:
                member_type = member.data_type or ""
                # Normalize the type name (remove paths like ROL_DataTypes/)
                clean_type = self._normalize_udt_name(member_type)
                if clean_type in udt_names and clean_type != udt_def.name:
                    if self.graph.create_udt_nested_type(
                        udt_def.name, member.name, clean_type
                    ):
                        count += 1
                        if verbose:
                            print(
                                f"[DEBUG] UDT '{udt_def.name}' contains type '{clean_type}' via member '{member.name}'"
                            )

        # 3. UDT-to-Tag references (for members with default values referencing tags)
        # This is less common but supported
        for udt_def in backup.udt_definitions:
            for member in udt_def.members:
                default_val = (
                    str(member.default_value or "")
                    if hasattr(member, "default_value")
                    else ""
                )
                if default_val and default_val.startswith("{"):
                    # Parse tag reference from default value
                    ref_match = re.match(r"\{(?:\[[^\]]+\])?([^}]+)\}", default_val)
                    if ref_match:
                        ref_name = ref_match.group(1).split("/")[0].split(".")[0]
                        if ref_name in tag_names:
                            if self.graph.create_udt_tag_reference(
                                udt_def.name, member.name, ref_name
                            ):
                                count += 1
                                if verbose:
                                    print(
                                        f"[DEBUG] UDT '{udt_def.name}' references Tag '{ref_name}' via member '{member.name}'"
                                    )

        return count

    def _create_cross_references(
        self, backup: IgnitionBackup, verbose: bool = False
    ) -> Dict[str, int]:
        """Create cross-reference relationships between scripts, queries, and views.

        Parses script content to find:
        - Script → Script calls (e.g., Util.secondsToText())
        - Script → NamedQuery calls (e.g., system.db.runNamedQuery())
        - View event scripts → NamedQuery calls
        - View event scripts → Script module calls

        Returns:
            Dict with counts of relationships created by type
        """
        import re

        counts = {
            "script_to_script": 0,
            "script_to_query": 0,
            "view_to_query": 0,
            "view_to_script": 0,
            "event_to_query": 0,
            "event_to_script": 0,
        }

        # Build lookup sets for matching
        script_modules = set()  # Top-level script module names
        script_paths = set()  # Full script paths
        query_paths = set()  # Named query paths

        for script in backup.scripts:
            # Extract module name (first segment of path)
            module_name = script.path.split("/")[0]
            script_modules.add(module_name)
            script_paths.add(script.path)
            # Also add the qualified name used by neo4j
            qualified = f"{script.project}/{script.path}"
            script_paths.add(qualified)

        if verbose:
            print(f"[DEBUG] Script modules found: {sorted(script_modules)}", flush=True)

        for query in backup.named_queries:
            # Query path is folder_path/name or just id
            if query.folder_path:
                query_paths.add(f"{query.folder_path}/{query.name}")
            query_paths.add(query.name)
            if query.id:
                query_paths.add(query.id)

        # Regex patterns for detection
        # runNamedQuery(path="GIS/GetAreaById", ...) or runNamedQuery("project", "path", ...)
        query_pattern = re.compile(
            r'runNamedQuery\s*\(\s*(?:path\s*=\s*)?["\']([^"\']+)["\']', re.IGNORECASE
        )
        # Module.function() calls - matches Util.secondsToText, Gateway.getClients, etc.
        module_call_pattern = re.compile(
            r"\b([A-Z][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\("
        )
        # Import patterns:
        # - from pss import assets
        # - from pss.assets import something
        # - import pss.assets
        # - import pss
        import_from_pattern = re.compile(
            r"^\s*from\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s+import",
            re.MULTILINE
        )
        import_pattern = re.compile(
            r"^\s*import\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)",
            re.MULTILINE
        )

        # 1. Parse script library for cross-references
        for script in backup.scripts:
            if not script.script_text:
                if verbose:
                    print(
                        f"[DEBUG] Script '{script.path}' has no code, skipping",
                        flush=True,
                    )
                continue

            script_name = f"{script.project}/{script.path}"

            # Find named query calls
            query_matches = list(query_pattern.finditer(script.script_text))
            module_matches = list(module_call_pattern.finditer(script.script_text))
            import_from_matches = list(import_from_pattern.finditer(script.script_text))
            import_matches = list(import_pattern.finditer(script.script_text))
            
            if verbose and (query_matches or module_matches or import_from_matches or import_matches):
                print(
                    f"[DEBUG] Script '{script_name}': {len(query_matches)} query calls, "
                    f"{len(module_matches)} module calls, {len(import_from_matches) + len(import_matches)} imports",
                    flush=True,
                )

            for match in query_matches:
                query_path = match.group(1)
                if verbose:
                    print(
                        f"[DEBUG] Trying to link Script '{script_name}' → Query '{query_path}'",
                        flush=True,
                    )
                if self.graph.create_query_usage(
                    "Script", script_name, query_path, script.project
                ):
                    counts["script_to_query"] += 1
                    if verbose:
                        print(f"  [OK] Created relationship", flush=True)
                elif verbose:
                    print(f"  [WARN] No matching query found in Neo4j", flush=True)

            # Find script module calls
            for match in module_matches:
                module_name = match.group(1)
                function_name = match.group(2)

                if verbose:
                    print(
                        f"[DEBUG] Trying to link Script '{script_name}' → Script module '{module_name}.{function_name}()'",
                        flush=True,
                    )

                # Skip system modules and self-references
                if module_name in (
                    "system",
                    "self",
                    "java",
                    "str",
                    "int",
                    "len",
                    "range",
                ):
                    continue

                # Check if this is a known script module
                if module_name in script_modules:
                    if self.graph.create_script_call(
                        "Script",
                        script_name,
                        module_name,
                        function_name,
                        script.project,
                    ):
                        counts["script_to_script"] += 1
                        if verbose:
                            print(f"  [OK] Created relationship", flush=True)
                    elif verbose:
                        print(f"  [WARN] No matching script found in Neo4j", flush=True)

            # Find imports: "from pss import assets" or "from pss.assets import func"
            for match in import_from_matches:
                import_path = match.group(1)
                # Get the top-level module name
                top_module = import_path.split(".")[0]
                
                if verbose:
                    print(
                        f"[DEBUG] Trying to link Script '{script_name}' → imports '{import_path}'",
                        flush=True,
                    )
                
                # Skip standard library imports
                if top_module in ("system", "java", "os", "re", "json", "math", "datetime", "time"):
                    continue
                
                if top_module in script_modules:
                    if self.graph.create_script_call(
                        "Script",
                        script_name,
                        import_path,
                        "import",
                        script.project,
                    ):
                        counts["script_to_script"] += 1
                        if verbose:
                            print(f"  [OK] Created import relationship", flush=True)
                    elif verbose:
                        print(f"  [WARN] No matching script found in Neo4j", flush=True)

            # Find imports: "import pss" or "import pss.assets"
            for match in import_matches:
                import_path = match.group(1)
                top_module = import_path.split(".")[0]
                
                if verbose:
                    print(
                        f"[DEBUG] Trying to link Script '{script_name}' → imports '{import_path}'",
                        flush=True,
                    )
                
                if top_module in ("system", "java", "os", "re", "json", "math", "datetime", "time"):
                    continue
                
                if top_module in script_modules:
                    if self.graph.create_script_call(
                        "Script",
                        script_name,
                        import_path,
                        "import",
                        script.project,
                    ):
                        counts["script_to_script"] += 1
                        if verbose:
                            print(f"  [OK] Created import relationship", flush=True)
                    elif verbose:
                        print(f"  [WARN] No matching script found in Neo4j", flush=True)

        # 2. Parse gateway events for cross-references
        for event in backup.gateway_events:
            if not event.script:
                continue

            # Build event name matching neo4j storage
            if event.script_type in ("startup", "shutdown"):
                event_name = f"{event.project}/{event.script_type}"
            elif event.script_type == "timer" and event.name:
                event_name = f"{event.project}/timer/{event.name}"
            elif event.script_type == "message_handler" and event.name:
                event_name = f"{event.project}/message/{event.name}"
            else:
                event_name = f"{event.project}/{event.script_type}"

            # Find named query calls
            for match in query_pattern.finditer(event.script):
                query_path = match.group(1)
                if self.graph.create_query_usage(
                    "GatewayEvent", event_name, query_path, event.project
                ):
                    counts["event_to_query"] += 1
                    if verbose:
                        print(f"[DEBUG] Event '{event_name}' → Query '{query_path}'")

            # Find script module calls
            for match in module_call_pattern.finditer(event.script):
                module_name = match.group(1)
                function_name = match.group(2)

                if module_name in (
                    "system",
                    "self",
                    "java",
                    "str",
                    "int",
                    "len",
                    "range",
                ):
                    continue

                if module_name in script_modules:
                    if self.graph.create_script_call(
                        "GatewayEvent",
                        event_name,
                        module_name,
                        function_name,
                        event.project,
                    ):
                        counts["event_to_script"] += 1
                        if verbose:
                            print(
                                f"[DEBUG] Event '{event_name}' → Script '{module_name}.{function_name}()'"
                            )

        # 3. Parse view event scripts for cross-references
        # This requires traversing the view component tree
        for window in backup.windows:
            view_name = self._qualify_name(window.name, window.project)

            # Recursively extract event scripts from components
            event_scripts = self._extract_event_scripts(window.components)

            for component_path, event_type, script_text in event_scripts:
                # Find named query calls
                for match in query_pattern.finditer(script_text):
                    query_path = match.group(1)
                    if self.graph.create_query_usage(
                        "View", view_name, query_path, window.project
                    ):
                        counts["view_to_query"] += 1
                        if verbose:
                            print(f"[DEBUG] View '{view_name}' → Query '{query_path}'")

                # Find script module calls
                for match in module_call_pattern.finditer(script_text):
                    module_name = match.group(1)
                    function_name = match.group(2)

                    if module_name in (
                        "system",
                        "self",
                        "java",
                        "str",
                        "int",
                        "len",
                        "range",
                    ):
                        continue

                    if module_name in script_modules:
                        if self.graph.create_script_call(
                            "View",
                            view_name,
                            module_name,
                            function_name,
                            window.project,
                        ):
                            counts["view_to_script"] += 1
                            if verbose:
                                print(
                                    f"[DEBUG] View '{view_name}' → Script '{module_name}.{function_name}()'"
                                )

        return counts

    def _extract_event_scripts(
        self, components: List, parent_path: str = ""
    ) -> List[tuple]:
        """Recursively extract event scripts from UI components.

        Returns:
            List of (component_path, event_type, script_text) tuples
        """
        results = []

        for comp in components:
            comp_name = comp.name or "unnamed"
            comp_path = f"{parent_path}/{comp_name}" if parent_path else comp_name

            # Check for event scripts in props
            # Ignition stores event scripts in props with keys like "events" or in bindings
            if hasattr(comp, "props") and isinstance(comp.props, dict):
                # Check for events object
                events = comp.props.get("events", {})
                if isinstance(events, dict):
                    for event_type, event_data in events.items():
                        if isinstance(event_data, dict):
                            script = event_data.get("script", "")
                            if script:
                                results.append((comp_path, event_type, script))
                        elif isinstance(event_data, str) and event_data:
                            results.append((comp_path, event_type, event_data))

            # Recurse into children
            if hasattr(comp, "children") and comp.children:
                results.extend(self._extract_event_scripts(comp.children, comp_path))

        return results

    def _infer_component_purpose(self, comp) -> str:
        """Infer semantic purpose from component type and props."""
        comp_type = comp.component_type.lower()

        # Common component type patterns
        if "button" in comp_type:
            return "User action trigger"
        elif "label" in comp_type or "text" in comp_type:
            return "Information display"
        elif "led" in comp_type or "indicator" in comp_type:
            return "Status indicator"
        elif "input" in comp_type or "field" in comp_type or "numeric" in comp_type:
            return "User data entry"
        elif "dropdown" in comp_type or "select" in comp_type:
            return "User selection"
        elif "toggle" in comp_type or "switch" in comp_type or "checkbox" in comp_type:
            return "Binary control"
        elif "table" in comp_type or "grid" in comp_type:
            return "Data table display"
        elif "chart" in comp_type or "graph" in comp_type or "trend" in comp_type:
            return "Data visualization"
        elif "image" in comp_type or "icon" in comp_type:
            return "Visual representation"
        elif "container" in comp_type or "view" in comp_type or "flex" in comp_type:
            return "Layout container"
        else:
            return ""

    def _extract_relevant_props(self, comp) -> dict:
        """Extract props relevant for troubleshooting."""
        relevant = {}

        # Keys that are useful for understanding UI issues
        useful_keys = [
            "text",
            "label",
            "title",
            "placeholder",
            "tooltip",
            "enabled",
            "visible",
            "editable",
            "readonly",
            "min",
            "max",
            "step",
            "format",
            "style",
            "classes",
        ]

        for key in useful_keys:
            if key in comp.props:
                val = comp.props[key]
                # Only include simple values, not complex objects
                if isinstance(val, (str, int, float, bool)):
                    relevant[key] = val

        return relevant

    def _resolve_binding_to_udt(
        self, tag_ref: str, udt_names: set, member_to_udt: dict
    ) -> Optional[str]:
        """Resolve a tag binding to its UDT type."""
        # Clean and split the tag reference
        cleaned = (
            tag_ref.replace("{", "")
            .replace("}", "")
            .replace("[default]", "")
            .replace("[.]", "")
        )
        segments = []
        for part in cleaned.split("/"):
            segments.extend(part.split("."))

        for segment in segments:
            # Check member map
            if segment in member_to_udt:
                return member_to_udt[segment]

            # Check HMI patterns
            if segment.startswith("HMI_"):
                hmi_type = segment[4:]
                matched = self._match_hmi_to_udt(hmi_type, udt_names, verbose=False)
                if matched:
                    return matched

        return None

    def _extract_view_udt_mappings(
        self, backup: IgnitionBackup, verbose: bool = False
    ) -> Dict[str, set]:
        """Extract which views reference which UDTs based on tag bindings.

        Analyzes tag bindings in views to determine which UDT types each view uses.
        Uses multiple strategies:
        1. Direct tag instance matching
        2. UDT member name matching (for parameterized views like {TagPath}/HMI_MotorControl)
        3. View name to UDT name matching by convention

        Returns:
            Dict mapping view names to sets of UDT names they reference
        """
        if verbose:
            print(f"\n[DEBUG] === View-to-UDT Mapping Analysis ===")
            print(f"[DEBUG] Found {len(backup.udt_instances)} UDT instances")
            print(f"[DEBUG] Found {len(backup.windows)} windows/views")
            print(f"[DEBUG] Found {len(backup.udt_definitions)} UDT definitions")

        # Build a map of tag paths to their UDT types (normalized names)
        tag_to_udt = {}

        # Map UDT instances to their type (normalized to base name)
        for inst in backup.udt_instances:
            # Normalize type_id: "Types/MotorReversingControl" -> "MotorReversingControl"
            udt_name = self._normalize_udt_name(inst.type_id)

            # Tag path is typically the instance name
            tag_to_udt[inst.name] = udt_name
            # Also add with common path prefixes
            tag_to_udt[f"[default]{inst.name}"] = udt_name
            tag_to_udt[f"[.]/{inst.name}"] = udt_name

            if verbose:
                print(
                    f"[DEBUG] UDT instance: '{inst.name}' -> type '{udt_name}' (raw: {inst.type_id})"
                )

        # Build member-to-UDT map from UDT definitions
        # Maps member names like "HMI_MotorControl" to their parent UDT "MotorReversingControl"
        member_to_udt = {}
        udt_names = set()
        for udt_def in backup.udt_definitions:
            udt_names.add(udt_def.name)
            for member in udt_def.members:
                member_to_udt[member.name] = udt_def.name
                if verbose:
                    print(
                        f"[DEBUG] UDT member: '{member.name}' -> parent UDT '{udt_def.name}'"
                    )

        # Map view names to UDTs they reference
        view_udt_map: Dict[str, set] = {}

        for window in backup.windows:
            # Use project-qualified view name
            view_name = self._qualify_name(window.name, window.project)
            udts_used = set()

            # Extract all tag references from this view
            tag_refs = self._get_component_tag_refs(window.components)

            if verbose and tag_refs:
                print(f"[DEBUG] View '{view_name}' has {len(tag_refs)} tag bindings")
                for ref in list(tag_refs)[:5]:  # Show first 5
                    print(f"[DEBUG]   - {ref}")
                if len(tag_refs) > 5:
                    print(f"[DEBUG]   ... and {len(tag_refs) - 5} more")

            for tag_ref in tag_refs:
                # Strategy 1: Direct tag instance matching
                udt_type = self._resolve_tag_to_udt(tag_ref, tag_to_udt, backup)
                if udt_type:
                    udts_used.add(udt_type)
                    continue

                # Strategy 2: UDT member or HMI structure matching for parameterized views
                # e.g., "{TagPath}/HMI_MotorControl/iStatus" -> matches "HMI_MotorControl" to "MotorReversingControl"
                matched_udt = self._match_tag_to_udt_member(
                    tag_ref, member_to_udt, udt_names, verbose
                )
                if matched_udt:
                    udts_used.add(matched_udt)

            if udts_used:
                view_udt_map[view_name] = udts_used
                if verbose:
                    print(
                        f"[INFO] View '{view_name}' uses UDTs: {', '.join(udts_used)}"
                    )
            elif verbose and tag_refs:
                print(
                    f"[DEBUG] View '{view_name}' has tag bindings but no UDT matches found"
                )

        if verbose:
            print(f"[DEBUG] === End View-to-UDT Analysis ===\n")

        return view_udt_map

    def _match_tag_to_udt_member(
        self,
        tag_ref: str,
        member_to_udt: Dict[str, str],
        udt_names: set,
        verbose: bool = False,
    ) -> Optional[str]:
        """Match a tag reference to a UDT via member name or HMI structure.

        For parameterized views like "{TagPath}/HMI_MotorControl/iStatus",
        extract path segments and look for matches in member_to_udt map.

        Also matches HMI_* patterns to infer UDT types:
        - HMI_DigitalInput -> DigitalInput
        - HMI_MotorControl -> MotorReversingControl (via fuzzy match)
        - HMI_ValveControl -> ValveSolenoidControl (via fuzzy match)
        """
        # Split on / and . to get all path segments
        # "{TagPath}/HMI_MotorControl/iStatus" -> ["TagPath", "HMI_MotorControl", "iStatus"]
        cleaned = (
            tag_ref.replace("{", "")
            .replace("}", "")
            .replace("[default]", "")
            .replace("[.]", "")
        )
        segments = []
        for part in cleaned.split("/"):
            segments.extend(part.split("."))

        for segment in segments:
            # Strategy 1: Direct member match
            if segment in member_to_udt:
                if verbose:
                    print(
                        f"[DEBUG]   Matched member '{segment}' -> UDT '{member_to_udt[segment]}'"
                    )
                return member_to_udt[segment]

            # Strategy 2: HMI structure name matching
            if segment.startswith("HMI_"):
                hmi_type = segment[4:]  # Remove "HMI_" prefix
                matched = self._match_hmi_to_udt(hmi_type, udt_names, verbose)
                if matched:
                    return matched

        return None

    def _match_hmi_to_udt(
        self, hmi_type: str, udt_names: set, verbose: bool = False
    ) -> Optional[str]:
        """Match an HMI structure name to a UDT.

        Handles patterns like:
        - DigitalInput -> DigitalInput (exact)
        - MotorControl -> MotorReversingControl (prefix)
        - ValveControl -> ValveSolenoidControl (contains)
        """
        hmi_lower = hmi_type.lower()

        for udt_name in udt_names:
            udt_lower = udt_name.lower()

            # Exact match
            if hmi_lower == udt_lower:
                if verbose:
                    print(
                        f"[DEBUG]   Matched HMI '{hmi_type}' -> UDT '{udt_name}' (exact)"
                    )
                return udt_name

            # HMI type is prefix of UDT (MotorControl -> MotorReversingControl)
            if udt_lower.startswith(hmi_lower):
                if verbose:
                    print(
                        f"[DEBUG]   Matched HMI '{hmi_type}' -> UDT '{udt_name}' (prefix)"
                    )
                return udt_name

            # HMI type contained in UDT name (Valve -> ValveSolenoidControl)
            # Only if hmi_type is reasonably long to avoid false matches
            if len(hmi_lower) >= 5 and hmi_lower in udt_lower:
                if verbose:
                    print(
                        f"[DEBUG]   Matched HMI '{hmi_type}' -> UDT '{udt_name}' (contains)"
                    )
                return udt_name

            # Check for common abbreviation patterns
            # ValveControl -> ValveSolenoidControl (Valve matches)
            if hmi_lower.endswith("control"):
                base = hmi_lower[:-7]  # Remove "control"
                if base and udt_lower.startswith(base):
                    if verbose:
                        print(
                            f"[DEBUG]   Matched HMI '{hmi_type}' -> UDT '{udt_name}' (base prefix)"
                        )
                    return udt_name

        return None

    def _normalize_udt_name(self, type_id: str) -> str:
        """Normalize a UDT type_id to its base name.

        Handles formats like:
        - "Types/MotorReversingControl" -> "MotorReversingControl"
        - "com.example/MyUDT" -> "MyUDT"
        - "MotorReversingControl" -> "MotorReversingControl"
        """
        if "/" in type_id:
            return type_id.split("/")[-1]
        return type_id

    def _qualify_name(self, name: str, project: Optional[str]) -> str:
        """Create a project-qualified name.

        Args:
            name: Resource name
            project: Project name (or None for gateway-wide resources)

        Returns:
            Qualified name in format "project/name" or just "name" if no project
        """
        if project:
            return f"{project}/{name}"
        return name

    def _to_string(self, value: Any) -> str:
        """Convert any value to a string for Neo4j storage.

        Neo4j only accepts primitive types. Complex objects like dicts/lists
        need to be converted to strings.

        Args:
            value: Any value (could be None, str, dict, list, etc.)

        Returns:
            String representation or empty string if None
        """
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list)):
            import json

            return json.dumps(value)
        return str(value)

    def _build_event_name(self, event) -> str:
        """Build a qualified name for a gateway event.

        Args:
            event: GatewayEventScript object

        Returns:
            Qualified name like "Project/startup" or "Project/timer/TimerName"
        """
        if event.script_type in ("startup", "shutdown"):
            return f"{event.project}/{event.script_type}"
        elif event.script_type == "timer" and event.name:
            return f"{event.project}/timer/{event.name}"
        elif event.script_type == "message_handler" and event.name:
            return f"{event.project}/message/{event.name}"
        else:
            return f"{event.project}/{event.script_type}"

    def _create_projects(self, backup, verbose: bool = False) -> None:
        """Create Project nodes and inheritance relationships.

        Args:
            backup: Parsed IgnitionBackup with projects dict
            verbose: Print detailed progress
        """
        from ignition_parser import Project

        # First pass: create all project nodes
        for proj_name, proj in backup.projects.items():
            if isinstance(proj, Project):
                self.graph.create_project(
                    name=proj.name,
                    title=proj.title,
                    description=proj.description,
                    parent=proj.parent,
                    enabled=proj.enabled,
                    inheritable=proj.inheritable,
                )
                if verbose:
                    parent_info = (
                        f" (inherits from {proj.parent})" if proj.parent else ""
                    )
                    print(f"  [OK] Created project: {proj.name}{parent_info}")

    def resolve_resource(
        self, resource_type: str, name: str, current_project: str
    ) -> Optional[str]:
        """Find a resource by checking current project then parent projects.

        Implements inheritance resolution: child project resources override parent.

        Args:
            resource_type: Node type (View, Script, NamedQuery)
            name: Unqualified resource name
            current_project: Starting project to search from

        Returns:
            Qualified resource name if found, None otherwise
        """
        # Build inheritance chain: [current, parent, grandparent, ...]
        chain = self.graph.get_project_inheritance_chain(current_project)

        for project in chain:
            qualified_name = f"{project}/{name}"
            # Check if resource exists
            with self.graph.session() as session:
                result = session.run(
                    f"""
                    MATCH (n:{resource_type} {{name: $name}})
                    RETURN n.name as name
                    LIMIT 1
                """,
                    {"name": qualified_name},
                )
                if result.single():
                    return qualified_name

        return None

    def _get_component_tag_refs(self, components: List) -> set:
        """Recursively extract all tag references from UI components."""
        refs = set()
        for comp in components:
            for binding in comp.bindings:
                if binding.binding_type == "tag" and binding.target:
                    refs.add(binding.target)
            if comp.children:
                refs.update(self._get_component_tag_refs(comp.children))
        return refs

    def _resolve_tag_to_udt(
        self, tag_ref: str, tag_to_udt: Dict[str, str], backup: IgnitionBackup
    ) -> Optional[str]:
        """Resolve a tag reference to its UDT type (normalized name).

        Tag references can be in various formats:
        - [default]Equipment/Motor01.Status
        - [.]Equipment/Motor01.Command
        - Motor01.Running
        """
        # Direct lookup (already normalized in tag_to_udt)
        if tag_ref in tag_to_udt:
            return tag_to_udt[tag_ref]

        # Try extracting the base tag (before any member access)
        # e.g., "[default]Equipment/Motor01.Status" -> "Motor01"
        parts = tag_ref.replace("[default]", "").replace("[.]", "").split("/")

        for part in parts:
            # Split off any member access
            base_tag = part.split(".")[0]

            # Look for this in our UDT instances
            for inst in backup.udt_instances:
                if inst.name == base_tag or inst.name.endswith("/" + base_tag):
                    # Return normalized name
                    return self._normalize_udt_name(inst.type_id)

        return None

    def _query_llm(self, context: str, verbose: bool = False) -> Dict[str, Any]:
        """Query Claude API for analysis with tool support."""

        system_prompt = """You are an expert in industrial automation and SCADA systems, specializing in Ignition by Inductive Automation. Your task is to analyze Ignition configurations and generate semantic ontologies.

You have access to tools to query the existing ontology database:
- get_schema: Discover what node types exist (AOI, UDT, Tag, etc.)
- run_query: Execute Cypher queries to explore existing data
- get_node: Get details of specific components

USE THESE TOOLS to explore what PLC components already exist. This helps you:
- Identify how SCADA UDTs might map to existing PLC AOIs
- Find relationships between SCADA and PLC components
- Use consistent terminology with the PLC layer
- Build on existing knowledge

Focus on the industrial/operational meaning, not just the technical structure. Identify patterns like:
- Equipment templates (motors, valves, sensors)
- HMI patterns (dashboards, control panels, data displays)
- Data pathways (OPC to tag to UI binding)
- Hierarchical organization (areas, lines, equipment)"""

        user_prompt = f"""Analyze this Ignition SCADA configuration and generate a semantic ontology.

FIRST, use the available tools to explore existing data:
1. Use get_schema to see the current graph structure
2. Query for AOI nodes to understand PLC components
3. Look for existing UDT or Equipment nodes

THEN, provide your analysis as a structured JSON object with these fields:
- "system_purpose": string describing what this SCADA system monitors/controls
- "udt_semantics": object mapping UDT names to their industrial purpose
- "equipment_instances": array of {{name, type, purpose, plc_connection}} for each UDT instance
- "data_flows": array describing how data moves from PLC to UI
- "view_purposes": object mapping view names to their operational purpose
- "tag_categories": object grouping tags by their function (control, status, setpoint, etc.)
- "integration_points": array of external system connections (OPC servers, databases, etc.)
- "operational_patterns": array of identified patterns in the configuration

Be concise but informative. Focus on industrial/operational semantics.

## Configuration to Analyze:

{context}"""

        if verbose:
            print("[INFO] Querying Claude API with tool support...")

        result = self._client.query_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=20000,
            use_tools=True,
            verbose=verbose,
        )

        if verbose and result.get("tool_calls"):
            print(f"[INFO] Claude made {len(result['tool_calls'])} tool calls")

        if result.get("data"):
            return result["data"]
        else:
            return {
                "error": result.get("error", "Unknown error"),
                "raw_response": result.get("raw_text", "")[:1000],
            }

    def get_all_udts(self) -> List[Dict]:
        """Get all UDTs from Neo4j."""
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (u:UDT)
                RETURN u.name as name, u.purpose as purpose, u.source_file as source_file
            """
            )
            return [dict(r) for r in result]

    def get_all_equipment(self) -> List[Dict]:
        """Get all equipment from Neo4j."""
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (e:Equipment)
                OPTIONAL MATCH (e)-[:INSTANCE_OF]->(u:UDT)
                RETURN e.name as name, e.type as type, e.purpose as purpose,
                       u.name as udt_name
            """
            )
            return [dict(r) for r in result]

    def get_all_views(self) -> List[Dict]:
        """Get all views from Neo4j."""
        with self.graph.session() as session:
            result = session.run(
                """
                MATCH (v:View)
                RETURN v.name as name, v.path as path, v.purpose as purpose
            """
            )
            return [dict(r) for r in result]


def main():
    """CLI for Ignition ontology analyzer."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Analyze Ignition backup JSON and generate semantic ontology (stored in Neo4j)"
    )
    parser.add_argument("input", nargs="?", help="Path to Ignition backup JSON file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--model", default="claude-sonnet-4-5-20250929", help="Claude model"
    )
    parser.add_argument(
        "--no-tools", action="store_true", help="Disable Neo4j tool calls"
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip AI analysis (create entities only, use incremental_analyzer.py later)",
    )
    parser.add_argument(
        "--list-udts", action="store_true", help="List all UDTs in Neo4j"
    )
    parser.add_argument(
        "--list-equipment", action="store_true", help="List all equipment in Neo4j"
    )
    parser.add_argument(
        "--list-views", action="store_true", help="List all views in Neo4j"
    )
    parser.add_argument(
        "--status", action="store_true", help="Show semantic analysis status"
    )
    parser.add_argument("--export", metavar="FILE", help="Export analysis to JSON file")
    parser.add_argument(
        "--script-library",
        metavar="DIR",
        help="Path to script_library directory (auto-detected if not specified)",
    )
    parser.add_argument(
        "--named-queries",
        metavar="DIR",
        help="Path to named_queries_library directory (auto-detected if not specified)",
    )

    args = parser.parse_args()

    client = ClaudeClient(model=args.model, enable_tools=not args.no_tools)
    analyzer = IgnitionOntologyAnalyzer(client=client)

    try:
        if args.status:
            status = analyzer.graph.get_semantic_status_counts()
            print("\n=== Semantic Analysis Status ===\n")
            for item_type in ["UDT", "Equipment", "View", "ViewComponent"]:
                counts = status.get(item_type, {})
                pending = counts.get("pending", 0)
                complete = counts.get("complete", 0)
                total = (
                    pending
                    + complete
                    + counts.get("in_progress", 0)
                    + counts.get("review", 0)
                )
                if total > 0:
                    pct = (complete / total * 100) if total > 0 else 0
                    print(
                        f"  {item_type:15} {complete:3}/{total:<3} complete ({pct:.0f}%)"
                    )
            print()

        elif args.list_udts:
            udts = analyzer.get_all_udts()
            print(f"\n[INFO] Found {len(udts)} UDTs:\n")
            for udt in udts:
                print(f"  {udt['name']}: {udt.get('purpose', 'N/A')[:80]}...")

        elif args.list_equipment:
            equipment = analyzer.get_all_equipment()
            print(f"\n[INFO] Found {len(equipment)} equipment instances:\n")
            for eq in equipment:
                udt = f" (UDT: {eq['udt_name']})" if eq.get("udt_name") else ""
                print(f"  {eq['name']}: {eq.get('type', 'N/A')}{udt}")

        elif args.list_views:
            views = analyzer.get_all_views()
            print(f"\n[INFO] Found {len(views)} views:\n")
            for v in views:
                print(f"  {v['name']}: {v.get('purpose', 'N/A')[:60]}...")

        elif args.input:
            # Parse backup with optional content directories
            ignition_parser = IgnitionParser()
            backup = ignition_parser.parse_file(
                args.input,
                script_library_path=getattr(args, "script_library", None),
                named_queries_path=getattr(args, "named_queries", None),
            )

            # Show what content directories were found
            if args.verbose:
                if ignition_parser.script_library_path:
                    print(
                        f"[INFO] Script library: {ignition_parser.script_library_path}"
                    )
                else:
                    print("[WARN] Script library not found - scripts will have no code")
                if ignition_parser.named_queries_path:
                    print(f"[INFO] Named queries: {ignition_parser.named_queries_path}")
                else:
                    print(
                        "[WARN] Named queries library not found - queries will have no SQL"
                    )

            # Count inlined content
            scripts_with_text = sum(1 for s in backup.scripts if s.script_text)
            queries_with_text = sum(1 for q in backup.named_queries if q.query_text)

            print(
                f"[INFO] Parsed: {len(backup.udt_definitions)} UDTs, {len(backup.udt_instances)} instances, {len(backup.windows)} views"
            )
            print(
                f"[INFO] Inlined: {scripts_with_text}/{len(backup.scripts)} scripts, "
                f"{queries_with_text}/{len(backup.named_queries)} queries"
            )

            # Analyze and store in Neo4j
            ontology = analyzer.analyze_backup(
                backup, verbose=args.verbose, skip_ai=args.skip_ai
            )

            # Export if requested
            if args.export:
                with open(args.export, "w", encoding="utf-8") as f:
                    json.dump(ontology, f, indent=2)
                print(f"[OK] Exported analysis to {args.export}")
            elif not args.skip_ai:
                print("\n=== Ignition Ontology ===")
                print(
                    f"System Purpose: {ontology['analysis'].get('system_purpose', 'N/A')}"
                )
                print(
                    f"UDTs: {list(ontology['analysis'].get('udt_semantics', {}).keys())}"
                )

        else:
            parser.print_help()

    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
