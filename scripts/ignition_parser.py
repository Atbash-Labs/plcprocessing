#!/usr/bin/env python3
"""
Parser for Ignition SCADA backup JSON files.
Extracts UDT definitions, tag instances, windows, queries, and bindings.
"""

import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Set
from pathlib import Path


@dataclass
class UDTParameter:
    """Parameter definition in a UDT."""

    name: str
    data_type: str
    value: Optional[Any] = None


@dataclass
class UDTMember:
    """Member (child tag) in a UDT."""

    name: str
    data_type: str
    tag_type: str  # opc, memory, expression, etc.
    opc_item_path: Optional[str] = None
    expression: Optional[str] = None
    server_name: Optional[Any] = None


@dataclass
class UDTDefinition:
    """User Defined Type definition."""

    name: str
    id: str
    parameters: Dict[str, UDTParameter] = field(default_factory=dict)
    members: List[UDTMember] = field(default_factory=list)
    parent_name: Optional[str] = None
    folder_name: Optional[str] = None


@dataclass
class UDTInstance:
    """Instance of a UDT."""

    name: str
    type_id: str
    id: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    folder_name: Optional[str] = None


@dataclass
class Tag:
    """Standalone tag (not part of UDT)."""

    name: str
    tag_type: str  # query, memory, opc, expression, etc.
    data_type: Optional[str] = None
    folder_name: Optional[str] = None
    # Type-specific fields
    query: Optional[str] = None
    datasource: Optional[str] = None
    opc_item_path: Optional[str] = None
    expression: Optional[str] = None
    initial_value: Optional[Any] = None


@dataclass
class Binding:
    """Data binding in a UI component."""

    property_path: str
    binding_type: str  # tag, query, expression
    target: str  # tag path, query path, or expression
    bidirectional: bool = False


@dataclass
class UIComponent:
    """UI component in a window."""

    name: str
    component_type: str
    bindings: List[Binding] = field(default_factory=list)
    children: List["UIComponent"] = field(default_factory=list)
    props: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Window:
    """Perspective view or Vision window."""

    name: str
    path: str
    window_type: str  # perspective, vision
    title: Optional[str] = None
    components: List[UIComponent] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    project: Optional[str] = None  # Project this window belongs to


@dataclass
class NamedQuery:
    """Named query definition."""

    name: str
    id: str
    folder_path: Optional[str] = None
    project: Optional[str] = None
    query_text: str = ""  # Actual SQL from query.sql file


@dataclass
class Project:
    """Ignition project definition."""

    name: str
    title: str
    description: str
    parent: Optional[str] = None  # Parent project name for inheritance
    enabled: bool = True
    inheritable: bool = False


@dataclass
class Script:
    """Script from script_library (project-specific)."""

    name: str
    path: str
    project: str
    scope: str = "A"  # A=All, G=Gateway, C=Client, D=Designer
    script_text: str = ""  # Full code from code.py file


@dataclass
class GatewayEventScript:
    """Gateway event script (project-specific)."""

    project: str
    script_type: str  # startup, shutdown, timer, message_handler
    name: Optional[str] = None  # For timer scripts and message handlers
    script: str = ""
    delay: Optional[int] = None  # For timer scripts (ms)


@dataclass
class IgnitionBackup:
    """Parsed Ignition backup structure."""

    file_path: str
    version: str

    # Core elements (gateway-wide)
    udt_definitions: List[UDTDefinition] = field(default_factory=list)
    udt_instances: List[UDTInstance] = field(default_factory=list)
    tags: List[Tag] = field(default_factory=list)

    # Project-specific resources
    windows: List[Window] = field(default_factory=list)
    named_queries: List[NamedQuery] = field(default_factory=list)
    scripts: List[Script] = field(default_factory=list)
    gateway_events: List[GatewayEventScript] = field(default_factory=list)

    # Project definitions with inheritance
    projects: Dict[str, Project] = field(default_factory=dict)

    # Organizational
    folders: Dict[str, Any] = field(default_factory=dict)

    # Connections
    db_connections: List[Dict] = field(default_factory=list)
    servers: List[Dict] = field(default_factory=list)


class IgnitionParser:
    """Parser for Ignition backup JSON files."""

    def __init__(self):
        """Initialize parser with optional content directories."""
        self.script_library_path: Optional[Path] = None
        self.named_queries_path: Optional[Path] = None

    def parse_file(
        self,
        file_path: str,
        script_library_path: Optional[str] = None,
        named_queries_path: Optional[str] = None,
    ) -> IgnitionBackup:
        """Parse an Ignition backup JSON file.

        Args:
            file_path: Path to the JSON backup file
            script_library_path: Optional path to script_library directory.
                                 If None, tries to find it relative to file_path.
            named_queries_path: Optional path to named_queries_library directory.
                               If None, tries to find it relative to file_path.
        """
        json_path = Path(file_path)
        json_dir = json_path.parent

        # Resolve script library path
        if script_library_path:
            self.script_library_path = Path(script_library_path)
        else:
            # Try to find relative to JSON file
            candidate = json_dir / "script_library"
            if candidate.exists():
                self.script_library_path = candidate

        # Resolve named queries path
        if named_queries_path:
            self.named_queries_path = Path(named_queries_path)
        else:
            # Try to find relative to JSON file
            candidate = json_dir / "named_queries_library"
            if candidate.exists():
                self.named_queries_path = candidate

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        backup = IgnitionBackup(
            file_path=file_path, version=data.get("version", "unknown")
        )

        # Parse projects first (needed for other resources)
        backup.projects = self._parse_projects(data.get("projects", {}))

        # Parse gateway-wide elements
        backup.udt_definitions = self._parse_udt_definitions(
            data.get("udt_definitions", [])
        )
        backup.udt_instances = self._parse_udt_instances(data.get("udt_instances", []))
        backup.tags = self._parse_tags(data.get("tags", []))

        # Parse project-specific elements (with project tracking)
        backup.windows = self._parse_windows(data.get("windows", []))
        backup.named_queries = self._parse_named_queries(data.get("named_queries", []))
        backup.scripts = self._parse_scripts(data.get("scripts", []))
        backup.gateway_events = self._parse_gateway_events(
            data.get("gateway_events", [])
        )

        # Other metadata
        backup.folders = data.get("folders", {})
        backup.db_connections = data.get("db_connections", [])
        backup.servers = data.get("servers", [])

        return backup

    def _read_script_file(self, project: str, script_name: str) -> str:
        """Read script content from script_library directory.

        Args:
            project: Project name (directory in script_library)
            script_name: Script name/path (e.g., "Gateway" or "geoFence/config")

        Returns:
            Script content or empty string if not found

        Directory structure: script_dir/project_name/script_name/code.py
        """
        if not self.script_library_path:
            return ""

        # script_dir/project_name/script_name/code.py
        code_file = self.script_library_path / project / script_name / "code.py"

        if code_file.exists():
            try:
                return code_file.read_text(encoding="utf-8")
            except Exception:
                return ""
        return ""

    def _read_query_file(
        self, project: str, folder_path: Optional[str], query_name: str
    ) -> str:
        """Read query SQL from named_queries_library directory.

        Args:
            project: Project name (directory in named_queries_library)
            folder_path: Folder path within project (can have spaces)
            query_name: Query name (can have spaces)

        Returns:
            SQL content or empty string if not found

        Directory structure: nq_dir/project_name/folder_path/query_name/query.sql
        """
        if not self.named_queries_path:
            return ""

        # Build path: nq_dir/project_name/folder_path/query_name/query.sql
        # folder_path and query_name can have spaces
        if folder_path:
            query_file = (
                self.named_queries_path
                / project
                / folder_path
                / query_name
                / "query.sql"
            )
        else:
            query_file = self.named_queries_path / project / query_name / "query.sql"

        if query_file.exists():
            try:
                return query_file.read_text(encoding="utf-8")
            except Exception:
                return ""
        return ""

    def _parse_projects(self, projects_data: Dict[str, Any]) -> Dict[str, Project]:
        """Parse project definitions with inheritance info."""
        projects = {}

        for proj_name, proj_data in projects_data.items():
            if isinstance(proj_data, dict):
                projects[proj_name] = Project(
                    name=proj_name,
                    title=proj_data.get("title", ""),
                    description=proj_data.get("description", ""),
                    parent=proj_data.get("parent")
                    or None,  # Convert empty string to None
                    enabled=proj_data.get("enabled", True),
                    inheritable=proj_data.get("inheritable", False),
                )

        return projects

    def _parse_udt_definitions(self, udt_list: List[Dict]) -> List[UDTDefinition]:
        """Parse UDT definitions."""
        definitions = []

        for udt_data in udt_list:
            # Parse parameters
            params = {}
            for param_name, param_data in udt_data.get("parameters", {}).items():
                if isinstance(param_data, dict):
                    params[param_name] = UDTParameter(
                        name=param_name,
                        data_type=param_data.get("dataType", "Unknown"),
                        value=param_data.get("value"),
                    )

            # Parse members
            members = []
            for member_data in udt_data.get("members", []):
                members.append(
                    UDTMember(
                        name=member_data.get("name", ""),
                        data_type=member_data.get("data_type", "Unknown"),
                        tag_type=member_data.get("type", "memory"),
                        opc_item_path=member_data.get("opc_item_path"),
                        expression=member_data.get("expression"),
                        server_name=member_data.get("server_name"),
                    )
                )

            definitions.append(
                UDTDefinition(
                    name=udt_data.get("name", ""),
                    id=udt_data.get("id", ""),
                    parameters=params,
                    members=members,
                    parent_name=udt_data.get("parent_name"),
                    folder_name=udt_data.get("folder_name"),
                )
            )

        return definitions

    def _parse_udt_instances(self, instance_list: List[Dict]) -> List[UDTInstance]:
        """Parse UDT instances."""
        instances = []

        for inst_data in instance_list:
            # Parse parameter values
            params = {}
            for param_name, param_data in inst_data.get("parameters", {}).items():
                if isinstance(param_data, dict):
                    params[param_name] = param_data.get("value")
                else:
                    params[param_name] = param_data

            instances.append(
                UDTInstance(
                    name=inst_data.get("name", ""),
                    type_id=inst_data.get("typeId", ""),
                    id=inst_data.get("id", ""),
                    parameters=params,
                    folder_name=inst_data.get("folder_name"),
                )
            )

        return instances

    def _parse_tags(self, tag_list: List[Dict]) -> List[Tag]:
        """Parse standalone tags."""
        tags = []

        for tag_data in tag_list:
            # Skip tags without names
            tag_name = tag_data.get("name", "")
            if not tag_name:
                continue

            tags.append(
                Tag(
                    name=tag_name,
                    tag_type=tag_data.get("type", "memory"),
                    data_type=tag_data.get("data_type"),
                    folder_name=tag_data.get("folder_name"),
                    query=tag_data.get("query"),
                    datasource=tag_data.get("datasource"),
                    opc_item_path=tag_data.get("opc_item_path"),
                    expression=tag_data.get("expression"),
                    initial_value=tag_data.get("initial_value"),
                )
            )

        return tags

    def _parse_windows(self, window_list: List[Dict]) -> List[Window]:
        """Parse windows/views with project association."""
        windows = []

        for project_windows in window_list:
            # window_list is [{project_name: [windows]}]
            for project_name, proj_windows in project_windows.items():
                for win_data in proj_windows:
                    # Parse root container recursively
                    components = []
                    root = win_data.get("root_container")
                    if root:
                        components = [self._parse_component(root)]

                    windows.append(
                        Window(
                            name=win_data.get("name", ""),
                            path=win_data.get("path", ""),
                            window_type=win_data.get("window_type", "perspective"),
                            title=win_data.get("title"),
                            components=components,
                            params=win_data.get("params", {}),
                            project=project_name,
                        )
                    )

        return windows

    def _parse_component(self, comp_data: Dict) -> UIComponent:
        """Recursively parse a UI component."""
        # Extract bindings
        bindings = []
        for prop_path, binding_data in comp_data.get("bindings", {}).items():
            if isinstance(binding_data, dict):
                binding_type = binding_data.get("type", "unknown")

                if binding_type == "tag":
                    target = binding_data.get("tag", "")
                elif binding_type == "query":
                    config = binding_data.get("config", {})
                    target = config.get("queryPath", "")
                elif binding_type == "expr":
                    target = binding_data.get("expression", "")
                else:
                    target = str(binding_data)

                bindings.append(
                    Binding(
                        property_path=prop_path,
                        binding_type=binding_type,
                        target=target,
                        bidirectional=binding_data.get("bidirectional", False),
                    )
                )

        # Parse children recursively
        children = []
        for child_data in comp_data.get("children", []):
            children.append(self._parse_component(child_data))

        return UIComponent(
            name=comp_data.get("meta", {}).get("name", ""),
            component_type=comp_data.get("type", "unknown"),
            bindings=bindings,
            children=children,
            props=comp_data.get("props", {}),
        )

    def _parse_named_queries(self, query_list: List[Dict]) -> List[NamedQuery]:
        """Parse named query references with project association."""
        queries = []

        for project_queries in query_list:
            for project_name, proj_queries in project_queries.items():
                for query_data in proj_queries:
                    query_name = query_data.get("name", "")
                    query_id = query_data.get("id", "")
                    folder_path = query_data.get("folder_path")

                    # Read actual SQL from file using folder_path and name
                    # Directory structure: nq_dir/project_name/folder_path/query_name/query.sql
                    # Both folder_path and query_name can have spaces
                    query_text = self._read_query_file(
                        project_name, folder_path, query_name
                    )

                    queries.append(
                        NamedQuery(
                            name=query_name,
                            id=query_id,
                            folder_path=folder_path,
                            project=project_name,
                            query_text=query_text,
                        )
                    )

        return queries

    def _parse_scripts(self, scripts_list: List[Dict]) -> List[Script]:
        """Parse script library entries with project association."""
        scripts = []

        for project_scripts in scripts_list:
            for project_name, proj_scripts in project_scripts.items():
                for script_entry in proj_scripts:
                    # Each script_entry is {path: {metadata}}
                    for script_path, script_data in script_entry.items():
                        if isinstance(script_data, dict):
                            # Read actual code from file
                            script_text = self._read_script_file(
                                project_name, script_path
                            )

                            scripts.append(
                                Script(
                                    name=script_path.split("/")[-1],  # Last segment
                                    path=script_path,
                                    project=project_name,
                                    scope=script_data.get("scope", "A"),
                                    script_text=script_text,
                                )
                            )

        return scripts

    def _parse_gateway_events(
        self, events_list: List[Dict]
    ) -> List[GatewayEventScript]:
        """Parse gateway event scripts (startup, shutdown, timer, message handlers)."""
        events = []

        for project_events in events_list:
            for project_name, proj_events in project_events.items():
                if not isinstance(proj_events, dict):
                    continue

                script_config = proj_events.get("scriptConfig", {})

                # Startup script
                startup = script_config.get("startupScript", "")
                if startup and startup.strip():
                    events.append(
                        GatewayEventScript(
                            project=project_name,
                            script_type="startup",
                            script=startup[:500],  # Preview
                        )
                    )

                # Shutdown script
                shutdown = script_config.get("shutdownScript", "")
                if shutdown and shutdown.strip():
                    events.append(
                        GatewayEventScript(
                            project=project_name,
                            script_type="shutdown",
                            script=shutdown[:500],
                        )
                    )

                # Timer scripts
                for timer in script_config.get("timerScripts", []):
                    if isinstance(timer, dict):
                        events.append(
                            GatewayEventScript(
                                project=project_name,
                                script_type="timer",
                                name=timer.get("name", ""),
                                script=timer.get("script", "")[:500],
                                delay=timer.get("delay"),
                            )
                        )

                # Message handlers
                for handler in script_config.get("messageHandlers", []):
                    if isinstance(handler, dict):
                        events.append(
                            GatewayEventScript(
                                project=project_name,
                                script_type="message_handler",
                                name=handler.get(
                                    "messageType", handler.get("name", "")
                                ),
                                script=handler.get("script", "")[:500],
                            )
                        )

        return events

    def get_all_tag_references(self, backup: IgnitionBackup) -> Set[str]:
        """Extract all tag references from bindings."""
        refs = set()

        for window in backup.windows:
            refs.update(self._extract_component_refs(window.components))

        return refs

    def _extract_component_refs(self, components: List[UIComponent]) -> Set[str]:
        """Recursively extract tag references from components."""
        refs = set()

        for comp in components:
            for binding in comp.bindings:
                if binding.binding_type == "tag":
                    refs.add(binding.target)
            refs.update(self._extract_component_refs(comp.children))

        return refs

    def get_udt_hierarchy(self, backup: IgnitionBackup) -> Dict[str, List[str]]:
        """Build UDT inheritance hierarchy."""
        hierarchy = {}

        for udt in backup.udt_definitions:
            if udt.parent_name:
                if udt.parent_name not in hierarchy:
                    hierarchy[udt.parent_name] = []
                hierarchy[udt.parent_name].append(udt.name)

        return hierarchy


def main():
    """Test the parser."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python ignition_parser.py <backup.json>")
        sys.exit(1)

    file_path = sys.argv[1]
    parser = IgnitionParser()
    backup = parser.parse_file(file_path)

    print(f"\n=== Ignition Backup: {backup.file_path} ===")
    print(f"Version: {backup.version}")

    # Show projects with inheritance
    print(f"\n--- Projects ({len(backup.projects)}) ---")
    for proj_name, proj in backup.projects.items():
        parent_info = f" (inherits from {proj.parent})" if proj.parent else ""
        inheritable = " [inheritable]" if proj.inheritable else ""
        print(f"  {proj_name}{parent_info}{inheritable}")

    print(f"\n--- UDT Definitions ({len(backup.udt_definitions)}) ---")
    for udt in backup.udt_definitions[:5]:
        parent = f" (extends {udt.parent_name})" if udt.parent_name else ""
        print(f"  {udt.name}{parent}")
        for param_name in list(udt.parameters.keys())[:3]:
            param = udt.parameters[param_name]
            print(f"    param: {param.name}: {param.data_type}")
        for member in udt.members[:3]:
            print(f"    member: {member.name}: {member.data_type} [{member.tag_type}]")

    print(f"\n--- UDT Instances ({len(backup.udt_instances)}) ---")
    for inst in backup.udt_instances[:5]:
        print(f"  {inst.name}: {inst.type_id}")

    print(f"\n--- Tags ({len(backup.tags)}) ---")
    for tag in backup.tags[:5]:
        print(f"  {tag.name}: {tag.tag_type}")

    print(f"\n--- Windows ({len(backup.windows)}) ---")
    for window in backup.windows[:5]:
        binding_count = sum(
            len(c.bindings) + sum(len(cc.bindings) for cc in c.children)
            for c in window.components
        )
        proj_info = f" [{window.project}]" if window.project else ""
        print(f"  {window.name} ({window.path}){proj_info} - {binding_count} bindings")

    print(f"\n--- Named Queries ({len(backup.named_queries)}) ---")
    for query in backup.named_queries[:5]:
        proj_info = f" [{query.project}]" if query.project else ""
        print(f"  {query.name} ({query.folder_path}){proj_info}")

    print(f"\n--- Scripts ({len(backup.scripts)}) ---")
    for script in backup.scripts[:5]:
        print(f"  {script.project}/{script.path}")

    print(f"\n--- Gateway Events ({len(backup.gateway_events)}) ---")
    for event in backup.gateway_events[:10]:
        name_info = f": {event.name}" if event.name else ""
        delay_info = f" ({event.delay}ms)" if event.delay else ""
        print(f"  {event.project}/{event.script_type}{name_info}{delay_info}")

    # Show tag references from UI
    tag_refs = parser.get_all_tag_references(backup)
    print(f"\n--- Tag References in UI ({len(tag_refs)}) ---")
    for ref in list(tag_refs)[:5]:
        print(f"  {ref}")

    # Show UDT hierarchy
    hierarchy = parser.get_udt_hierarchy(backup)
    if hierarchy:
        print(f"\n--- UDT Inheritance ---")
        for parent, children in hierarchy.items():
            print(f"  {parent} -> {', '.join(children)}")


if __name__ == "__main__":
    main()
