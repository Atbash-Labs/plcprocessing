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
    children: List['UIComponent'] = field(default_factory=list)
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


@dataclass
class NamedQuery:
    """Named query definition."""
    name: str
    id: str
    folder_path: Optional[str] = None
    # Note: actual SQL not typically in backup, just references


@dataclass
class IgnitionBackup:
    """Parsed Ignition backup structure."""
    file_path: str
    version: str

    # Core elements
    udt_definitions: List[UDTDefinition] = field(default_factory=list)
    udt_instances: List[UDTInstance] = field(default_factory=list)
    tags: List[Tag] = field(default_factory=list)
    windows: List[Window] = field(default_factory=list)
    named_queries: List[NamedQuery] = field(default_factory=list)

    # Organizational
    folders: Dict[str, Any] = field(default_factory=dict)
    projects: Dict[str, Any] = field(default_factory=dict)

    # Connections
    db_connections: List[Dict] = field(default_factory=list)
    servers: List[Dict] = field(default_factory=list)


class IgnitionParser:
    """Parser for Ignition backup JSON files."""

    def parse_file(self, file_path: str) -> IgnitionBackup:
        """Parse an Ignition backup JSON file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        backup = IgnitionBackup(
            file_path=file_path,
            version=data.get('version', 'unknown')
        )

        # Parse each section
        backup.udt_definitions = self._parse_udt_definitions(data.get('udt_definitions', []))
        backup.udt_instances = self._parse_udt_instances(data.get('udt_instances', []))
        backup.tags = self._parse_tags(data.get('tags', []))
        backup.windows = self._parse_windows(data.get('windows', []))
        backup.named_queries = self._parse_named_queries(data.get('named_queries', []))
        backup.folders = data.get('folders', {})
        backup.projects = data.get('projects', {})
        backup.db_connections = data.get('db_connections', [])
        backup.servers = data.get('servers', [])

        return backup

    def _parse_udt_definitions(self, udt_list: List[Dict]) -> List[UDTDefinition]:
        """Parse UDT definitions."""
        definitions = []

        for udt_data in udt_list:
            # Parse parameters
            params = {}
            for param_name, param_data in udt_data.get('parameters', {}).items():
                if isinstance(param_data, dict):
                    params[param_name] = UDTParameter(
                        name=param_name,
                        data_type=param_data.get('dataType', 'Unknown'),
                        value=param_data.get('value')
                    )

            # Parse members
            members = []
            for member_data in udt_data.get('members', []):
                members.append(UDTMember(
                    name=member_data.get('name', ''),
                    data_type=member_data.get('data_type', 'Unknown'),
                    tag_type=member_data.get('type', 'memory'),
                    opc_item_path=member_data.get('opc_item_path'),
                    expression=member_data.get('expression'),
                    server_name=member_data.get('server_name')
                ))

            definitions.append(UDTDefinition(
                name=udt_data.get('name', ''),
                id=udt_data.get('id', ''),
                parameters=params,
                members=members,
                parent_name=udt_data.get('parent_name'),
                folder_name=udt_data.get('folder_name')
            ))

        return definitions

    def _parse_udt_instances(self, instance_list: List[Dict]) -> List[UDTInstance]:
        """Parse UDT instances."""
        instances = []

        for inst_data in instance_list:
            # Parse parameter values
            params = {}
            for param_name, param_data in inst_data.get('parameters', {}).items():
                if isinstance(param_data, dict):
                    params[param_name] = param_data.get('value')
                else:
                    params[param_name] = param_data

            instances.append(UDTInstance(
                name=inst_data.get('name', ''),
                type_id=inst_data.get('typeId', ''),
                id=inst_data.get('id', ''),
                parameters=params,
                folder_name=inst_data.get('folder_name')
            ))

        return instances

    def _parse_tags(self, tag_list: List[Dict]) -> List[Tag]:
        """Parse standalone tags."""
        tags = []

        for tag_data in tag_list:
            tags.append(Tag(
                name=tag_data.get('name', ''),
                tag_type=tag_data.get('type', 'memory'),
                data_type=tag_data.get('data_type'),
                folder_name=tag_data.get('folder_name'),
                query=tag_data.get('query'),
                datasource=tag_data.get('datasource'),
                opc_item_path=tag_data.get('opc_item_path'),
                expression=tag_data.get('expression'),
                initial_value=tag_data.get('initial_value')
            ))

        return tags

    def _parse_windows(self, window_list: List[Dict]) -> List[Window]:
        """Parse windows/views."""
        windows = []

        for project_windows in window_list:
            # window_list is [{project_name: [windows]}]
            for project_name, proj_windows in project_windows.items():
                for win_data in proj_windows:
                    # Parse root container recursively
                    components = []
                    root = win_data.get('root_container')
                    if root:
                        components = [self._parse_component(root)]

                    windows.append(Window(
                        name=win_data.get('name', ''),
                        path=win_data.get('path', ''),
                        window_type=win_data.get('window_type', 'perspective'),
                        title=win_data.get('title'),
                        components=components,
                        params=win_data.get('params', {})
                    ))

        return windows

    def _parse_component(self, comp_data: Dict) -> UIComponent:
        """Recursively parse a UI component."""
        # Extract bindings
        bindings = []
        for prop_path, binding_data in comp_data.get('bindings', {}).items():
            if isinstance(binding_data, dict):
                binding_type = binding_data.get('type', 'unknown')

                if binding_type == 'tag':
                    target = binding_data.get('tag', '')
                elif binding_type == 'query':
                    config = binding_data.get('config', {})
                    target = config.get('queryPath', '')
                elif binding_type == 'expr':
                    target = binding_data.get('expression', '')
                else:
                    target = str(binding_data)

                bindings.append(Binding(
                    property_path=prop_path,
                    binding_type=binding_type,
                    target=target,
                    bidirectional=binding_data.get('bidirectional', False)
                ))

        # Parse children recursively
        children = []
        for child_data in comp_data.get('children', []):
            children.append(self._parse_component(child_data))

        return UIComponent(
            name=comp_data.get('meta', {}).get('name', ''),
            component_type=comp_data.get('type', 'unknown'),
            bindings=bindings,
            children=children,
            props=comp_data.get('props', {})
        )

    def _parse_named_queries(self, query_list: List[Dict]) -> List[NamedQuery]:
        """Parse named query references."""
        queries = []

        for project_queries in query_list:
            for project_name, proj_queries in project_queries.items():
                for query_data in proj_queries:
                    queries.append(NamedQuery(
                        name=query_data.get('name', ''),
                        id=query_data.get('id', ''),
                        folder_path=query_data.get('folder_path')
                    ))

        return queries

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
                if binding.binding_type == 'tag':
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
        print(f"  {window.name} ({window.path}) - {binding_count} bindings")

    print(f"\n--- Named Queries ({len(backup.named_queries)}) ---")
    for query in backup.named_queries[:5]:
        print(f"  {query.name} ({query.folder_path})")

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
