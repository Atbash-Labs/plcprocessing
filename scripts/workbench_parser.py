#!/usr/bin/env python3
"""
Parser for Axilon Workbench backup files (project.json format).
Extracts views, named queries, scripts, and tags into the same IgnitionBackup
dataclass used by ignition_parser.py for downstream compatibility.
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Optional, Any

from ignition_parser import (
    IgnitionBackup,
    Window,
    UIComponent,
    Binding,
    NamedQuery,
    Script,
    Tag,
    UDTDefinition,
    UDTInstance,
    UDTMember,
    UDTParameter,
    Project,
    GatewayEventScript,
)


class WorkbenchParser:
    """Parser for Axilon Workbench backup files."""

    def __init__(self):
        """Initialize the workbench parser."""
        self.base_dir: Optional[Path] = None

    @staticmethod
    def is_workbench_format(data: Dict) -> bool:
        """Check if the JSON data is in workbench format.

        Args:
            data: Parsed JSON data

        Returns:
            True if this is a workbench backup format
        """
        return data.get("__typeName") == "WorkbenchState"

    def parse_file(self, file_path: str) -> IgnitionBackup:
        """Parse a workbench project.json file.

        Args:
            file_path: Path to the project.json file

        Returns:
            IgnitionBackup with parsed data
        """
        json_path = Path(file_path)
        self.base_dir = json_path.parent

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not self.is_workbench_format(data):
            raise ValueError(
                f"Not a workbench format file. Expected __typeName='WorkbenchState', "
                f"got '{data.get('__typeName', 'none')}'"
            )

        backup = IgnitionBackup(
            file_path=file_path,
            version=data.get("version", "unknown"),
        )

        # Resources are nested under 'root' in workbench format
        root = data.get("root", {})

        # Parse projects from resource projectName fields
        backup.projects = self._discover_projects(root)

        # Parse windows/views
        backup.windows = self._parse_windows(root.get("windows", []))

        # Parse named queries (SQL is inline!)
        backup.named_queries = self._parse_named_queries(root.get("namedQueries", []))

        # Parse scripts (metadata in JSON, code from files)
        backup.scripts = self._parse_scripts(root.get("scripts", []))

        # Parse tags from root.tags (inline in project.json) - these are the main tags
        backup.tags = self._parse_inline_tags(root.get("tags", []))

        # Parse UDT definitions from root.udtDefinitions
        backup.udt_definitions = self._parse_udt_definitions(
            root.get("udtDefinitions", [])
        )

        # UDT instances would need to be parsed from tag providers - skip for now
        backup.udt_instances = []

        # Optionally merge with tag-backups if present (for additional system tags)
        backup_tags, _, _ = self._parse_tag_backups()
        # Only add tags from backups that aren't already in inline tags
        inline_tag_names = {t.name for t in backup.tags}
        for tag in backup_tags:
            if tag.name not in inline_tag_names:
                backup.tags.append(tag)

        # Gateway events are not included in workbench backups
        backup.gateway_events = []

        return backup

    def _discover_projects(self, data: Dict) -> Dict[str, Project]:
        """Discover projects from resource projectName fields.

        Args:
            data: Full workbench JSON data

        Returns:
            Dict mapping project names to Project objects
        """
        project_names = set()

        # Collect from windows
        for window in data.get("windows", []):
            if proj := window.get("projectName"):
                project_names.add(proj)

        # Collect from named queries
        for query in data.get("namedQueries", []):
            if proj := query.get("projectName"):
                project_names.add(proj)

        # Collect from scripts
        for script in data.get("scripts", []):
            if proj := script.get("projectName"):
                project_names.add(proj)

        # Create Project objects with minimal info (workbench doesn't include full project metadata)
        projects = {}
        for name in project_names:
            projects[name] = Project(
                name=name,
                title=name,
                description="",
                parent=None,
                enabled=True,
                inheritable=False,
            )

        return projects

    def _parse_windows(self, windows_list: List[Dict]) -> List[Window]:
        """Parse windows from workbench flat list format.

        Args:
            windows_list: List of window objects with projectName field

        Returns:
            List of Window objects
        """
        windows = []

        for win_data in windows_list:
            project_name = win_data.get("projectName", "")

            # Skip windows without names
            name = win_data.get("title") or win_data.get("name", "")
            if not name:
                continue

            # Parse root container recursively
            components = []
            root = win_data.get("rootContainer")
            if root:
                components = [self._parse_component(root)]

            windows.append(
                Window(
                    name=name,
                    path=win_data.get("path", ""),
                    window_type=win_data.get("windowType", "perspective"),
                    title=win_data.get("title"),
                    components=components,
                    params=win_data.get("params", {}),
                    project=project_name,
                )
            )

        return windows

    def _parse_component(self, comp_data: Dict) -> UIComponent:
        """Recursively parse a UI component from workbench format.

        Workbench stores bindings in propConfig.{property}.binding instead of
        a top-level bindings object.

        Args:
            comp_data: Component data dict

        Returns:
            UIComponent object
        """
        # Extract bindings from propConfig
        bindings = []
        prop_config = comp_data.get("propConfig", {})

        for prop_path, config in prop_config.items():
            binding_data = config.get("binding")
            if not binding_data:
                continue

            binding_type = binding_data.get("type", "unknown")
            binding_config = binding_data.get("config", {})

            # Extract target based on binding type
            if binding_type == "tag":
                target = binding_config.get("tagPath", "")
            elif binding_type == "property":
                target = binding_config.get("path", "")
            elif binding_type == "query":
                target = binding_config.get("queryPath", "")
            elif binding_type == "expr" or binding_type == "expression":
                target = binding_config.get("expression", "")
            else:
                target = str(binding_config)

            bindings.append(
                Binding(
                    property_path=prop_path,
                    binding_type=binding_type,
                    target=target,
                    bidirectional=binding_config.get("bidirectional", False),
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

    def _parse_named_queries(self, queries_list: List[Dict]) -> List[NamedQuery]:
        """Parse named queries from workbench format.

        Workbench format has SQL inline in the 'query' field.

        Args:
            queries_list: List of query objects with projectName field

        Returns:
            List of NamedQuery objects
        """
        queries = []

        for query_data in queries_list:
            project_name = query_data.get("projectName", "")
            query_name = query_data.get("queryName", "")

            if not query_name:
                continue

            # Build folder path from workbench format
            # Normalize backslashes to forward slashes for consistency
            folder_path = query_data.get("folderPath", "").replace("\\", "/")

            # Build ID similar to baseline format
            if folder_path:
                query_id = f"{folder_path}/{query_name}"
            else:
                query_id = query_name

            queries.append(
                NamedQuery(
                    name=query_name,
                    id=query_id,
                    folder_path=folder_path,
                    project=project_name,
                    query_text=query_data.get("query", ""),  # SQL is inline!
                )
            )

        return queries

    def _parse_scripts(self, scripts_list: List[Dict]) -> List[Script]:
        """Parse scripts from workbench format.

        Script metadata is in JSON, but code must be read from scripts/ directory.

        Args:
            scripts_list: List of script metadata objects

        Returns:
            List of Script objects
        """
        scripts = []

        for script_data in scripts_list:
            project_name = script_data.get("projectName", "")

            # Path is an array in workbench format
            path_parts = script_data.get("path", [])
            if not path_parts:
                continue

            script_path = "/".join(path_parts)
            script_name = path_parts[-1] if path_parts else ""

            # Read script code from file system
            script_text = self._read_script_file(project_name, script_path)

            scripts.append(
                Script(
                    name=script_name,
                    path=script_path,
                    project=project_name,
                    scope=script_data.get("scope", "A"),
                    script_text=script_text,
                )
            )

        return scripts

    def _read_script_file(self, project: str, script_path: str) -> str:
        """Read script code from scripts/ directory.

        Args:
            project: Project name
            script_path: Script path (e.g., "utility/tags")

        Returns:
            Script content or empty string if not found
        """
        if not self.base_dir:
            return ""

        # scripts/{project}/{path}/code.py
        code_file = self.base_dir / "scripts" / project / script_path / "code.py"

        if code_file.exists():
            try:
                return code_file.read_text(encoding="utf-8")
            except Exception:
                return ""
        return ""

    def _parse_inline_tags(self, tags_list: List[Dict]) -> List[Tag]:
        """Parse tags from root.tags in project.json.

        These are the main project tags (QueryTag, MemoryTag, OpcTag, etc.)

        Args:
            tags_list: List of tag objects from project.json

        Returns:
            List of Tag objects
        """
        tags = []

        for tag_data in tags_list:
            tag_name = tag_data.get("name", "")
            if not tag_name:
                continue

            # Map workbench type to tag_type
            wb_type = tag_data.get("type", "Memory").lower()
            if wb_type == "query":
                tag_type = "query"
            elif wb_type == "opc":
                tag_type = "opc"
            elif wb_type == "expression":
                tag_type = "expression"
            elif wb_type == "derived":
                tag_type = "derived"
            else:
                tag_type = "memory"

            tags.append(
                Tag(
                    name=tag_name,
                    tag_type=tag_type,
                    data_type=tag_data.get("dataType"),
                    folder_name=tag_data.get("folderName", ""),
                    query=tag_data.get("query"),
                    datasource=tag_data.get("datasource"),
                    opc_item_path=tag_data.get("opcItemPath"),
                    expression=tag_data.get("expression"),
                    initial_value=tag_data.get("value"),
                )
            )

        return tags

    def _parse_udt_definitions(self, udt_list: List[Dict]) -> List[UDTDefinition]:
        """Parse UDT definitions from root.udtDefinitions in project.json.

        Args:
            udt_list: List of UDT definition objects

        Returns:
            List of UDTDefinition objects
        """
        definitions = []

        for udt_data in udt_list:
            udt_name = udt_data.get("name", "")
            if not udt_name:
                continue

            # Parse parameters
            parameters = {}
            for param_name, param_data in udt_data.get("parameters", {}).items():
                if isinstance(param_data, dict):
                    parameters[param_name] = UDTParameter(
                        name=param_name,
                        data_type=param_data.get("dataType", "Unknown"),
                        value=param_data.get("value"),
                    )

            # Parse members
            members = []
            for member_data in udt_data.get("members", []):
                member_name = member_data.get("name", "")
                if not member_name:
                    continue

                # Map type to tag_type
                member_type = member_data.get("type", "memory").lower()

                # Handle serverName which can be a string or a binding object
                server_name = member_data.get("serverName")
                if isinstance(server_name, dict):
                    server_name = server_name.get("binding", "")

                members.append(
                    UDTMember(
                        name=member_name,
                        data_type=member_data.get("dataType", "Unknown"),
                        tag_type=member_type,
                        opc_item_path=member_data.get("opcItemPath"),
                        expression=member_data.get("expression"),
                        server_name=server_name,
                    )
                )

            definitions.append(
                UDTDefinition(
                    name=udt_name,
                    id=udt_data.get("id", udt_name),
                    parameters=parameters,
                    members=members,
                    parent_name=udt_data.get("parentName"),
                    folder_name=udt_data.get("folderName"),
                )
            )

        return definitions

    def _parse_tag_backups(self) -> tuple:
        """Parse tags from tag-backups directory.

        Uses the most recent timestamp folder.

        Returns:
            Tuple of (tags, udt_definitions, udt_instances)
        """
        tags = []
        udt_definitions = []
        udt_instances = []

        if not self.base_dir:
            return tags, udt_definitions, udt_instances

        tag_backups_dir = self.base_dir / "tag-backups"
        if not tag_backups_dir.exists():
            return tags, udt_definitions, udt_instances

        # Find the most recent timestamp folder
        timestamp_dirs = [
            d for d in tag_backups_dir.iterdir() if d.is_dir() and d.name.isdigit()
        ]

        if not timestamp_dirs:
            return tags, udt_definitions, udt_instances

        # Sort by timestamp (folder name) descending
        latest_dir = sorted(timestamp_dirs, key=lambda d: int(d.name), reverse=True)[0]

        # Parse each provider file
        for provider_file in latest_dir.glob("*.json"):
            provider_name = provider_file.stem  # e.g., "default", "Sample_Tags"

            try:
                with open(provider_file, "r", encoding="utf-8") as f:
                    provider_data = json.load(f)
            except Exception:
                continue

            # Parse tags recursively from provider
            self._parse_tags_recursive(
                provider_data.get("tags", []),
                provider_name,
                "",  # parent folder path
                tags,
                udt_definitions,
                udt_instances,
            )

        return tags, udt_definitions, udt_instances

    def _parse_tags_recursive(
        self,
        tag_list: List[Dict],
        provider: str,
        folder_path: str,
        tags: List[Tag],
        udt_definitions: List[UDTDefinition],
        udt_instances: List[UDTInstance],
    ):
        """Recursively parse tags from tag backup format.

        Args:
            tag_list: List of tag objects
            provider: Tag provider name
            folder_path: Current folder path
            tags: Output list for standalone tags
            udt_definitions: Output list for UDT definitions
            udt_instances: Output list for UDT instances
        """
        for tag_data in tag_list:
            tag_name = tag_data.get("name", "")
            tag_type = tag_data.get("tagType", "")

            if not tag_name:
                continue

            # Build full path
            full_path = f"{folder_path}/{tag_name}" if folder_path else tag_name

            if tag_type == "Folder":
                # Recurse into folder (but skip _types_ for now)
                if tag_name == "_types_":
                    # Parse UDT definitions from _types_ folder
                    self._parse_udt_types(
                        tag_data.get("tags", []),
                        udt_definitions,
                    )
                else:
                    self._parse_tags_recursive(
                        tag_data.get("tags", []),
                        provider,
                        full_path,
                        tags,
                        udt_definitions,
                        udt_instances,
                    )

            elif tag_type == "UdtInstance":
                # UDT instance
                udt_instances.append(
                    UDTInstance(
                        name=tag_name,
                        type_id=tag_data.get("typeId", ""),
                        id=tag_data.get("tagId", tag_name),
                        parameters=tag_data.get("parameters", {}),
                        folder_name=folder_path,
                    )
                )

            elif tag_type == "AtomicTag":
                # Standalone tag
                value_source = tag_data.get("valueSource", "memory")

                tags.append(
                    Tag(
                        name=tag_name,
                        tag_type=value_source,
                        data_type=tag_data.get("dataType"),
                        folder_name=folder_path,
                        opc_item_path=tag_data.get("opcItemPath"),
                        expression=tag_data.get("expression"),
                        query=tag_data.get("query"),
                        datasource=tag_data.get("datasource"),
                        initial_value=tag_data.get("value"),
                    )
                )

    def _parse_udt_types(
        self,
        types_list: List[Dict],
        udt_definitions: List[UDTDefinition],
        parent_folder: str = "",
    ):
        """Parse UDT definitions from _types_ folder.

        Args:
            types_list: List of type objects
            udt_definitions: Output list for UDT definitions
            parent_folder: Parent folder path for nested types
        """
        for type_data in types_list:
            type_name = type_data.get("name", "")
            tag_type = type_data.get("tagType", "")

            if not type_name:
                continue

            if tag_type == "Folder":
                # Nested folder in _types_
                folder_path = (
                    f"{parent_folder}/{type_name}" if parent_folder else type_name
                )
                self._parse_udt_types(
                    type_data.get("tags", []),
                    udt_definitions,
                    folder_path,
                )

            elif tag_type == "UdtType":
                # UDT definition
                parameters = {}
                for param_name, param_data in type_data.get("parameters", {}).items():
                    if isinstance(param_data, dict):
                        parameters[param_name] = UDTParameter(
                            name=param_name,
                            data_type=param_data.get("dataType", "Unknown"),
                            value=param_data.get("value"),
                        )

                members = []
                for member_data in type_data.get("tags", []):
                    member_type = member_data.get("tagType", "")
                    if member_type == "AtomicTag":
                        members.append(
                            UDTMember(
                                name=member_data.get("name", ""),
                                data_type=member_data.get("dataType", "Unknown"),
                                tag_type=member_data.get("valueSource", "memory"),
                                opc_item_path=member_data.get("opcItemPath"),
                                expression=member_data.get("expression"),
                                server_name=member_data.get("opcServer"),
                            )
                        )

                udt_definitions.append(
                    UDTDefinition(
                        name=type_name,
                        id=type_data.get("typeId", type_name),
                        parameters=parameters,
                        members=members,
                        parent_name=type_data.get("parentType"),
                        folder_name=parent_folder,
                    )
                )


def main():
    """Test the workbench parser."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python workbench_parser.py <project.json>")
        sys.exit(1)

    file_path = sys.argv[1]
    parser = WorkbenchParser()

    try:
        backup = parser.parse_file(file_path)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(f"\n=== Workbench Backup: {backup.file_path} ===")
    print(f"Version: {backup.version}")

    # Show projects
    print(f"\n--- Projects ({len(backup.projects)}) ---")
    for proj_name in backup.projects:
        print(f"  {proj_name}")

    print(f"\n--- Windows/Views ({len(backup.windows)}) ---")
    for window in backup.windows[:10]:
        binding_count = sum(
            len(c.bindings) + sum(len(cc.bindings) for cc in c.children)
            for c in window.components
        )
        proj_info = f" [{window.project}]" if window.project else ""
        print(f"  {window.name} ({window.path}){proj_info} - {binding_count} bindings")
    if len(backup.windows) > 10:
        print(f"  ... and {len(backup.windows) - 10} more")

    print(f"\n--- Named Queries ({len(backup.named_queries)}) ---")
    for query in backup.named_queries[:10]:
        proj_info = f" [{query.project}]" if query.project else ""
        has_sql = " (has SQL)" if query.query_text else ""
        print(f"  {query.name} ({query.folder_path}){proj_info}{has_sql}")
    if len(backup.named_queries) > 10:
        print(f"  ... and {len(backup.named_queries) - 10} more")

    print(f"\n--- Scripts ({len(backup.scripts)}) ---")
    for script in backup.scripts[:10]:
        has_code = " (has code)" if script.script_text else ""
        print(f"  {script.project}/{script.path}{has_code}")
    if len(backup.scripts) > 10:
        print(f"  ... and {len(backup.scripts) - 10} more")

    print(f"\n--- Tags ({len(backup.tags)}) ---")
    for tag in backup.tags[:10]:
        print(f"  {tag.name}: {tag.tag_type}")
    if len(backup.tags) > 10:
        print(f"  ... and {len(backup.tags) - 10} more")

    print(f"\n--- UDT Definitions ({len(backup.udt_definitions)}) ---")
    for udt in backup.udt_definitions[:5]:
        print(f"  {udt.name}")

    print(f"\n--- UDT Instances ({len(backup.udt_instances)}) ---")
    for inst in backup.udt_instances[:5]:
        print(f"  {inst.name}: {inst.type_id}")


if __name__ == "__main__":
    main()
