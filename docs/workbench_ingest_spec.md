# Workbench Backup Ingest Specification

This document describes the format differences between the current `baselinejson.json` ingest format and the Axilon Workbench `project.json` backup format, and what changes are needed to support the latter as a new ingest source.

## Overview

The Workbench backup is a self-contained export that includes:
- `project.json` - Main file containing windows, named queries, scripts metadata, and AI conversation history
- `scripts/{ProjectName}/{ScriptPath}/code.py` - Script source files
- `named-queries/{ProjectName}/{QueryPath}/query.sql` - Query files (redundant - SQL is also inline in project.json)
- `perspective/{ProjectName}/views/{ViewName}/view.json` - View files (redundant - views are also in project.json)
- `tag-backups/*.json` - Tag exports (separate from project resources)

## Format Differences

### 1. Windows/Views

#### Current Format (`baselinejson.json`)
```json
{
  "windows": [
    {
      "ProjectName": [
        {
          "window_type": "perspective",
          "name": "ViewName",
          "title": "ViewTitle",
          "path": "Folder/ViewName",
          "root_container": {
            "type": "ia.container.flex",
            "meta": { "name": "root" },
            "children": [
              {
                "type": "ia.display.table",
                "meta": { "name": "Table" },
                "bindings": {
                  "props.data": {
                    "type": "tag",
                    "tag": "[default]TagName",
                    "bidirectional": false
                  }
                }
              }
            ]
          }
        }
      ]
    }
  ]
}
```

#### Workbench Format (`project.json`)
```json
{
  "windows": [
    {
      "__typeName": "Window",
      "id": "ProjectName/Folder/ViewName",
      "projectName": "ProjectName",
      "windowType": "perspective",
      "title": "ViewName",
      "path": "Folder/ViewName",
      "rootContainer": {
        "type": "ia.container.flex",
        "meta": { "name": "root" },
        "propConfig": {},
        "children": [
          {
            "type": "ia.display.table",
            "meta": { "name": "Table" },
            "propConfig": {
              "props.data": {
                "binding": {
                  "type": "tag",
                  "config": {
                    "tagPath": "[default]TagName",
                    "bidirectional": false
                  }
                }
              }
            }
          }
        ]
      }
    }
  ]
}
```

#### Key Differences - Windows

| Aspect | Current | Workbench |
|--------|---------|-----------|
| List structure | Grouped: `[{project: [windows]}]` | Flat: `[{projectName, ...window}]` |
| Root container key | `root_container` | `rootContainer` |
| Window type key | `window_type` | `windowType` |
| Bindings location | `component.bindings` | `component.propConfig.{prop}.binding` |
| Tag path field | `binding.tag` | `binding.config.tagPath` |
| Binding config | Flat object | Nested under `config` key |

---

### 2. Named Queries

#### Current Format (`baselinejson.json`)
```json
{
  "named_queries": [
    {
      "ProjectName": [
        {
          "name": "GetDataWeekStarts",
          "folder_path": "Charts",
          "id": "Charts/GetDataWeekStarts"
        }
      ]
    }
  ]
}
```
*Note: SQL is read from external file `named_queries_library/{project}/{folder}/{name}/query.sql`*

#### Workbench Format (`project.json`)
```json
{
  "namedQueries": [
    {
      "__typeName": "NamedQuery",
      "id": "ProjectName-Charts/GetDataWeekStarts",
      "projectName": "ProjectName",
      "queryName": "GetDataWeekStarts",
      "folderPath": "Charts",
      "query": "SELECT ... FROM ... WHERE ...",
      "dbName": "DatabaseName",
      "queryType": "Query",
      "syntax": "MySQL",
      "parameters": [
        {
          "type": "Parameter",
          "identifier": "paramName",
          "datatype": "String"
        }
      ]
    }
  ]
}
```

#### Key Differences - Named Queries

| Aspect | Current | Workbench |
|--------|---------|-----------|
| Key name | `named_queries` | `namedQueries` |
| List structure | Grouped: `[{project: [queries]}]` | Flat: `[{projectName, ...query}]` |
| Query name field | `name` | `queryName` |
| Folder path field | `folder_path` | `folderPath` |
| SQL content | External file | Inline in `query` field ✓ |
| Database name | Not included | `dbName` field ✓ |
| Parameters | Not included | `parameters` array ✓ |
| Query type | Not included | `queryType` field |
| SQL syntax | Not included | `syntax` field |

**Advantage**: Workbench format has SQL inline and includes more metadata (database, parameters).

---

### 3. Scripts

#### Current Format (`baselinejson.json`)
```json
{
  "scripts": [
    {
      "ProjectName": [
        {
          "ScriptPath": {
            "scope": "A"
          }
        }
      ]
    }
  ]
}
```
*Note: Script code is read from `script_library/{project}/{path}/code.py`*

#### Workbench Format (`project.json`)
```json
{
  "scripts": [
    {
      "__typeName": "Script",
      "id": "ProjectName\\script_path\\code.py",
      "path": ["script_path"],
      "fileName": "code.py",
      "projectName": "ProjectName",
      "scope": "A",
      "hintScope": 7,
      "contentHash": "abc123..."
    }
  ]
}
```
*Note: Script code must still be read from `scripts/{project}/{path}/code.py`*

#### Key Differences - Scripts

| Aspect | Current | Workbench |
|--------|---------|-----------|
| List structure | Grouped: `[{project: [{path: metadata}]}]` | Flat: `[{projectName, path, ...}]` |
| Path format | String key in object | Array in `path` field |
| Script code | External: `script_library/...` | External: `scripts/...` |
| Content hash | Not included | `contentHash` field |

**Note**: Neither format includes script code inline - both require reading from the file system.

---

### 4. Tags

#### Current Format (`baselinejson.json`)
Tags are parsed from `udt_definitions`, `udt_instances`, and `tags` arrays in the JSON.

#### Workbench Format
Tags are **not included** in `project.json`. They exist in:
- `tag-backups/{timestamp}/{provider}.json` - Timestamped tag snapshots by provider

**Tag backup structure:**
```json
{
  "tagType": "Provider",
  "name": "",
  "tags": [
    {
      "name": "Writeable",
      "tagType": "Folder",
      "tags": [
        {
          "name": "WriteableInteger1",
          "tagType": "AtomicTag",
          "valueSource": "opc",
          "dataType": "Int4",
          "opcServer": "Ignition OPC UA Server",
          "opcItemPath": "ns=1;s=[Sample_Device]_Meta:Writeable/WriteableInteger1",
          "alarms": [
            {
              "name": "Hi",
              "mode": "AboveValue",
              "setpointA": 90.0,
              "priority": "Critical",
              "displayPath": "Level Hi Alarm",
              "notes": "Level is too high"
            }
          ]
        },
        {
          "name": "MemoryTag",
          "tagType": "AtomicTag",
          "valueSource": "memory"
        }
      ]
    },
    {
      "name": "_types_",
      "tagType": "Folder"
    }
  ]
}
```

**Tag Types in Workbench Backups:**
- `AtomicTag` - Standard tag (OPC, memory, expression, etc.)
- `Folder` - Tag folder (recursive `tags` array)
- `UdtInstance` - UDT instance (would have `typeId` field)
- `UdtType` - UDT definition (in `_types_` folder)

**Key Fields:**
| Field | Description |
|-------|-------------|
| `name` | Tag name |
| `tagType` | AtomicTag, Folder, UdtInstance, UdtType |
| `valueSource` | opc, memory, expression, derived, query |
| `dataType` | Int4, Float8, Boolean, String, etc. |
| `opcServer` | OPC server name |
| `opcItemPath` | OPC item path |
| `alarms` | Array of alarm configurations |
| `tags` | Child tags (for folders) |

**Note:** Tag backups are timestamped snapshots. Use the most recent timestamp folder to get current tags. Providers (default, System, Sample_Tags, etc.) are separate files.

---

### 5. Gateway Events

#### Current Format (`baselinejson.json`)
```json
{
  "gateway_events": [
    {
      "ProjectName": {
        "scriptConfig": {
          "startupScript": "...",
          "shutdownScript": "...",
          "timerScripts": [
            { "name": "TimerName", "script": "...", "delay": 5000 }
          ],
          "messageHandlers": [
            { "messageType": "HandlerName", "script": "..." }
          ]
        }
      }
    }
  ]
}
```

#### Workbench Format
**Not included** in `project.json`. Gateway event scripts are project-level resources stored on the gateway, not exported in workbench backups.

---

### 6. Projects

#### Current Format (`baselinejson.json`)
```json
{
  "projects": {
    "ProjectName": {
      "title": "",
      "description": "",
      "enabled": true,
      "inheritable": false,
      "parent": ""
    }
  }
}
```

#### Workbench Format (`project.json`)
Projects are inferred from the `projectName` field in each resource. The workbench `project.json` root structure is:
```json
{
  "__typeName": "WorkbenchState",
  "root": {
    "__typeName": "ProjectRoot",
    "folders": [...],
    "projects": [...]
  },
  "windows": [...],
  "namedQueries": [...],
  "scripts": [...],
  "version": "1.0.0",
  "metadata": {...},
  "conversationMessages": [...]
}
```

---

## Implementation Approach

### Option A: Separate Parser Class (Recommended)

Create `WorkbenchParser` class that:
1. Detects workbench format via `__typeName: "WorkbenchState"`
2. Parses the different structures
3. Outputs the same `IgnitionBackup` dataclass

```python
class WorkbenchParser:
    def parse_file(self, file_path: str) -> IgnitionBackup:
        # Load project.json
        # Transform to IgnitionBackup format
        pass
    
    def _parse_windows(self, windows: List[Dict]) -> List[Window]:
        # Handle flat list with projectName
        # Transform propConfig bindings to bindings format
        pass
    
    def _parse_named_queries(self, queries: List[Dict]) -> List[NamedQuery]:
        # Handle flat list, inline SQL
        pass
    
    def _parse_scripts(self, scripts: List[Dict], base_dir: Path) -> List[Script]:
        # Handle flat list, read code from scripts/ directory
        pass
```

### Option B: Extend Existing Parser

Add format detection and conditional logic to `IgnitionParser`:
```python
def parse_file(self, file_path: str, ...) -> IgnitionBackup:
    with open(file_path) as f:
        data = json.load(f)
    
    if data.get("__typeName") == "WorkbenchState":
        return self._parse_workbench_format(data, file_path)
    else:
        return self._parse_baseline_format(data, file_path)
```

---

## Binding Transformation

The most complex change is extracting bindings from the workbench format:

### Current Parser Logic
```python
for prop_path, binding_data in comp_data.get("bindings", {}).items():
    binding_type = binding_data.get("type")
    if binding_type == "tag":
        target = binding_data.get("tag", "")
```

### Workbench Format Requires
```python
for prop_path, prop_config in comp_data.get("propConfig", {}).items():
    binding = prop_config.get("binding")
    if not binding:
        continue
    binding_type = binding.get("type")
    config = binding.get("config", {})
    if binding_type == "tag":
        target = config.get("tagPath", "")
    elif binding_type == "property":
        target = config.get("path", "")
    # Transform scripts are in binding.get("transforms", [])
```

---

## File System Layout

### Current Expected Layout
```
/path/to/backup/
├── backup.json                    # Main file
├── script_library/
│   └── {project}/
│       └── {script_path}/
│           └── code.py
└── named_queries_library/
    └── {project}/
        └── {folder_path}/
            └── {query_name}/
                └── query.sql
```

### Workbench Layout
```
/workbench-backup/
├── project.json                   # Main file (includes inline SQL)
├── scripts/
│   └── {project}/
│       └── {script_path}/
│           ├── code.py
│           └── resource.json
├── named-queries/                 # Redundant if using inline SQL
│   └── {project}/
│       └── {folder_path}/
│           └── {query_name}/
│               ├── query.sql
│               └── resource.json
├── perspective/
│   └── {project}/
│       └── views/
│           └── {view_name}/
│               ├── view.json
│               ├── resource.json
│               └── thumbnail.png
└── tag-backups/
    └── *.json
```

---

## Summary of Changes

| Component | Effort | Notes |
|-----------|--------|-------|
| Format detection | Low | Check for `__typeName` |
| Window list restructuring | Medium | Flat → grouped, camelCase keys |
| Binding extraction | Medium | `propConfig.*.binding` → `bindings` |
| Named query parsing | Low | Simpler - SQL is inline |
| Script parsing | Low | Different directory path (`scripts/` vs `script_library/`) |
| Tag parsing | Medium | Separate `tag-backups/` files, hierarchical structure |
| Gateway events | N/A | Not included in workbench backups |
| Project discovery | Low | Infer from resource `projectName` fields |

**Total estimated effort**: 1-2 days for a clean implementation.

---

## Quick Reference: Field Mappings

### Windows
| baselinejson | workbench |
|--------------|-----------|
| `root_container` | `rootContainer` |
| `window_type` | `windowType` |
| `component.bindings.{prop}.tag` | `component.propConfig.{prop}.binding.config.tagPath` |
| `component.bindings.{prop}.type` | `component.propConfig.{prop}.binding.type` |

### Named Queries
| baselinejson | workbench |
|--------------|-----------|
| `named_queries` | `namedQueries` |
| `name` | `queryName` |
| `folder_path` | `folderPath` |
| (external file) | `query` (inline) |
| (not present) | `dbName` |
| (not present) | `parameters` |

### Scripts
| baselinejson | workbench |
|--------------|-----------|
| `{path: {scope}}` | `{path: [], scope, projectName}` |
| `script_library/` | `scripts/` |

### Tags
| baselinejson | workbench |
|--------------|-----------|
| `tags`, `udt_instances`, `udt_definitions` | `tag-backups/{timestamp}/{provider}.json` |
| `type` (opc, memory, etc.) | `valueSource` |
| `opc_item_path` | `opcItemPath` |
| `data_type` | `dataType` |
| `server_name` | `opcServer` |
