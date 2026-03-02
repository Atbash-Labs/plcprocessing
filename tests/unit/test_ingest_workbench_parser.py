import json
from pathlib import Path

from workbench_parser import WorkbenchParser


def test_parse_workbench_project_json_with_inline_resources(tmp_path):
    root = Path(tmp_path)

    # Script file expected by WorkbenchParser._read_script_file
    script_file = root / "scripts" / "PlantA" / "utility" / "tags" / "code.py"
    script_file.parent.mkdir(parents=True, exist_ok=True)
    script_file.write_text("def read_tag():\n    return 42\n", encoding="utf-8")

    data = {
        "__typeName": "WorkbenchState",
        "version": "1.2.3",
        "root": {
            "windows": [
                {
                    "projectName": "PlantA",
                    "title": "MainView",
                    "path": "main/view",
                    "windowType": "perspective",
                    "rootContainer": {
                        "meta": {"name": "Root"},
                        "type": "ia.container",
                        "propConfig": {
                            "props.value": {
                                "binding": {
                                    "type": "tag",
                                    "config": {
                                        "tagPath": "[default]Line/Speed",
                                        "bidirectional": True,
                                    },
                                }
                            }
                        },
                        "children": [],
                    },
                }
            ],
            "namedQueries": [
                {
                    "projectName": "PlantA",
                    "queryName": "GetBatches",
                    "folderPath": "Prod\\Ops",
                    "query": "SELECT * FROM batches",
                }
            ],
            "scripts": [
                {
                    "projectName": "PlantA",
                    "path": ["utility", "tags"],
                    "scope": "A",
                }
            ],
            "tags": [
                {
                    "name": "LineSpeed",
                    "type": "Opc",
                    "dataType": "Float8",
                    "opcItemPath": "[default]Line/Speed",
                },
                {
                    "name": "BatchCount",
                    "type": "Memory",
                    "dataType": "Int4",
                    "value": 7,
                },
            ],
            "udtDefinitions": [
                {
                    "name": "MotorUDT",
                    "id": "MotorUDT",
                    "parameters": {
                        "area": {"dataType": "String", "value": "A1"}
                    },
                    "members": [
                        {
                            "name": "Run",
                            "type": "opc",
                            "dataType": "Boolean",
                            "opcItemPath": "[default]Motor/Run",
                            "serverName": {"binding": "default"},
                        }
                    ],
                }
            ],
        },
    }

    project_json = root / "project.json"
    project_json.write_text(json.dumps(data), encoding="utf-8")

    parser = WorkbenchParser()
    backup = parser.parse_file(str(project_json))

    assert "PlantA" in backup.projects
    assert len(backup.windows) == 1
    assert backup.windows[0].name == "MainView"
    assert backup.windows[0].components[0].bindings[0].target == "[default]Line/Speed"

    assert len(backup.named_queries) == 1
    assert backup.named_queries[0].id == "Prod/Ops/GetBatches"
    assert "SELECT" in backup.named_queries[0].query_text

    assert len(backup.scripts) == 1
    assert "return 42" in backup.scripts[0].script_text

    tag_types = {t.name: t.tag_type for t in backup.tags}
    assert tag_types["LineSpeed"] == "opc"
    assert tag_types["BatchCount"] == "memory"

    assert len(backup.udt_definitions) == 1
    udt = backup.udt_definitions[0]
    assert "area" in udt.parameters
    assert udt.members[0].server_name == "default"

