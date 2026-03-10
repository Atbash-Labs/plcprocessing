from anomaly_monitor import (
    _last_segment_from_tag_path,
    _looks_like_live_tag_path,
    derive_subsystems_for_tag,
    infer_tag_group,
)


def test_infer_tag_group_prefers_folder_name():
    group = infer_tag_group("[default]Area1/Pump101/Speed", folder_name="LineA/Area1")
    assert group == "LineA"


def test_infer_tag_group_from_tag_path():
    group = infer_tag_group("[default]Boiler/Feedwater/Flow")
    assert group == "Boiler"


def test_infer_tag_group_none_for_flat_tag():
    assert infer_tag_group("[default]SingleTag") is None


def test_derive_subsystems_auto_with_priority():
    subsystems, primary = derive_subsystems_for_tag(
        tag_meta={
            "path": "[default]Line1/PumpA/Pressure",
            "folder_name": "Line1/PumpA",
            "views": ["Overview/Main"],
            "equipment": ["PumpA"],
        },
        subsystem_mode="auto",
        priority=["equipment", "view", "group"],
    )
    subsystem_ids = {item["id"] for item in subsystems}
    assert "equipment:pumpa" in subsystem_ids
    assert "view:overview/main" in subsystem_ids
    assert "group:line1" in subsystem_ids
    assert primary["type"] == "equipment"
    assert primary["name"] == "PumpA"


def test_derive_subsystems_global_mode():
    subsystems, primary = derive_subsystems_for_tag(
        tag_meta={
            "path": "[default]Line1/PumpA/Pressure",
            "views": ["Overview/Main"],
            "equipment": ["PumpA"],
        },
        subsystem_mode="global",
    )
    assert subsystems == [{"type": "global", "name": "all", "id": "global:all"}]
    assert primary == {"type": "global", "name": "all", "id": "global:all"}


def test_derive_subsystems_falls_back_to_global_when_no_ontology_links():
    subsystems, primary = derive_subsystems_for_tag(
        tag_meta={"path": "[default]TagOnly"},
        subsystem_mode="auto",
    )
    assert len(subsystems) == 1
    assert primary["id"] == "global:all"


def test_tag_path_helpers_identify_live_paths():
    assert _looks_like_live_tag_path("[default]Line/Pump/Speed")
    assert _looks_like_live_tag_path("Line/Pump/Speed")
    assert not _looks_like_live_tag_path("SimpleTagNameOnly")
    assert not _looks_like_live_tag_path("{../props.value}")


def test_last_segment_from_tag_path():
    assert _last_segment_from_tag_path("[default]Line/Pump/Speed") == "Speed"
    assert _last_segment_from_tag_path("Line/Pump/Speed") == "Speed"
    assert _last_segment_from_tag_path("Speed") == "Speed"
