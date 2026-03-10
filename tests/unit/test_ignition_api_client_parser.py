from ignition_api_client import IgnitionApiClient


def test_parse_tags_response_infers_timestamp_when_missing():
    paths = ["[default]Feed_Storage/Tank1_Level"]
    fallback_ts = "2026-03-02T00:00:00+00:00"
    payload = {
        "tags": [
            {
                "tagPath": paths[0],
                "value": 42.5,
                "quality": "Good",
            }
        ]
    }

    rows = IgnitionApiClient._parse_tags_response(paths, payload, fallback_timestamp=fallback_ts)
    assert len(rows) == 1
    assert rows[0].path == paths[0]
    assert rows[0].timestamp == fallback_ts
    assert rows[0].config is not None
    assert rows[0].config.get("timestamp_inferred") is True


def test_parse_tags_response_supports_alt_keys():
    paths = ["[default]Feed_Storage/Tank1_Pressure"]
    payload = {
        "items": [
            {
                "path": paths[0],
                "v": 101.3,
                "q": "Good",
                "ts": "1710000000000",
                "data_type": "Float8",
            }
        ]
    }

    rows = IgnitionApiClient._parse_tags_response(paths, payload)
    assert len(rows) == 1
    assert rows[0].path == paths[0]
    assert rows[0].value == 101.3
    assert rows[0].quality == "Good"
    assert rows[0].timestamp == "1710000000000"
    assert rows[0].data_type == "Float8"
