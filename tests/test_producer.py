import json

from aircraft_pm.producer import serialize_record


def test_serialize_record_json_bytes():
    payload = {"engine_id": 1, "cycle": 2}
    raw = serialize_record(payload)
    assert isinstance(raw, bytes)
    assert json.loads(raw.decode("utf-8")) == payload
