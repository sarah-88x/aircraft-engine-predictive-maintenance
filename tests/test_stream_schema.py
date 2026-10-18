from aircraft_pm.jobs.streaming_job import SCHEMA


def test_stream_schema_contains_required_fields():
    fields = {f.name for f in SCHEMA.fields}
    assert {"engine_id", "cycle", "temperature", "vibration"}.issubset(fields)
