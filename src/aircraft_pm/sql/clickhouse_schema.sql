CREATE DATABASE IF NOT EXISTS aircraft;

CREATE TABLE IF NOT EXISTS aircraft.telemetry (
    event_time DateTime,
    engine_id UInt32,
    unit_id UInt32,
    cycle UInt32,
    temperature Float64,
    pressure Float64,
    vibration Float64,
    source_dataset String,
    kafka_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY (engine_id, cycle, event_time);

CREATE TABLE IF NOT EXISTS aircraft.health_results (
    event_time DateTime,
    engine_id UInt32,
    cycle UInt32,
    health_score Float64,
    temperature Float64,
    vibration Float64,
    rul Float64
) ENGINE = MergeTree ORDER BY (engine_id, cycle, event_time);

CREATE TABLE IF NOT EXISTS aircraft.alerts (
    event_time DateTime,
    engine_id UInt32,
    cycle UInt32,
    alert_type String,
    temperature Float64,
    vibration Float64
) ENGINE = MergeTree ORDER BY (event_time, engine_id);

CREATE TABLE IF NOT EXISTS aircraft.predictions (
    event_time DateTime DEFAULT now(),
    engine_id UInt32,
    cycle UInt32,
    prediction Float64,
    model_name String
) ENGINE = MergeTree ORDER BY (event_time, engine_id);

CREATE TABLE IF NOT EXISTS aircraft.pipeline_metrics (
    event_time DateTime DEFAULT now(),
    stage String,
    rows UInt64,
    engines UInt64,
    avg_rul Float64
) ENGINE = MergeTree ORDER BY event_time;
