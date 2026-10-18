from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass

import pandas as pd
from kafka import KafkaProducer


@dataclass
class ReplayConfig:
    input_parquet: str
    bootstrap_servers: str = "kafka:9092"
    topic: str = "engine-telemetry"
    key_field: str = "engine_id"
    batch_size: int = 100
    rate_per_sec: float = 10.0
    loop: bool = False
    event_delay_sec: float = 0.0


def serialize_record(record: dict) -> bytes:
    return json.dumps(record, ensure_ascii=False).encode("utf-8")


def replay(config: ReplayConfig) -> int:
    producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
    sent = 0
    sleep_per_batch = 1.0 / max(config.rate_per_sec, 0.1)

    while True:
        frame = pd.read_parquet(config.input_parquet)
        sort_cols = [
            c for c in ("engine_id", "unit_id", "cycle", "event_time") if c in frame.columns
        ]
        if sort_cols:
            frame = frame.sort_values(sort_cols)

        records = frame.to_dict(orient="records")
        for i in range(0, len(records), config.batch_size):
            batch = records[i : i + config.batch_size]
            for rec in batch:
                key_value = str(rec.get(config.key_field, "unknown")).encode("utf-8")
                producer.send(config.topic, key=key_value, value=serialize_record(rec))
                sent += 1
            producer.flush()
            if config.event_delay_sec > 0:
                time.sleep(config.event_delay_sec)
            time.sleep(sleep_per_batch)

        if not config.loop:
            break
    producer.flush()
    return sent


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Replay canonical parquet records to Kafka")
    p.add_argument("--input-parquet", required=True)
    p.add_argument("--bootstrap-servers", default="kafka:9092")
    p.add_argument("--topic", default="engine-telemetry")
    p.add_argument("--key-field", default="engine_id")
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--rate-per-sec", type=float, default=10.0)
    p.add_argument("--event-delay-sec", type=float, default=0.0)
    p.add_argument("--loop", action="store_true")
    return p


def main() -> None:
    args = build_parser().parse_args()
    cfg = ReplayConfig(
        input_parquet=args.input_parquet,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        key_field=args.key_field,
        batch_size=args.batch_size,
        rate_per_sec=args.rate_per_sec,
        loop=args.loop,
        event_delay_sec=args.event_delay_sec,
    )
    replay(cfg)


if __name__ == "__main__":
    main()
