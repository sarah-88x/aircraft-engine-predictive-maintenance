from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass

import clickhouse_connect
import requests
from kafka import KafkaConsumer, TopicPartition


@dataclass
class StatusReport:
    kafka: dict
    spark: dict
    hdfs: dict
    clickhouse: dict


def kafka_status(bootstrap_servers: str, topic: str, group: str) -> dict:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group,
        enable_auto_commit=False,
    )
    parts = consumer.partitions_for_topic(topic) or set()
    offsets = {}
    lag = 0
    for p in parts:
        tp = TopicPartition(topic, p)
        end_offset = consumer.end_offsets([tp])[tp]
        committed = consumer.committed(tp) or 0
        offsets[p] = {"end": end_offset, "committed": committed, "lag": end_offset - committed}
        lag += end_offset - committed
    consumer.close()
    return {"topic": topic, "partitions": offsets, "total_lag": lag}


def spark_status(master_url: str) -> dict:
    try:
        resp = requests.get(f"{master_url.rstrip('/')}/json/", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def hdfs_status(paths: list[str]) -> dict:
    return {
        "paths": paths,
        "note": "Verify via `hdfs dfs -ls` in namenode container for exact existence.",
    }


def clickhouse_status(url: str | None, db: str, tables: list[str]) -> dict:
    if not url:
        return {"enabled": False}
    try:
        client = clickhouse_connect.get_client(url=url, database=db)
        counts = {}
        for table in tables:
            counts[table] = client.query(f"SELECT count() FROM {table}").first_item
        return {"enabled": True, "rows": counts}
    except Exception as exc:  # noqa: BLE001
        return {"enabled": True, "error": str(exc)}


def main() -> None:
    p = argparse.ArgumentParser(description="Pipeline health/status report")
    p.add_argument("--bootstrap-servers", default="kafka:9092")
    p.add_argument("--topic", default="engine-telemetry")
    p.add_argument("--group", default="aircraft-monitor")
    p.add_argument("--spark-master-ui", default="http://spark-master:8080")
    p.add_argument(
        "--hdfs-path",
        action="append",
        default=["/aircraft/raw", "/aircraft/checkpoints"],
    )
    p.add_argument("--clickhouse-url", default=None)
    p.add_argument("--clickhouse-db", default="aircraft")
    args = p.parse_args()

    report = StatusReport(
        kafka=kafka_status(args.bootstrap_servers, args.topic, args.group),
        spark=spark_status(args.spark_master_ui),
        hdfs=hdfs_status(args.hdfs_path),
        clickhouse=clickhouse_status(
            args.clickhouse_url,
            args.clickhouse_db,
            ["telemetry", "health_results", "alerts", "predictions", "pipeline_metrics"],
        ),
    )
    print(json.dumps(asdict(report), indent=2, default=str))


if __name__ == "__main__":
    main()
