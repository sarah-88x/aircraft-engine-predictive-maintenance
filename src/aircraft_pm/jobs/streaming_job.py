from __future__ import annotations

import argparse
import os
from argparse import Namespace

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA = StructType(
    [
        StructField("engine_id", IntegerType()),
        StructField("unit_id", IntegerType()),
        StructField("cycle", IntegerType()),
        StructField("event_time", TimestampType()),
        StructField("temperature", DoubleType()),
        StructField("pressure", DoubleType()),
        StructField("vibration", DoubleType()),
        StructField("rul", DoubleType()),
        StructField("source_dataset", StringType()),
    ]
)


def _clickhouse_writer(url: str, db: str, table: str):
    def writer(batch_df, _batch_id):
        rows = batch_df.toPandas()
        if rows.empty:
            return
        client = clickhouse_connect.get_client(url=url, database=db)
        client.insert_df(table, rows)

    return writer


def run_stream(args: Namespace):
    spark = SparkSession.builder.appName("aircraft-stream").getOrCreate()

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    json_str = source.selectExpr("CAST(value AS STRING) AS value", "timestamp AS kafka_timestamp")
    parsed = json_str.withColumn("data", F.from_json(F.col("value"), SCHEMA))

    invalid = parsed.filter(F.col("data").isNull()).select("value", "kafka_timestamp")
    valid = parsed.filter(F.col("data").isNotNull()).select("data.*", "kafka_timestamp")

    raw_q = (
        valid.writeStream.format("parquet")
        .option("path", f"{args.raw_output}/stream")
        .option("checkpointLocation", f"{args.checkpoint_base}/raw")
        .outputMode("append")
        .start()
    )

    if args.dead_letter_topic:
        dead_q = (
            invalid.selectExpr("CAST(value AS STRING) AS value")
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", args.dead_letter_topic)
            .option("checkpointLocation", f"{args.checkpoint_base}/dlq-kafka")
            .start()
        )
    else:
        dead_q = (
            invalid.writeStream.format("parquet")
            .option("path", args.dead_letter_path)
            .option("checkpointLocation", f"{args.checkpoint_base}/dlq-file")
            .outputMode("append")
            .start()
        )

    health = valid.withColumn("health_score", F.col("temperature") + 0.5 * F.col("vibration"))
    alerts = health.filter(
        (F.col("temperature") > F.lit(args.temp_threshold))
        | (F.col("vibration") > F.lit(args.vib_threshold))
    ).withColumn("alert_type", F.lit("threshold_breach"))

    health_to_kafka = health.select(F.to_json(F.struct(*health.columns)).alias("value"))
    alerts_to_kafka = alerts.select(F.to_json(F.struct(*alerts.columns)).alias("value"))

    health_q = (
        health_to_kafka.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", args.results_topic)
        .option("checkpointLocation", f"{args.checkpoint_base}/health-kafka")
        .start()
    )

    alerts_q = (
        alerts_to_kafka.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", args.alerts_topic)
        .option("checkpointLocation", f"{args.checkpoint_base}/alerts-kafka")
        .start()
    )

    lake_q = (
        health.writeStream.format("parquet")
        .option("path", f"{args.raw_output}/health")
        .option("checkpointLocation", f"{args.checkpoint_base}/health-parquet")
        .outputMode("append")
        .start()
    )

    clickhouse_q = None
    if args.clickhouse_url:
        clickhouse_q = (
            health.writeStream.foreachBatch(
                _clickhouse_writer(args.clickhouse_url, args.clickhouse_db, args.clickhouse_table)
            )
            .option("checkpointLocation", f"{args.checkpoint_base}/clickhouse")
            .outputMode("append")
            .start()
        )

    spark.streams.awaitAnyTermination()

    for q in [raw_q, dead_q, health_q, alerts_q, lake_q, clickhouse_q]:
        if q:
            q.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="Spark structured streaming for engine telemetry")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    p.add_argument("--input-topic", default=os.getenv("ENGINE_TOPIC", "engine-telemetry"))
    p.add_argument(
        "--results-topic",
        default=os.getenv("HEALTH_RESULTS_TOPIC", "engine-health-results"),
    )
    p.add_argument("--alerts-topic", default=os.getenv("ALERTS_TOPIC", "maintenance-alerts"))
    p.add_argument("--dead-letter-topic", default=None)
    p.add_argument(
        "--dead-letter-path",
        default=os.getenv("QUARANTINE_PATH", "/aircraft/quarantine/invalid_messages"),
    )
    p.add_argument("--raw-output", default=os.getenv("RAW_PATH", "/aircraft/raw"))
    p.add_argument(
        "--checkpoint-base",
        default=os.getenv("CHECKPOINT_PATH", "/aircraft/checkpoints"),
    )
    p.add_argument("--temp-threshold", type=float, default=95.0)
    p.add_argument("--vib-threshold", type=float, default=30.0)
    p.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_URL"))
    p.add_argument("--clickhouse-db", default=os.getenv("CLICKHOUSE_DB", "aircraft"))
    p.add_argument("--clickhouse-table", default="health_results")
    args = p.parse_args()
    run_stream(args)


if __name__ == "__main__":
    main()
