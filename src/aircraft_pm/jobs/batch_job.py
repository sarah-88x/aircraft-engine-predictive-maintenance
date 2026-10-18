from __future__ import annotations

import argparse
import os

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def derive_rul(df):
    if "rul" in df.columns:
        return df.withColumn("rul", F.col("rul").cast("double"))

    if "engine_id" in df.columns:
        id_col = "engine_id"
    elif "unit_id" in df.columns:
        id_col = "unit_id"
    else:
        id_col = None
    if id_col and "cycle" in df.columns:
        max_cycle = F.max("cycle").over(Window.partitionBy(id_col))
        return df.withColumn("rul", (max_cycle - F.col("cycle")).cast("double"))
    return df


def run_batch(
    input_path: str,
    output_path: str,
    clickhouse_url: str | None,
    clickhouse_db: str = "aircraft",
):
    spark = SparkSession.builder.appName("aircraft-batch").getOrCreate()
    df = spark.read.parquet(input_path)

    if "cycle" not in df.columns:
        if "engine_id" in df.columns:
            id_col = "engine_id"
        elif "unit_id" in df.columns:
            id_col = "unit_id"
        else:
            id_col = None
        if id_col and "event_time" in df.columns:
            w = Window.partitionBy(id_col).orderBy("event_time")
            df = df.withColumn("cycle", F.row_number().over(w))

    casted = df
    for col_name, dtype in [("engine_id", "int"), ("unit_id", "int"), ("cycle", "int")]:
        if col_name in casted.columns:
            casted = casted.withColumn(col_name, F.col(col_name).cast(dtype))

    casted = derive_rul(casted)
    processed = casted.withColumn("processed_at", F.current_timestamp())
    processed.write.mode("overwrite").parquet(output_path)

    metrics = processed.agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.coalesce(F.col("engine_id"), F.col("unit_id"))).alias("engines"),
        F.avg("rul").alias("avg_rul"),
    ).collect()[0]

    if clickhouse_url:
        client = clickhouse_connect.get_client(url=clickhouse_url, database=clickhouse_db)
        client.command(
            "CREATE TABLE IF NOT EXISTS pipeline_metrics ("
            "event_time DateTime DEFAULT now(), stage String, "
            "rows UInt64, engines UInt64, avg_rul Float64"
            ") ENGINE MergeTree ORDER BY event_time"
        )
        client.insert(
            "pipeline_metrics",
            [
                [
                    "batch",
                    int(metrics["rows"]),
                    int(metrics["engines"]),
                    float(metrics["avg_rul"] or 0.0),
                ]
            ],
            column_names=["stage", "rows", "engines", "avg_rul"],
        )
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark batch processing for aircraft parquet data")
    parser.add_argument("--input", default=os.getenv("RAW_PATH", "/aircraft/raw"))
    parser.add_argument("--output", default=os.getenv("PROCESSED_PATH", "/aircraft/processed"))
    parser.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_URL"))
    parser.add_argument("--clickhouse-db", default=os.getenv("CLICKHOUSE_DB", "aircraft"))
    args = parser.parse_args()
    run_batch(args.input, args.output, args.clickhouse_url, args.clickhouse_db)


if __name__ == "__main__":
    main()
