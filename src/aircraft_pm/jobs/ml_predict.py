from __future__ import annotations

import argparse

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession


def predict(model_path: str, input_path: str, output_path: str):
    spark = SparkSession.builder.appName("aircraft-rul-predict").getOrCreate()
    model = PipelineModel.load(model_path)
    df = spark.read.parquet(input_path)
    model.transform(df).write.mode("overwrite").parquet(output_path)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run RUL model inference")
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    predict(args.model_path, args.input, args.output)


if __name__ == "__main__":
    main()
