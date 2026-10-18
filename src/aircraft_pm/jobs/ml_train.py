from __future__ import annotations

import argparse
import os

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession


def split_by_engine(df: DataFrame, seed: int = 42):
    if "engine_id" in df.columns:
        id_col = "engine_id"
    elif "unit_id" in df.columns:
        id_col = "unit_id"
    else:
        raise ValueError("Expected engine_id or unit_id column for leakage-safe split")

    ids = df.select(id_col).distinct().randomSplit([0.7, 0.15, 0.15], seed=seed)
    train = df.join(ids[0], on=id_col, how="inner")
    valid = df.join(ids[1], on=id_col, how="inner")
    test = df.join(ids[2], on=id_col, how="inner")
    return train, valid, test


def train(input_path: str, model_path: str):
    spark = SparkSession.builder.appName("aircraft-rul-train").getOrCreate()
    df = spark.read.parquet(input_path)
    if "rul" not in df.columns:
        raise ValueError("RUL labels are missing; cannot train supervised model")

    feature_cols = [c for c in ["cycle", "temperature", "pressure", "vibration"] if c in df.columns]
    if not feature_cols:
        raise ValueError("No usable feature columns found")

    selected_cols = feature_cols + ["rul"]
    for id_col in ("engine_id", "unit_id"):
        if id_col in df.columns:
            selected_cols.append(id_col)

    train_df, valid_df, test_df = split_by_engine(df.select(*selected_cols))

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    models = {
        "linear_regression": LinearRegression(featuresCol="features", labelCol="rul", maxIter=50),
        "random_forest": RandomForestRegressor(featuresCol="features", labelCol="rul", numTrees=30),
        "gbt": GBTRegressor(featuresCol="features", labelCol="rul", maxIter=30),
    }

    evaluator_rmse = RegressionEvaluator(
        labelCol="rul",
        predictionCol="prediction",
        metricName="rmse",
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="rul",
        predictionCol="prediction",
        metricName="mae",
    )

    best_name = None
    best_rmse = float("inf")
    for name, estimator in models.items():
        model = Pipeline(stages=[assembler, estimator]).fit(train_df)
        pred = model.transform(valid_df)
        rmse = evaluator_rmse.evaluate(pred)
        mae = evaluator_mae.evaluate(pred)
        if rmse < best_rmse:
            best_rmse = rmse
            best_name = name
        model.write().overwrite().save(f"{model_path}/{name}")
        print({"model": name, "rmse": rmse, "mae": mae})

    if best_name:
        best_model = PipelineModel.load(f"{model_path}/{best_name}")
        test_pred = best_model.transform(test_df)
        print(
            {
                "best_model": best_name,
                "test_rmse": evaluator_rmse.evaluate(test_pred),
                "test_mae": evaluator_mae.evaluate(test_pred),
            }
        )

    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="Train RUL regression models")
    p.add_argument("--input", default=os.getenv("PROCESSED_PATH", "/aircraft/processed"))
    p.add_argument("--model-path", default=os.getenv("MODEL_PATH", "/aircraft/models"))
    args = p.parse_args()
    train(args.input, args.model_path)


if __name__ == "__main__":
    main()
