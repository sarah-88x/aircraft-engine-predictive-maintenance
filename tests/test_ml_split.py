from pyspark.sql import SparkSession

from aircraft_pm.jobs.ml_train import split_by_engine


def test_split_by_engine_no_leakage():
    spark = SparkSession.builder.master("local[1]").appName("test-split").getOrCreate()
    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 4.0), (3, 5.0), (3, 6.0)],
        ["engine_id", "rul"],
    )
    train, valid, test = split_by_engine(df)

    def ids(frame):
        return {r[0] for r in frame.select("engine_id").distinct().collect()}

    train_ids, valid_ids, test_ids = ids(train), ids(valid), ids(test)
    assert train_ids.isdisjoint(valid_ids)
    assert train_ids.isdisjoint(test_ids)
    assert valid_ids.isdisjoint(test_ids)
    spark.stop()
