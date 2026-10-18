from pyspark.sql import SparkSession

from aircraft_pm.jobs.batch_job import derive_rul


def test_derive_rul_from_cycle_when_missing():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, 1), (1, 2), (1, 3)], ["engine_id", "cycle"])
    out = derive_rul(df)
    vals = [r["rul"] for r in out.orderBy("cycle").collect()]
    assert vals == [2.0, 1.0, 0.0]
    spark.stop()
