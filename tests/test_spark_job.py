"""Spark tests run in CI (pyspark installed via the [spark] extra) and are
skipped when pyspark is not available locally."""

import pytest

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield s
    s.stop()


def test_spark_matches_pandas(spark, landing):
    from hc_lakehouse import medallion, spark_job

    bronze = spark_job.read_bronze_hl7(spark, str(landing))
    enc = spark_job.silver_encounters(bronze, "test-salt")
    gold = spark_job.gold_encounter_summary(enc).toPandas()

    pdf = medallion.build_gold(medallion.build_silver(medallion.build_bronze(landing), salt="test-salt"))
    expected = pdf["encounter_summary"]
    assert len(gold) == len(expected)
    assert int(gold["readmitted_within_30d"].sum()) == int(expected["readmitted_within_30d"].sum())
