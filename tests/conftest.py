import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("tdd_pipeline_tests") \
        .getOrCreate()
    yield spark
    spark.stop()
