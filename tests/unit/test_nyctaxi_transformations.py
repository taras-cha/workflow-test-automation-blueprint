import pytest
from pyspark.sql import SparkSession
from nyctaxi_functions import *

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.getOrCreate()
    yield spark


@pytest.mark.unit_test
def test_calculate_avg_distance(spark_fixture):
    df = spark_fixture.read.format("csv").option("header", True).option("inferSchema", True).load("tests/resources/data/nyc_taxi/sample_nyc_taxi.csv")
    avg = calculate_avg_distance(df).first()["avg(trip_distance)"]
    assert round(avg) == 1