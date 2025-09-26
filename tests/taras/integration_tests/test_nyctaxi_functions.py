import pyspark.sql.connect.session
import pytest
from taras.nyctaxi_functions import *

@pytest.mark.integration_test
def test_get_spark():
  spark = get_spark()
  assert isinstance(spark, SparkSession)

@pytest.mark.integration_test
def test_get_nyctaxi_trips():
  df = get_nyctaxi_trips()
  assert df.count() > 0