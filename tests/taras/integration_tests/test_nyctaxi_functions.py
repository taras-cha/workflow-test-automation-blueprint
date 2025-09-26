import pyspark.sql.connect.session
import pytest
from taras.nyctaxi_functions import *


@pytest.mark.integration_test
def test_get_nyctaxi_trips():
  df = get_nyctaxi_trips()
  assert df.count() > 0