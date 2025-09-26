try:
    from databricks.connect import DatabricksSession as SparkSession
    from pyspark.sql import DataFrame
except ImportError:
    from pyspark.sql import DataFrame, SparkSession as SparkSession


def get_spark() -> SparkSession:
  # This should be changed to use Spark in Local mode for unit tests
  spark = SparkSession.builder.getOrCreate()
  return spark

def get_nyctaxi_trips() -> DataFrame:
  spark = get_spark()
  df = spark.read.table("samples.nyctaxi.trips")
  print(df.count())
  return df