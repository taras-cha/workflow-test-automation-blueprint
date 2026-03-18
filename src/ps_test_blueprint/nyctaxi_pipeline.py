from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Raw NYC Taxi trips as streaming table")
def nyctaxi_trips_raw():
    return spark.readStream.table("samples.nyctaxi.trips")


@dp.materialized_view(comment="Average trip distance by pickup zip")
def avg_distance():
    return (
        dp.read("nyctaxi_trips_raw")
        .groupBy("pickup_zip")
        .agg(F.avg("trip_distance").alias("avg"))
    )
