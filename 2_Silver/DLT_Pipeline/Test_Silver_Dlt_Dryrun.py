import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Load Data Incrementally

@dlt.view(
    name = "trans_airports"
)
def stage_flights():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/bronze/bronzevolume/airports/data")
            
    return df

#Create Streaming Table

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("airport_id"),
    stored_as_scd_type = 1
)