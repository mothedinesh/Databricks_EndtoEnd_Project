import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

########################## Bookings Data

#Load Data Incrementally

@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():

    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")
    return df

#Create Streaming View

@dlt.view(
  name = "trans_bookings"
)
def trans_bookings():

    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
            .withColumn("ModifiedDate",current_timestamp())\
            .withColumn("booking_date",to_date(col("booking_date")))\
            .drop("_rescued_data")

    return df

#Create Dictionary for rules

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
    }

#Write data from Streaming view into a Streaming Table

@dlt.table(
    name = "silver_bookings"
)
#@dlt.expect_all(rules) #This will check if above rules are meeting. If not it will throw warning
@dlt.expect_all_or_drop(rules) #This will check if above rules are meeting. If not it will drop records
#@dlt.expect_all_or_fail(rules) #This will check if above rules are meeting. If not it will fail
def silver_bookings():

    df = spark.readStream.table("trans_bookings")
    return df

########################## Flights Data

#Load Data Incrementally

@dlt.view(
    name = "trans_flights"
)
def stage_flights():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/bronze/bronzevolume/flights/data")
    
    df = df.withColumn("flight_date",to_date(col("flight_date")))\
            .drop("_rescued_data")\
            .withColumn("modifiedDate",current_timestamp())
            
    return df

#Create Streaming Table

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

########################## Customers Data

#Load Data Incrementally

@dlt.view(
    name = "trans_customers"
)
def stage_flights():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/bronze/bronzevolume/customers/data")

    df = df.drop("_rescued_data")\
            .withColumn("modifiedDate",current_timestamp())
                
    return df

#Create Streaming Table

dlt.create_streaming_table("silver_customers")

dlt.create_auto_cdc_flow(
    target = "silver_customers",
    source = "trans_customers",
    keys = ["passenger_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

########################## Airports Data

#Load Data Incrementally

@dlt.view(
    name = "trans_airports"
)
def stage_flights():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/bronze/bronzevolume/airports/data")

    df = df.drop("_rescued_data")\
            .withColumn("modifiedDate",current_timestamp())
            
    return df

#Create Streaming Table

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

'''
########################## Silver Business View

@dlt.table(
    name = "silver_business"
)
def silver_business():

    df = dlt.readStream("silver_bookings")\
            .join(dlt.readStream("silver_customers"),['passenger_id'])\
            .join(dlt.readStream("silver_flights"),['flight_id'])\
            .join(dlt.readStream("silver_airports"),['airport_id'])\
            .drop("modifiedDate")

    return df

########################## Silver Materialized View

@dlt.table( 
    name = "silver_business_mat"
)
def silver_business_mat():
    df = dlt.read("silver_business")
    return df
'''