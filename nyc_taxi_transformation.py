import sys
from typing import List

from pyspark.sql.types import ByteType, TimestampType, ShortType, StringType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.window import Window as window
import json
from pyspark.sql import SparkSession
LOCAL_ENV = False
spark = None
if LOCAL_ENV:
    # Create the Spark Session
    spark = SparkSession.builder.appName("MyPySparkApp").master("local[*]").getOrCreate()
else:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

column_mapping = {
    "vendorid": ("vendor_id", ShortType()),
    "tpep_pickup_datetime": ("tpep_pickup_datetime", TimestampType()),
    "tpep_dropoff_datetime": ("tpep_dropoff_datetime", TimestampType()),
    "passenger_count": ("passenger_count", ByteType()),
    "trip_distance": ("trip_distance", DoubleType()),
    "ratecodeid": ("rate_code_id", ShortType()),
    "store_and_fwd_flag": ("store_and_fwd_flag", StringType()),
    "pulocationid": ("pu_location_id", ShortType()),
    "dolocationid": ("do_location_id", ShortType()),
    "payment_type": ("payment_type", ShortType()),
    "fare_amount": ("fare_amount", DoubleType()),
    "extra": ("extra", DoubleType()),
    "mta_tax": ("mta_tax", DoubleType()),
    "tip_amount": ("tip_amount", DoubleType()),
    "tolls_amount": ("toll_amount", DoubleType()),
    "improvement_surcharge": ("improvement_surcharge", DoubleType()),
    "total_amount": ("total_amount", DoubleType()),
    "congestion_surcharge": ("congestion_surcharge", DoubleType()),
    "airport_fee": ("airport_fee", DoubleType())
}

lookup_column_mapping = {
    "LocationID": ("location_id", ShortType()),
    "Borough": ("borough", StringType()),
    "Zone": ("zone", StringType()),
    "service_zone": ("service_zone", StringType())
}
if LOCAL_ENV:
    path_list = [
        r"C:\Shiva\Python\Data_Engineering\nyc_trip\nyc_taxi_trip_pipeline\yellow_taxi_trip\yellow_tripdata_2023-01.parquet",
        r"C:\Shiva\Python\Data_Engineering\nyc_trip\nyc_taxi_trip_pipeline\yellow_taxi_trip\yellow_tripdata_2023-02.parquet",
        r"C:\Shiva\Python\Data_Engineering\nyc_trip\nyc_taxi_trip_pipeline\yellow_taxi_trip\yellow_tripdata_2023-03.parquet"]
    zone_lookup_path = r"C:\Users\shivashankar.k\Downloads\taxi_zone_lookup.csv"
    curated_base_path = r"C:\Shiva\Python\Data_Engineering\report_csv\curated"
    quarantined_base_path = r"C:\Shiva\Python\Data_Engineering\report_csv\rejects\trips"
else:
    path_list = ["s3://nyc-raw-yellow-taxi-trip/raw/trips/year=2023/month=01/yellow_tripdata_2023-01.parquet",
                 "s3://nyc-raw-yellow-taxi-trip/raw/trips/year=2023/month=02/yellow_tripdata_2023-02.parquet",
                 "s3://nyc-raw-yellow-taxi-trip/raw/trips/year=2023/month=03/yellow_tripdata_2023-03.parquet"]
    zone_lookup_path = "s3://nyc-raw-yellow-taxi-trip/raw/zone_lookup/taxi_zone_lookup.csv"
    curated_base_path = "s3://nyc-raw-yellow-taxi-trip/curated"
    quarantined_base_path = "s3://nyc-raw-yellow-taxi-trip/rejects/trips"

def standardize_dataframe_column(raw_df, column_mapping_dict):
    raw_df = raw_df.toDF(*[c.lower() for c in raw_df.columns])
    projection_expression = [F.col(c).cast(c_type).alias(std_c) for c, (std_c, c_type) in column_mapping_dict.items()]
    raw_df = raw_df.select(*projection_expression)
    return raw_df

def trim_column_label(raw_df):
    for col_name in raw_df.columns:
        cleaned_name = col_name.strip().replace('"', '').strip()
        raw_df = raw_df.withColumnRenamed(col_name, cleaned_name)
    return raw_df

def standardize_raw_types(paths: List):
    std_df = None
    for path in paths:
        raw_df_temp = spark.read.parquet(path)
        std_df_temp = standardize_dataframe_column(raw_df_temp, column_mapping)
        if std_df is None:
            std_df = std_df_temp
        else:
            std_df = std_df.union(std_df_temp)
    row_count = std_df.count()
    print("Total row count on combining parquet::: ", row_count)
    return std_df

def get_invalid_projection_expression():
    invalid_projection_expression = F.filter(
        F.array(
            F.when(F.col("total_amount").isNull() | F.isnan("total_amount"), "Null or NaN Total Amount"),
            F.when(F.col("total_amount") <= 0, "Negative or zero Total Amount"),
            F.when(F.col("trip_distance").isNull() | F.isnan("trip_distance"), "Null or NaN Trip Distance"),
            F.when(F.col("trip_distance") <= 0, "Negative or zero Trip Distance"),
            F.when((F.col("fare_amount").isNull()) | (F.isnan(F.col("fare_amount"))) | (F.col("fare_amount") <= 0),
                   "Invalid or Empty Fare Amount"),
            F.when(F.col("tpep_dropoff_datetime").isNull(), "Null Dropoff Datetime"),
            F.when(F.col("tpep_pickup_datetime").isNull(), "Null Pickup Datetime"),
            F.when(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"), "Invalid Trip Duration"),
            F.when(F.col("pu_location_id").isNull(), "Pick up location id is Null or empty"),
            F.when(F.col("do_location_id").isNull(), "Drop off location id is Null or empty"),
            F.when(F.to_date("tpep_pickup_datetime") < F.to_date(F.lit("2023-01-01")), "Pickup Date Before 2023-01-01"),
            F.when(F.to_date("tpep_pickup_datetime") > F.to_date(F.lit("2023-03-31")), "Pickup Date After 2023-03-31")
        ),
        lambda x: x.isNotNull()
    )
    return invalid_projection_expression

def get_transformation_expression() -> List:
    trip_duration_expression = (
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60).alias(
        "trip_duration_minutes")
    pickup_date_expression = F.to_date(F.col("tpep_pickup_datetime")).alias("pickup_date")
    pickup_hour_expression = F.hour(F.col("tpep_pickup_datetime")).alias("pickup_hour")
    trip_speed_expression = (
        F.when(
            (F.col("trip_distance") > 0) &
            (
                    (
                            F.unix_timestamp("tpep_dropoff_datetime") -
                            F.unix_timestamp("tpep_pickup_datetime")
                    ) > 0
            ),
            F.col("trip_distance") * 3600 /
            (
                    F.unix_timestamp("tpep_dropoff_datetime") -
                    F.unix_timestamp("tpep_pickup_datetime")
            )
        )
        .otherwise(None)
        .alias("trip_speed_mph")
    )
    expression_list = [trip_duration_expression, pickup_date_expression, pickup_hour_expression, trip_speed_expression]
    return expression_list

def get_lookup_zone_dataframe():
    raw_lookup_df = spark.read.csv(zone_lookup_path, header=True)
    raw_lookup_df = trim_column_label(raw_lookup_df)
    std_lookup_df = standardize_dataframe_column(raw_lookup_df, lookup_column_mapping)
    return std_lookup_df

def filter_and_transform_dataframe(df):
    invalid_projection_expression = get_invalid_projection_expression()
    # df = df.withColumn("quarantine_reason", invalid_projection_expression)
    df = df.withColumn(
        "validation_errors",
        invalid_projection_expression
    ).withColumn(
        "validation_status",
        F.when(F.size("validation_errors") == 0, "Valid")
        .otherwise("Invalid")
    )
    # invalid_df = df.filter(F.col("quarantine_reason") != "Valid")
    invalid_df = df.filter(F.col("validation_status") == "Invalid")
    expression_list = get_transformation_expression()
    # df = df.filter(F.col("quarantine_reason") == "Valid").distinct().select("*", *expression_list)
    valid_df = df.filter(F.col("validation_Status") == "Valid").distinct().select("*", *expression_list)
    # df = df.drop("quarantine_reason")
    valid_df = valid_df.drop("validation_status")
    valid_df = valid_df.drop("validation_errors")
    invalid_df = invalid_df.drop("validation_status")
    return valid_df, invalid_df

def join_lookup_zone(df, lookup_df):
    pu_df = df.join(
        F.broadcast(lookup_df.alias("pu_lookup")),
        df.pu_location_id == F.col("pu_lookup.location_id"),
        how="left"
    ).select(
        df["*"],
        F.col("pu_lookup.zone").alias("pickup_zone"),
        F.col("pu_lookup.borough").alias("pickup_borough"),
        F.col("pu_lookup.service_zone").alias("pickup_service_zone")
    )
    join_df = pu_df.join(
        F.broadcast(lookup_df.alias("do_lookup")),
        pu_df.do_location_id == F.col("do_lookup.location_id"),
        how="left"
    ).select(
        pu_df["*"],
        F.col("do_lookup.zone").alias("drop_zone"),
        F.col("do_lookup.borough").alias("drop_borough"),
        F.col("do_lookup.service_zone").alias("drop_service_zone")
    )
    join_df = join_df.withColumn(
        "is_airport_trip",
        F.col("pickup_service_zone").contains("Airport") | F.col("drop_service_zone").contains("Airport")
    )
    return join_df

def convert_dataframe_parquet(df, parquet_path):
    df.coalesce(5).write.mode("append").parquet(parquet_path)
    return None

def get_aggregate_dataframe(join_df):
    aggregate_df = join_df.groupBy("pickup_date", "pickup_borough").agg(
        F.sum(F.col("total_amount")).alias("revenue"),
        F.count("*").alias("trip_count")
    )
    return aggregate_df

def get_zone_revenue_aggregate(join_df):
    revenue_by_date_df = join_df.groupBy("pickup_date", "pickup_zone").agg(
        F.sum("total_amount").alias("revenue")
    )
    window_spec = window.partitionBy("pickup_date").orderBy(F.col("revenue").desc())
    agg_zone_df = revenue_by_date_df.withColumn(
        "rank", F.dense_rank().over(window_spec)
    )
    return agg_zone_df

if __name__ == "__main__":
    std_dtypes_df = standardize_raw_types(path_list)
    df, invalid_df = filter_and_transform_dataframe(std_dtypes_df)
    lookup_df = get_lookup_zone_dataframe()
    join_df = join_lookup_zone(df, lookup_df)
    invalid_parquet_path = f"{quarantined_base_path}/{datetime.today().strftime('%Y-%m-%d')}"
    convert_dataframe_parquet(invalid_df, invalid_parquet_path)
    join_df.coalesce(5).write.mode("append").partitionBy("pickup_date").parquet(f"{curated_base_path}/trips")
    agg_zone_df = get_zone_revenue_aggregate(join_df)
    aggregate_df = get_aggregate_dataframe(join_df)
    aggregate_df.coalesce(5).write.mode("append").partitionBy("pickup_date").parquet(
        f"{curated_base_path}/borough_daily_revenue"
    )
    agg_zone_df.coalesce(5).write.mode("append").partitionBy("pickup_date").parquet(f"{curated_base_path}/zone_daily_revenue")






