import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

path_list = ["s3://nyc-raw-yellow-taxi-trip/raw/trips/year=2023/month=01/yellow_tripdata_2023-01.parquet",
             "s3://nyc-raw-yellow-taxi-trip/raw/trips/year=2023/month=02/yellow_tripdata_2023-02.parquet",
             "s3://nyc-raw-yellow-taxi-trip/raw/trips/year=2023/month=03/yellow_tripdata_2023-03.parquet"]
report_path = "s3://nyc-raw-yellow-taxi-trip/profile_report"
for path in path_list:
    df = spark.read.parquet(path)
    row_count = df.count()
    print("Row count::: ", df.count())
    #Numerical Analysis
    fil_cols = [col for col, dtype in df.dtypes if dtype in ("int", "bigint", "double")]
    df_stat = df.select(fil_cols).describe()
    #Finding Null Count
    df_null_count = df.select([
        F.count(F.when(F.col(c).isNull(), F.lit(1))).cast("string").alias(c)
        for c in fil_cols
    ])
    df_null_count = df_null_count.withColumn("summary", F.lit("null_count"))
    df_null_count = df_null_count.select(["summary"] + fil_cols)
    df_stat = df_stat.union(df_null_count)
    df_stat.coalesce(1).write.mode("append").option("header", True).csv(f"{report_path}/numerical_analysis")
    #Categorical Analysis
    cat_cols = ['payment_type', 'RatecodeID', 'VendorID']
    df_frequency = None
    for col in cat_cols:
        df_frequency_temp = (
            df.groupBy(F.col(col).alias("category_column"))
            .count()
            .withColumn("category_type", F.lit(col))
        )
        print("unique::: ", df_frequency_temp.count())
        if df_frequency is None:
            df_frequency = df_frequency_temp
        else:
            df_frequency = df_frequency.union(df_frequency_temp)
    df_frequency.coalesce(1).write.mode("append").option("header", True).csv(f"{report_path}/categprical_analysis")
    #count of trips where dropoff < pickup, trip_distance<=0, or fare_amount <= 0
    df_inv = df.filter((F.col("fare_amount")<= 0) |
                       (F.col("trip_distance") <= 0) |
                       (F.col("tpep_dropoff_datetime")< F.col("tpep_pickup_datetime")))
    invalid_trip_count = df_inv.count()
    print("Count of trips with  dropoff is earlier than pickup, trip_distance, fare_amount less than zero::: ", invalid_trip_count)
    #Exact and approximate duplicate row count
    exact_duplicate_count = df.count() - df.distinct().count()
    print("Exact duplicate count::: ", exact_duplicate_count)
    df_qual = spark.createDataFrame(
        [(row_count, exact_duplicate_count, invalid_trip_count)],
        "row_count LONG, exact_duplicate_count LONG, invalid_trip_count LONG"
    )
    df_qual.coalesce(1).write.mode("append").option("header", "true").csv(f"{report_path}/quality_analysis")
    #Schema drift
    #Airport_fee is the only column not present in Jan, Instead it is present as airport_fee
    print("Airport_fee is the only column not present in Jan, Instead it is present as airport_fee")

    # Sample 10% (0.1) of the DataFrame
    sample_fraction = 0.1
    sampled_count = df.sample(fraction=sample_fraction, seed=42).count()
    # Scale back up to estimate total rows
    approx_total_rows = int(sampled_count / sample_fraction)
    print(f"Approximate row count: {approx_total_rows}")
