from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import json

# 1. Detect if the script is running in AWS Glue
IS_GLUE = "GLUE_COMMAND_CRITERIA" in os.environ

# if IS_GLUE:
#     # --- AWS GLUE ENVIRONMENT ---
#     # Glue handles all S3 configurations automatically under the hood
#     spark = SparkSession.builder \
#         .appName("GlueReadParquetFromS3") \
#         .getOrCreate()
#
#     # Path to your live production S3 data
#     path = "s3://your-bucket-name/path/to/data.parquet"
#
# else:
#     # --- LOCAL PYCHARM JUPYTER NOTEBOOK ENVIRONMENT ---
#     # Simple, lightweight initialization for local machine files
#     spark = SparkSession.builder \
#         .appName("OptimizedLocalSession") \
#         .master("local[*]") \
#         .config("spark.driver.memory", "4g") \
#         .config("spark.executor.memory", "4g") \
#         .getOrCreate()
#
# # 2. Universal Read and Show (Works identically on both Local and Glue)
# BASE_DIR = Path.cwd().parent
# print(BASE_DIR)
# for path in Path(f"{BASE_DIR}/yellow_taxi_trip").iterdir():
#     print(f"Analyzing metadata for file::: " + path.name)
#     df = spark.read.parquet(path)
#     month_list = ['Jan', 'Feb', 'Mar']

spark = SparkSession.builder \
        .appName("GlueReadParquetFromS3") \
        .getOrCreate()
path = "s3://your-bucket-name/path/to/data.parquet"
df = spark.read.parquet(path)
fil_cols = [col for col, dtype in df.dtypes if dtype in ("int", "bigint", "double")]
# stats = {}
# cat_stats = {}
# row_count = df.count()
# for col in fil_cols:
#     describe_rows = df.select(col).describe().collect()
#     stats[col] = {row['summary']: row[col] for row in describe_rows}
#     stats[col]['null_count'] = str(row_count - int(stats[col]['count']))
# print('Statistical Analysis::: ', json.dumps(stats, indent= 4))
df_stat = df.select(fil_cols).describe()
#Finding Null Count
df_null_count = df.select([
    F.count(F.when(F.col(c).isNull(), F.lit(1))).cast("string").alias(c)
    for c in fil_cols
])
df_null_count = df_null_count.withColumn("summary", F.lit("null_count"))
df_null_count = df_null_count.select(["summary"] + fil_cols)
df_stat = df_stat.union(df_null_count)
cat_col = ['payment_type', 'RatecodeID', 'VendorID']




