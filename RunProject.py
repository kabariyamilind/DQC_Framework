import sys
import time
from pyspark.sql import SparkSession
import os
from pyspark.sql import Row

from pyspark.sql.functions import udf,col, concat_ws

print(os.getcwd())
sys.path.append(os.getcwd()+"/src/main/python/")
print(sys.path)
from src.main.python.main import process_start


spark = SparkSession.builder \
        .master("yarn") \
        .appName("DQC_framework") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseMode", "CORRECTED") \
        .config('spark.sql.session.timeZone', 'IST') \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config('spark.sql.analyzer.maxIterations', '300') \
        .config('spark.sql.optimizer.maxIterations', '300') \
        .enableHiveSupport() \
        .getOrCreate()
job_arg = sys.argv[1]
env = sys.argv[2]
print(job_arg+'----------------')
print(env+'----------------')

process_start(spark, job_arg, env)