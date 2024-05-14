import pytest
from pyspark.sql import SparkSession
import sys
import os

root_dir = os.getcwd().split("\\")[:-2]
print(root_dir)
root_dir = "\\".join(root_dir)
sys.path.append(root_dir+"\\main\\resource")
print(root_dir)


@pytest.fixture()
def sparksession():
    spark = SparkSession.builder \
        .master("local") \
        .appName("test_App") \
        .config("spark.sql.debug.maxToStringFields", 100000) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.jars", "jar path") \
        .getOrCreate()
    yield spark