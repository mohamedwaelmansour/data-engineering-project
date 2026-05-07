import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

 
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
os.environ["HADOOP_USER_NAME"] = "root"

 
spark = SparkSession.builder \
    .appName("Bank_ETL_Bronze_YARN") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

 
schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    StructField("processed_at", StringType(), True)
])

 
SOURCE_PATH = "file:///opt/airflow/data/raw_transactions/"
HDFS_PATH = "hdfs://hadoop-namenode:9000/bank_project/bronze/transactions"

print(" Starting Ingestion: Local JSON -> HDFS Bronze...")

try:
    df = spark.read.schema(schema).json(SOURCE_PATH)
    record_count = df.count()
    
    if record_count > 0:
        bronze_df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("source_file", input_file_name())
        
        bronze_df.write.mode("append").parquet(HDFS_PATH)
        print(f" Success! Ingested {record_count} records into HDFS.")
    else:
        print("No records found to ingest.")
except Exception as e:
    print(f" Error during ingestion: {e}")
finally:
    spark.stop()