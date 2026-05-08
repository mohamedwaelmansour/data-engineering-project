import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("Bank_ETL_Bronze_Local_to_HDFS") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.sql.files.ignoreMissingFiles", "true") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
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

SOURCE_PATH = "file:///opt/airflow/data/raw_transactions/*.json"
HDFS_PATH = "hdfs://hadoop-namenode:9000/bank_project/bronze/transactions"

print("Starting Idempotent Ingestion: Local JSON -> HDFS Bronze...")

try:
    df = spark.read.schema(schema).json(SOURCE_PATH)
    record_count = df.count()
    
    if record_count > 0:
        # إضافة تاريخ المعالجة للتقسيم (Partitioning) لضمان عدم التكرار
        bronze_df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("source_file", input_file_name()) \
                      .withColumn("ingestion_date", to_date(current_timestamp()))
        
        # استخدام overwrite مع التقسيم لضمان الثبات (Idempotency)
        bronze_df.write.mode("overwrite").partitionBy("ingestion_date").parquet(HDFS_PATH)
        print(f"Success! Ingested {record_count} records into HDFS (Idempotent Mode).")
    else:
        print("No records found to ingest.")
except Exception as e:
    print(f"Error during ingestion: {e}")
finally:
    spark.stop()