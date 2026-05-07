import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("Bank_StarSchema_Transformation") \
    .master("yarn") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .getOrCreate()
 
BRONZE_PATH = "hdfs://hadoop-namenode:9000/bank_project/raw_transactions/"
GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/bank_project/gold/"

try:
    print("Reading Raw Data from Bronze...")
    df_raw = spark.read.json(BRONZE_PATH)
    
     
    print(f"Total records found: {df_raw.count()}")

    df_cleaned = df_raw.withColumn("amount", F.col("amount").cast("double")) \
                       .withColumn("timestamp_fixed", F.to_timestamp("processed_at")) \
                       .withColumn("transaction_id", F.monotonically_increasing_id())

    print("Building Star Schema Layers...")

    #dim_time 
    dim_time = df_cleaned.select("timestamp_fixed").distinct() \
        .withColumn("time_key", F.date_format("timestamp_fixed", "yyyyMMddHHmmss")) \
        .withColumn("hour", F.hour("timestamp_fixed")) \
        .withColumn("day", F.dayofmonth("timestamp_fixed")) \
        .withColumn("month", F.month("timestamp_fixed")) \
        .withColumn("year", F.year("timestamp_fixed")) \
        .withColumn("day_of_week", F.dayofweek("timestamp_fixed"))

    #dim_customers
    senders = df_cleaned.select(F.col("nameOrig").alias("customer_name"))
    receivers = df_cleaned.select(F.col("nameDest").alias("customer_name"))
    dim_customers = senders.union(receivers).distinct() \
        .withColumn("customer_id", F.monotonically_increasing_id())
    #fact_transactions
    fact_transactions = df_cleaned \
        .withColumn("time_key", F.date_format("timestamp_fixed", "yyyyMMddHHmmss")) \
        .withColumn("is_high_value", F.when(F.col("amount") > 500000, "Yes").otherwise("No")) \
        .select(
            "transaction_id", "time_key", 
            F.col("nameOrig").alias("sender_name"), 
            F.col("nameDest").alias("receiver_name"), 
            "type", "amount", "oldbalanceOrg", "newbalanceOrig", "isFraud", "is_high_value"
        )

    print(f"Saving Star Schema to HDFS Gold Layer at: {GOLD_BASE_PATH}")
    
    
    dim_time.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_time")
    dim_customers.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_customers")
    fact_transactions.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}fact_transactions")

    print("Success! Star Schema folders created in Gold Layer.")
    fact_transactions.show(5)

except Exception as e:
    print(f"Transformation failed: {e}")

finally: 
    spark.stop()