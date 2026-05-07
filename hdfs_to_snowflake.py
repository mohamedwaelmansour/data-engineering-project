import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('Gold_to_Snowflake_Upload') \
    .master('local[*]') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.30") \
    .getOrCreate()

 
sf_options = {
    "sfURL": "xxxxxx", 
    "sfUser": "xxxxx",
    "sfPassword": "xxxxxxx", 
    "sfDatabase": "BANK_PROJECT_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "BANK_WH",
    "sfRole": "ACCOUNTADMIN"
}

try:
    print(" Reading Gold Data from HDFS...")
    df_fact = spark.read.parquet("/bank_project/gold/fact_transactions")
    df_customers = spark.read.parquet("/bank_project/gold/dim_customers")
    df_time = spark.read.parquet("/bank_project/gold/dim_time") 

    print(" Uploading to Snowflake Cloud...")
    
    
    df_fact.write.format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", "FACT_TRANSACTIONS") \
        .mode("overwrite").save()
 
    df_customers.write.format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", "DIM_CUSTOMERS") \
        .mode("overwrite").save()
 
    df_time.write.format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", "DIM_TIME") \
        .mode("overwrite").save()

    print("Done! All 3 tables uploaded to Snowflake.")

except Exception as e:
    print(f"Error: {e}")

finally:
    spark.stop()
