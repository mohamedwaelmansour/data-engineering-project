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
    "sfURL": "FQYKETH-PJ72382.snowflakecomputing.com", 
    "sfUser": "MOHAMED",
    "sfPassword": "N3gemdFUv9EfEGe", 
    "sfDatabase": "BANK_PROJECT_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "BANK_WH",
    "sfRole": "ACCOUNTADMIN"
}
try:
    print("Reading Gold Data from HDFS...")
    df_fact = spark.read.parquet("/bank_project/gold/fact_transactions")
    df_customers = spark.read.parquet("/bank_project/gold/dim_customers")
    df_time = spark.read.parquet("/bank_project/gold/dim_time") 

    print("Uploading to Snowflake (Overwrite mode for Idempotency)...")
    
    # الـ Overwrite هنا بيمسح الجدول في سنو فليك ويكتبه من جديد بالداتا الصح
    for table_name, df in [("FACT_TRANSACTIONS", df_fact), 
                            ("DIM_CUSTOMERS", df_customers), 
                            ("DIM_TIME", df_time)]:
        df.write.format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", table_name) \
            .mode("overwrite").save()
        print(f"Table {table_name} uploaded successfully.")

    print("Done! All data synchronized with Snowflake.")
except Exception as e:
    print(f"Error during Snowflake upload: {e}")
finally:
    spark.stop()