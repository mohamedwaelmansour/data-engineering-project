from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bank_etl_pipeline',
    default_args=default_args,
    description='Full ETL Pipeline using Python execution',
    schedule_interval=None,
    catchup=False
) as dag:
 
 
    run_simulator = BashOperator(
        task_id='run_bank_simulator',
        bash_command='python3 /opt/airflow/scripts/bank_simulator.py'
    )

   
    ingest_data = BashOperator(
        task_id='ingest_to_bronze',
        bash_command='docker exec spark-jupyter spark-submit --master yarn /opt/airflow/scripts/ingest_to_bronze.py'
    )

     
    transform_data = BashOperator(
        task_id='transform_to_gold',
        bash_command='docker exec spark-jupyter spark-submit --master yarn /opt/airflow/scripts/transform_to_star_schema.py'
    )

     
    upload_snowflake = BashOperator(
        task_id='upload_to_snowflake',
        bash_command='docker exec spark-jupyter spark-submit --jars /opt/spark/jars/snowflake-jdbc-3.14.4.jar,/opt/spark/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar /opt/airflow/scripts/hdfs_to_snowflake.py'
    )

    run_simulator >> ingest_data >> transform_data >> upload_snowflake