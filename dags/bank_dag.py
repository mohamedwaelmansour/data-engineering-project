from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# الإعدادات الافتراضية
default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1), 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bank_etl_pipeline',
    default_args=default_args,
    description='Full ETL Pipeline: Simulator -> Bronze -> Gold -> Snowflake',
    schedule_interval='@daily', 
    catchup=False 
) as dag:

    # 1. تشغيل السميوليتر لإنتاج بيانات اليوم
    run_simulator = BashOperator(
        task_id='run_bank_simulator',
        bash_command='python3 /opt/airflow/scripts/bank_simulator.py'
    )

    # 2. نقل البيانات لطبقة البرونز (تم إزالة --master yarn ليعمل الـ local[*] اللي في كود البايثون)
    ingest_to_bronze = BashOperator(
        task_id='ingest_to_bronze',
        bash_command='docker exec spark-jupyter spark-submit /opt/airflow/scripts/ingest_to_bronze.py'
    )

    # 3. التحويل لـ Star Schema (الطبقة الذهبية - هنا نستخدم yarn عادي لأن الداتا في HDFS)
    transform_data = BashOperator(
        task_id='transform_to_gold',
        bash_command='docker exec spark-jupyter spark-submit --master yarn /opt/airflow/scripts/transform_to_star_schema.py'
    )

    # 4. الرفع النهائي للسحابة (Snowflake)
    upload_snowflake = BashOperator(
        task_id='upload_to_snowflake',
        bash_command='docker exec spark-jupyter spark-submit --jars /opt/spark/jars/snowflake-jdbc-3.14.4.jar,/opt/spark/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar /opt/airflow/scripts/hdfs_to_snowflake.py'
    )

    # ترتيب التنفيذ (تم تعديل الاسم لـ ingest_to_bronze ليتوافق مع المعرف فوق)
    run_simulator >> ingest_to_bronze >> transform_data >> upload_snowflake