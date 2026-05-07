 # 🏦 End-to-End Bank Transactions ETL Pipeline
---

## 🏗️ System Architecture
![System Architecture](./workflow%20etl.png)
This pipeline follows a modern **Lakehouse Architecture** divided into three distinct layers:

1. **Data Generation:** A Python script (`simulator.py`) generates mock transactional data in **JSON** format.
2. **Ingestion (Bronze):** Raw JSON data is ingested and stored in **HDFS** for persistence.
3. **Processing (Gold):** **PySpark** cleans the data, transforms it into a **Star Schema**, and saves it as optimized **Parquet** files in HDFS.
4. **Cloud Loading:** The final structured data is loaded into **Snowflake** Cloud Data Warehouse for analytics.
5. **Orchestration:** **Apache Airflow** manages and automates the entire workflow.

---

## 📊 Data Modeling (Star Schema)
![Star Schema](./star%20schema.png)
The data is structured to ensure high-performance analytical queries:

* **Fact Table:** `FACT_TRANSACTIONS` – Contains transaction amounts, balances, and fraud flags.
* **Dimension Tables:**
    * `DIM_CUSTOMERS`: Unique sender and receiver information.
    * `DIM_TIME`: Temporal details including Hour, Day, Month, Year, and Weekday.

---

## 🛠️ Tech Stack & Environment
* **Infrastructure:** Docker & Docker-Compose (WSL2 Ubuntu).
* **Storage:** HDFS (Hadoop Distributed File System).
* **Processing:** Apache Spark (PySpark running on YARN).
* **Orchestration:** Apache Airflow.
* **Data Warehouse:** Snowflake.

---

## ✅ Final Validation
* **Execution Status:** All Airflow tasks completed successfully (Green DAG).
* **Data Integrity:** 1,000 records successfully processed and verified in Snowflake via SQL query.
* ---
## 📸 Execution & Validation Screenshots

Here is the visual evidence of the successful pipeline execution across all platforms:

### 1. Airflow Orchestration (DAG Run)
Successful execution of the entire DAG, moving data from simulator to cloud.
![Airflow DAG Success](./image_14.png)

### 2. Infrastructure & Job Monitoring (Hadoop/Yarn)
Validation that the Spark jobs ran successfully on the distributed cluster.
![Hadoop Finished Applications](./image_13.png)

### 3. Data Storage (HDFS Directory Structure)
Proof of data persistence in HDFS, showing both `raw_transactions` (Bronze) and `gold` layers.
![HDFS Browsing Directory](./image_11.png)

### 4. Final Data Load (Snowflake Verification)
Final validation in Snowflake, showing the loaded `FACT_TRANSACTIONS` table with all 1,000 rows.
![Snowflake Data Verification](./image_12.png)
