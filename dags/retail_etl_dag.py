from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import oracledb
import os

# --- CONFIGURATION ---
DB_USER = os.getenv('ORACLE_USER', 'RETAIL_DW')
DB_PASS = os.getenv('ORACLE_PASS', 'RetailPass123')
DB_DSN  = os.getenv('ORACLE_CONN_STRING', 'oracle-db:1521/xepdb1')

# --- PYTHON FUNCTION (Task 2 Logic) ---
def run_oracle_procedure():
    print(f"--- Connecting to Oracle at {DB_DSN} ---")
    try:
        connection = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        cursor = connection.cursor()
        
        print("--- Executing Stored Procedure: pkg_etl_retail.load_daily_sales ---")
        cursor.callproc("pkg_etl_retail.load_daily_sales")
        
        print("✅ Success! ETL Job Completed.")
        connection.close()
    except Exception as e:
        print(f"❌ Error connecting to Oracle: {e}")
        raise

# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='V4: Retail ETL orchestrated by Airflow',
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'oracle', 'v4'],
) as dag:

    # TASK 0: Cleanup (Reset Schema)
    # This runs the same SQL script used in Jenkins to Drop/Recreate tables
    t0_cleanup = BashOperator(
        task_id='reset_schema',
        bash_command='python /opt/airflow/scripts/data_truncate.py /opt/airflow/sql/init_oracle/03_ddl_tables.sql',
    )

    # TASK 1: Generate Data
    t1_generate = BashOperator(
        task_id='generate_sales_data',
        bash_command='python /opt/airflow/scripts/generate_data.py',
    )

    # TASK 2: Run ETL
    t2_trigger_db = PythonOperator(
        task_id='trigger_stored_procedure',
        python_callable=run_oracle_procedure,
    )

    # PIPELINE DEPENDENCY
    # 1. Reset Tables -> 2. Generate CSV -> 3. Load to Oracle
    t0_cleanup >> t1_generate >> t2_trigger_db