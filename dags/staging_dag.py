import re
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def clean_names(name):
    return re.sub(r'\s"[^"]+"\s', ' ', name).strip()

def process_csv():
    file = "/opt/airflow/dags/hockey_players.csv"
    df = pd.read_csv(file)

    # Select relevant columns and clean names
    df_filtered = df.iloc[:, [0, 2]]
    df_filtered.iloc[:, 0] = df_filtered.iloc[:, 0].apply(clean_names)

    df_filtered.to_csv(file, index=False)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 8),
    'retries': 1,
}

dag = DAG(
    'process_hockey_data',
    default_args=default_args,
    description='A simple DAG to clean hockey data',
    schedule_interval=None,
)

process_task = PythonOperator(
    task_id='process_csv_task',
    python_callable=process_csv,
    dag=dag,
)
