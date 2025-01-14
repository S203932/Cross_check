import string
import requests
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2025, 1, 8),
    'schedule_interval': None,
    'retries': 1,
}

dag = DAG(
    'data_ingestion_dag',
    default_args=default_args,
    description='A DAG to scrape hockey data and trigger processing',
)

def check_connection():
    try:
        requests.get("https://www.hockeydb.com", timeout=10)
    except requests.exceptions.ConnectionError:
        return "offline_source"
    return "online_source"
        

connection_check_node = BranchPythonOperator(
    task_id='connection_check',
    dag=dag,
    python_callable=check_connection,
    trigger_rule='all_success',
)

def scrape_data_online(output_folder: str):
    base_url = "https://www.hockeydb.com/ihdb/players/player_ind_{}.html"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
    }
    all_data = []

    # Loop through all letters of the alphabet
    for letter in string.ascii_lowercase:
        url = base_url.format(letter)
        
        try:
            # Make request to the website
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find and parse the first table on the page
            tables = soup.find_all('table')
            if tables:
                df = pd.read_html(str(tables[0]))[0]
                all_data.append(df)
        
        except Exception as e:
            print(f"Failed to process {url}: {e}")
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.drop_duplicates(inplace=True)
        
        combined_df.to_csv(f"{output_folder}raw_player_data.csv", index=False)

scrape_task_online = PythonOperator(
    task_id='online_source',
    python_callable=scrape_data_online,
    op_kwargs={"output_folder": "/opt/airflow/data/"},
    dag=dag,
)

def scrape_data_offline(output_folder: str):
    df = pd.read_csv(f"{output_folder}offline_data.csv")
    df.to_csv(f"{output_folder}raw_player_data.csv", index=False)

scrape_task_offline = PythonOperator(
    task_id='offline_source',
    python_callable=scrape_data_offline,
    op_kwargs={"output_folder": "/opt/airflow/data/"},
    dag=dag,
)

def trigger_staging_dag():
    trigger_dag(dag_id='data_wrangling_dag', run_id=f"manual__{datetime.now().isoformat()}")

trigger_task = PythonOperator(
    task_id='trigger_staging_task',
    python_callable=trigger_staging_dag,
    trigger_rule='one_success',
    dag=dag,
)

connection_check_node >> [scrape_task_online, scrape_task_offline] >> trigger_task
