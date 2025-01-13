import string
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
from datetime import datetime

def scrape_hockey_data():
    base_url = "https://www.hockeydb.com/ihdb/players/player_ind_{}.html"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
    }
    all_data = []

    # Loop through all letters of the alphabet
    # for letter in string.ascii_lowercase:
    for letter in ['a', 'b']:
        url = base_url.format(letter)
        print(f"Processing {url}...")
        
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
        
        combined_df.to_csv("/opt/airflow/dags/hockey_players.csv", index=False)

def trigger_process_csv_dag():
    trigger_dag(dag_id='process_hockey_data', run_id=f"manual__{datetime.now().isoformat()}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 8),
    'retries': 1,
}

dag = DAG(
    'scrape_hockey_data',
    default_args=default_args,
    description='A DAG to scrape hockey data and trigger processing',
    schedule_interval=None,
)

scrape_task = PythonOperator(
    task_id='scrape_hockey_data_task',
    python_callable=scrape_hockey_data,
    dag=dag,
)

trigger_task = PythonOperator(
    task_id='trigger_process_csv_task',
    python_callable=trigger_process_csv_dag,
    dag=dag,
)

scrape_task >> trigger_task
