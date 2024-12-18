import string
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Database connection (SQLite for this example)
DATABASE_PATH = "/tmp/hockey_players.db"
SQL_ENGINE = create_engine(f'sqlite:///{DATABASE_PATH}', echo=False)

def scrape_hockey_data():
    base_url = "https://www.hockeydb.com/ihdb/players/player_ind_{}.html"
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5'
        }
    all_data = []

    # Loop through all letters of the alphabet
    for letter in string.ascii_lowercase:
        url = base_url.format(letter)
        
        try:
            # Make request to the website
            req = urllib.request.Request(url=url, headers=headers)
            with urllib.request.urlopen(req) as f:
                xhtml = f.read().decode('utf-8')
            
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(xhtml, 'html.parser')
            tables = soup.find_all('table')
            
            if tables:
                # Assume the first table contains the data we need
                df = pd.read_html(str(tables[0]))[0]
                all_data.append(df)
        
        except Exception as e:
            print(f"Failed to process {url}: {e}")
    
    # Combine all the data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        # Save to SQL database
        combined_df.to_sql('hockey_players', con=SQL_ENGINE, if_exists='replace', index=False)

# Define the DAG
with DAG(
    dag_id='scrape_hockey_to_sql',
    default_args=default_args,
    description='A DAG to scrape hockey players data and save to a database',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime(2024, 12, 15),
    catchup=False,
    tags=['webscraping', 'hockey', 'database'],
) as dag:

    # Define the task
    scrape_task = PythonOperator(
        task_id='scrape_hockey_data',
        python_callable=scrape_hockey_data,
    )

    # Set the task in the DAG
    scrape_task
