import re
import pandas as pd
import datetime as dt
import requests
import json
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2025, 1, 8),
    'schedule_interval': None,
    'retries': 1,
}

dag = DAG(
    dag_id='data_wrangling_dag',
    default_args=default_args,
    description='A DAG to clean and enrich data',
)

load_dotenv()
API_KEY = os.getenv('TMDB_API_KEY')
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmNmVmNWZhMzllM2I3MDFlNGZhYmQyOThjNTE5ZjJhZCIsIm5iZiI6MTczMjcyNTM2MS44NjIwMDAyLCJzdWIiOiI2NzQ3NGE3MTBmZDdmODIzZTBjOWFhYmIiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.GqcOH4BR7uWn9JdcurVQV5ZNnWrJW7tJ6EubPIYBHD8"
}

def clean_data(output_folder: str):
    df = pd.read_csv(f'{output_folder}/raw_player_data.csv')

    # Remove unnecessary columns
    df_cleaned = df.iloc[:, [0, 2]]

    # Remove rows with missing birthdates
    df_cleaned = df_cleaned.dropna(subset=[df_cleaned.columns[1]])
    df_cleaned.to_csv(f'{output_folder}/player_data.csv', index=False)

clean_data = PythonOperator(
    task_id='clean_data_task',
    python_callable=clean_data,
    op_kwargs={"output_folder": "/opt/airflow/data"},
    dag=dag,
)

def split_names(name):
    parts = name.split()
    first_name = parts[0]
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    return first_name, last_name

def clean_names(name):
    return re.sub(r'\s"[^"]+"\s', ' ', name).strip()

def wrangle_data(output_folder: str):
    df = pd.read_csv(f'{output_folder}/player_data.csv')
    
    # Clean names and reassign to a new column
    df['Name'] = df.iloc[:, 0].apply(clean_names)
    
    # Convert birthdates to standard format
    df['Birthday'] = pd.to_datetime(df.iloc[:, 1], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
    
    # Split names into first and last name
    df_split = df['Name'].apply(lambda x: pd.Series(split_names(x)))
    df_split.columns = ['First Name', 'Last Name']
    
    # Combine cleaned data
    df_wrangled = pd.concat([df_split, df['Birthday']], axis=1)
    df_wrangled.to_csv(f'{output_folder}/player_data.csv', index=False)

wrangle_data = PythonOperator(
    task_id='wrangle_data_task',
    python_callable=wrangle_data,
    op_kwargs={"output_folder": "/opt/airflow/data"},
    dag=dag,
)

delete_task = BashOperator(
    task_id='delete_raw_data',
    bash_command='rm /opt/airflow/data/raw_player_data.csv',
    dag=dag,
)

def check_connection():
    try:
        url = "https://api.themoviedb.org/3/authentication"
        requests.get(url, headers=HEADERS)
    except requests.exceptions.ConnectionError:
        return "offline_source"
    return "online_source"

connection_check = BranchPythonOperator(
    task_id='connection_check',
    python_callable=check_connection,
    dag=dag,
    trigger_rule='all_success',
)

def call_api_online(output_folder: str):
    with open(f'{output_folder}/player_data.csv', 'r') as file: 
        
        #Reading player information line by line
        for line in file:
            currentLine = line.split(',')
            firstName = currentLine[0]
            lastName = currentLine[1]
            birthday = currentLine[2].strip('\n')
            
            try:
                urlName = "https://api.themoviedb.org/3/search/person?query="+firstName+"%20"+lastName+"&include_adult=false&language=en-US&page=1"

                response = requests.get(urlName, headers=HEADERS)
                print(response.text)
                json_object_all = json.loads(response.text)

                # Get ID of people who match the name
                for i in range(0,len(json_object_all['results'])):
                    id : str = str(json_object_all['results'][i]['id'])

                    # Checking if the birthday aligns with the player
                    urlId = 'https://api.themoviedb.org/3/person/'+id
                    response = requests.get(urlId, headers=HEADERS)
                    json_object_id = json.loads(response.text)
                    fetch_birthday: str = str(json_object_id['birthday'])

                    if(fetch_birthday == birthday):
                        # Getting the list of credits for the specific person if their is a birthday match
                        urlCredits = 'https://api.themoviedb.org/3/person/'+id+'/combined_credits'
                        response = requests.get(urlCredits,headers=HEADERS)
                        json_object_final = json.loads(response.text)

                        if len(json_object_final) != 0:
                            with open(f'{output_folder}/movie_data/{firstName}_{lastName}_{birthday}.json', "w") as outfile:
                                try:
                                    json.dump(json_object_final,outfile)
                                    outfile.close()
                                except: 
                                    print("Could not write to file")
                        else: 
                            print("Fetched object was empty therefore no file was created")

                        break
                    else:
                        print(fetch_birthday+" is not equal to "+birthday)
            except:
                print("Issue with value of the current player, skipping")

enrich_data_online = PythonOperator(
    task_id='online_source',
    python_callable=call_api_online,
    dag=dag,
    op_kwargs={"output_folder": "/opt/airflow/data",},
    trigger_rule='all_success',
)

enrich_data_offline = BashOperator(
    task_id='offline_source',
    bash_command='cp /opt/airflow/data/offline_movie_data/* /opt/airflow/data/movie_data/',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='none_failed'
)

clean_data >> wrangle_data >> delete_task >> connection_check 
connection_check >> [enrich_data_online, enrich_data_offline] >> end
