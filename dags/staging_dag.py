import re
import pandas as pd
from airflow import DAG
import datetime as dt
import requests
import json
from airflow.operators.python_operator import PythonOperator
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
    template_searchpath=["/opt/airflow/data/"], 
)

# Split the name into first and last names
def split_names(name):
    parts = name.split()
    first_name = parts[0]
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    return first_name, last_name

# Remove quotes from names
def clean_names(name):
    return re.sub(r'\s"[^"]+"\s', ' ', name).strip()

def process_csv(output_folder: str):
    df = pd.read_csv(f'{output_folder}/raw_player_data.csv')

    # Select relevant columns and clean names
    df_filtered = df.iloc[:, [0, 2]].copy()
    df_filtered = df_filtered.dropna(subset=[df_filtered.columns[1]])

    # Clean the names
    df_filtered.iloc[:, 0] = df_filtered.iloc[:, 0].apply(clean_names)

    # Split the name column into two columns: First Name and Last Name
    df_split = df_filtered.iloc[:, 0].apply(lambda x: pd.Series(split_names(x)))
    df_split.columns = ['First Name', 'Last Name']

    # Convert the date column to the desired format
    df_filtered['Formatted Date'] = pd.to_datetime(df_filtered.iloc[:, 1], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

    # Combine results into the final DataFrame
    df_result = pd.concat([df_split, df_filtered['Formatted Date']], axis=1)

    # Save the cleaned data
    df_result.to_csv(f'{output_folder}/cleaned_player_data.csv', index=False)
    
process_task = PythonOperator(
    task_id='process_csv_task',
    python_callable=process_csv,
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
        requests.get("https://www.hockeydb.com", timeout=10)
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
    # Opening the players file
    with open(f'{output_folder}/cleaned_player_data.csv', 'r') as file: 
        
        #Reading player information line by line
        for line in file:
            currentLine = line.split(',')
            firstName = currentLine[0]
            lastName = currentLine[1]
            birthday = currentLine[2]
            
            try:
                urlName = "https://api.themoviedb.org/3/search/person?query="+firstName+"%20"+lastName+"&include_adult=false&language=en-US&page=1"

                headers = {
                    "accept": "application/json",
                    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmNmVmNWZhMzllM2I3MDFlNGZhYmQyOThjNTE5ZjJhZCIsIm5iZiI6MTczMjcyNTM2MS44NjIwMDAyLCJzdWIiOiI2NzQ3NGE3MTBmZDdmODIzZTBjOWFhYmIiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.GqcOH4BR7uWn9JdcurVQV5ZNnWrJW7tJ6EubPIYBHD8"
                }

                response = requests.get(urlName, headers=headers)
                json_object_all = json.loads(response.text)

                #formatted = json.dumps(json_object_all['results'][0]['id'], indent=3)

                ## Get ID of people who match the name
                for i in range(0,len(json_object_all['results'])):
                    id : str = str(json_object_all['results'][i]['id'])

                    # Checking if the birthday aligns with the player
                    urlId = 'https://api.themoviedb.org/3/person/'+id
                    response = requests.get(urlId, headers=headers)
                    json_object_id = json.loads(response.text)
                    fetch_birthday: str = str(json_object_id['birthday'])

                    if(fetch_birthday == birthday):
                        # Getting the list of credits for the specific person if their is a birthday match
                        urlCredits = 'https://api.themoviedb.org/3/person/'+id+'/combined_credits'
                        response = requests.get(urlCredits,headers=headers)
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
                        print(fetch_birthday + " is not equal to "+birthday)
            except:
                print("Issue with value of the current player, skipping")

enriching_task_online = PythonOperator(
    task_id='online_source',
    python_callable=call_api_online,
    dag=dag,
    op_kwargs={"output_folder": "/opt/airflow/data",},
    trigger_rule='all_success',
)

def call_api_offline(output_folder: str):
    # Opening the players file
    with open(f'{output_folder}/cleaned_player_data.csv', 'r') as file: 
        
        #Reading player information line by line
        for line in file:
            currentLine = line.split(',')
            firstName = currentLine[0]
            lastName = currentLine[1]
            
        #TODO: Implement the offline API call here

enriching_task_offline = PythonOperator(
    task_id='offline_source',
    python_callable=call_api_offline,
    dag=dag,
    op_kwargs={"output_folder": "/opt/airflow/data",},
    trigger_rule='all_success',
)

end = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='none_failed'
)

process_task >> delete_task >> connection_check >> [enriching_task_online, enriching_task_offline] >> end
