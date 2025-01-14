import airflow
import datetime
import requests
from pprint import pprint
import pandas as pd
import os
from airflow import DAG
import json
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

# Opening the csv to read the data
def openHockeyPlayerCsv(output_folder: str):
    
    print('Start!!!')
    # Opening the players file
    with open(f'{output_folder}/hockey_players.csv', 'r') as file: 
        
        #Reading player information line by line
        for line in file:
            print("New line")
            currentLine = line.split(',')

            firstName = currentLine[0]
            lastName = currentLine[1]
            birthday = currentLine[2]
            
            try:
                print(firstName+" "+lastName)
                print(f'Birthday: {birthday}')

                # convertedBirthday = str(birthday.replace('\n',''))
                # print(f'Converted Birthday: {convertedBirthday}')

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
                    #print(id)

                    # Checking if the birthday aligns with the player
                    urlId = 'https://api.themoviedb.org/3/person/'+id

                    response = requests.get(urlId, headers=headers)

                    json_object_id = json.loads(response.text)

                    fetch_birthday: str = str(json_object_id['birthday'])
                    #print(fetch_birthday)

                    if(fetch_birthday == birthday):
                        # Getting the list of credits for the specific person if their is a birthday match
                        print(id)

                        urlCredits = 'https://api.themoviedb.org/3/person/'+id+'/combined_credits'
                        response = requests.get(urlCredits,headers=headers)
                        json_object_final = json.loads(response.text)

                        ## Printing the final data 
                        formatted = json.dumps(json_object_final, indent=3)
                        print(formatted)


                        print("About to write")

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
                
            print('Done with line')


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 0,
    #'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    dag_id='fetching_api_data',
    default_args=default_args,
    catchup=False,
)

task_one = PythonOperator(
    task_id='get_spreadsheet',
    dag=dag,
    python_callable=openHockeyPlayerCsv,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='none_failed'
)


task_one >> end

#>> task_two >> task_three >> task_four >> task_five
