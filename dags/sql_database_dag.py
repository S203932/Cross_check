import airflow
import datetime
import sqlite3
import csv
import os
import json 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

sql = DAG(
    dag_id='sql_dag',
    default_args=default_args_dict,
    catchup=False,
)

def check_if_database(epoch: int, output_folder: str):
    conn = sqlite3.connect('{output_folder}\\Hockey_entertainment.db')
    c = conn.cursor()
    listOfTables = c.execute("""SELECT * FROM sqlite_master WHERE type='table'
    ; """).fetchall()
    conn.close()

    print("Done with database check")

    if not listOfTables: 
        return 'create_database'
    else:
        return 'populate_database'




def create_database(epoch: int,output_folder: str):
    conn = sqlite3.connect('{output_folder}\\Hockey_entertainment.db')
    c = conn.cursor()

    ## The Players table
    c.execute("""CREATE TABLE IF NOT EXISTS PLAYERS(
    FIRST_NAME TEXT,
    LAST_NAME TEXT, 
    BIRTHDAY DATE, 
    AMOUNT_MOVIES INTEGER, 
    AMOUNT_TV INTEGER, 
    PRIMARY KEY(FIRST_NAME, LAST_NAME, BIRTHDAY)
    );""")

    # YEARS_ACTIVE INTEGER, has been removed from PLAYERS as it didn't seem relevant to the questions


    ## Movies table - pk should propably also be cp(title, date)
    c.execute("""CREATE TABLE IF NOT EXISTS MOVIES(
    TITLE TEXT,
    ORIGINAL_LANGUAGE TEXT,
    RELEASE DATE, VOTE_AVERAGE REAL,
    VOTE_COUNT INTEGER,
    PRIMARY KEY(TITLE)
    );""")

    ## TV show table - pk should propably also be cp(title, date)
    c.execute("""CREATE TABLE IF NOT EXISTS TV(
    TITLE TEXT,
    ORIGINAL_LANGUAGE TEXT,
    RELEASE DATE,
    VOTE_AVERAGE REAL,
    VOTE_COUNT INTEGER,
    PRIMARY KEY(TITLE)
    );""")

    ## Character table
    c.execute("""CREATE TABLE IF NOT EXISTS CHARACTER(
    TITLE TEXT,
    MEDIA_TYPE TEXT,
    RELEASE DATE,
    CHARACTER_NAME TEXT,
    PLAYER_FIRSTNAME TEXT,
    PLAYER_LASTNAME TEXT,
    PRIMARY KEY(TITLE, CHARACTER_NAME, PLAYER_FIRSTNAME, PLAYER_LASTNAME)
    );""")

    ## Player credits
    c.execute("""CREATE TABLE IF NOT EXISTS CREDITS(
    PLAYER_FIRSTNAME TEXT,
    PLAYER_LASTNAME TEXT,
    TITLE TEXT,
    MEDIA_TYPE TEXT,
    RELEASE DATE,
    ORIGINAL_LANGUAGE TEXT,
    CHARACTER_NAME TEXT,
    PRIMARY KEY(PLAYER_FIRSTNAME, PLAYER_FIRSTNAME, TITLE, MEDIA_TYPE, RELEASE),
    FOREIGN KEY(PLAYER_FIRSTNAME) REFERENCES PLAYERS(FIRST_NAME),
    FOREIGN KEY(PLAYER_LASTNAME) REFERENCES PLAYERS(LAST_NAME),
    FOREIGN KEY(MEDIA_TYPE) REFERENCES CHARACTER(MEDIA_TYPE),
    FOREIGN KEY(RELEASE) REFERENCES CHARACTER(RELEASE),
    FOREIGN KEY(CHARACTER_NAME) REFERENCES CHARACTER(CHARACTER_NAME)
    );""")
    

    print(c.fetchall())

    conn.commit()
    conn.close()





def populate_database(epoch: int, output_folder: str):
    conn = sqlite3.connect('{output_folder}\hockey_entertainment.db')
    c = conn.cursor()


    c.execute("""CREATE TABLE IF NOT EXISTS PLAYERS(
        FIRST_NAME TEXT,
        LAST_NAME TEXT, 
        BIRTHDAY DATE, 
        AMOUNT_MOVIES INTEGER, 
        AMOUNT_TV INTEGER, 
        PRIMARY KEY(FIRST_NAME, LAST_NAME, BIRTHDAY)
        );""")


    # Need to open csv file with all hockey players
    with open(f'{output_folder}/hockey_players.csv', mode ='r')as file:
        players = csv.reader(file)

        # skip first line
        counter = 0
        for lines in players:
            if(counter != 0):


                # Getting the First name, last name and birthday in correct format 
            
                try:
                    name = lines[0]
                    name = name.split(' ')
                    firstName = name[0]
                    lastName = name[1]

                    birthday = lines[1]
                    birthday = birthday.split('/')

                    # Birtday now in format YYYY-MM-DD
                    convertedBirthday = str(birthday[2].replace('\n','')+'-'+birthday[0]+'-'+birthday[1])


                    # Now first name, last name and birthday is in place, so to search the local
                    # files for a match

                    
                except:
                    print("Error in formatting")
                movies_amount = 0
                tv_amount = 0
                undefined = 0
                for root, dirs, files in os.walk('{output_folder}\\movie_data'):
                    for file in files:
                        # file is a string of the filename
                        if(file == f'{firstName}_{lastName}_{convertedBirthday}.json'):
                            print("Found Player")
                            # Open and read the JSON file
                            with open(f'{output_folder}\\movie_data\\{file}', 'r') as file:
                                json_object = json.load(file)
                                #formatted = json.dumps(json_object['cast'], indent=3)
                                json_apperances = json_object['cast']
                                for media in json_apperances:
                                    if(media['media_type'] == 'movie'):
                                        movies_amount += 1
                                    elif(media['media_type'] == 'tv'):
                                        tv_amount += 1
                                    else: 
                                        undefined += 1
                                
            
                
                entry = f'''INSERT INTO PLAYERS (
                FIRST_NAME ,LAST_NAME , BIRTHDAY , AMOUNT_MOVIES , AMOUNT_TV) VALUES 
                ("{firstName}","{lastName}","{convertedBirthday}","{movies_amount}","{tv_amount}")'''

                c.execute(entry)
            counter += 1

    #sql_query = """SELECT * FROM PLAYERS;"""
    conn.commit()
    #c.execute(sql_query)

    #print(len(c.fetchall()))



    conn.close()
    
    

    

task_one = BranchPythonOperator(
    task_id='check_database',
    dag=sql,
    python_callable=check_if_database,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)
    


task_two = PythonOperator(
    task_id='create_database',
    dag=sql,
    python_callable=create_database,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='none_failed_min_one_success',
    depends_on_past=False,
)


task_three = PythonOperator(
    task_id='populate_database',
    dag=sql,
    python_callable=populate_database,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='none_failed_min_one_success',
    depends_on_past=False,
)


end = DummyOperator(
    task_id='end',
    dag=sql,
    trigger_rule='none_failed'
)


task_one >> task_two >> task_three >> end
task_one >> task_three >> end 