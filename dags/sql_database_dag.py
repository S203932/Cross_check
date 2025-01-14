import airflow
import datetime
import sqlite3
import csv
import os
import json 
from sqlalchemy import create_engine
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

def populate_database_players(epoch: int, output_folder: str):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()  # this returns psycopg2.connect() object
    #c = conn.cursor()


    # Need to open csv file with all hockey players
    with open(f'{output_folder}/player_data.csv', mode ='r')as file:
        players = csv.reader(file)

        # skip first line
        counter = 0
        for lines in players:
            print(f'players: {lines}')
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
                for root, dirs, files in os.walk(f'{output_folder}/movie_data'):
                    for file in files:
                        #print(f'file:{file}')
                        # file is a string of the filename
                        if(file == f'{firstName}_{lastName}_{convertedBirthday}.json'):
                            movies_amount = 0
                            tv_amount = 0
                            undefined = 0
                            print("Found Player")
                            # Open and read the JSON file
                            with open(f'{output_folder}/movie_data/{file}', 'r') as file:
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
                                
            
                firstName = firstName.replace('\'',' ')
                lastName = lastName.replace('\'',' ')
                entry = f'INSERT INTO PLAYERS (FIRST_NAME ,LAST_NAME , BIRTHDAY , AMOUNT_MOVIES , AMOUNT_TV) VALUES (\'{firstName}\',\'{lastName}\',\'{convertedBirthday}\',\'{movies_amount}\',\'{tv_amount}\')'

                try:
                    hook.run(sql=entry)
                except:
                    print(f'Error with data, not entered')
            counter += 1

    #sql_query = """SELECT * FROM PLAYERS;"""
    conn.commit()
    #c.execute(sql_query)

    #print(len(c.fetchall()))
    conn.close()



def populate_database_others(epoch: int, output_folder: str):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    for file in os.listdir(f'{output_folder}/movie_data'):
        name = file
        print(name)
        name = name.split('_')
        date = name[2]
        date = date.split('.')
        date = date[0]
        # Open and read the JSON file
        with open(f'{output_folder}/movie_data/{file}', 'r') as file:
            movies = 0
            tv = 0
            df = hook.get_pandas_df(f'SELECT AMOUNT_MOVIES, AMOUNT_TV FROM PLAYERS WHERE FIRST_NAME = \'{name[0].replace("'","''")}\' AND LAST_NAME = \'{name[1].replace("'","''")}\' AND BIRTHDAY = \'{date}\'')
            for index, rows in df.iterrows():
                movies = rows.amount_movies
                tv = rows.amount_tv
            json_object = json.load(file)
            #formatted = json.dumps(json_object['cast'], indent=3)
            json_apperances = json_object['cast']
            for media in json_apperances:
                # If movie then entry into movie table
                if(media['media_type'] == 'movie'):
                    entry = f'INSERT INTO MOVIES (TITLE ,ORIGINAL_LANGUAGE , RELEASE , VOTE_AVERAGE , VOTE_COUNT) VALUES (\'{media['original_title'].replace('\'',' ').replace('"','')}\',\'{media['original_language'].replace('\'',' ').replace('"','')}\',\'{media['release_date']}\',\'{media['vote_average']}\',\'{media['vote_count']}\')'
                    try:
                        hook.run(sql=entry)
                    except:
                        print(f'Error with data, not entered')

                    # Adding it to the Character table
                    entry = f'INSERT INTO CREDITS (TITLE ,MEDIA_TYPE , RELEASE , CHARACTER_NAME , PLAYER_FIRSTNAME, PLAYER_LASTNAME, ORIGINAL_LANGUAGE, AMOUNT_MOVIES, AMOUNT_TV ) VALUES (\'{media['original_title'].replace('\'',' ')}\',\'{media['media_type']}\',\'{media['release_date']}\',\'{media['character'].replace('\'',' ')}\',\'{name[0].replace('\'',' ')}\',\'{name[1].replace('\'',' ')}\',\'{media['original_language'].replace('\'',' ')}\', \'{movies}\',\'{tv}\')'
                    try:
                        hook.run(sql=entry)
                    except:
                        print(f'Error with data, not entered')
                
                # If tv then entry into tv table
                else:
                    entry = f'INSERT INTO TV (TITLE ,ORIGINAL_LANGUAGE , RELEASE , VOTE_AVERAGE , VOTE_COUNT) VALUES (\'{media['original_name'].replace('\'',' ')}\',\'{media['original_language'].replace('\'',' ')}\',\'{media['first_air_date']}\',\'{media['vote_average']}\',\'{media['vote_count']}\')'
                    try:
                        hook.run(sql=entry)
                    except: 
                        print(f'Error with data, not entered')

                    # Adding it to the Character table
                    entry = f'INSERT INTO CREDITS (TITLE ,MEDIA_TYPE , RELEASE , CHARACTER_NAME , PLAYER_FIRSTNAME, PLAYER_LASTNAME, ORIGINAL_LANGUAGE, AMOUNT_MOVIES, AMOUNT_TV) VALUES (\'{media['original_name'].replace('\'',' ')}\',\'{media['media_type']}\',\'{media['first_air_date']}\',\'{media['character'].replace('\'',' ')}\',\'{name[0].replace('\'',' ')}\',\'{name[1].replace('\'',' ')}\',\'{media['original_language'].replace('\'',' ')}\', \'{movies}\',\'{tv}\')'
                    try:
                        hook.run(sql=entry)
                    except:
                        print(f'Error with data, not entered')


                
    
    conn.commit()
    conn.close()




task_populate_database_players = PythonOperator(
    task_id='populate_database_players',
    dag=sql,
    python_callable=populate_database_players,
    op_kwargs={
        "output_folder": "/opt/airflow/data",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='none_failed_min_one_success',
    depends_on_past=False,
)

task_populate_database_others = PythonOperator(
    task_id='populate_database_others',
    dag=sql,
    python_callable=populate_database_others,
    op_kwargs={
        "output_folder": "/opt/airflow/data",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='none_failed_min_one_success',
    depends_on_past=False,
)


task_create_players_table = PostgresOperator(
    task_id='create_players_table',
    dag=sql,
    postgres_conn_id='postgres_default',
    sql='DROP TABLE IF EXISTS PLAYERS; '\
        'CREATE TABLE IF NOT EXISTS PLAYERS ('\
        '   FIRST_NAME TEXT NOT NULL,'\
        '   LAST_NAME TEXT NOT NULL,'\
        '   BIRTHDAY DATE NOT NULL,'\
        '   AMOUNT_MOVIES INT NOT NULL,'\
        '   AMOUNT_TV INT NOT NULL,'\
        '   PRIMARY KEY(FIRST_NAME, LAST_NAME, BIRTHDAY));',
    trigger_rule='all_success',
    autocommit=True,
) 

task_create_movies_table = PostgresOperator(
    task_id='create_movies_table',
    dag=sql,
    postgres_conn_id='postgres_default',
    sql='DROP TABLE IF EXISTS MOVIES; '\
        'CREATE TABLE IF NOT EXISTS MOVIES( '\
        '   TITLE TEXT NOT NULL,'\
        '   ORIGINAL_LANGUAGE TEXT NOT NULL, '\
        '   RELEASE DATE NOT NULL,'\
        '   VOTE_AVERAGE REAL NOT NULL,'\
        '   VOTE_COUNT INTEGER NOT NULL,'\
        '   PRIMARY KEY(TITLE, RELEASE)); ',
    trigger_rule='all_success',
    autocommit=True,
    )

task_create_tv_table = PostgresOperator(
    task_id='create_tv_table',
    dag=sql,
    postgres_conn_id='postgres_default',
    sql='DROP TABLE IF EXISTS TV; '\
        'CREATE TABLE IF NOT EXISTS TV( '\
        '   TITLE TEXT NOT NULL,'\
        '   ORIGINAL_LANGUAGE TEXT NOT NULL, '\
        '   RELEASE DATE NOT NULL,'\
        '   VOTE_AVERAGE REAL NOT NULL,'\
        '   VOTE_COUNT INTEGER NOT NULL,'\
        '   PRIMARY KEY(TITLE, RELEASE)); ',
    trigger_rule='all_success',
    autocommit=True,
    )


task_create_credits_table = PostgresOperator(
    task_id='create_credits_table',
    dag=sql,
    postgres_conn_id='postgres_default',
    sql='DROP TABLE IF EXISTS CREDITS; '\
        'CREATE TABLE IF NOT EXISTS CREDITS( '\
        '   TITLE TEXT NOT NULL,'\
        '   MEDIA_TYPE TEXT NOT NULL,'\
        '   RELEASE DATE NOT NULL,'\
        '   CHARACTER_NAME TEXT NOT NULL,'\
        '   PLAYER_FIRSTNAME TEXT NOT NULL,'\
        '   PLAYER_LASTNAME TEXT NOT NULL,'\
        '   ORIGINAL_LANGUAGE TEXT NOT NULL, '\
        '   AMOUNT_MOVIES INT NOT NULL,'\
        '   AMOUNT_TV INT NOT NULL,'\
        '   PRIMARY KEY(TITLE, CHARACTER_NAME, PLAYER_FIRSTNAME, PLAYER_LASTNAME, RELEASE)); ',
    trigger_rule='all_success',
    autocommit=True,
    )


   
 

end = DummyOperator(
    task_id='end',
    dag=sql,
    trigger_rule='none_failed'
)



task_create_players_table >> task_create_movies_table >> task_create_tv_table >> task_create_credits_table 

task_create_credits_table  >> task_populate_database_players 

task_populate_database_players >> task_populate_database_others >> end



# Former stuff, might be useful later 


'''
def check_if_database(epoch: int, output_folder: str):
    conn = sqlite3.connect(f'{output_folder}\\hockey_entertainment.db')
    c = conn.cursor()
    listOfTables = c.execute("""SELECT * FROM sqlite_master WHERE type='table'
    ; """).fetchall()
    conn.close()

    print("Done with database check")

    if not listOfTables: 
        return 'create_database'
    else:
        return 'populate_database_players'
'''



'''
def create_database(epoch: int,output_folder: str):
    conn = sqlite3.connect(f'{output_folder}\\hockey_entertainment.db')
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
    RELEASE DATE,
    VOTE_AVERAGE REAL,
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
    ORIGINAL_LANGUAGE TEXT,
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
    FOREIGN KEY(TITLE) REFERENCES CHARACTER(TITLE)
    FOREIGN KEY(MEDIA_TYPE) REFERENCES CHARACTER(MEDIA_TYPE),
    FOREIGN KEY(RELEASE) REFERENCES CHARACTER(RELEASE),
    FOREIGN KEY(CHARACTER_NAME) REFERENCES CHARACTER(CHARACTER_NAME)
    );""")
    

    print(c.fetchall())

    conn.commit()
    conn.close()
'''


'''
task_one = BranchPythonOperator(
    task_id='check_database',
    dag=sql,
    python_callable=check_if_database,
    op_kwargs={
        "output_folder": "/opt/airflow/data",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)
'''
'''
task_one = PythonOperator(
    task_id='create_database',
    dag=sql,
    python_callable=create_database,
    op_kwargs={
        "output_folder": "/opt/airflow/data",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='none_failed_min_one_success',
    depends_on_past=False,
)
'''


'''
task_create_credits_table = PostgresOperator(
    task_id='create_credits_table',
    dag=sql,
    postgres_conn_id='postgres_default',
    sql='DROP TABLE IF EXISTS CREDITS; '\
        'CREATE TABLE IF NOT EXISTS CREDITS( '\
        '   PLAYER_FIRSTNAME TEXT NOT NULL,'\
        '   PLAYER_LASTNAME TEXT NOT NULL,'\
        '   TITLE TEXT NOT NULL,'\
        '   MEDIA_TYPE TEXT NOT NULL,'\
        '   RELEASE DATE NOT NULL,'\
        '   ORIGINAL_LANGUAGE TEXT NOT NULL, '\
        '   CHARACTER_NAME TEXT NOT NULL,'\
        '   PRIMARY KEY(PLAYER_FIRSTNAME, PLAYER_LASTNAME, TITLE, MEDIA_TYPE, RELEASE)'\
        #'   FOREIGN KEY(PLAYER_FIRSTNAME) REFERENCES PLAYERS(FIRST_NAME),'\
        #'   FOREIGN KEY(PLAYER_LASTNAME) REFERENCES PLAYERS(LAST_NAME),'\
        #'   FOREIGN KEY(TITLE) REFERENCES CHARACTER(TITLE),'\
        #'   FOREIGN KEY(MEDIA_TYPE) REFERENCES CHARACTER(MEDIA_TYPE),'\
        #'   FOREIGN KEY(RELEASE) REFERENCES CHARACTER(RELEASE),'\
        #'   FOREIGN KEY(CHARACTER_NAME) REFERENCES CHARACTER(CHARACTER_NAME)',
        ');',
    trigger_rule='all_success',
    autocommit=True,
    )
'''


'''
task_populate_database_combine = PythonOperator(
    task_id='populate_database_combine',
    dag=sql,
    python_callable=populate_database_combine,
    op_kwargs={
        "output_folder": "/opt/airflow/data",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='none_failed_min_one_success',
    depends_on_past=False,
)
'''


#task_one >> task_two >> task_three >> task_four >> end
#task_one >> task_three >> task_four >> end 