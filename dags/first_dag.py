import airflow
import datetime
import urllib.request 
from pprint import pprint
from html_table_parser.parser import HTMLTableParser
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

first_dag = DAG(
    dag_id='first_dag',
    default_args=default_args_dict,
    catchup=False,
)

# Fetching the content from the website 
def url_get_contents(url):

    # Need headers as most sites will respond with 403 if not
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5'}

    #headers={'User-Agent': 'Mozilla/5.0'}


    #making request to the website
    req = urllib.request.Request(url=url, headers=headers)
    f = urllib.request.urlopen(req)

    #reading contents of the website
    return f.read()




def _get_NHL_Players(epoch: int,output_folder: str):

    # Link to website with the players
    xhtml = url_get_contents('https://www.hockeydb.com/ihdb/players/player_ind_b.html').decode('utf-8')

    # Defining the HTMLTableParser, which will scrape the site
    p = HTMLTableParser()

    # Feeding the data into the parser
    p.feed(xhtml)

    #Obtaining data from parser
    #pprint(p.tables[0])

    # Converting data into a pandas data frame
    print("\n\nPANDAS DATAFRAME\n")
    print(pd.DataFrame(p.tables[0]))

    table = pd.DataFrame(p.tables[0])

    #Save to the retrieved table to a csv
    table.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_unfiltered_players.csv')
    





task_one = PythonOperator(
    task_id='get_spreadsheet',
    dag=first_dag,
    python_callable=_get_NHL_Players,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
        "url": "https://www.hockeydb.com/ihdb/players/player_ind_a.html"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

end = DummyOperator(
    task_id='end',
    dag=first_dag,
    trigger_rule='none_failed'
)


task_one >> end

#>> task_two >> task_three >> task_four >> task_five
