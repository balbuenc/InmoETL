from airflow import DAG
from datetime import datetime, timedelta
import requests
import json


import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'INMO',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}



@dag('INMO_fracciones_ETL',default_args=default_args, schedule_interval=timedelta(days=3), start_date=days_ago(15), tags=['Fracciones_ETL'])
def etl_inmo_fracciones():
    @task()
    def extract():
        result = requests.get(
        "http://inmo.opencloudpy.com:5000/api/fracciones",
        data=json.dumps("{}"))
        return result.text

    @task()
    def transform(data):
        df = pd.read_json(data)
        filtered_df = df[df['iD_FRACCION'] > 124]
        return filtered_df.to_json()

    @task()
    def load(data):
        df = pd.read_json(data)
        now = datetime.now()
        filename = f"fracciones_{now.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        filename = "/home/azureuser/csv/" + filename
        print(filename)
        df.to_csv(filename)



    inmo_fracciones = extract()
    transformed_inmo_fracciones = transform(inmo_fracciones)
    load(transformed_inmo_fracciones)

etl_inmo_fracciones_dag = etl_inmo_fracciones()





