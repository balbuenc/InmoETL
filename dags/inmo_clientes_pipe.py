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



@dag('INMO_Clientes_ETL',default_args=default_args, schedule_interval=timedelta(days=3), start_date=days_ago(15), tags=['Clientes_ETL'])
def etl_inmo_clientes():
    @task()
    def extract():
        result = requests.get(
        "http://inmo.opencloudpy.com:5000/api/clientes",
        data=json.dumps("{}"))
        return result.text

    @task()
    def transform(data):
        df = pd.read_json(data)
        filtered_df = df[df['sexo'] == 'M']
        return filtered_df.to_json()
    
    @task()
    def group(data):
        df = pd.read_json(data)
        grouped_df = df.groupby("barrio")["estado"].count()
        print(grouped_df)
        return grouped_df.to_json()
    

    @task()
    def load(data):
        df = pd.read_json(data)
        now = datetime.now()
        filename = f"clientes_{now.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        filename = "/home/azureuser/csv/" + filename
        print(filename)
        df.to_csv(filename)



    inmo_clientes = extract()
    transformed_inmo_clientes = transform(inmo_clientes)
    grouped_clientes = group(transformed_inmo_clientes)
    load(grouped_clientes)

etl_inmo_clientes_dag = etl_inmo_clientes()





