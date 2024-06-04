from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


azure_container='projeto-airflow'
DB_URL = Variable.get('DB_URL')
azure_string = Variable.get('azure_string')
postgres_table_name = Variable.get('postgres_table_name')
prefixo = Variable.get('prefixo')
azure_xml = Variable.get('azure_xml')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def consulta_db(engine):
    query = "SELECT DISTINCT date_price FROM tabela_polygon"
    resultado_query = engine.execute(query)
    return [row[0].strftime('%Y-%m-%d') for row in resultado_query.fetchall()]


def lista_blobs():
    url = azure_xml
    resultado = requests.get(url)
    resultado.raise_for_status()
    root = ET.fromstring(resultado.content)
    lista_blobs = [blob.find('Name').text 
                   for blob in root.findall('Blobs/Blob')]
    return lista_blobs


def transformar(**kwargs):
    engine = create_engine(DB_URL) 
    lista_datas = consulta_db(engine)
    lista_blobs = kwargs['task_instance'].xcom_pull(task_ids='lista_blobs')
    lista_dfs = []
    for blob in lista_blobs:
        if blob.startswith('preco_acoes'):
            if blob[12:22] not in lista_datas:
                data = requests.get(f'https://projetokoru.blob.core.windows.net/projeto-airflow/{blob}').json()
                df = pd.DataFrame(data['results'])
                df = df.fillna(0)
                df = df.rename(columns={
                    'T': 'stock',
                    'v': 'volume',
                    'vw': 'avg_volume',
                    'o': 'open_price',
                    'c': 'close_price',
                    'h': 'high_price',
                    'l': 'low_price',
                    't': 'date_price',
                    'n': 'transactions'
                }
                )
                df['date_price'] = pd.to_datetime(df['date_price'], unit='ms')
                df['volume'] = df['volume'].astype(int)
                df['transactions'] = df['transactions'].astype(int)
                df['date_price'] = df['date_price'].dt.date
                df['load_date'] = datetime.now()
                lista_dfs.append(df)
                print('Arquivo transformado com sucesso!')
        else:
            print('Nenhum blob encontrado com o prefixo especificado.')
    return lista_dfs, lista_datas


def carregar(**kwargs):    
    lista_dfs, lista_datas = kwargs['task_instance'].xcom_pull(task_ids='transformar')       
    engine = create_engine(DB_URL)    
    for df in lista_dfs:
        df.to_sql(postgres_table_name, engine, if_exists='append', index=False)
        print('Dados carregados com sucesso!')
        time.sleep(5)
    return lista_dfs


dag = DAG(
    'dag_transf_carregar',
    default_args=default_args,
    description='DAG para transformar e carregar um blob no banco de dados POSTGRES',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 6, 4),
    catchup=False,
)

lista_blobs = PythonOperator(
    task_id='lista_blobs',
    python_callable=lista_blobs,
    provide_context=True,
    dag=dag
)

transformar = PythonOperator(
    task_id='transformar',
    python_callable=transformar,
    provide_context=True,
    dag=dag
)

carregar = PythonOperator(
    task_id='carregar',
    python_callable=carregar,
    provide_context=True,
    dag=dag
)

lista_blobs >> transformar >> carregar