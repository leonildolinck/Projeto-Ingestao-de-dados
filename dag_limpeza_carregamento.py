from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.azure.transfers.blob import BlobSensor
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import json
import pandas as pd
import ta
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

azure_container = 'projeto-airflow'
azure_string = "DefaultEndpointsProtocol=https;AccountName=projetokoru;AccountKey=ii99xnIASPtva3tw1Vx6CmVYQih8WGbVy+MHG6ACVofXHIZc1GmGZPX7uLPIW1hQRczVCb+eSpde+ASt+27lNQ==;EndpointSuffix=core.windows.net"
DB_URL = 'postgresql+psycopg2://koru_j8mm_user:mVPYJRzo9Ve20CebTRI6pEbK3vSIldcL@dpg-cobdglv109ks738hlstg-a.oregon-postgres.render.com/koru_j8mm'
postgres_table_name = 'tabela_polygon'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'azure_blob_to_postgres',
    default_args=default_args,
    description='DAG to load data from Azure Blob to Postgres, process it and store it',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

sensor_blob = BlobSensor(
    task_id='sensor_blob',
    blob_conn_id='azure_blob_default',
    container_name=azure_container,
    blob_name='preco_acoes*',
    wildcard_match=True,
    poke_interval=10,
    timeout=60,
    dag=dag
)

def limpeza_arquivo(**kwargs):
    file_path = "preco_acoes.json"
    with open(file_path, 'r') as f:
        data = json.load(f)

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
    })
    df['date_price'] = pd.to_datetime(df['date_price'], unit='ms')
    df['volume'] = df['volume'].astype(int)
    df['transactions'] = df['transactions'].astype(int)
    df['date_price'] = df['date_price'].dt.date
    df['sma_20'] = df['close_price'].rolling(window=20).mean()
    df['ema_20'] = df['close_price'].ewm(span=20, adjust=False).mean()
    df['rsi'] = ta.momentum.RSIIndicator(df['close_price'], window=14).rsi()
    bollinger = ta.volatility.BollingerBands(df['close_price'], window=20, window_dev=2)
    df['bollinger_high'] = bollinger.bollinger_hband()
    df['bollinger_low'] = bollinger.bollinger_lband()
    return df

limpeza_task = PythonOperator(
    task_id='limpeza_arquivo',
    python_callable=limpeza_arquivo,
    provide_context=True,
    dag=dag
)

def carregar(**kwargs):
    dados = kwargs['task_instance'].xcom_pull(task_ids='limpeza_arquivo')
    engine = create_engine(DB_URL)
    dados.to_sql(postgres_table_name, engine, if_exists='append', index=False)

carregar_task = PythonOperator(
    task_id='carregar',
    python_callable=carregar,
    provide_context=True,
    dag=dag
)

def excluir_blob(**kwargs):
    blob_name = 'preco_acoes.json'
    blob_hook = WasbHook(azure_conn_id='azure_blob_default')
    blob_hook.delete_blob(azure_container, blob_name)

excluir_blob_task = PythonOperator(
    task_id='excluir_blob',
    python_callable=excluir_blob,
    provide_context=True,
    dag=dag
)

sensor_blob >> limpeza_task >> carregar_task >> excluir_blob_task
