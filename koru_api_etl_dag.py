from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import json
from sqlalchemy import create_engine

API='MmSQRjLxBhlH0kcXnF6VvcF9R9Ducx_N'
url_base = 'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/'
#data = date.today()
data = '2024-05-21'


def obter_lista():
    resultado = requests.get(
        f'{url_base}{data}?adjusted=true&apiKey={API}'
    ).json()
    print('oi')
    print(resultado)
    return resultado


def salvar_arquivo(**kwargs):
    dados = kwargs['task_instance'].xcom_pull(task_ids='obter_lista')
    with open(f"preco_acoes.json", "w") as outfile:
        json.dump(dados, outfile)


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
    }
    )
    df['date_price'] = pd.to_datetime(df['date_price'], unit='ms')
    df['volume'] = df['volume'].astype(int)
    df['transactions'] = df['transactions'].astype(int)
    df['date_price'] = df['date_price'].dt.date
    dados = df
    return dados


def carregar(**kwargs):
    dados = kwargs['task_instance'].xcom_pull(task_ids='limpeza_arquivo')
    engine = create_engine(
        'postgresql://koru_j8mm_user:mVPYJRzo9Ve20CebTRI6pEbK3vSIldcL@dpg-cobdglv109ks738hlstg-a.oregon-postgres.render.com/koru_j8mm')
    dataframe.to_sql(dados, engine, index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'koru_api_etl_dag',
    default_args=default_args,
    description='Extrair dados de uma API, transformar e carregar no banco de dados POSTGRES',
    schedule=timedelta(days=1),
)


extract_task = PythonOperator(
    task_id='obter_lista',
    python_callable=obter_lista,
    dag=dag,
)

save_task = PythonOperator(
    task_id='salvar_arquivo',
    python_callable=salvar_arquivo,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='limpeza_arquivo',
    python_callable=limpeza_arquivo,
    dag=dag,
)

load_task = PythonOperator(
    task_id='carregar',
    python_callable=carregar,
    dag=dag,
)


extract_task >> save_task >> transform_task >> load_task
