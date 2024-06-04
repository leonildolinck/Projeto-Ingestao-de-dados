from datetime import datetime, timedelta
import requests
import json
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine



azure_container='projeto-airflow'
API_KEY = Variable.get('API_KEY')
API_URL = Variable.get('API_URL')
DB_URL = Variable.get('DB_URL') 
azure_string = Variable.get('azure_string')
postgres_table_name = Variable.get('postgres_table_name')
prefixo = Variable.get('prefixo')
dias = 90


def consulta_db(engine):
    query = "SELECT DISTINCT date_price FROM tabela_polygon"
    resultado_query = engine.execute(query)
    return [row[0] for row in resultado_query.fetchall()]


def consulta_api(data, api_key):
    url = f"{API_URL}{data}?adjusted=true&apiKey={API_KEY}"
    headers = {'apiKey': API_KEY}
    resultado = requests.get(url, headers=headers)
    resultado.raise_for_status()
    return resultado.json()


def upload_bucket(data, bucket, arquivo):
    blob_service_client = BlobServiceClient.from_connection_string(azure_string)
    blob_client = blob_service_client.get_blob_client(container=bucket, blob=arquivo)
    if blob_client.exists():
        print(f"Arquivo {arquivo} já existe no bucket. Pulando para o próximo.")
        return
    blob_client.upload_blob(json.dumps(data))


def obter_lista(*args):
    engine = create_engine(DB_URL)
    connection = engine.connect()

    try:
        lista_datas = consulta_db(engine)
        data_atual = datetime.utcnow().date()
        data_atual = data_atual - timedelta(days=1)

        for data in (data_atual - timedelta(days=i) for i in range(dias)):
            if data in lista_datas:
                print(f"Dia {data} já registrado no database. Pulando para a próxima data.")
                continue

            time.sleep(15)
            resultado = consulta_api(data, API_KEY)

            if resultado.get('queryCount', 0) == 0:
                print(f"Sem registros para o dia {data}.")
                continue

            blob_name = f"preco_acoes-{data}.json"
            upload_bucket(resultado, azure_container, blob_name)
            print(f"Arquivo enviado com sucesso para Azure Blob Storage: {blob_name}")

    finally:
        connection.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_consulta_api',
    default_args=default_args,
    description='Extrair dados de uma API, transformar e carregar no banco de dados POSTGRES',
    schedule=timedelta(days=1),
    max_active_runs=1,
)

extract_task = PythonOperator(
    task_id='obter_lista',
    python_callable=obter_lista,
    dag=dag,
)