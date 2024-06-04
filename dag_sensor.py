from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from azure.storage.blob import BlobServiceClient


azure_container = Variable.get('azure_container')
DB_URL = Variable.get('DB_URL')
azure_string = Variable.get('azure_string')
postgres_table_name = Variable.get('postgres_table_name')
prefixo = Variable.get('prefixo')


blob_service_client = BlobServiceClient.from_connection_string(azure_string)
container_client = blob_service_client.get_container_client(azure_container)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 4),
}

with DAG('dag_sensor', default_args=default_args, schedule_interval='*/30 * * * *', catchup=False) as dag:
    azure_wasb_prefix_sensor = WasbPrefixSensor(
        container_name='projeto-airflow',
        prefix='preco_acoes',
        task_id="azure_wasb_prefix_sensor"
    )

    trigger_dag = TriggerDagRunOperator(
        task_id='dag_transf_carregar',
        trigger_dag_id='dag_transf_carregar',
        poke_interval=10,
        dag=dag
    )

    azure_wasb_prefix_sensor >> trigger_dag