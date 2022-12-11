from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}


def download_from_s3(key: str, bucket_name: str, local_path: str):
    hook = S3Hook('s3_conn')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name


# Creo los DAGs de Airflow
# 1. Traer los datasets de S3 a la instancia de EC2"

with DAG(
    "download_from_s3",
    start_date=datetime(2022, 12, 31), # Fecha de inicio el 31 de diciembre de 2022
    schedule_interval=None,  # Sin actualización programada
    catchup=False  # Catchup
) as dag:

    task_1_download_from_s3 = PythonOperator(
    task_id='download_from_s3',
    pyhton_callable=download_from_s3,
    op_kwargs={
        'key': 'atractivos_dashboard.csv'
        'local_path': '/home/ubuntu/tp-final-itba-ml/'
    }

  )


'''
# 2. Cargar los datasets en la base de datos de RDS


with DAG(
    "load_db",
    start_date=datetime(2022, 12, 31), # Fecha de inicio el 31 de diciembre de 2022
    schedule_interval=None,  #Sin actualización programada
    catchup=False  # Catchup
)   as dag:




# 3. Procesar el dataset de comentarios para obtener los datasets que alimentarán el modelo

with DAG(
    "etl",
    start_date=datetime(2022, 12, 31), # Fecha de inicio el 31 de diciembre de 2022
    schedule_interval=None,  #Sin actualización programada
    catchup=False  # Catchup
)   as dag:


# 4. Ejecutar el modelo

with DAG(
    "ML",
    start_date=datetime(2022, 12, 31), # Fecha de inicio el 31 de diciembre de 2022
    schedule_interval=None,  #Sin actualización programada
    catchup=False  # Catchup
)   as dag:


'''