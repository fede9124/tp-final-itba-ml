from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta
import boto3

AWS_ACCESS_KEY_ID = 'AKIAYZ3EH2CA3B2EDMXT'
AWS_SECRET_ACCESS_KEY = '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'

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


def download_from_s3(Bucket, Key, Filename):
    s3 = boto3.resource(
        "s3",
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    bucket = s3.Bucket(Bucket)
    bucket.download_file(Key, Filename)


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
    python_callable=download_from_s3,
    op_kwargs={
        'Bucket': 'tp-ml-bucket',
        'Key': 'Santa Cruz/raw_data/atractivos_dashboard.csv',
        'Filename': '/home/ubuntu/tp-final-itba-ml/Airflow/data/atractivos_dashboard_csv'
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