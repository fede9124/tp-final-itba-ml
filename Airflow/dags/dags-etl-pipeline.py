from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
import boto3
import pandas as pd
import os


AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
KEY = 'Santa Cruz/raw_data/atractivos_dashboard.csv'
FILENAME = '/home/ubuntu/tp-final-itba-ml/Airflow/data/atractivos_dashboard_csv'



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}


def separate_reviews():
    df = pd.read_csv('/opt/airflow/data/comentarios_dashboard.csv', sep=';')
    
    df_esp = df.loc[(df.language == 'es'), ]
    df_esp = df_esp.reset_index(drop=True)
    df_esp.to_csv('/opt/airflow/data/spanish_data.csv', index = False, sep=',')

    df_eng = df.loc[(df.language == 'en'), ]
    df_eng = df_eng.reset_index(drop=True)
    df_eng.to_csv('/opt/airflow/data/english_data.csv', index = False, sep=',')

    df_pt = df.loc[(df.language == 'pt'), ]
    df_pt = df_pt.reset_index(drop=True)
    df_pt.to_csv('/opt/airflow/data/portuguese_data.csv', index = False, sep=',')


   
# DAG para separar comentarios

with DAG(
    "separate_reviews",
    default_args=default_args,
    catchup=False  # Catchup

) as dag:

    task_separate_reviews = PythonOperator(
    task_id='separate_reviews',
    python_callable=separate_reviews
    )






'''
def download_from_s3(Bucket, Key, Filename):
    print(AWS_ACCESS_KEY_ID)
    s3 = boto3.resource(
        "s3",
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
        )
    bucket = s3.Bucket(Bucket)
    bucket.download_file(Key, Filename)
    print(Key)


# Creo los DAGs de Airflow

'''




'''
# 1. Traer los datasets de S3 a la instancia de EC2"
with DAG(
    "download_from_s3",
    default_args=default_args,
    catchup=False  # Catchup

) as dag:

    task_download_from_s3 = PythonOperator(
    task_id='download_from_s3',
    python_callable=download_from_s3,
    op_kwargs={
        'Bucket': BUCKET_NAME,
        'Key': KEY,
        'Filename': FILENAME}
    )
  
  

# 2. Cargar los datasets en la base de datos de RDS


with DAG(
    "load_db",
    catchup=False  # Catchup
)   as dag:




# 3. Procesar el dataset de comentarios para obtener los datasets que alimentarán el modelo

with DAG(
    "etl",
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