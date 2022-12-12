
from airflow import DAG
from airflow.models import taskinstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook


from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


import pandas as pd
import boto3
import os
from datetime import datetime
from datetime import timedelta

from utils.NLP_utils import preprocesamiento

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


def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('s3_connection')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name


def rename_file(ti, new_name: str) -> None:
    download_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(download_file_name[0].split('/')[:-1])
    os.rename(src=download_file_name[0], dst=f"{downloaded_file_path}/{new_name}")


def upload_data(file, table_name):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()
    df = pd.read_csv(f'/opt/airflow/data/{file}')
    df.to_sql(table_name=table_name, con=engine, if_exists= 'replace', index= False)
    

def separate_reviews():
    lang = ['es', 'en', 'pt']
    df = pd.read_csv('/opt/airflow/data/comentarios.csv', sep=';')
    for i in lang: 
        df.loc[(df.language == {i}), ]
        df = df.reset_index(drop=True)
        df.to_csv(f'/opt/airflow/data/{i}_data_raw.csv', index = False, sep=',')


def preprocess():
    lang = ['es', 'en', 'pt']
    lang_long = {'es': 'spanish',
                'en': 'english',
                'pt': 'portuguese'}
    for i in lang:
        df = pd.read_csv(f'/opt/airflow/data/{i}_data_raw.csv', sep=',')
        df['text_norm'] = df.text.apply(preprocesamiento, language = lang_long.get(i), pos_tag=False, remove_typos=False)
        df.to_csv(f'/opt/airflow/data/{i}_data_processed.csv', sep=',')



# Creo los DAGs de Airflow

# DAG para bajar archivo
with DAG(
    'atractivos',
    default_args=default_args,
    catchup=False

) as dag:
    # Download a file
    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'key': 'Santa Cruz/raw_data/atractivos_dashboard.csv',
            'bucket_name': 'tp-ml-bucket',
            'local_path': '/opt/airflow/data/'
        }
    )


    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'atractivos.csv'
        }
    )


    task_upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'file': 'atractivos.csv',
            'table_name': 'atractivos'
        }
    )

    task_download_from_s3 >> task_rename_file >> task_upload_data



# DAG de comentarios 

with DAG(
    'comentarios',
    default_args=default_args,
    catchup=False  # Catchup

) as dag:

    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'key': 'Santa Cruz/raw_data/comentarios_dashboard.csv',
            'bucket_name': 'tp-ml-bucket',
            'local_path': '/opt/airflow/data/'
        }
    )

    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'comentarios.csv'
        }
    )

    task_upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'file': 'comentarios.csv',
            'table_name': 'comentarios'
        }
    )


    task_separate_reviews = PythonOperator(
    task_id='separate_reviews',
    python_callable=separate_reviews
    )


    task_preprocess = PythonOperator(
    task_id='preprocess',
    python_callable=preprocess
    )

    task_download_from_s3 >> task_rename_file >> task_upload_data >> task_separate_reviews >> task_preprocess


 
'''
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