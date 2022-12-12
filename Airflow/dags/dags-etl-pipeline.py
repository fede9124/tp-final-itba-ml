
from airflow import DAG
from airflow.models import taskinstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

import pandas as pd
import boto3
import os
from datetime import datetime
from datetime import timedelta


#from NLP_utils import preprocesamiento


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


'''
def preprocess():
    df_esp = pd.read_csv('/opt/airflow/data/spanish_data.csv', sep=',')
    df_esp['text_norm'] = df_esp.text.apply(preprocesamiento, language = 'spanish', pos_tag=False, remove_typos=False)
'''

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

    task_download_from_s3 >> task_rename_file


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


    task_separate_reviews = PythonOperator(
    task_id='separate_reviews',
    python_callable=separate_reviews
    )


    task_download_from_s3 >> task_rename_file >> task_separate_reviews




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