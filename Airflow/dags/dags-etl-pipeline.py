
from airflow import DAG
from airflow.models import taskinstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import boto3
from datetime import datetime
from datetime import timedelta
from utils.NLP_ML_esp import cleaning
from utils.NLP_ML_esp import stoplist
from utils.NLP_ML_esp import z_score_model


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
    from airflow.hooks.S3_hook import S3Hook
    hook = S3Hook('s3_connection')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name


def rename_file(ti, new_name: str) -> None:
    import os
    download_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(download_file_name[0].split('/')[:-1])
    os.rename(src=download_file_name[0], dst=f"{downloaded_file_path}/{new_name}")


def upload_data(file, table_name):
    from airflow.hooks.postgres_hook import PostgresHook
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Engine
    import pandas as pd
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()
    df = pd.read_csv(f'/opt/airflow/data/{file}')
    df.to_sql(name=table_name, con=engine, if_exists= 'replace', index= False)
    

def separate_reviews():
    import pandas as pd
    lang = ['es', 'en', 'pt']
    df = pd.read_csv('/opt/airflow/data/comentarios_nlp.csv', sep=',')
    for i in lang: 
        df = df.loc[(df.language == i), ]
        df = df.reset_index(drop=True)
        df.to_csv(f'/opt/airflow/data/{i}_comentarios_raw.csv', index = False, sep=',')


def preprocess():
    from utils.NLP_utils import preprocesamiento
    import pandas as pd
    lang = ['es', 'en', 'pt']
    lang_long = {'es': 'spanish',
                'en': 'english',
                'pt': 'portuguese'}
    for i in lang:
        df = pd.read_csv(f'/opt/airflow/data/{i}_comentarios_raw.csv', sep=',')
        df['text_norm'] = df.text.apply(preprocesamiento, language = lang_long.get(i), pos_tag=False, remove_typos=False)
        df.to_csv(f'/opt/airflow/data/{i}_comentarios_processed.csv', sep=',')



# Creo los DAGs de Airflow

# Tareas sobre atractivos
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
            'new_name': 'atractivos_dashboard.csv'
        }
    )

    task_upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'file': 'atractivos_dashboard.csv',
            'table_name': 'atractivos'
        }
    )

    task_download_from_s3 >> task_rename_file >> task_upload_data


# Tareas sobre valoraciones

with DAG(
    'valoraciones',
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
            'new_name': 'comentarios_dashboard.csv'
        }
    )

    task_upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'file': 'comentarios_dashboard.csv',
            'table_name': 'valoracion'
        }
    )

    task_download_from_s3 >> task_rename_file >> task_upload_data

# Tareas sobre comentarios

with DAG(
    'comentarios',
    default_args=default_args,
    catchup=False  # Catchup

) as dag:

    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'key': 'Santa Cruz/raw_data/comentarios_nlp.csv',
            'bucket_name': 'tp-ml-bucket',
            'local_path': '/opt/airflow/data/'
        }
    )

    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'comentarios_nlp.csv'
        }
    )

    task_upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'file': 'comentarios_nlp.csv',
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


 

# Modelos NLP sobre comentarios en español

with DAG(
    'NLP_español',
    default_args=default_args,
    catchup=False  # Catchup

) as dag:

    task_cleaning = PythonOperator(
        task_id='cleaning',
        python_callable=cleaning,
    )

    task_stoplist = PythonOperator(
        task_id='stopwords',
        python_callable=stoplist,
    )

    task_z_score_model = PythonOperator(
        task_id='z_score_model',
        python_callable=z_score_model,
    )

    task_cleaning >> task_stoplist >> task_z_score_model

