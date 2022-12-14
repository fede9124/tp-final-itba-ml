
from airflow import DAG
from airflow.models import taskinstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import boto3
from datetime import datetime
from datetime import timedelta



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


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    from airflow.hooks.S3_hook import S3Hook
    hook = S3Hook('s3_connection')
    file_name = hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


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


def clean_es():
    import pandas as pd
    df = pd.read_csv(f'/opt/airflow/data/es_comentarios_processed.csv', sep=',')
    df['text_norm'] = df['text_norm'].str.replace(r'cabo virg??n', 'cabo virgenes')
    df['text_norm'] = df['text_norm'].str.replace(r' recomeir ', ' recomendar ')
    df['text_norm'] = df['text_norm'].str.replace(r'^recomeir ', 'recomendar ')
    df['text_norm'] = df['text_norm'].str.replace(r' excursi??n ', ' excursion ')
    df['text_norm'] = df['text_norm'].str.replace(r'^excursi??n ', 'excursion ')
    df['text_norm'] = df['text_norm'].str.replace(r' increibl ', ' increible ')
    df['text_norm'] = df['text_norm'].str.replace(r'^increibl ', 'increible ')
    df['text_norm'] = df['text_norm'].str.replace(r' paisaj ', ' paisaje ')
    df['text_norm'] = df['text_norm'].str.replace(r'^paisaj ', 'paisaje ')
    df['text_norm'] = df['text_norm'].str.replace(r' t??n ', ' tener ')
    df['text_norm'] = df['text_norm'].str.replace(r'^t??n ', 'tener ')
    df['text_norm'] = df['text_norm'].str.replace(r' pod ', ' poder ')
    df['text_norm'] = df['text_norm'].str.replace(r'^pod ', 'poder ')
    df['text_norm'] = df['text_norm'].str.replace(r' perdertir ', ' perder ')
    df['text_norm'] = df['text_norm'].str.replace(r'^perdertir ', 'perder ')
    df['text_norm'] = df['text_norm'].str.replace(r' magall??n ', ' magallanes ')
    df['text_norm'] = df['text_norm'].str.replace(r'^magall??n ', 'magallanes ')
    df['text_norm'] = df['text_norm'].str.replace(r' escursion ', ' excursion ')
    df['text_norm'] = df['text_norm'].str.replace(r'^escursion ', 'excursion ')
    df['text_norm'] = df['text_norm'].str.replace(r' confiterio ', ' confiteria ')
    df['text_norm'] = df['text_norm'].str.replace(r'^confiterio ', 'confiteria ')
    df['text_norm'] = df['text_norm'].str.replace(r' chaltir ', ' chalten ')
    df['text_norm'] = df['text_norm'].str.replace(r'^chaltir ', 'chalten ')
    df['text_norm'] = df['text_norm'].str.replace(r' chaltar ', ' chalten ')
    df['text_norm'] = df['text_norm'].str.replace(r'^chaltar ', 'chalten ')
    df['text_norm'] = df['text_norm'].str.replace(r' olvir ', ' olvidar ')
    df['text_norm'] = df['text_norm'].str.replace(r'^olvir ', 'olvidar ')
    df['text_norm'] = df['text_norm'].str.replace(r' unicar ', ' unica ')
    df['text_norm'] = df['text_norm'].str.replace(r'^unicar ', 'unica ')
    df['text_norm'] = df['text_norm'].str.replace(r' bellisir ', ' bellisimo ')
    df['text_norm'] = df['text_norm'].str.replace(r'^bellisir ', 'bellisimo ')
    df['text_norm'] = df['text_norm'].str.replace(r' aprece??r ', ' apreciar ')
    df['text_norm'] = df['text_norm'].str.replace(r'^aprece??r ', 'apreciar ')
    df['text_norm'] = df['text_norm'].str.replace(r' inigualabl ', ' inigualable ')
    df['text_norm'] = df['text_norm'].str.replace(r'^inigualabl ', 'inigualable ')
    df['text_norm'] = df['text_norm'].str.replace(r' crist??n ', ' cristina ')
    df['text_norm'] = df['text_norm'].str.replace(r'^crist??n ', 'cristina ')
    df['text_norm'] = df['text_norm'].str.replace(r' gana ', ' ganas ')
    df['text_norm'] = df['text_norm'].str.replace(r'^gana ', 'ganas ')
    df.to_csv(f'/opt/airflow/data/es_comentarios_processed_clean.csv', sep=',')


def z_score_model_es():
    from utils.NLP_ML_esp import stoplist
    from utils.NLP_ML_esp import z_score_monroe_es
    import time
    import pandas as pd
    df = pd.read_csv(f'/opt/airflow/data/es_comentarios_processed_clean.csv', sep=',')
    t0 = time.time()
    ZScore = z_score_monroe_es(df, 'target', 'text_norm', 1, None, 10, stoplist())
    t1 = time.time()
    print('Took', (t1 - t0)/60, 'minutes')
    ZScore.to_csv(f'/opt/airflow/data/palabras_divisorias_es.csv', index=False)

'''

def bigrams_model():
    from utils.NLP_ML_esp import bigrams
    import pandas as pd
    df = pd.read_csv(f'/opt/airflow/data/es_comentarios_processed_clean.csv', sep=',')
    trainset = (df.text_norm.str.split(' ')).to_list()



def embeddings():
    import pandas as pd
    

'''

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




# Modelos NLP sobre comentarios en espa??ol

with DAG(
    'NLP_espa??ol',
    default_args=default_args,
    catchup=False  # Catchup

) as dag:

    task_cleaning = PythonOperator(
        task_id='cleaning',
        python_callable=clean_es,
    )

    task_z_score_model = PythonOperator(
        task_id='z_score_model',
        python_callable=z_score_model_es,
    )


    task_upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'file': 'palabras_divisorias_es.csv',
            'table_name': 'palabras_divisorias_es'
        }
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/opt/airflow/data/palabras_divisorias_es.csv',
            'key': 'Santa Cruz/processed_data/palabras_divisorias_es.csv',
            'bucket_name': 'tp-ml-bucket'
        }
    )

    task_cleaning >> task_z_score_model >> task_upload_data >> task_upload_to_s3

