
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


endpoint = ''


engine = create_engine(f'postgresql://postgres:{endpoint}@:5432/db-atractivos-comentarios')
if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))

files = ['atractivos.csv', 'comentarios.csv']

for i in files:
    df = pd.read_csv(f'/opt/airflow/data/{i}')
    df.to_sql({i}, engine, if_exists= 'replace', index= False)

