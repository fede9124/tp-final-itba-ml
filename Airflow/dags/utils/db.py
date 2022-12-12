import pandas as pd
from sqlalchemy import create_engine

host=''
port='5432'
user='postgres'
password=''

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/postgres')
# engine = create_engine('postgresql://postgres:postgres@creditcardclientattrition.cgxic12usgz2.us-east-1.rds.amazonaws.com
# :5432/postgres')
print('engine created')

#read model_database/raw_data/BankChurners.csv.zip to a pandas dataframe
df = pd.read_csv('model_database/raw_data/BankChurners.csv.gz', compression='gzip',delimiter=',', quotechar='"')
print('dataframe created')



print('columns renamed')
#create table
df.dtypes
df.to_sql('credit_card_clients', engine, if_exists='replace', index=False)






from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

engine = create_engine("postgres://localhost/mydb")
if not database_exists(engine.url):
    create_database(engine.url)

print(database_exists(engine.url))