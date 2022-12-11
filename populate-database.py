
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://scott:tiger@localhost:5432/mydatabase")

df = pd.read_csv(atractivos)
df = pd.read_csv(atractivos)

df.to_sql('comentarios', engine, if_exists= 'replace', index= False)
df.to_sql('atractivos', engine, if_exists= 'replace', index= False)
