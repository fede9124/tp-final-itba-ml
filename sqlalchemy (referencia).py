import csv
import boto3

from io import StringIO
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Date,  Integer, String
engine = create_engine('postgresql://<user>:<password>@<host>:<port>/<db>')

table_name = 'checking'
meta = MetaData()

table_name = Table(
   table_name, meta,
   Column('phone', BigInteger, primary_key = True),
   Column('first_name', String),
   schema = 'testing'
)

meta.create_all(engine)