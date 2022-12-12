import boto3
import os
from import datetime

## VER COMO PONER LAS ACCESS KEY
aws_access_key_id = os.environ.get('')
aws_secret_access_key = os.environ.get('')



BUCKET_NAME = os.environ.get('')
FILE_NAME_1 = os.environ.get('')
FILE_NAME_2 = os.environ.get('')



#def ()

s3 = boto3.client('s3', aws_access_key_id=... , aws_secret_access_key=...)


s3.download_file('BUCKET_NAME', 'OBJECT_NAME', f'/home/ubuntu/data/{FILE_NAME_1}')
s3.download_file('BUCKET_NAME', 'OBJECT_NAME', f'/home/ubuntu/data/{FILE_NAME_2}')

