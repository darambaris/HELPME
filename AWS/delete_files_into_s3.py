### How to delete files into s3 (with a common substring in their names) using boto3 library
import boto3
import datetime

AWS_ACCESS_KEY_ID= '__YOUR_ACCESS_KEY_ID__'
AWS_SECRET_ACCESS_KEY = '__YOUR_SECRET_ACCESS_KEY'
AWS_SESSION_TOKEN = '__YOUR_SESSION_TOKEN__'
S3_BUCKET_NAME = '__BUCKET_NAME__'
FILES_PATH = '__FILES_PATH__'

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

s3 = session.resource('s3')

string_start = '__YOUR_STRING_FILTER__'
bucket = s3.Bucket(S3_BUCKET_NAME)
key = FILES_PATH # common path for files. It can contain folders

for my_bucket_object in bucket.objects.filter(Prefix=key):
    
    # if the file contains string_start in its name
    if string_start in str(my_bucket_object.key):

        # print path and file name.
        print(f'deleting {str(my_bucket_object.key)}')
        
        # delete file
        my_bucket_object.delete()