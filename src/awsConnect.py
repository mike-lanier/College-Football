import boto3
from dotenv import load_dotenv
import os


def connectS3():
    load_dotenv()

    s3 = boto3.client(
        's3',
        aws_access_key_id = os.getenv('aws_access_key'),
        aws_secret_access_key = os.getenv('aws_secret_key'),
        region_name = os.getenv('aws_region')
    )
    return s3

def writeToBucket(file_data, file_name, bucket_env_var, folder_name=None):
    s3 = connectS3()
    bucket = os.getenv(bucket_env_var)
    
    if folder_name:
        file_name = folder_name + file_name
    else:
        file_name

    s3.put_object(Bucket=bucket, Key=file_name, Body=file_data)

