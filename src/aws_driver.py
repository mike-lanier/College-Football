import boto3
from dotenv import load_dotenv
import os
import json


class AWSOps:
    @staticmethod
    def connectS3():
        load_dotenv()

        s3 = boto3.client(
            's3',
            aws_access_key_id = os.getenv('aws_access_key'),
            aws_secret_access_key = os.getenv('aws_secret_key'),
            region_name = os.getenv('aws_region')
        )
        return s3

    @staticmethod
    def getBucketName(bucket_env_var):
        load_dotenv()
        return os.getenv(bucket_env_var)

    @staticmethod
    def fileObjToString(file_obj):
        return file_obj['Body'].read().decode('utf-8')

    @staticmethod
    def getJsonFileBody(s3_conn, bucket, key):
            file_obj = s3_conn.get_object(Bucket=bucket, Key=key)
            body = AWSOps.fileObjToString(file_obj)
            data = json.loads(body)
            return data