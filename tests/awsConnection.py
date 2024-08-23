import boto3
import json
import requests
import boto3.session
from dotenv import load_dotenv
import os

load_dotenv()

session = boto3.Session(
    aws_access_key_id = os.getenv('aws_access_key'),
    aws_secret_access_key = os.getenv('aws_secret_key'),
    region_name = os.getenv('aws_region')
)

s3 = session.resource('s3')

response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=401520150')

json_data = response.json()

file_data = json.dumps(json_data)

bucket_name = os.getenv('cfb_s3_bucket')

object = s3.Object(bucket_name, '401520150.json')

result = object.put(Body = json_data)

print("File uploaded successfully")