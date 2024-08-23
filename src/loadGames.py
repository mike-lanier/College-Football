import os
import json
from datetime import datetime
import pytz
import boto3
from dotenv import load_dotenv
from postgres_driver import PostgresDatabaseDriver


driver = PostgresDatabaseDriver()


def insertGameData(driver, games, filename, etl_ts):
    try:
        insert_statement = """
            INSERT INTO landing.raw_plays (play_json, filename, etl_ts)
            VALUES (%s, %s, %s)
        """
        for drive in games:
            for play in drive['plays']:
                play_json = json.dumps(play)
                driver.execute(insert_statement, (play_json, filename, etl_ts))
        driver.commit()
    except Exception as e:
        print("Error inserting events:", e)
        driver.conn.rollback()


def S3FilesToPostgres():
    if driver is None:
        return
    
    load_dotenv()

    s3 = boto3.client(
        's3',
        aws_access_key_id = os.getenv('aws_access_key'),
        aws_secret_access_key = os.getenv('aws_secret_key'),
        region_name = os.getenv('aws_region')
    )

    bucket_name = os.getenv('cfb_s3_bucket')
    file_list = s3.list_objects_v2(Bucket = bucket_name)

    try:
        if 'Contents' in file_list:
            eastern_tz = pytz.timezone('US/Eastern')
            ts = datetime.now(eastern_tz)
            etl_ts = ts.isoformat()

            for file in file_list['Contents']:
                key = file['Key']
                file_obj = s3.get_object(Bucket = bucket_name, Key = key)
                json_string = file_obj['Body'].read().decode('utf-8')
                data = json.loads(json_string)
                plays = data['drives']['previous']

                if plays:
                    insertGameData(driver, plays, key, etl_ts)
                    s3.delete_object(Bucket = bucket_name, Key = key)
                    print("Data transfer successful")
                else:
                    print(f"No plays found in {key}")
    finally:
        driver.close()



if __name__ == '__main__':
    S3FilesToPostgres()