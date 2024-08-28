import json
from datetime import datetime
import pytz
from postgres_driver import PostgresDatabaseDriver
from awsConnect import connectS3, getBucketName, getJsonFileBody


driver = PostgresDatabaseDriver()


def insert_schedule_data(driver, schedules, filename, etl_ts):
    try:
        insert_statement = """
            INSERT INTO landing.raw_schedule (sched_json, filename, etl_ts)
            VALUES (%s, %s, %s)
        """
        for event in schedules:
            event_json = json.dumps(event)
            driver.execute(insert_statement, (event_json, filename, etl_ts))
        driver.commit()
    except Exception as e:
        print("Error inserting events:", e)
        driver.conn.rollback()


def load_schedule_files_to_database():
    s3 = connectS3()
    bucket = getBucketName('cfb_s3_bucket')
    folder = 'schedules/'

    try:
        file_list = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
        eastern_tz = pytz.timezone('US/Eastern')

        if 'Contents' in file_list:
            for file in file_list['Contents']:
                key = file['Key']
                if not key.endswith('/'):
                    data = getJsonFileBody(s3, bucket, key)

                    events = data.get('events', [])

                    if events:
                        ts = datetime.now(eastern_tz)
                        etl_ts = ts.isoformat()
                        insert_schedule_data(driver, events, key, etl_ts)
                        print(f"[{key}] uploaded to database")
                    else:
                        print(f"No events found in [{key}]")
        else:
            print("No contents found")
            return
    except Exception as e:
        print("Error loading files to database:", e)
    finally:
        driver.close()


if __name__ == "__main__":
    load_schedule_files_to_database()