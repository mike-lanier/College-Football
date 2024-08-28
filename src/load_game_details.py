import json
from datetime import datetime
import pytz
from awsConnect import connectS3, getBucketName, getJsonFileBody
from postgres_driver import PostgresDatabaseDriver


driver = PostgresDatabaseDriver()


def insert_game_data(driver, games, filename, etl_ts):
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


def insert_team_data(driver, teams, filename, etl_ts):
    try:
        insert_statement = """
            INSERT INTO landing.raw_teams (team_json, filename, etl_ts)
            VALUES (%s, %s, %s)
        """
        for team in teams:
            team_json = json.dumps(team)
            driver.execute(insert_statement, (team_json, filename, etl_ts))
        driver.commit()
    except Exception as e:
        print("Error inserting events:", e)
        driver.conn.rollback()


def load_game_details_to_database():
    s3 = connectS3()
    bucket = getBucketName('cfb_s3_bucket')
    folder = 'games/'

    try:
        file_list = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
        eastern_tz = pytz.timezone('US/Eastern')

        if 'Contents' in file_list:
            for file in file_list['Contents']:
                key = file['Key']
                if not key.endswith('/'):
                    data = getJsonFileBody(s3, bucket, key)

                    teams = data['boxscore']['teams']
                    plays = data['drives']['previous']

                    if teams:
                        ts = datetime.now(eastern_tz)
                        etl_ts = ts.isoformat()
                        insert_team_data(driver, teams, key, etl_ts)
                        print(f"Teams in file [{key}] uploaded successfully")
                    else:
                        print(f"No teams found in [{key}]")

                    if plays:
                        ts = datetime.now(eastern_tz)
                        etl_ts = ts.isoformat()
                        insert_game_data(driver, plays, key, etl_ts)
                        # s3.delete_object(Bucket=bucket, Key=key)
                        print(f"Plays in file [{key}] uploaded successfully")
                    else:
                        print(f"No plays found in [{key}]")
        else:
            print("No contents found")
            return
    except Exception as e:
        print(f"Error loading files to database: {e}")
    finally:
        driver.close()



if __name__ == '__main__':
    load_game_details_to_database()