import json
import requests
from aws_driver import AWSOps


def upload_game_file(game_id, s3_conn, bucket_name):
    try:
        response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=' + game_id)
        
        if response.status_code == 200:
            json_data = response.json()
            file_data = json.dumps(json_data)
            folder_name = 'games/'
            file_name = folder_name + game_id + '.json'
        else:
            print("Request Failed")
            return

        s3_conn.put_object(Bucket=bucket_name, Key=file_name, Body=file_data)
        print(f"[{file_name}] successfully uploaded to bucket")
    except Exception as e:
        print(f"File upload failed: {e}")


def create_game_files():
    s3 = AWSOps.connectS3()
    bucket = AWSOps.getBucketName('cfb_s3_bucket')
    folder = 'schedules/'
    
    try:
        file_list = s3.list_objects_v2(Bucket=bucket, Prefix=folder)

        if 'Contents' in file_list:
            for file in file_list['Contents']:
                key = file['Key']
                if not key.endswith('/'):
                    data = AWSOps.getJsonFileBody(s3, bucket, key)

                    games = data['events']
                    
                    for game in games:
                        game_id = game['id']
                        upload_game_file(game_id, s3, bucket)
        else:
            print("No contents found")
    except Exception as e:
        print(f"Operation failed: {e}")


if __name__ == '__main__':
    create_game_files()
