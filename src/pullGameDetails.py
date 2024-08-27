import json
import requests
from awsConnect import connectS3, getBucketName, fileObjToString


def uploadGameFile(game_id, s3_conn, bucket_name):
    response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=' + game_id)
    
    if response.status_code == 200:
        json_data = response.json()
        file_data = json.dumps(json_data)
        folder_name = 'games/'
        file_name = folder_name + game_id + '.json'
    else:
        print(f"Failed Request: {Exception}")

    try:
        s3_conn.put_object(Bucket=bucket_name, Key=file_name, Body=file_data)
    except Exception as e:
        print(f"Failed: {e}")


def listScheduleFiles():
    s3 = connectS3()
    bucket = getBucketName('cfb_s3_bucket')
    folder = 'schedules/'
    file_list = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
    
    if 'Contents' in file_list:
        for file in file_list['Contents']:
            key = file['Key']
            if not key.endswith('/'):
                file_obj = s3.get_object(Bucket=bucket, Key=key)
                body = fileObjToString(file_obj)
                data = json.loads(body)
                games = data['events']
                
                for game in games:
                    game_id = game['id']
                    uploadGameFile(game_id, s3, bucket)
                    print("File upload successful")
    else:
        print("No files")



if __name__ == '__main__':
    listScheduleFiles()
