import json
import requests
import os
import time
import boto3
from dotenv import load_dotenv



def loopReadScheduleFiles():
    game_ids = []
    
    folder_path = './data/schedule_files/'

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'r') as file:
            data = json.load(file)
        events = data['events']
        event_ids = [item['id'] for item in events]
        
        for i in range(0, len(event_ids)):
            game_ids.append(event_ids[i])

    game_ids.sort()
    gameid_dict = {index: value for index, value in enumerate(game_ids)}

    return gameid_dict


def gameFilesToS3(start_index, end_index):
    dict = loopReadScheduleFiles()
    load_dotenv()

    session = boto3.Session(
        aws_access_key_id = os.getenv('aws_access_key'),
        aws_secret_access_key = os.getenv('aws_secret_key'),
        region_name = os.getenv('aws_region')
    )
    s3 = session.resource('s3')

    for game_id in range(start_index, end_index):
        response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=' + dict[game_id])
        json_data = response.json()
        file_data = json.dumps(json_data)
        bucket_name = os.getenv('cfb_s3_bucket')
        object = s3.Object(bucket_name, dict[game_id] + '.json')

        result = object.put(Body = file_data)

        time.sleep(1)
        


if __name__ == '__main__':
    
    try:
        output = gameFilesToS3(18, 20)
        print("Completed successfully")
    except Exception as e:
        print(f"Failed: {e}")
