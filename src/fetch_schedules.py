import requests
import json
from awsConnect import connectS3, getBucketName
from getWeek import week_start, week_end, week_id


def create_schedule_file():
    try:
        response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=' + week_start + '-' + week_end)
        if response.status_code == 200:
            json_data = response.json()
            file_data = json.dumps(json_data)
        else:
            print("Request Failed")
            return

        bucket = getBucketName('cfb_s3_bucket')
        folder = 'schedules/'
        file_name = folder + week_id + '.json'
        
        s3 = connectS3()
        s3.put_object(Bucket=bucket, Key=file_name, Body=file_data, ContentType='application/json')
        print(f"[{file_name}] successfully created and uploaded")

    except Exception as e:
        print(f"Operation failed: {e}")


if __name__ == '__main__':
    create_schedule_file()