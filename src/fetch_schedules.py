import requests
import json
from aws_driver import AWSOps
from get_week import CalculateWeek


def create_schedule_file():
    calc = CalculateWeek()
    week_id = calc.get_week_id()
    week_start = calc.get_week_start()
    week_end = calc.get_week_end()

    try:
        response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=' + week_start + '-' + week_end)
        if response.status_code == 200:
            json_data = response.json()
            file_data = json.dumps(json_data)
        else:
            print("Request Failed")
            return

        bucket = AWSOps.getBucketName('cfb_s3_bucket')
        folder = 'schedules/'
        file_name = folder + week_id + '.json'
        
        s3 = AWSOps.connectS3()
        s3.put_object(Bucket=bucket, Key=file_name, Body=file_data, ContentType='application/json')
        print(f"[{file_name}] successfully created and uploaded")

    except Exception as e:
        print(f"Operation failed: {e}")


if __name__ == '__main__':
    create_schedule_file()
