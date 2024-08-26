import requests
import json
from uuid import uuid4
from awsConnect import writeToBucket
from getWeek import week_start, week_end, week_id


def writeScheduleToS3Bucket():
    response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=' + week_start + '-' + week_end)
    json_data = response.json()
    file_data = json.dumps(json_data)

    file_id = str(uuid4())
    file_name = week_id + '_' + file_id

    bucket = 'cfb_s3_bucket'
    folder = 'schedules/'

    writeToBucket(file_data, file_name, bucket, folder)


if __name__ == '__main__':
    writeScheduleToS3Bucket()
