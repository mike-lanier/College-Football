import requests
import json
from uuid import uuid4
from getWeek import week_start, week_end, week_id


response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=' + week_start + '-' + week_end)
data = response.json()
file_id = str(uuid4())

with open('./data/schedule_files/' + week_id + '_' + file_id + '.json', 'w') as f:
    json.dump(data, f, indent=4)
