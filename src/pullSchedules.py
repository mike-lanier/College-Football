import requests
import json
from getWeek import week_start, week_end, week_id


response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=' + week_start + '-' + week_end)
data = response.json()

with open('./data/schedule_files/' + week_id + '.json', 'w') as f:
    json.dump(data, f, indent=4)
