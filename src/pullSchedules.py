import requests
import json
import time
import weekinfo as wk


for key in wk.weeks_2023:
    response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=' + wk.weeks_2023[key])
    data = response.json()

    with open('./data/schedule_files/NCAAF_Week' + key + '.json', 'w') as f:
        json.dump(data, f, indent=4)

    time.sleep(3)
    