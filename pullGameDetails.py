# import json

# with open('./schedule_files/NCAAF_Week22.json', 'r') as f:
#     data = json.load(f)


# events = data['events']

# game_ids = [item['id'] for item in events]

# print(game_ids)


###############################################################################


import json
import requests
import os
import time


def loopReadScheduleFiles():
    game_ids = []
    
    folder_path = './schedule_files/'

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
        


def getGameDetailsFiles(start_index, end_index):
    dict = loopReadScheduleFiles()

    for i in range(start_index, end_index):
        response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=' + dict[i])
        json_data = response.json()

        with open('./game_data/' + dict[i] + '.json', 'w') as f:
            json.dump(json_data, f, indent=4)
    
        time.sleep(3)

    




if __name__ == '__main__':
    
    try:
        output = getGameDetailsFiles(5, 10)
        print("Completed successfully")
    except Exception as e:
        print(f"Failed: {e}")
