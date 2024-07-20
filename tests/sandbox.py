import json
import os

source_folder = './data/game_data/'

for filename in os.listdir(source_folder):
    file_path = os.path.join(source_folder, filename)

    with open(file_path, 'r') as f:
        data = json.load(f)
        teams = data['boxscore']['teams']

        for team in teams:
            print(team)




