import json
import csv


json_file = './data/game_data/401520154.json'
csv_filename = json_file.split("/")[-1].split('.json')[0] + '.csv'
csv_output = './data/csv_games/' + csv_filename


with open(json_file, 'r') as f:
    data = json.load(f)

json_obj = data['drives']['previous']


with open(csv_output, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)

    for drive in json_obj:
        for play in drive['plays']:
            writer.writerow([json.dumps(play)]) # JSON string for easier upload to postgres JSONB column