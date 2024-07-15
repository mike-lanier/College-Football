import json
import csv
import os


def jsonScheduleToCsv():
    source_folder = './schedule_files/'
    destination_folder = './csv_schedule/'

    for filename in os.listdir(source_folder):
        file_path = os.path.join(source_folder, filename)

        with open(file_path, 'r') as f:
            data = json.load(f)

        json_filename = f.name.split("_")[2]
        csv_filename = destination_folder + json_filename.split(".")[0] + ".csv"

        columns = ['game_id', 'matchup', 'abbr', 'attendance', 'conference_game', 'game_date', 'neutral_site', 'venue_id', 'venue_name', 'venue_city', 'venue_state', 'broadcast_type', 'network']


        with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = columns, extrasaction='ignore')

            writer.writeheader()

            for event in data['events']:
                matchup = event['name']
                abbr = event['shortName']

                for competition in event['competitions']:
                    venue_id = competition['venue']['id']
                    venue_name = competition['venue']['fullName']
                    venue_city = competition['venue']['address']['city']
                    venue_state = competition['venue']['address'].get('state', '')
                    game_id = competition['id']
                    conference_game = competition['conferenceCompetition']
                    attendance = competition['attendance']
                    game_date = competition['date']
                    neutral_site = competition['neutralSite']

                    for details in competition['geoBroadcasts']:
                        broadcast_type = details['type']['shortName']
                        network = details['media']['shortName']

                        row = {
                            'matchup': matchup,
                            'abbr': abbr,
                            'venue_id': venue_id,
                            'venue_name': venue_name,
                            'broadcast_type': broadcast_type,
                            'network': network,
                            'venue_city': venue_city,
                            'venue_state': venue_state,
                            'conference_game': conference_game,
                            'attendance': attendance,
                            'neutral_site': neutral_site,
                            'game_id': game_id,
                            'game_date': game_date
                        }

                        filtered_row = {key: value for key, value in row.items() if key in columns}
                        writer.writerow(filtered_row)



if __name__ == "__main__":
    jsonScheduleToCsv()




