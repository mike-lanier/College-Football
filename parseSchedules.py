import json
import pandas as pd
import csv
import os


with open('schedule_files/NCAAF_Week1.json', 'r') as f:
    data = json.load(f)


csv_filename = 'filtered_games.csv'
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




