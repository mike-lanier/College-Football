import json
import csv
import os


def jsonScheduleToCsv():
    source_folder = './data/schedule_files/'
    destination_folder = './'
    csv_filename = "./tests/schedules.csv"

    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        for filename in os.listdir(source_folder):
            file_path = os.path.join(source_folder, filename)

            with open(file_path, 'r') as f:
                data = json.load(f)

                for event in data['events']:
                    writer.writerow([json.dumps(event)])



if __name__ == "__main__":
    jsonScheduleToCsv()


