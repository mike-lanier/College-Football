import os
import json
from postgres_driver import PostgresDatabaseDriver  # Assuming PostgresDatabaseDriver is defined in postgres_driver.py


def main():
    source_folder = './data/schedule_files/'

    try:
        conn = PostgresDatabaseDriver()
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return

    for filename in os.listdir(source_folder):
        file_path = os.path.join(source_folder, filename)

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                events = data.get('events', [])

                if events:
                    conn.insertScheduleData(events)
                else:
                    print(f"No events found in {filename}")

        except FileNotFoundError:
            print(f"File not found: {filename}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON file {filename}: {e}")
        except Exception as e:
            print(f"Error processing file {filename}: {e}")

    conn.close()


if __name__ == "__main__":
    main()
