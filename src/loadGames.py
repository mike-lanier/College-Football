import os
import json
from postgres_driver import PostgresDatabaseDriver


driver = PostgresDatabaseDriver()


def insertGameData(driver, games, filename):
    try:
        insert_statement = """
            INSERT INTO plays (play_json, filename)
            VALUES (%s, %s)
        """
        for drive in games:
            for play in drive['plays']:
                play_json = json.dumps(play)
                driver.execute(insert_statement, (play_json, filename))
        driver.commit()
    except Exception as e:
        print("Error inserting events:", e)
        driver.conn.rollback()


def main():
    source_folder = './data/game_data/'

    if driver is None:
        return

    try:
        for filename in os.listdir(source_folder):
            file_path = os.path.join(source_folder, filename)

            with open(file_path, 'r') as f:
                data = json.load(f)
                plays = data['drives']['previous']

                if plays:
                    insertGameData(driver, plays, filename)
                else:
                    print(f"No plays found in {filename}")

    finally:
        driver.close()



if __name__ == '__main__':
    main()
