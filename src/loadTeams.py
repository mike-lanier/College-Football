import os
import json
from datetime import datetime
import pytz
from postgres_driver import PostgresDatabaseDriver


driver = PostgresDatabaseDriver()


def insertTeamData(driver, teams, filename, etl_ts):
    try:
        insert_statement = """
            INSERT INTO landing.raw_teams (team_json, filename, etl_ts)
            VALUES (%s, %s, %s)
        """
        for team in teams:
            team_json = json.dumps(team)
            driver.execute(insert_statement, (team_json, filename, etl_ts))
        driver.commit()
    except Exception as e:
        print("Error inserting events:", e)
        driver.conn.rollback()


def main():
    source_folder = './data/game_data/processed_game_files/'

    if driver is None:
        return

    try:
        eastern_tz = pytz.timezone('US/Eastern')
        ts = datetime.now(eastern_tz)
        etl_ts = ts.isoformat()
        for filename in os.listdir(source_folder):
            file_path = os.path.join(source_folder, filename)

            with open(file_path, 'r') as f:
                data = json.load(f)
                teams = data['boxscore']['teams']

                if teams:
                    insertTeamData(driver, teams, filename, etl_ts)
                else:
                    print(f"No teams found in {filename}")

    finally:
        driver.close()


if __name__ == '__main__':
    main()