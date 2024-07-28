import os
import json
from postgres_driver import PostgresDatabaseDriver


driver = PostgresDatabaseDriver()


def insertTeamData(driver, teams):
    try:
        insert_statement = """
            INSERT INTO teams (team_json)
            VALUES (%s)
        """
        for team in teams:
            team_json = json.dumps(team)
            driver.execute(insert_statement, (team_json,))
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
                teams = data['boxscore']['teams']

                if teams:
                    insertTeamData(driver, teams)
                else:
                    print(f"No teams found in {filename}")

    finally:
        driver.close()


if __name__ == '__main__':
    main()