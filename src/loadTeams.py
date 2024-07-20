import os
import json
import psycopg2
import loadSchedules as mod


def insertTeamData(conn, teams):
    try:
        cur = conn.cursor()
        insert_statement = """
            INSERT INTO teams (team_json)
            VALUES (%s)
        """
        for team in teams:
            team_json = json.dumps(team)
            cur.execute(insert_statement, (team_json,))
        conn.commit()
        cur.close()
    except psycopg2.Error as e:
        print("Error inserting events:", e)


def main():
    source_folder = './data/game_data/'

    conn = mod.connectPostgres()
    if conn is None:
        return

    try:
        for filename in os.listdir(source_folder):
            file_path = os.path.join(source_folder, filename)

            with open(file_path, 'r') as f:
                data = json.load(f)
                teams = data['boxscore']['teams']

                if teams:
                    insertTeamData(conn, teams)
                else:
                    print(f"No teams found in {filename}")

    finally:
        conn.close()



if __name__ == '__main__':
    main()