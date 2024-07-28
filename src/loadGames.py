import os
import json
import psycopg2
from postgres_driver import PostgresDatabaseDriver


conn = PostgresDatabaseDriver()


def insertGameData(conn, games, filename):
    try:
        cur = conn.cursor()
        insert_statement = """
            INSERT INTO plays (play_json, filename)
            VALUES (%s, %s)
        """
        for drive in games:
            for play in drive['plays']:
                play_json = json.dumps(play)
                cur.execute(insert_statement, (play_json, filename))
        conn.commit()
    except psycopg2.Error as e:
        print("Error inserting events:", e)


def main():
    source_folder = './data/game_data/'

    if conn is None:
        return

    try:
        for filename in os.listdir(source_folder):
            file_path = os.path.join(source_folder, filename)

            with open(file_path, 'r') as f:
                data = json.load(f)
                plays = data['drives']['previous']

                if plays:
                    insertGameData(conn, plays, filename)
                else:
                    print(f"No plays found in {filename}")

    finally:
        conn.close()



if __name__ == '__main__':
    main()
