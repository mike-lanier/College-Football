import psycopg2
import os
import json
from dotenv import load_dotenv


class PostgresDatabaseDriver:

    def __init__(self):
        load_dotenv()
        self.conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        self.cur = self.conn.cursor()

    
    def close(self):
        self.cur.close()
        self.conn.close()

    
    def insertScheduleData(self, schedules):
        try:
            insert_statement = """
                INSERT INTO schedule (j)
                VALUES (%s)
            """
            for event in schedules:
                event_json = json.dumps(event)
                self.cur.execute(insert_statement, (event_json,))
            self.conn.commit()
            # self.cur.close()
        except psycopg2.Error as e:
            print("Error inserting events:", e)


    def insertGameData(self, games):
        try:
            insert_statement = """
                INSERT INTO plays (play_json)
                VALUES (%s)
            """
            for drive in games:
                for play in drive['plays']:
                    play_json = json.dumps(play)
                    self.cur.execute(insert_statement, (play_json,))
            self.conn.commit()
            self.cur.close()
        except psycopg2.Error as e:
            print("Error inserting events:", e)