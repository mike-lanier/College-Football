import os
import json
import psycopg2
from dotenv import load_dotenv


def connectPostgres():
    load_dotenv()

    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return None


def insertScheduleData(conn, schedules):
    try:
        cur = conn.cursor()
        insert_statement = """
            INSERT INTO schedule (j)
            VALUES (%s)
        """
        for event in schedules:
            event_json = json.dumps(event)
            cur.execute(insert_statement, (event_json,))
        conn.commit()
        cur.close()
    except psycopg2.Error as e:
        print("Error inserting events:", e)


def main():
    source_folder = './data/schedule_files/'

    conn = connectPostgres()
    if conn is None:
        return

    try:
        for filename in os.listdir(source_folder):
            file_path = os.path.join(source_folder, filename)

            with open(file_path, 'r') as f:
                data = json.load(f)
                events = data.get('events', [])

                if events:
                    insertScheduleData(conn, events)
                else:
                    print(f"No events found in {filename}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()