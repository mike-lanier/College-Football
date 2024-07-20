# import json
# import csv
# import os


# def jsonScheduleToCsv():
#     source_folder = './data/schedule_files/'

#     for filename in os.listdir(source_folder):
#         file_path = os.path.join(source_folder, filename)

#         with open(file_path, 'r') as f:
#             data = json.load(f)

#             for event in data['events']:



# if __name__ == "__main__":
#     jsonScheduleToCsv()

#################################################################################



import os
import json
import psycopg2
from dotenv import load_dotenv

# Function to establish PostgreSQL connection
def connect_to_postgres():
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

# Function to insert events into PostgreSQL
def insert_events(conn, events):
    try:
        cur = conn.cursor()
        insert_statement = """
            INSERT INTO schedule (j)
            VALUES (%s)
        """
        for event in events:
            event_json = json.dumps(event)
            cur.execute(insert_statement, (event_json,))
        conn.commit()
        cur.close()
    except psycopg2.Error as e:
        print("Error inserting events:", e)

# Main script
def main():
    source_folder = './data/schedule_files/'

    conn = connect_to_postgres()
    if conn is None:
        return

    try:
        for filename in os.listdir(source_folder):
            file_path = os.path.join(source_folder, filename)

            with open(file_path, 'r') as f:
                data = json.load(f)
                events = data.get('events', [])

                if events:
                    insert_events(conn, events)
                else:
                    print(f"No events found in {filename}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()



