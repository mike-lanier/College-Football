import os
import json
import modules as mod



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
    except mod.psycopg2.Error as e:
        print("Error inserting events:", e)


def main():
    source_folder = './data/schedule_files/'

    conn = mod.connectPostgres()
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