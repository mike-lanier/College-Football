import psycopg2
import os
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
    

    def cursor(self):
        return self.cur
    

    def commit(self):
        return self.conn.commit()

    
    def close(self):
        self.cur.close()
        self.conn.close()
