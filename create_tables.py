from dotenv import load_dotenv, find_dotenv
import os
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

load_dotenv()

HOST = os.environ['HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_PORT = os.environ['DB_PORT']

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST, 'my_dwh', DB_USER, DB_PASSWORD, DB_PORT)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
