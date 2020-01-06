from dotenv import load_dotenv, find_dotenv
import os
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

load_dotenv()

HOST = os.environ['HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_PORT = os.environ['DB_PORT']

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        print(f'Executed {query}')
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        print(f'Executed {query}')
        conn.commit()

def main():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST, 'my_dwh', DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
