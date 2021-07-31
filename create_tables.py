import configparser
import pandas as pd
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def get_connection():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    DWH_DB = config.get("CLUSTER", "DB_NAME")
    DWH_DB_USER = config.get("CLUSTER", "DB_USER")
    DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER", "DB_PORT")
    DWH_HOST = config.get("CLUSTER", "HOST")

    conn_string = f"postgresql://{DWH_DB_USER}:{DWH_DB_PASSWORD}@{DWH_HOST}:{DWH_PORT}/{DWH_DB}"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    return conn, cur


def main():
    conn, cur = get_connection()

    try:

        drop_tables(cur, conn)
        create_tables(cur, conn)

        cur.execute("SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'public';")
        res = cur.fetchall()
        print(f"Tables in database:\n{res}")

    except Exception as ex:
        conn.rollback()
        cur.execute("SELECT * FROM stl_load_errors;")
        res = cur.fetchall()

        cols = ["userid", "slice", "tbl", "starttime", "session", "query", "filename", "line_number",
               "colname", "type", "col_length", "position", "raw_line", "raw_field_value", "err_code", "err_reason",
               "is_partial", "start_offset"]

        df = pd.DataFrame(res, columns=cols)

        print(f"Error in SQL:\n{df}\n")
        print(f"Error in Python:\n{ex}")

        #cur.execute("SELECT * FROM STL_LOADERROR_DETAIL;")
        #res = cur.fetchall()
        #print(f"Error in detail: {res}")

    conn.close()


if __name__ == "__main__":
    main()
