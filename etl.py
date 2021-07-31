import configparser
import pandas as pd
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"Loading data successful ({query})")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"Inserting successful ({query})")


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
        load_staging_tables(cur, conn)
        # https://stackoverflow.com/questions/38737014/amazon-redshift-copy-always-return-s3serviceexceptionaccess-denied-status-403
        # https://stackoverflow.com/questions/40524700/s3-to-redshift-copy-with-access-denied
        # https://stackoverflow.com/questions/36398734/copying-data-from-s3-to-redshift-access-denied/40523899?noredirect=1#comment68288222_40523899
        # https://stackoverflow.com/questions/49139459/aws-redshift-copy-with-iam-role

        insert_tables(cur, conn)
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
