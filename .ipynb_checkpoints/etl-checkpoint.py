import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This function Load each the data from files in s3 to staging table with COPY command.
    
    Parameters: 
        cur: cursor to perform database operations
        conn: connection created to the database
    ''' 
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    This function Insert the data into from stage table to final table (fact and dimension tables).
    
    Parameters: 
    cur: cursor to perform database operations
    conn: connection created to the database
    
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    - Function connects to the dwh database and gets cursor to it.  
    - loads the s3 files into staging tables.  
    - and then from staging tables transform the data and insert into the dimension and fact tables. 
    - and finally, closes the database connection.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()