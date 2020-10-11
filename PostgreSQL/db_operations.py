"""
Created on: 10/2/2020

Author: FriscianViales

Encoding: utf-8
"""

# Import libraries:
import pandas as pd
import logging
import psycopg2
from psycopg2.extras import execute_values

# Logs configuration:
logging.basicConfig(level='INFO')


# Helper functions:
def ddl_operation(credentials, DDL_Statements):
    """ Connects to a Postgres database and performs
    a DDL operation based on a given SQL script. You
    have to pass a dictionary with the credentials to
    the DB. The SQL statement must be a DDL statement
    because the function will not return any object.
    For uploading data to the database, please use the
    function 'load_operation()' """

    try:

        # Connect to the DB:
        conn = psycopg2.connect(
            user=credentials['user'],
            password=credentials['password'],
            host=credentials['host'],
            port=credentials['port'],
            database=credentials['database'])

        # Confirm connection:
        logging.info('Connected to Postgres successfully.')
        # Build a cursor to execute SQL statements:
        cursor = conn.cursor()
        # Execute SQL:
        cursor.execute(DDL_Statements)
        # Commit transaction:
        conn.commit()
        # Confirm trasaction:
        logging.info('DDL statements executed successfully.')

    except Exception as e:

        # Report issues:
        logging.warning(f"Exception raised while executing database operation. --> {e}")

    finally:

        # Close database connection.
        if conn:

            cursor.close()
            conn.close()
            logging.info('Postgres connection closed.')

    return None


def dql_operation(credentials, SQL_Query):
    """ Connect to a Postgres DB, makes a query and return
    the results in a df. You have to pass a dictionary with
    the credentials to the DB and the SQL must be a SELECT
    statement in order to return a df. """

    try:

        # Connect to the DB:
        conn = psycopg2.connect(
            user=credentials['user'],
            password=credentials['password'],
            host=credentials['host'],
            port=credentials['port'],
            database=credentials['database'])

        # Confirm connection:
        logging.info('Connected to Postgres successfully.')
        # Build a cursor to execute SQL statements:
        cursor = conn.cursor()
        # Execute SQL:
        cursor.execute(SQL_Query)
        # Fetch data:
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        # Confirm trasaction:
        logging.info('SQL query executed successfully.')

    except Exception as e:

        # Report issues:
        logging.warning(f"Exception raised while executing database operation. --> {e}")

    finally:

        # Close database connection.
        if conn:
            cursor.close()
            conn.close()
            logging.info('Postgres connection closed.')

    return pd.DataFrame(results, columns=colnames)


def load_operation(credentials, df, table):
    """ Take a df and load data to a db table. """

    try:

        # Connect to the DB:
        conn = psycopg2.connect(
            user=credentials['user'],
            password=credentials['password'],
            host=credentials['host'],
            port=credentials['port'],
            database=credentials['database'])

        # Confirm connection:
        logging.info('Connected to Postgres successfully.')
        # Build cursor:
        cursor = conn.cursor()
        # Create list of tuples from the dataframe values:
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated df columns:
        cols = ','.join(list(df.columns))
        # SQL query to execute:
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        # Execute transaction:
        logging.info('Loading data...')
        execute_values(cursor, query, tuples)
        # Commit transaction and log confirmation:
        conn.commit()
        logging.info('Loaded data to Postgres table successfully.')

    except Exception as e:

        # Report issues:
        logging.warning(f"Exception raised while loading data: {e}")

    finally:

        # Close database connection.
        if conn:
            cursor.close()
            conn.close()
            logging.info('PostgreSQL connection closed.')

    return None
