#################################################################################
"""
This script creates tables in a PostgreSQL database using SQL queries defined
in a YAML file. It loads the database connection configuration from a `config.py`
file, checks for the existence of the `bronze` schema, and creates the tables 
specified in the YAML file. It uses `loguru` to manage logs and captures any 
errors that occur during the execution of the queries.

Main functions:
- `build_tables`: Creates a table by executing the corresponding SQL query.
- `create_all_tables`: Creates all the tables specified in the YAML file.
"""

#################################################################################

import os
import sys
import re
import yaml
#import psycopg2
from loguru import logger
#from config.utils import db_connexion

# Add yaml path file
yaml_file_table = os.path.join(os.path.dirname(__file__), "all_tables.yaml")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
#print(sys.path)
from config.utils import db_connexion
import config

# schemas/all_tables.yml
with open(yaml_file_table, 'r') as file:
    config_table = yaml.safe_load(file)

# Remove the default logger configuration
logger.remove()
logger.add("estate.log", rotation="900kb", level="DEBUG")
logger.add(sys.stderr, level="INFO")

"""
def db_connexion():

    conn = psycopg2.connect(
        dbname=config.db_name,
        user=config.user_name,
        password=config.credential,
        port=config.port,
        host=config.host
    )
    # Creation cursor
    cur = conn.cursor()

    return conn, cur
"""

def build_tables(sql_tables_query: object):
    """This function create a tables
    :param cols_tables: It is colomns of table
    """
    conn, cur   = db_connexion()

    if not conn or not cur:
        logger.error("Cannot establish database connection.")
        return

    try:
        schema_db = "gold"

        # Get name of table 
        table_m = None
        match = re.search(r"CREATE TABLE IF NOT EXISTS gold\.(\w+)", sql_tables_query)
        table_m = match.group(1)

        # Verify if silver schema exist in database and create the table
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schema_all = [row[0] for row in cur.fetchall()]
        if schema_db in schema_all:
            logger.info(f"The schema {schema_db} exist in database. Creation of table '{table_m}' starts...")

            cur.execute(sql_tables_query)
            conn.commit()

            # Get table name you have created
            cur.execute("""
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'gold'; 
                        """)
            #last_table = cur.fetchall()
            tables = [row[0] for row in cur.fetchall()]
            # print(tables)
            if table_m in tables:
                logger.info(f"The table '{table_m}' was created successfully.")
            else:
                logger.warning(f"Table '{table_m}' was not found after creation!")

        else:
            logger.warning(f"The {schema_db} do not exists. Please create de the schema.")
        
    except Exception as e:
        logger.error(f"Error : {e} when creating table '{table_m}'.")


    finally:
    # Close the connexion
        if cur:
            cur.close()
        if conn:
            conn.close()

def create_all_tables():
    """
    """
    # build_tables(config_table['prd_info_table'])
    # build_tables(config_table['sales_de_table'])
    # build_tables(config_table['cust_inf_table'])
    # build_tables(config_table['cust_az_table'])
    # build_tables(config_table['loc_a_table'])
    # build_tables(config_table['px_cat_table'])
    build_tables(config_table['customers_table'])


if __name__ == "__main__":
    create_all_tables()
