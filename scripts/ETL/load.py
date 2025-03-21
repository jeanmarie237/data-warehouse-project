"""
This script connects to a PostgreSQL database, cleans and transforms data from multiple tables, and loads the cleaned data into corresponding tables in the database. 
It uses configuration files (`config.ini` and `all_tables.yaml`) for database connection details and table schemas.

"""

import sys
import os 
import re
import yaml
import configparser
import psycopg2
import pandas as pd 
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from extract import read_data

from transforme import transform_data

from loguru import logger

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
file_yaml = os.path.join(os.path.dirname(__file__), "..", "schemas", "all_tables.yaml")


with open(file_yaml, 'r') as file:
    config_data = yaml.safe_load(file)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")

# Remove the default logger configuration
logger.remove()
logger.add("estate.log", rotation="900kb", level="DEBUG")
logger.add(sys.stderr, level="INFO")

def load_data_crm(df: object, create_table_PSQL: str, insert_PSQL: str, batchsize=1000):
    """This function loadded data.
    :params df, insert_PSQL:
    """

    try:
        # Connexion à la base de données
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH)
        conn = psycopg2.connect(
            host=config['POSTGRESQL']['host'],
            port=config['POSTGRESQL']['port'],
            dbname=config['POSTGRESQL']['database'],
            user=config['POSTGRESQL']['user'],
            password=config['POSTGRESQL']['password']
        )
        cur = conn.cursor()
        logger.info("Conneted on database PostgreSQL succeffuly.")

    except Exception as e:
        logger.error(f"❌ Error of connexion on PostgreSQL : {e}")
        return

    try:

        # Extraction name of table and schema from create_table_PSQL
        match_name = re.search(r"CREATE\s+TABLE\s+(\w+)\.(\w+)", create_table_PSQL, re.IGNORECASE)
        if match_name:
            schema_name = match_name.group(1)
            table_name = match_name.group(2)
        else:
            logger.error(f"❌ The table name or schema name has not finded in this request : {create_table_PSQL}")
            raise ValueError("The table name or schema name has finded in create_table_PSQL")

        logger.info(f"🛠️ Schema finded : {schema_name}, Table finded : {table_name}")
                
        # Execution of the resquest to create table
        logger.info(f"🛠️ Execution of the resquest to create table =============> : \n{create_table_PSQL}")
        cur.execute(create_table_PSQL)
        conn.commit()

        # Verify if table exist in schema
        cur.execute(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema_name}';")
        tables_on_schema = [row[0] for row in cur.fetchall()]

        if table_name not in tables_on_schema:
            raise ValueError(f"❌ The table '{table_name}' does not exist in schema '{schema_name}'.")
        
        logger.info(f"Table '{table_name}' created succefully.")

        # check of rows nomber
        num_rows = len(df)
        if num_rows == 0:
            logger.warning("⚠️ The DataFrame empty.")
            return
        
        logger.info(f"📥 Number of rows to load : {num_rows}")
        logger.info(f"🔍 Seeing ... : \n{df.head()}")

        # Conversion data on liste of tuples
        data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Loadding the data
        successful_batches = 0
        failed_batches = 0
        total_rows_inserted = 0
        for i in range(0, len(data_tuples), batchsize):
            batch = data_tuples[i:i+ batchsize]
            try:

                """
                for row in batch:
                    if "" in row:
                        print("🚨 Ligne avec valeur vide détectée :", row)
                """
                        
                #cur.execute(insert_PSQL, row)
                execute_values(cur, insert_PSQL, batch)
                conn.commit()
                successful_batches  += 1
                total_rows_inserted += len(batch)
                logger.info(f"Batch {i // batchsize + 1} : =====> {len(batch)} rows inserted succefully in {table_name}.")
            
            except Exception as e:
                conn.rollback()          # cancels the batch in error
                failed_batches += 1
                logger.error(f"Error in batch  {i // batchsize + 1} : {e}. Cancels the batch.")
                raise e  # Stop the process and raise the exception

        if successful_batches > 0:
            logger.info(f"✅ {successful_batches * batchsize} rows inserted in {table_name} in {schema_name} layer succefuly.")
        if failed_batches > 0:
            logger.warning(f"⚠️ {failed_batches} batches failed during the insertion.")

    except Exception as e:
        logger.error(f"Error during data loading: {e}.")
        conn.rollback()
        raise e  # Propagate the exception to the caller

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 PostgreSQL Connexion closed.")

