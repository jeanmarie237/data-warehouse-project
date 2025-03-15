"""
This script connects to a PostgreSQL database, cleans and transforms data from multiple tables, and loads the cleaned data into corresponding tables in the database. It uses configuration files (`config.ini` and `all_tables.yaml`) for database connection details and table schemas.

### Key Functions:
1. **load_data_crm()**: Loads cleaned data into PostgreSQL tables. It creates tables if they don't exist and handles batch insertion with error handling.
2. **Transform Functions**: Clean and transform data from source tables:
   - `transform_cust_info()`: Cleans customer information data.
   - `transform_crm_prd()`: Cleans product data.
   - `transform_crm_sales()`: Cleans sales data.
   - `transform_erp_cust()`: Cleans ERP customer data.
   - `transform_erp_loc()`: Cleans location data.
   - `transform_erp_px()`: Cleans product category data.
3. **connection_db()**: Establishes a connection to the PostgreSQL database.

### Workflow:
- Reads configuration and schema details from `config.ini` and `all_tables.yaml`.
- Cleans and transforms data using the transform functions.
- Loads the cleaned data into the database using `load_data_crm()`.

### Dependencies:
- `pandas`: For data manipulation.
- `psycopg2`: For PostgreSQL database interaction.
- `loguru`: For logging.
- `yaml`: For reading schema configurations.
- `configparser`: For reading database configurations.

### Usage:
- Ensure `config.ini` and `all_tables.yaml` are correctly configured.
- Run the script to clean, transform, and load data into the database.
"""

import sys
import os 
import re
import yaml
import configparser
import psycopg2
import pandas as pd 
from psycopg2.extras import execute_values
from extract import read_data, reading_crm
from transform import (
    transform_cust_info,
    transform_crm_prd,
    transform_crm_sales,
    transform_erp_cust, 
    transform_erp_loc,
    transform_erp_px,
    connection_db
)

from loguru import logger

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
file_yaml = os.path.join(os.path.dirname(__file__), "..", "schemas", "all_tables.yaml")

from config.utils import db_connexion
#import config
#from scripts.schemas.tables import db_connexion

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

        # Extraction dynamique du nom de la table et du schéma depuis create_table_PSQL
        match_name = re.search(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)\.(\w+)", create_table_PSQL, re.IGNORECASE)
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




#da = read_data(config_data['prd_info'])
#lp = load_data_crm(da, config_data['prd_info_table'], config_data['prd_insert_PSQL'], batchsize=1000)

# row_cust = transform_cust_info()
# lod_cust = load_data_crm(row_cust, config_data['cust_inf_table_s'], config_data['cust_insert_s'], batchsize=1000)

# row_prd = transform_crm_prd()
# ld_prd = load_data_crm(row_prd, config_data['prd_info_table_s'], config_data['prd_insert_PSQL_s'], batchsize=50)
# print(ld_prd)

# row_sales = transform_crm_sales()
# lod_sales = load_data_crm(row_sales, config_data['sales_de_table_s'], config_data['sales_insert_PSQL_s'], batchsize=1000)

# row_erp_cust = transform_erp_cust()
# lod_erp_cust = load_data_crm(row_erp_cust, config_data['cust_az_table_s'], config_data['cust_az_insert_PSQL_s'], batchsize=1000)

# row_erp_loc = transform_erp_loc()
# lod_erp_loc = load_data_crm(row_erp_loc, config_data['loc_a_table_s'], config_data['loc_a_insert_PSQL_s'], batchsize=1000)

row_erp_px = transform_erp_px()
lod_erp_px = load_data_crm(row_erp_px, config_data['px_cat_table_s'], config_data['px_cat_insert_PSQL_s'], batchsize=5)