import os
import sys
import pandas as pd
import psycopg2
import configparser
import yaml

from loguru import logger
from airflow.providers.postgres.hooks.postgres import PostgresHook

#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
# Construire le chemin complet vers config.ini
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")

file_yaml = os.path.join(os.path.dirname(__file__), "..", "schemas", "all_tables.yaml")

#from config.utils import db_connexion

with open(file_yaml, 'r') as file:
    config_data = yaml.safe_load(file)

# Remove the default logger configuration
logger.remove()
logger.add("estate.log", rotation="900kb", level="DEBUG")
logger.add(sys.stderr, level="INFO")


def transform_data(query: str):
    """
    """
    try:
        # hook = PostgresHook(postgres_conn_id='postgres_conn') 
        # conn = hook.get_conn()
        # cur = conn.cursor()
        # POSTGRES_CONN_ID='postgres_conn'
        # logger.info("Connexion à la base de données PostgreSQL réussie.")
        # logger.info("Conneted on database PostgreSQL succeffuly.")
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

    except Exception as e:
        logger.error(f"❌ Error of connexion on PostgreSQL : {e}")
        return

    try:
        row_df = pd.read_sql_query(query, conn)
        logger.info(f"Traitement od table crm_cst_info is finished. The len of table is {len(row_df)}.")

    except Exception as e:
        logger.error(f" Error : {e} during the traitement.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")

    return row_df


def test_transform():
    """
    Fonction de test pour exécuter la requête et retourner le DataFrame.
    """
    query = config_data['query_cust_info']
    logger.info(f"🔍 Exécution de la requête : {query}")
    df = transform_data(query)
    
    if df is not None:
        logger.info(f"📊 Données récupérées :\n{df.head()}")
    else:
        logger.error("❌ Aucune donnée récupérée.")
    
    return df

# Exécution du test
if __name__ == "__main__":
    test_transform()