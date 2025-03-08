import sys
import os 
import re
import yaml
import configparser
import psycopg2
import pandas as pd 
from extract import read_data, reading_crm
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



# Obtenir le chemin absolu du répertoire contenant load.py
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

# Construire le chemin complet vers config.ini
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")


# Remove the default logger configuration
logger.remove()
logger.add("estate.log", rotation="900kb", level="DEBUG")
logger.add(sys.stderr, level="INFO")


def load_data_crm(df: object, create_table_PSQL: str, insert_PSQL: str):
    """Cette fonction permet de charger les données dans la base de données
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
        logger.error(f"❌ Erreur de connexion à PostgreSQL : {e}")
        return

    try:

        # Extraction dynamique du nom de la table et du schéma depuis create_table_PSQL
        table_name = None
        match_name = re.search(r"CREATE TABLE IF NOT EXISTS bronze\.(\w+)", create_table_PSQL)
        if match_name:
            table_name = match_name.group(1)
        else:
            raise ValueError("Le nom de la table n'a pas été trouvé dans create_table_PSQL")

        schema_name = None
        match_sch = re.search(r"CREATE TABLE IF NOT EXISTS (\w+)\.", create_table_PSQL)
        if match_sch:
            schema_name = match_sch.group(1)
        else:
            raise ValueError("Le nom du schéma n'a pas été trouvé dans create_table_PSQL")

        # Exécution de la requête CREATE TABLE
        logger.info(f"🛠️ Exécution de la requête de création de la table : \n{create_table_PSQL}")
        cur.execute(create_table_PSQL)
        conn.commit()

        # Vérification si la table existe déjà dans le schéma
        
        cur.execute(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema_name}';")
        tables_on_schema = [row[0] for row in cur.fetchall()]

        if table_name not in tables_on_schema:
            raise ValueError(f"❌ La table '{table_name}' n'existe pas dans le schéma '{schema_name}' après création.")
        
        logger.info(f"Table '{table_name}' créée avec succès. Le chargement peut commencer...")


        # Vérification des données à insérer
        num_rows = len(df)
        if num_rows == 0:
            logger.warning("⚠️ Le DataFrame est vide. Aucune donnée à insérer.")
            return
        
        logger.info(f"📥 Nombre de lignes à insérer : {num_rows}")
        logger.info(f"🔍 Aperçu des données : \n{df.head()}")

        # Chargement des données
        for row in df.itertuples(index=False):
            try:
                cur.execute(insert_PSQL, row)
            except Exception as e:
                logger.error(f"❌ Erreur lors de l'insertion de la ligne {row} : {e}")
                conn.rollback()

        conn.commit()
        logger.info(f"✅ {num_rows} lignes insérées avec succès dans '{table_name}'.")

    except Exception as e:
        logger.error(f"Erreur : {e} lors du chargement des données.")
        conn.rollback()

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion PostgreSQL fermée.")

#da = read_data(config_data['cust_az1'])
#lp = load_data_crm(da, config_data['cust_az_table'], config_data['cust_az_insert_PSQL'])
#df_p = read_data(config_data['cust_inf_table'])
#t = read_data(config_data['prd_info'])
#p = read_data(config_data['cust_info'], "crm_cust_info")
#p = reading_crm()
#print(lp)

    
