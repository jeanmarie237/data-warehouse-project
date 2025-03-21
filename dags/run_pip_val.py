import os
import sys
import subprocess
from datetime import datetime, timedelta

import pandas as pd
import yaml
from airflow import DAG
from airflow.models.xcom_arg import XComArg  # Passage de données entre tâches
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from loguru import logger

# Configuration des chemins
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts/ETL"))
)
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config"))
)


# Ajouter le chemin absolu du dossier `tests` au sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tests", "dags"))
)

# Importer le module
import test_data_quality


from scripts.ETL.extract import read_data
from scripts.ETL.load import load_data_crm
from scripts.ETL.transforme import transform_data

# Chemins des fichiers
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts"))
SCHEMA_FILE = os.path.join(BASE_DIR, "schemas", "all_tables.yaml")

# Chargement du fichier de configuration YAML
with open(SCHEMA_FILE, "r", encoding="utf-8") as file:
    config_data = yaml.safe_load(file)

# Configuration du logger
logger.remove()
logger.add("estate.log", rotation="900kb", level="DEBUG")
logger.add(sys.stderr, level="INFO")

# Définition des arguments par défaut du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    "data_processing_dag_biss",
    default_args=default_args,
    description="A simple data processing pipeline",
    schedule_interval=timedelta(days=1),  # Change to a cron expression for scheduling
)

# Liste des tables et requêtes
tables_bronze = [
    "cust_inf_table",
    "prd_info_table",
    "sales_de_table",
    "cust_az_table",
    "loc_a_table",
    "px_cat_table",
]
insert_bronze = [
    "cust_insert",
    "prd_insert_PSQL",
    "sales_insert_PSQL",
    "cust_az_insert_PSQL",
    "loc_a_insert_PSQL",
    "px_cat_insert_PSQL",
]
table_silver = [
    "cust_inf_table_s",
    "prd_info_table_s",
    "sales_de_table_s",
    "cust_az_table_s",
    "loc_a_table_s",
    "px_cat_table_s",
]
insert_silver = [
    "cust_insert_s",
    "prd_insert_PSQL_s",
    "sales_insert_PSQL_s",
    "cust_az_insert_PSQL_s",
    "loc_a_insert_PSQL_s",
    "px_cat_insert_PSQL_s",
]

query_f_bronze = [
    "query_cust_info",
    "query_prd",
    "query_sales_s",
    "query_erp_cust",
    "query_loc",
    "query_px",
]
query_f_silver = ["query_dim_cust", "query_product", "query_sales"]

tables_gold = ["customers_table", "products_table", "fact_sales_table"]
insert_gold = ["customers_insert", "products_insert", "sales_insert"]

# Connexion à la base de données PostgreSQL
hook = PostgresHook(postgres_conn_id="postgres_conn_id")
conn = hook.get_conn()
cur = conn.cursor()
POSTGRES_CONN_ID = "postgres_conn_id"
logger.info("Connexion à la base de données PostgreSQL réussie.")


# Fonction pour lire les données depuis les sources
def task_read_data(**kwargs):
    links_data_sources = [
        "cust_info",
        "prd_info",
        "sales_in",
        "cust_az1",
        "loc_a1",
        "px_cat",
    ]
    links_readed = {}

    try:
        for link in links_data_sources:
            df = read_data(config_data[link])
            logger.info(f"Exploring data for {link} : \n{df.head(3)}")
            links_readed[link] = df.to_dict(orient="records")

        logger.info(f"Data to be returned: {links_readed.keys()}")
        return links_readed
    except Exception as e:
        logger.error(f"Error reading {link}: {e}")
        raise


# Fonction pour transformer les données
def task_transform_data(query_list: list):
    """This function cleans data from the database."""
    query_transformed = {}
    try:
        for query in query_list:
            df = pd.read_sql_query(config_data[query], conn)
            if df is None or df.empty:
                print(
                    f"The transformation returned a None Dataframe for query: {query}."
                )
                raise ValueError(f"The transformation failed for query: {query}.")
            print(
                f"Transformation OK :\nThe '{query}' has {len(df)} rows.\n{df.head(3)}"
            )
            query_transformed[query] = (
                df  # .to_dict(orient="records")  # Convertir en dictionnaire pour XCom
            )
        print(
            f"Transformation of data finished successfully. Number of queries transformed: {len(query_transformed)}."
        )
        return query_transformed
    except Exception as e:
        logger.error(f"Error in the transform_task : {e}.")
        raise


# Fonction pour charger les données dans les tables
def load_all_data_g(tables_names: list, insert_data: list, id_task: str, **kwargs):
    """This function loads data into the specified tables."""
    ti = kwargs["ti"]
    dictionary_data = ti.xcom_pull(task_ids=id_task, key="return_value")
    logger.info(f"Data retrieved from XCom: {dictionary_data}")
    if not dictionary_data:
        print("No data retrieved from XCom.")
        return
    print(f"Data to be loaded: {dictionary_data}")

    try:
        for (sources, data), table_item, insert_item in zip(
            dictionary_data.items(), tables_names, insert_data
        ):
            print(f"Source: {sources}, Number of records: {len(data)}")
            df = pd.DataFrame(data)
            print(f"DataFrame created for {sources}: {df.shape}")
            print(f"Sample data: \n{df.head(2)}")
            logger.info(f"Loading data from {sources} to {table_item}")
            load_data_crm(
                df, config_data[table_item], config_data[insert_item], batchsize=1000
            )
            logger.info(f"Number of rows loaded {sources} : {len(data)}")
        logger.info("Data loaded into all tables successfully")
    except Exception as e:
        logger.error(f"Error loading data from {sources}: {e}")
        raise


# Fonction pour déboguer XCom
def debug_xcom(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="transf_from_bronze", key="return_value")
    print(f"Debug XCom data: {data}")
    return data


# Définition des tâches
task_read_from_sources = PythonOperator(
    task_id="read_from_source",
    python_callable=task_read_data,
    provide_context=True,
    dag=dag,
)

task_load_to_bronze = PythonOperator(
    task_id="load_to_bronze",
    python_callable=lambda **kwargs: load_all_data_g(
        tables_bronze, insert_bronze, id_task="read_from_source", **kwargs
    ),
    provide_context=True,
    dag=dag,
)

task_transform_data_bronze = PythonOperator(
    task_id="transf_from_bronze",
    python_callable=lambda **kwargs: task_transform_data(query_f_bronze),
    provide_context=True,
    dag=dag,
)

task_debug_xcom = PythonOperator(
    task_id="debug_xcom", python_callable=debug_xcom, provide_context=True, dag=dag
)

task_load_from_bronze_to_silver = PythonOperator(
    task_id="load_to_silver",
    python_callable=lambda **kwargs: load_all_data_g(
        table_silver, insert_silver, id_task="transf_from_bronze", **kwargs
    ),
    provide_context=True,
    dag=dag,
)

task_transform_data_silver = PythonOperator(
    task_id="transf_from_silver",
    python_callable=lambda **kwargs: task_transform_data(query_f_silver),
    provide_context=True,
    dag=dag,
)

task_load_from_silver_to_gold = PythonOperator(
    task_id="load_to_gold",
    python_callable=lambda **kwargs: load_all_data_g(
        tables_gold, insert_gold, id_task="transf_from_silver", **kwargs
    ),
    provide_context=True,
    dag=dag,
)


# Tâche pour exécuter les tests de qualité des données
task_data_quality_tests = BashOperator(
    task_id="run_quality_tests",
    bash_command="pytest /app/tests/dags/test_data_quality.py",
    dag=dag,
)


# Définition des dépendances entre les tâches
(
    task_read_from_sources
    >> task_load_to_bronze
    >> task_transform_data_bronze
    >> task_debug_xcom
    >> task_load_from_bronze_to_silver
    >> task_transform_data_silver
    >> task_load_from_silver_to_gold
    >> task_data_quality_tests
)
