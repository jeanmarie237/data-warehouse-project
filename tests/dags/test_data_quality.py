import configparser
import os
import sys
import warnings

import pandas as pd
import psycopg2
import pytest
import yaml
from loguru import logger
from sqlalchemy import create_engine

# Ajouter le répertoire racine du projet à sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(project_root)


logger.remove()
logger.add("estate.log", rotation="900kb", level="DEBUG")
logger.add(sys.stderr, level="INFO")

warnings.filterwarnings(
    "ignore", category=UserWarning, message="pandas only supports SQLAlchemy"
)

# Configuration de la base de donnée en local : test en local
# Connexion database :
# def connexion_db():
#     """
#     """
#     config_path = os.path.join(project_root, "config.ini")
#     config = configparser.ConfigParser()
#     config.read(config_path)
#     try:
#         # Connexion à la base de données
#         conn = psycopg2.connect(
#             host=config['POSTGRESQL']['host'],
#             port=config['POSTGRESQL']['port'],
#             dbname=config['POSTGRESQL']['database'],
#             user=config['POSTGRESQL']['user'],
#             password=config['POSTGRESQL']['password']
#         )
#         #cur = conn.cursor()
#         logger.info("✅ Connected to database PostgreSQL succeffuly.")
#         return conn
#     except Exception as e:
#         logger.error(f"❌ Error connecting to PostgreSQL : {e}")
#         return None


#Configuration de la base de données en environement de test airflow:
def connexion_db():
    try:
        conn = psycopg2.connect(
            host="postgres",  # postgres Utilisez le nom du service Docker
            port="5432",
            dbname="DWH_01",
            user="postgres",
            password="postgres",
        )
        return conn
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL : {e}")
        return None



# Définir la fixture `conn` pour pytest
@pytest.fixture(scope="module")
def conn():
    """Fixture to provide a database connection."""
    connection = connexion_db()
    if connection is None:
        pytest.fail("🚨 Connection to database failed. Test suite cancelled.")
    yield connection
    connection.close()
    logger.info("✅ Database connection closed.")


def test_duplicates_values(conn):
    """Checks for Nulls or Duplicates  in Primary Key : cst_id"""

    query = """
    SELECT COUNT(*)
    FROM
        (SELECT
            cst_id,
            COUNT(*)
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1 OR cst_id IS NULL) AS duplicates;
    """
    try:
        duplicate_count = pd.read_sql(query, conn).iloc[0, 0]
        assert (
            duplicate_count == 0
        ), f"⚠️ Warning : {duplicate_count} values are duplicated!"
        logger.info("✅ Test passed: No duplicate or NULL values found in cst_id.")
        print(
            "Test passed : No duplicate or NULL values found in cst_id for table crm_cust_info."
        )
    except Exception as e:
        pytest.fail(f"Error while running the SQL query : {e}")


def test_duplicates_values_prduct(conn):
    """Checks for Nulls or Duplicates  in Primary Key : cst_id"""

    query = """
    SELECT COUNT(*)
    FROM
        (SELECT
            prd_id,
            COUNT(*)
        FROM silver.crm_prd_info
        GROUP BY prd_id
        HAVING COUNT(*) > 1 OR prd_id IS NULL) AS duplicates;
    """
    try:
        duplicate_count = pd.read_sql(query, conn).iloc[0, 0]
        assert (
            duplicate_count == 0
        ), f"⚠️ Warning : {duplicate_count} values are duplicated!"
        logger.info("✅ Test passed: No duplicate or NULL values found in prd_id.")
        print(
            "Test passed : No duplicate or NULL values found in prd_id for table crm_prd_info."
        )
    except Exception as e:
        pytest.fail(f"Error while running the SQL query : {e}")


def test_data_standardization(conn):
    """Checks if cst_marital_satus values are standardizied"""

    query = """
    SELECT DISTINCT cst_marital_status 
    FROM silver.crm_cust_info;
    """
    try:
        df = pd.read_sql(query, conn)
        expected_values = {"Single", "Married", "n/a"}
        invalid_values = set(df["cst_marital_status"]) - expected_values
        assert (
            not invalid_values
        ), f"⚠️ Warning: Unexpected values found in cst_marital_status: {invalid_values}"
        logger.info("✅ Test passed: invalid values are not in cst_marital_status.")
        print(
            "Test passed : standadizate data on columns crm_cust_info for table crm_cust_info."
        )
    except Exception as e:
        pytest.fail(f"🚨 Error while running the SQL query: {e}")


def test_data_standardization_prd(conn):
    """Checks if cst_marital_satus values are standardizied"""

    query = """
    SELECT DISTINCT prd_line 
    FROM silver.crm_prd_info;
    """
    try:
        df = pd.read_sql(query, conn)
        expected_values = {"Other Sales", "Road", "Mountain", "n/a", "Touring"}
        invalid_values = set(df["prd_line"]) - expected_values
        assert (
            not invalid_values
        ), f"⚠️ Warning: Unexpected values found in prd_line: {invalid_values}"
        logger.info("✅ Test passed: invalid values are not in prd_line.")
        print(
            "Test passed : standadizate data on columns prd_line for table crm_prd_info."
        )
    except Exception as e:
        pytest.fail(f"🚨 Error while running the SQL query: {e}")


# Verify cost in table product
def test_prd_cost(conn):
    """Checks for invalid prd_cost values (less than 0 or NULL)"""

    query = """
    SELECT prd_cost
    FROM silver.crm_prd_info
    WHERE prd_cost < 0 OR prd_cost IS NULL;
    """
    try:
        df = pd.read_sql(query, conn)
        assert df.empty, f"⚠️ Warning: Found invalid prd_cost values: {df}"
        logger.info("✅ Test passed: No invalid prd_cost values found.")
        print("Test passed : cost data on columns prd_cost for table crm_prd_info.")
    except Exception as e:
        pytest.fail(f"🚨 Error while running the SQL query: {e}")


# Test sales
def test_sls_due_dt(conn):
    """Check if sls_due_dt values meet the criteria."""

    query = """
    SELECT 
        NULLIF(sls_due_dt, 0) AS sls_due_dt 
    FROM bronze.crm_sales_details
    WHERE sls_due_dt <= 0 
        OR LENGTH(TO_CHAR(sls_due_dt, 'FM99999999')) != 8 
        OR sls_due_dt > 20500101 
        OR sls_due_dt < 19000101;
    """
    try:
        df = pd.read_sql(query, conn)
        assert df.empty, f"⚠️ Found invalid sls_due_dt values: {df}"
        logger.info("✅ Test passed: No invalid sls_due_dt values found.")
        print(
            "Test passed : No invalid date on columns sls_due_dt for table crm_sales_details."
        )
    except Exception as e:
        pytest.fail(f"🚨 Error while running the SQL query: {e}")


# Test Location table
def test_data_standardization_lac(conn):
    """Checks if cntry values are standardizied"""

    query = """
    SELECT DISTINCT 
        cntry 
    FROM silver.erp_loc_a101
    ORDER BY cntry;
    """
    try:
        df = pd.read_sql(query, conn)
        expected_values = {
            "Australia",
            "Canada",
            "France",
            "Germany",
            "n/a",
            "NaN",
            "United Kingdom",
            "United States",
        }
        invalid_values = set(df["cntry"]) - expected_values
        assert (
            not invalid_values
        ), f"⚠️ Warning: Unexpected values found in cntry: {invalid_values}"
        logger.info("✅ Test passed: invalid values are not in cntry.")
        print("Test passed : invalid values are not in cntry for table erp_loc_a101.")
    except Exception as e:
        pytest.fail(f"🚨 Error while running the SQL query: {e}")


# Test tables our data model
def test_duplicates_dim_customers(conn):
    """Checks for Nulls or Duplicates  in Primary Key : cst_id"""

    query = """
    SELECT COUNT(*)
    FROM
        (SELECT
            customer_key,
            COUNT(*)
        FROM gold.dim_customers
        GROUP BY customer_key
        HAVING COUNT(*) > 1 OR customer_key IS NULL) AS duplicates;
    """
    try:
        duplicate_count = pd.read_sql(query, conn).iloc[0, 0]
        assert (
            duplicate_count == 0
        ), f"⚠️ Warning : {duplicate_count} values are duplicated!"
        logger.info(
            "✅ Test passed: No duplicate or NULL values found in customer_key."
        )
        print(
            "Test passed : No duplicate or NULL values found in customer_key for table dim_customers."
        )
    except Exception as e:
        pytest.fail(f"Error while running the SQL query : {e}")


def test_duplicates_dim_products(conn):
    """Checks for Nulls or Duplicates  in Primary Key : cst_id"""

    query = """
    SELECT COUNT(*)
    FROM
        (SELECT
            product_key,
            COUNT(*)
        FROM gold.dim_products
        GROUP BY product_key
        HAVING COUNT(*) > 1 OR product_key IS NULL) AS duplicates;
    """
    try:
        duplicate_count = pd.read_sql(query, conn).iloc[0, 0]
        assert (
            duplicate_count == 0
        ), f"⚠️ Warning : {duplicate_count} values are duplicated!"
        logger.info("✅ Test passed: No duplicate or NULL values found in product_key.")
        print(
            "Test passed : No duplicate or NULL values found in product_key for table dim_products."
        )
    except Exception as e:
        pytest.fail(f"Error while running the SQL query : {e}")


def test_duplicates_fact_sales_join(conn):
    """Checks for Nulls or Duplicates in customer_key or product_key after joining tables."""

    query = """
    SELECT COUNT(*)
    FROM (
        SELECT 
            f.customer_key, 
            f.product_key, 
            COUNT(*) 
        FROM gold.fact_sales f
        LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
        LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
        WHERE p.product_key IS NULL OR c.customer_key IS NULL
        GROUP BY f.customer_key, f.product_key
        HAVING COUNT(*) > 1 OR f.customer_key IS NULL OR f.product_key IS NULL
    ) AS duplicates;
    """
    try:
        duplicate_count = pd.read_sql(query, conn).iloc[0, 0]
        assert (
            duplicate_count == 0
        ), f"⚠️ Warning : {duplicate_count} duplicate or NULL values found in customer_key or product_key!"
        logger.info(
            "✅ Test passed: No duplicate or NULL values found in customer_key or product_key after join."
        )
        print(
            "✅ Test passed: No duplicate or NULL values found in customer_key or product_key after join."
        )
    except Exception as e:
        pytest.fail(f"🚨 Error while running the SQL query: {e}")


def run_tests(conn):
    """Exécute tous les tests de données"""
    conn = connexion_db()
    # if conn is None:
    #     pytest.fail("🚨 La connexion à la base de données a échoué. Suite de tests annulée.")

    try:
        # Exécuter vos tests
        test_duplicates_values(conn)
        test_duplicates_values_prduct(conn)
        test_data_standardization(conn)
        test_data_standardization_prd(conn)
        test_prd_cost(conn)
        test_sls_due_dt(conn)
        test_data_standardization_lac(conn)
        test_duplicates_dim_customers(conn)
        test_duplicates_dim_products(conn)
        test_duplicates_fact_sales_join(conn)

        logger.info("✅ Tous les tests ont réussi avec succès.")
    except Exception as e:
        logger.error(f"🚨 Erreur lors de l'exécution des tests : {e}")
    finally:
        if conn is not None:
            conn.close()  # Fermer la connexion après l'exécution de tous les tests.
            logger.info("✅ Connexion à la base de données fermée.")


if __name__ == "__main__":
    run_tests()
