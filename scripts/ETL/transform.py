"""
This script connects to a PostgreSQL database, cleans and transforms data from multiple tables, and returns the results as pandas DataFrames.

Main functions:
1. **connection_db()**: Connects to the database using `config.ini`.
2. **transform_cust_info()**: Cleans data from the `bronze.crm_cust_info` table (names, marital statuses, genders, dates).
3. **transform_crm_prd()**: Cleans data from the `bronze.crm_prd_info` table (product keys, costs, product lines).
4. **transform_crm_sales()**: Cleans data from the `bronze.crm_sales_details` table (dates, prices, quantities, sales).
5. **transform_erp_cust()**: Cleans data from the `bronze.erp_cust_az12` table (customer IDs, birthdates, genders).
6. **transform_erp_loc()**: Cleans data from the `bronze.erp_loc_a101` table (customer IDs, countries).
7. **transform_erp_px()**: Cleans data from the `bronze.erp_px_cat_g1v2` table (selects relevant columns).

Uses `loguru` for logging and `pandas` for data manipulation.
"""


import os
import sys
import pandas as pd
import psycopg2
import configparser

from loguru import logger

#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
# Construire le chemin complet vers config.ini
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")

# Remove the default logger configuration
logger.remove()
logger.add("estate.log", rotation="900kb", level="DEBUG")
logger.add(sys.stderr, level="INFO")

def connection_db():
    """
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
        return conn, cur  

    except Exception as e:
        logger.error(f"❌ Error of connexion on PostgreSQL : {e}")
        raise Exception("Echec of connexion of data base PostgreSQL")  

def transform_cust_info():
    """
    """
    result = connection_db()

    if result is None:
        logger.error("❌ Connexion à la base de données échouée. L'opération est annulée.")
        return None  # Retourner None si la connexion échoue


    try:

        conn, cur = result
        row_cst_info = []

        query_cust_info = """
                                SELECT 
                                    cst_id,
                                    cst_key,
                                    TRIM(cst_firstname) AS cst_firstname,
                                    TRIM(cst_lastname) AS cst_lastname,
                                        CASE 
                                            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                                            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                                            ELSE 'n/a'
                                        END cst_marital_status,
                                        CASE 
                                            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                                            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                                            ELSE 'n/a'
                                        END cst_gndr,
                                    CASE 
                                        WHEN cst_create_date = 'NaN' THEN '2025-10-07 00:00:00+00'
                                        WHEN cst_create_date = 'NaT' THEN '2025-10-07 00:00:00+00'
                                        ELSE TO_TIMESTAMP(cst_create_date, 'YYYY-MM-DD HH24:MI:SS')
                                    END AS cst_create_date
                                FROM(
                                    SELECT
                                        *,
                                        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
                                    FROM bronze.crm_cust_info
                                    WHERE cst_id IS NOT NULL
                                ) c WHERE flag_last = 1;
                          """
        #cur.execute(query_cust_info)
        # Retrive data that we cleaned
        #row_cst_info = cur.fetchall()
        row_cst_info = pd.read_sql_query(query_cust_info, conn)
        row_cst_info['cst_create_date'] = row_cst_info['cst_create_date'].where(
            pd.notnull(row_cst_info['cst_create_date']), None
        ) 
        # conn.commit()
        logger.info(f"Traitement od table crm_cst_info is finished. The len of table is {len(row_cst_info)}.")

    except Exception as e:
        logger.error(f" Error : {e} during the traitement.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")

    return row_cst_info

# Cleaning data of table crm_prd_info
def transform_crm_prd():
    """
    """
    result = connection_db()
    conn, cur = result

    query_prd = """
    WITH correct_form AS (
        SELECT 
            prd_id,
            --prd_key,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
            prd_nm,
            CASE
                WHEN prd_cost = 'NaN' THEN 0
                ELSE COALESCE(prd_cost, 0)
            END AS prd_cost,
            CASE
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            -- Fonction de glissante avec valeur par defauts. Très important pour la gestion des NaN et des NULL
            COALESCE(
                MAX(prd_start_dt) FILTER (WHERE prd_start_dt <> 'NaN' AND prd_start_dt IS NOT NULL) 
                OVER (ORDER BY prd_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                '2007-12-28 00:00:00'
            ) AS prd_start_dt,
            COALESCE(
                MAX(prd_end_dt) FILTER (WHERE prd_end_dt <> 'NaN' AND prd_end_dt IS NOT NULL) 
                OVER (ORDER BY prd_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                '2007-12-28 00:00:00'
            ) AS prd_end_dt
        FROM bronze.crm_prd_info
    )
    SELECT 
        prd_id,
        cat_id,
        prd_key, 
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt::DATE AS prd_start_dt,  -- Conversion en timestamp
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt):: DATE AS prd_end_dt
    FROM correct_form;
    """


    df_prd = []
    try:
        logger.info("Cleaning data start ...")
        #cur.execute(query_prd)
        #df_prd = cur.fetchall()
        df_prd = pd.read_sql_query(query_prd, conn)
        #return df

        logger.info(f"Process of cleaning is finish. \n{df_prd.head()}")

    except Exception as e:
        logger.error(f"Error during process of cleaning : {e}.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")
    
    return df_prd

# Transform data sales
def transform_crm_sales():
    """
    """
    query_sales = """
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        --sls_order_dt,
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN '2010-12-29 00:00:00'
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS TIMESTAMP)
        END AS sls_order_dt,
        --sls_ship_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8  THEN '2011-01-05 00:00:00'
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS TIMESTAMP)
        END AS sls_ship_dt,
        --sls_due_dt,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8  THEN '2011-01-10 00:00:00'
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS TIMESTAMP)
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,   
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price

    FROM bronze.crm_sales_details;
    """

    result = connection_db()
    conn, cur = result
    df_sales = []

    try:
        logger.info("Cleaning data start ...")
        df_sales = pd.read_sql_query(query_sales, conn)
        logger.info(f"Process of cleaning is finish. \n{df_sales.head()}")

    except Exception as e:
        logger.error(f"Error during process of cleaning : {e}.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")
    
    return df_sales

# Transform data of table erp_cust
def transform_erp_cust():
    """
    """
    query_erp_cust="""
    SELECT 
        CASE 
            WHEN LENGTH(cid) > 4 THEN SUBSTRING(cid FROM 4)
            ELSE cid
        END AS cid,
        CASE  
            WHEN bdate::DATE > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE')  THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')  THEN 'Male'
            ELSE 'n/a'
        END AS gen 
    FROM bronze.erp_cust_az12;
    """

    result = connection_db()
    conn, cur = result
    df_erp_cust = []
    try:
        logger.info("Cleaning data start ...")
        df_erp_cust = pd.read_sql_query(query_erp_cust, conn)
        logger.info(f"Process of cleaning is finish. \n{df_erp_cust.head()}")

    except Exception as e:
        logger.error(f"Error during process of cleaning : {e}.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")
    
    return df_erp_cust

# Transform data of table erp_loc
def transform_erp_loc():
    """
    """
    query_loc = """
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'Unated States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;
    """

    result = connection_db()
    conn, cur = result
    df_loc = []

    try:
        logger.info("Cleaning data start ...")
        df_loc = pd.read_sql_query(query_loc, conn)
        logger.info(f"Process of cleaning is finish. \n{df_loc.head()}")

    except Exception as e:
        logger.error(f"Error during process of cleaning : {e}.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")
    
    return df_loc

# Transform data of table px
def transform_erp_px():
    """
    """
    query_px = """
    SELECT
        id,
        cat,
        subcat,
        maintenance 
    FROM bronze.erp_px_cat_g1v2;
    """

    result = connection_db()
    conn, cur = result
    df_px = []

    try:
        logger.info("Cleaning data start ...")
        df_px = pd.read_sql_query(query_px, conn)
        logger.info(f"Process of cleaning is finish. \n{df_px.head()}")

    except Exception as e:
        logger.error(f"Error during process of cleaning : {e}.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")
    
    return df_px

# Build Data model 
def dim_customers():
    """
    """

    query_dim_cust = """
    --CREATE OR REPLACE VIEW gold.dim_customers AS 
    SELECT
        ROW_NUMBER() OVER (ORDER BY cst_id) AS custumer_key,
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name,
        la.cntry AS country,
        ci.cst_marital_status AS marital_status,
        --ci.cst_gndr,
        CASE 
            WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
            ELSE COALESCE(ca.gen, 'n/a')
        END AS gender,
        ca.bdate AS birthdate,
        ci.cst_create_date AS create_date
        --ca.gen,
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la  ON ci.cst_key = la.cid;
    """

    #query_fetch_data = "SELECT * FROM gold.dim_customers;"

    
    result = connection_db()
    conn, cur = result
    dim_cust = []

    try:
        logger.info("Building dimension table customers start ...")
        dim_cust = pd.read_sql_query(query_dim_cust, conn)
        logger.info(f"Process of Building is finish. \n{dim_cust.head()}")

    except Exception as e:
        logger.error(f"Error during process of Building : {e}.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")

    
    return dim_cust


def dim_product():
    """
    """

    query_product = """
    SELECT
        ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
        pn.prd_id AS product_id,
        pn.prd_key AS product_number,
        pn.prd_nm AS product_name,
        pn.cat_id AS category_id,
        pc.cat AS category,
        pc.subcat AS subcategory,
        pc.maintenance,
        pn.prd_cost AS cost,
        pn.prd_line AS product_line,
        pn.prd_start_dt AS start_date
        --pn.prd_end_dt
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL;
    """

    result = connection_db()
    conn, cur = result
    dim_prd = []

    try:
        logger.info("Building dimension table customers start ...")
        dim_prd = pd.read_sql_query(query_product, conn)
        logger.info(f"Process of Building is finish. \n{dim_prd.head()}")

    except Exception as e:
        logger.error(f"Error during process of Building : {e}.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")

    return dim_prd


# Build fact table sales
def fact_sales():
    """
    """

    query_sales = """
    --CREATE VIEW gold.fact_sales AS 
    SELECT 
        sd.sls_ord_num AS order_number,
        pr.product_key,
        cu.customer_key,
        --sd.sls_prd_key,
        --sd.sls_cust_id,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt AS shipping_date,
        sd.sls_due_dt AS due_date,
        sd.sls_sales AS sales_amount,
        sd.sls_quantity AS quantity,
        sd.sls_price AS price
    FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_products pr ON sd.sls_prd_key  = pr.product_number
    LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id;
    """
    result = connection_db()
    conn, cur = result
    df_sls = []

    try:
        logger.info("Building dimension table customers start ...")
        df_sls = pd.read_sql_query(query_sales, conn)
        logger.info(f"Process of Building is finish. \n{df_sls.head()}")

    except Exception as e:
        logger.error(f"Error during process of Building : {e}.")

    finally:
        cur.close()
        conn.close()
        logger.info("🔌 Connexion of PostgreSQL closed.")

    return df_sls