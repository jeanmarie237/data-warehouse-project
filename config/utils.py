import psycopg2
from config.config import user_name, credential, host, port, db_name

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

def db_connexion():
    """
    """
    conn = psycopg2.connect(
        dbname=db_name,
        user=user_name,
        password=credential,
        port=port,
        host=host
    )
    # Creation cursor
    cur = conn.cursor()

    return conn, cur