import os
import sys

import psycopg2

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from loguru import logger
from sqlalchemy import create_engine, text

from config.config import *

# Remove the default logger configuration
logger.remove()

# Log to a file with DEBUG level
logger.add("estate.log", rotation="900kb", level="DEBUG")

# Log to the console with INFO level (less verbose)
logger.add(sys.stderr, level="INFO")

url_db = f"postgresql+psycopg2://{user_name}:{credential}@{host}:{port}/{db_name}"


engine = create_engine(url_db)


def create_schema(url_db: object, schema_name: str):
    """This function creates the schema in the PostgreSQL Database.

    :param url_db:        Url of connexion on a database
    :param schema_name:   Name of the schema we are going to create
    """

    # Verify the connexion
    with engine.connect() as connection:
        resul = connection.execute(text("SELECT version();"))
        text_l = resul.fetchall()
        logger.info(f"vesion of database is {text_l}")

    try:
        logger.info("Connexion of the database.")
        with engine.begin() as connexion:
            # We create the schema with SQL command
            create_schema_query = text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            connexion.execute(create_schema_query)
            logger.info(f"The schema {schema_name} has been created succefully.")

    except Exception as e:
        logger.error(
            f"Error {e} when creating the schema {schema_name}. Please go to check..."
        )

    with engine.connect() as connexion:
        check_schama_query = connexion.execute(
            text("SELECT schema_name FROM information_schema.schemata;")
        )
        # We check all the database schemas
        schemas = [row[0] for row in check_schama_query.fetchall()]
        logger.info(f"Schemas existing : {schemas}")

    if schema_name in schemas:
        logger.info(
            f"This schema has been created and added succefully in the database : {db_name}."
        )
    else:
        logger.info(
            f"The schema {schema_name} has not exist in the database : {db_name}."
        )


def main():
    create_schema(url_db, "bronze")
    create_schema(url_db, "silver")
    create_schema(url_db, "gold")


if __name__ == "__main__":
    main()
