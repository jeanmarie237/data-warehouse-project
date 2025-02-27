import sys
import os
import psycopg2

# Ajouter le répertoire parent "mon_projet" au chemin de recherche des modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from loguru import logger
from sqlalchemy import create_engine, text
from config.config import*

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import Table
from sqlalchemy import MetaData

database_url = f"postgresql+psycopg2://{user_name}:{credential}@{host}:{port}/postgres"

engine = create_engine(database_url)

# Verifier la connexion
with engine.connect() as connection:
    resul = connection.execute(text("SELECT version();"))
    text_l = resul.fetchall()
    logger.info(f"La version de la base de données est {text_l}")

shema_nom = "bronze_test"

with engine.begin() as connection:
    resul = connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {shema_nom};"))
    logger.info(f"Schema {shema_nom} crée.")

    # Définir le schéma comme actif
    connection.execute(text(f"SET search_path TO {shema_nom};"))

Base = declarative_base()  # Associer le schéma

class Utilisateur(Base):
    __tablename__ = "utilisateur"
    __table_args__ = {"schema": shema_nom}  # Définir explicitement le schéma

    id = Column(Integer, primary_key=True, autoincrement=True)
    nom = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)

Base.metadata.create_all(engine)

# Vérifier si le schéma est bien listé
with engine.connect() as connection:
    result = connection.execute(text("SELECT schema_name FROM information_schema.schemata;"))
    schemas = [row[0] for row in result.fetchall()]
    logger.info(f"Schémas existants : {schemas}")

    if shema_nom in schemas:
        logger.info(f"Le schéma '{shema_nom}' a bien été créé.")
    else:
        logger.info(f"Le schéma '{shema_nom}' n'existe pas.")