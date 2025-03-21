import sys
import os
import pandas as pd
import pytest
import yaml

# Ajouter le répertoire racine du projet à sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(project_root)

# Importer la fonction à tester
from scripts.ETL.extract import read_data

# Charger le fichier YAML de configuration
path_ymal = os.path.join(project_root, "scripts", "schemas", "all_tables.yaml")
with open(path_ymal, 'r') as file:
    config_data = yaml.safe_load(file)


def test_read_data_valid_csv():
    df = read_data(config_data['px_cat'])
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_read_data_invalid_csv():
    """
    Teste que la fonction read_data retourne un DataFrame vide
    lorsqu'un fichier CSV invalide est fourni.
    """
    df = read_data(config_data['px_cat_p'])
    assert df.empty

def test_read_data_missing_file():
    """
    Teste que la fonction read_data retourne un DataFrame vide
    lorsqu'un fichier manquant est fourni.
    """
    df = read_data("missing_file.csv")
    assert df.empty

def test_read_data_logging(caplog):
    """
    Teste que la fonction read_data génère les logs appropriés.
    """
    read_data(config_data['px_cat'])
    # Vérifier que le message de log est présent
    assert any("Reading this" in record.message for record in caplog.records)