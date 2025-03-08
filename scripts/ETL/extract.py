import os
import sys
import yaml
import pandas as pd
from loguru import logger

yml_file = os.path.join(os.path.dirname(__file__), "..", "schemas", "all_tables.yaml")

with open(yml_file, 'r') as file:
    config_data = yaml.safe_load(file)

# Remove the default logger configuration
logger.remove()
logger.add("estate.log", rotation="900kb", level="DEBUG")
logger.add(sys.stderr, level="INFO")

# print(config_data)

def read_data(file_path:str):
    """
    """
    df = pd.DataFrame()  # 

    try:
        if isinstance(file_path, str) and file_path.endswith(".csv"):
            logger.info(f"Reading this {file_path}.")
            df = pd.read_csv(file_path)
        else:
            logger.warning("No file ends whith .csv please go check in all_tables.yaml")
    except Exception as e:
        logger.error(f" Error : '{e}' when reading this {file_path}")
    return df

def reading_crm():
    """This fuction reading all files"""

    read_data(config_data['prd_info'])
    read_data(config_data['cust_info'])
    read_data(config_data['sales_in'])

if __name__ == "__main__":
    reading_crm() 