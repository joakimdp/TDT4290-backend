# Python libraries
import getopt
import configparser
import ast
import logging
import sys
import time
from typing import Dict

# pip libraries
import pandas as pd
from db_manager import DbManager
from decouple import config
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine

# Regobs
from apis.regobs.regobs import Regobs
from apis.regobs.regobs_initializer import RegobsData, RegobsInitializer

# Xgeo
from apis.xgeo.xgeo_initializer import XgeoData, XgeoInitializer
from apis.xgeo.xgeo import Xgeo

# Frost
from apis.frost.frost import Frost
from apis.frost.frost_initializer import FrostInitializer, FrostObservation

# Skredvarsel
from apis.skredvarsel.skredvarsel import Skredvarsel
from apis.skredvarsel.skredvarsel_initializer import SkredvarselData, SkredvarselInitializer


def create_db_connection() -> Engine:
    server = config('DATABASE_SERVER')
    database = config('DATABASE_NAME')
    username = config('DATABASE_USERNAME')
    password = config('DATABASE_PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = (
        f'mssql+pyodbc://{username}:{password}@{server}/{database}'
        f'?driver={driver}?Trusted_Connection=yes'
    )
    engine = create_engine(connection_string, connect_args={'timeout': 4000})

    return engine


def get_table_dict_for_apis_in_list(api_list, avalanche_incident_list) -> Dict[str, pd.DataFrame]:
    table_dict = {}

    for api in api_list:
        logging.info('Fetching data for: {}'.format(api.__class__.__name__))
        api_table_dict = api.get_data(avalanche_incident_list)
        table_dict.update(api_table_dict)

    return table_dict


def insert_data_for_table_dict(table_dict: Dict[str, pd.DataFrame], db_manager: DbManager, if_exists: str) -> None:
    for table_name, rows in table_dict.items():
        logging.info('Inserting data into {}...'.format(table_name))
        db_manager.insert_dataframe(table_name, rows, if_exists)


def insert_regobs_data_to_database(regobs_data: pd.DataFrame, db_manager: DbManager, if_exists: str) -> None:
    logging.info('Inserting RegObs data into database table..')
    db_manager.insert_dataframe('regobs_data', regobs_data, if_exists)


def initialize_tables(initializer_list: list, engine: Engine) -> None:
    for initializer_class in initializer_list:
        initializer_class(engine).initialize_tables()


def parse_command_line_arguments() -> bool:
    argumentList = sys.argv[1:]
    short_options = 'hf'
    long_options = ['help', 'force-update']

    try:
        # Parsing argument
        arguments, values = getopt.getopt(
            argumentList, short_options, long_options)

        force_update = False
        # checking each argument
        for currentArgument, currentValue in arguments:
            if currentArgument in ("-h", "--help"):
                print('Usage: python main.py\n')
                print(
                    'Run without command line arguments for incremental update of the database\n')
                print(
                    'Optional arguments:\n\t-f, --force-update\t Delete and update entire database.')
                sys.exit()
            elif currentArgument in ("-f", "--force-update"):
                force_update = True

    except getopt.error as err:
        # output error, and return with an error code
        logging.exception(err)

    return force_update


def load_configuration():
    configuration = configparser.ConfigParser()
    configuration.read('configuration.ini')
    fetch_regobs = configuration.getboolean('MAIN', 'fetch_regobs')
    api_fetch_list = ast.literal_eval(
        configuration.get('MAIN', 'api_fetch_list'))
    api_delete_list = ast.literal_eval(
        configuration.get('MAIN', 'api_delete_list'))
    api_initialize_list = ast.literal_eval(
        configuration.get('MAIN', 'api_initialize_list'))

    # Convert string values to corresponding Python classes
    api_fetch_list = [eval(entry + '()') for entry in api_fetch_list]
    api_delete_list = [eval(entry) for entry in api_delete_list]
    api_initialize_list = [eval(entry) for entry in api_initialize_list]

    return fetch_regobs, api_fetch_list, api_delete_list, api_initialize_list
