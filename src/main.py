#!/usr/bin/env python3

import time
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine
from decouple import config
from db_inserter import DbInserter
from util.avalanche_incident import create_avalanche_incident_list
# RegObs
from apis.regobs.regobs import Regobs
from apis.regobs.regobs_initializer import RegobsInitializer
# xGeo
from apis.xgeo.xgeo import Xgeo
from apis.xgeo.xgeo_initializer import XgeoInitializer
# Frost
from apis.frost.frost import Frost
from apis.frost.frost_initializer import FrostInitializer
# Skredvarsel
from apis.skredvarsel.skredvarsel import Skredvarsel
from apis.skredvarsel.skredvarsel_initializer import SkredvarselInitializer


def create_db_connection() -> Engine:
    server = 'avalanche-server.database.windows.net,1433'
    database = 'avalanche-db'
    username = config('DBUSERNAME')
    password = config('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = (
        f'mssql+pyodbc://{username}:{password}@{server}/{database}'
        f'?driver={driver}?Trusted_Connection=yes'
    )
    engine = create_engine(connection_string, connect_args={'timeout': 4000})

    return engine


def get_table_dict_for_apis_in_list(api_list, avalanche_incident_list):
    table_dict = {}

    for api in api_list:
        print(
            f'{time.ctime(time.time())}: '
            f'Fetching for {api.__class__.__name__}...'
        )
        api_table_dict = api.get_data(avalanche_incident_list)
        table_dict.update(api_table_dict)

    return table_dict


def insert_data_for_table_dict(table_dict, db_inserter):
    for table_name, rows in table_dict.items():
        print(
            f'{time.ctime(time.time())}: '
            f'Inserting data into {table_name}...'
        )
        db_inserter.insert(table_name, rows, 'replace')
        print(
            f'{time.ctime(time.time())}: '
            f'successfully imported into database table {table_name}'
        )


def insert_regobs_data_to_database(regobs_data, db_inserter):
    print('Inserting RegObs data into database table..')
    db_inserter.insert('regobs_data', regobs_data, 'replace')
    print('Data successfully imported to database table')


def initialize_tables(initializer_list, engine):
    for initializer_class in initializer_list:
        initializer_class(engine).initialize_tables()


def main():
    # Get data for regobs
    processed_regobs_data = Regobs().get_data()

    # Get data for rest of APIs
    avalanche_incident_list = create_avalanche_incident_list(
        processed_regobs_data)
    api_list = [
        Xgeo(),
        Frost(),
        Skredvarsel()
    ]
    print('Fetching data from APIs')
    api_table_dict = get_table_dict_for_apis_in_list(
        api_list, avalanche_incident_list)

    # Create engine
    engine = create_db_connection()
    db_inserter = DbInserter(engine)

    print('Initializing tables')
    initializer_class_list = [
        RegobsInitializer,
        XgeoInitializer,
        FrostInitializer,
        SkredvarselInitializer
    ]
    initialize_tables(initializer_class_list, engine)

    print('Inserting data into database')
    insert_regobs_data_to_database(processed_regobs_data, db_inserter)
    insert_data_for_table_dict(api_table_dict, db_inserter)

    print('Done')


if __name__ == '__main__':
    main()
