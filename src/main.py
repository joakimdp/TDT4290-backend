from apis.regobs.regobs_fetcher import RegobsFetcher
from apis.regobs.regobs_initializer import RegobsInitializer
from apis.regobs.regobs_processor import RegobsProcessor
from apis.xgeo.xgeo_fetcher import XgeoFetcher
from apis.xgeo.xgeo_processor import XgeoProcessor
from apis.xgeo.xgeo_initializer import XgeoInitializer
from apis.xgeo.xgeo import Xgeo
from db_inserter import DbInserter
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine
from decouple import config
from datetime import date
import pandas as pd
from util.avalanche_incident import AvalancheIncident, create_avalanche_incident_list
import time


def get_xgeo_data():
    start_time = time.time()

    # Fetch and process RegObs data
    regobs_fetcher = RegobsFetcher()
    fetched_regobs_data = regobs_fetcher.fetch()

    print('Processing RegObs data..')
    regobs_processor = RegobsProcessor()
    processed_regobs_data = regobs_processor.process(fetched_regobs_data)

    aval_list = create_avalanche_incident_list(processed_regobs_data)

    print("Fetching data from xgeo")
    # aval_list = aval_list[-50:]

    dataframe_dict = XgeoFetcher().fetch(aval_list)

    print("-------------------------------")
    for key, value in dataframe_dict.items():
        print("Data for id " + str(key) + ":")
        print()
        # print(value)
        print(value)
        print()
        print("-------------------------------")

    end_time = time.time()
    print("Finished in " + str(end_time - start_time) + " seconds")
    return XgeoProcessor().process(dataframe_dict)


def main():
    # Initialize database connection and database tables
    engine = create_db_connection()

    regobs_initializer = RegobsInitializer(engine)
    regobs_initializer.initialize_tables()

    XgeoInitializer(engine).initialize_tables()

    xgeo_rows = get_xgeo_data()

    print('Inserting Xgeo data into database table..')
    db_inserter = DbInserter(engine)
    db_inserter.insert('xgeo_data', xgeo_rows, 'replace')
    print('Data successfully imported to database table')

    # Insert Regobs data into database table

    # print('Inserting RegObs data into database table..')
    # db_inserter = DbInserter(engine)
    # db_inserter.insert('regobs_data', processed_regobs_data, 'replace')
    # print('Data successfully imported to database table')


def create_db_connection() -> Engine:
    server = 'avalanche-server.database.windows.net,1433'
    database = 'avalanche-db'
    username = config('USERNAME')
    password = config('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = 'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}?Trusted_Connection=yes'.format(
        username=username, password=password, server=server, database=database, driver=driver)
    engine = create_engine(connection_string, connect_args={'timeout': 4000})

    return engine


if __name__ == "__main__":
    main()
