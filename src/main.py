from apis.regobs.regobs_fetcher import RegobsFetcher
from apis.regobs.regobs_initializer import RegobsInitializer
from apis.regobs.regobs_processor import RegobsProcessor
from apis.xgeo.xgeo_initializer import XgeoInitializer
from apis.xgeo.xgeo import Xgeo
from db_inserter import DbInserter
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine
from decouple import config
from util.avalanche_incident import create_avalanche_incident_list


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


def get_table_dict_for_apis_in_list(api_list, avalanche_incident_list):
    table_dict = {}

    for api in api_list:
        api_table_dict = api.get_data(avalanche_incident_list)
        table_dict.update(api_table_dict)

    return table_dict


def insert_data_for_table_dict(table_dict, db_inserter):
    for table_name, rows in table_dict.items():
        print('Inserting data into {}...'.format(table_name))
        db_inserter.insert(table_name, rows, 'replace')
        print('{} successfully imported into database table'.format(table_name))


def fetch_regobs_data():
    regobs_fetcher = RegobsFetcher()
    fetched_regobs_data = regobs_fetcher.fetch()
    regobs_processor = RegobsProcessor()
    return regobs_processor.process(fetched_regobs_data)


def insert_regobs_data_to_database(regobs_data, db_inserter):
    print('Inserting RegObs data into database table..')
    db_inserter.insert('regobs_data', regobs_data, 'replace')
    print('Data successfully imported to database table')


def initialize_tables(initializer_list, engine):
    for initializer_class in initializer_list:
        initializer_class(engine).initialize_tables()


def main():
    # Get data for regobs
    processed_regobs_data = fetch_regobs_data()

    # Get data for rest of APIs
    avalanche_incident_list = create_avalanche_incident_list(processed_regobs_data)
    api_list = [Xgeo()]
    api_table_dict = get_table_dict_for_apis_in_list(api_list, avalanche_incident_list)

    # Create engine
    engine = create_db_connection()
    db_inserter = DbInserter(engine)

    print("Initializing tables")
    initializer_class_list = [RegobsInitializer, XgeoInitializer]
    initialize_tables(initializer_class_list, engine)

    print("Inserting data into database")
    insert_regobs_data_to_database(processed_regobs_data, db_inserter)
    insert_data_for_table_dict(api_table_dict, db_inserter)

    print("Done")


if __name__ == "__main__":
    main()
