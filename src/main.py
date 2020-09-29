from apis.regobs.regobs_fetcher import RegobsFetcher
from apis.regobs.regobs_initializer import RegobsInitializer
from apis.regobs.regobs_processor import RegobsProcessor
from db_inserter import DbInserter
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine
from decouple import config


def main():
    # Initialize database connection and database tables
    engine = create_db_connection()

    regobs_initializer = RegobsInitializer(engine)
    regobs_initializer.initialize_tables()

    # Fetch and process RegObs data
    regobs_fetcher = RegobsFetcher()
    fetched_regobs_data = regobs_fetcher.fetch()

    print('Processing RegObs data..')
    regobs_processor = RegobsProcessor()
    processed_regobs_data = regobs_processor.process(fetched_regobs_data)

    # TODO: generate API list

    # Insert Regobs data into database table
    print('Inserting RegObs data into database table..')
    db_inserter = DbInserter(engine)
    db_inserter.insert('regobs_data', processed_regobs_data, 'replace')
    print('Data successfully imported to database table')


def create_db_connection() -> Engine:
    server = 'avalanche-server.database.windows.net,1433'
    database = 'avalanche-db'
    username = config('USERNAME')
    password = config('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = 'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}?Trusted_Connection=yes'.format(
        username=username, password=password, server=server, database=database, driver=driver)
    engine = create_engine(connection_string)

    return engine


if __name__ == "__main__":
    main()
