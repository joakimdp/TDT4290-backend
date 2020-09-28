from apis.regobs.regobs_fetcher import RegobsFetcher
from apis.regobs.regobs_initializer import RegobsInitializer
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine
from decouple import config


def main():
    regobs_fetcher = RegobsFetcher()
    # Uncomment to fetch Regobs data
    """ data_frame = regobs_fetcher.fetch()
    print(data_frame.isnull().sum())
    print(data_frame) """

    engine = create_db_connection()

    regobs_initializer = RegobsInitializer(engine)
    regobs_initializer.initialize_tables()


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
