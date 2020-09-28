import pyodbc
import pandas as pd
from sqlalchemy.engine.base import Engine


class DbInserter():
    """
    Class for inserting data to a table in the database.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def insert(self, table_name: str, data_frame: pd.DataFrame, if_exists: str) -> None:
        data_frame.to_sql(table_name, con=self.engine,
                          if_exists=if_exists, chunksize=50, method='multi')
