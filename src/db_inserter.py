import pyodbc
import pandas as pd
from sqlalchemy.engine.base import Engine


class DbInserter():
    """
    Class for inserting data to a table in the database.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def insert(self, table_name: str, data_frame: pd.DataFrame) -> None:
        data_frame.to_sql(table_name, con=self.engine,
                          if_exists='replace', chunksize=50, method='multi')
