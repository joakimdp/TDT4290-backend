from typing import List, Type

import pandas as pd
import pyodbc
import sqlalchemy as sqla
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import sessionmaker

from apis.frost.frost_initializer import FrostObservation, FrostSource
from util.csv import to_csv
from util.dataframe_difference import dataframe_difference


class DbManager():
    """
    Class for managing data in the database.
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.Session = sessionmaker(bind=engine)

    def insert_dataframe(self, table_name: str, data_frame: pd.DataFrame, if_exists: str) -> None:

        if table_name == 'frost_sources' and if_exists == 'append':
            data_frame.set_index('id', inplace=True)
            if 'index' in data_frame:
                data_frame.drop(columns=['index'], inplace=True)
            for i in range(len(data_frame)):
                try:
                    data_frame.iloc[i:i+1].to_sql(name=table_name,
                                                  if_exists='append', con=self.engine, chunksize=5, method='multi')
                except IntegrityError:
                    continue
        else:
            # Remove index column if exists
            data_frame.set_index('reg_id', inplace=True)
            if 'index' in data_frame:
                data_frame.drop(columns=['index'], inplace=True)

            data_frame.to_sql(table_name, con=self.engine,
                              if_exists=if_exists, chunksize=5, method='multi')

    def delete_rows_with_reg_id(self, reg_ids: List[str], table_class: Type) -> None:

        session = self.Session()
        for reg_id in reg_ids:
            session.query(table_class).filter(
                table_class.reg_id == reg_id).delete()
            session.commit()

    def query_all_data_from_table(self, table_name: str, index_col: str = None) -> pd.DataFrame:
        connection = self.engine.connect()
        metadata = sqla.MetaData()
        table = sqla.Table(table_name, metadata,
                           autoload=True, autoload_with=self.engine)
        query = sqla.select([table])
        db_data = pd.read_sql(query, connection, index_col=index_col)
        db_data.sort_index(inplace=True)
        return db_data
