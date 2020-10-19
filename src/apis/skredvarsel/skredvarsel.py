from typing import Dict

import apis.api as api
import pandas as pd
from apis.fetcher import Fetcher
from apis.processor import Processor
from apis.skredvarsel.skredvarsel_fetcher import SkredvarselFetcher
from apis.skredvarsel.skredvarsel_initializer import (SkredvarselData,
                                                      SkredvarselInitializer)
from apis.skredvarsel.skredvarsel_processor import SkredvarselProcessor


class Skredvarsel():
    table_name = SkredvarselData.__tablename__

    def get_data(self, avalanche_incident_list: list) -> Dict[str, pd.DataFrame]:
        """
        Input as a list of AvalancheIncident objects and output is a
        list of database rows ready to be put into the database.
        """
        dataframe_dict = SkredvarselFetcher().fetch(avalanche_incident_list)
        database_rows = SkredvarselProcessor().process(dataframe_dict)
        return {Skredvarsel.table_name: database_rows}
