from apis.api import Api
from apis.xgeo.xgeo_fetcher import XgeoFetcher
from apis.xgeo.xgeo_processor import XgeoProcessor
from apis.xgeo.xgeo_initializer import XgeoData


class Xgeo(Api):
    table_name = XgeoData.__tablename__

    def get_data(self, avalance_incident_list):
        """
        Input as a list of AvalancheIncident objects and output is a
        list of database rows ready to be put into the database.
        """
        dataframe_dict = XgeoFetcher().fetch(avalance_incident_list)
        database_rows = XgeoProcessor().process(dataframe_dict)
        return database_rows
