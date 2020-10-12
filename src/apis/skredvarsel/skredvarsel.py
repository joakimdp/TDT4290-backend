import apis.api as api
from apis.skredvarsel.skredvarsel_fetcher import SkredvarselFetcher
from apis.skredvarsel.skredvarsel_processor import SkredvarselProcessor
from apis.skredvarsel.skredvarsel_initializer import SkredvarselInitializer
from apis.skredvarsel.skredvarsel_initializer import SkredvarselData
from apis.processor import Processor
from apis.fetcher import Fetcher
import pandas as pd



class Skredvarsel():
    table_name = SkredvarselData.__tablename__
    
    def get_data(self, avalanche_incident_list):
        """
        Input as a list of AvalancheIncident objects and output is a
        list of database rows ready to be put into the database.
        """
        dataframe_dict = SkredvarselFetcher().fetch(avalanche_incident_list)
        database_rows = SkredvarselProcessor().process(dataframe_dict)
        return {Skredvarsel.table_name: database_rows}


    '''
    def get_data(self, avalanche_incident_list, engine, db_inserter):
        table = SkredvarselInitializer(engine)
        table.initialize_tables()
        fetch = False
        print("Ferdig med å intialisere")
        if fetch:
            raw_data = SkredvarselFetcher().fetch(avalanche_incident_list)
            print("Ferdig med å fetche data")
            processed_data = SkredvarselProcessor().process(raw_data)
            processed_data.to_csv("skredvarsel_data.csv", index = False)
        else:
            processed_data = pd.read_csv("src/skredvarsel_data.csv")
        print("Ferdig med å prosessere data")
        db_inserter.insert('skredvarsel_data', processed_data, 'replace')
        print("Ferdig med å inserte")
        '''

    
    
