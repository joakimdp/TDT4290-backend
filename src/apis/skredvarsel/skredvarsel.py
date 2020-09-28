import apis.api as api
from skredvarsel_fetcher import SkredvarselFetcher
from skredvarsel_processor import SkrevarselProcessor


class Skredvarsel(api.Processor):
    def get_data(self, avalanche_incident_list):
        skred_data = SkredvarselFetcher.fetch(avalanche_incident_list)
        processed_data = SkrevarselProcessor.process(skred_data)
        return processed_data
        pass

    
    
