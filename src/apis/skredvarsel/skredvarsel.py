import apis.api as api
import skredvarsel_fetcher as sf


class Regobs(api.Processor):
    def get_data(self):
        skred_data = sf.SkredvarselFetcher.fetch()
        return skred_data
        pass
    
    
