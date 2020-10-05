from apis.api import Api
from apis.regobs.regobs_fetcher import RegobsFetcher
from apis.regobs.regobs_processor import RegobsProcessor


class Regobs(Api):
    def get_data(self):
        regobs_fetcher = RegobsFetcher()
        fetched_regobs_data = regobs_fetcher.fetch()
        regobs_processor = RegobsProcessor()
        return regobs_processor.process(fetched_regobs_data)
