from apis.api import Api
from apis.regobs.regobs_fetcher import RegobsFetcher
from apis.regobs.regobs_processor import RegobsProcessor
from util.csv import to_csv, read_csv


class Regobs(Api):
    def get_data(self):
        # regobs_fetcher = RegobsFetcher()
        # fetched_regobs_data = regobs_fetcher.fetch()
        # regobs_processor = RegobsProcessor()
        # processed = regobs_processor.process(fetched_regobs_data)
        # to_csv(processed, 'csv_files/regobs-backup.csv')
        processed = read_csv('csv_files/regobs-6.csv')
        return processed
