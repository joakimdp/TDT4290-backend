from apis.regobs.regobs_fetcher import RegobsFetcher
from apis.regobs.regobs_processor import RegobsProcessor
import utm

if __name__ == "__main__":
    
    regobs_fetcher = RegobsFetcher()
    data_frame = regobs_fetcher.fetch()
    
    print(data_frame.isnull().sum())
    print(data_frame)
    
    regobs_processor = RegobsProcessor(data_frame)
    g = regobs_processor.process()
    print(g)
    