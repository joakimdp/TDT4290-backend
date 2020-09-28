# TODO: Implement this

from apis.regobs.regobs_fetcher import RegobsFetcher


if __name__ == "__main__":
    regobs_fetcher = RegobsFetcher()
    data_frame = regobs_fetcher.fetch()
    print(data_frame.isnull().sum())
    print(data_frame)
