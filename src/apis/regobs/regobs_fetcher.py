import apis.fetcher as fetcher


class RegobsFetcher(fetcher.Fetcher):
    def fetch(self):
        return "Hei, dette er data fetchet fra regobs"
