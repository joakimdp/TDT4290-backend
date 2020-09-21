from abc import ABC


class Fetcher(ABC):
    """
    Abstract class for handling fetching of data.
    """

    def fetch(self):
        pass
