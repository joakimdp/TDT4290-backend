from abc import ABC, abstractmethod


class Fetcher(ABC):
    """
    Abstract class for handling fetching of data.
    """
    @abstractmethod
    def fetch(self):
        pass
