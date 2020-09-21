from abc import ABC, abstractmethod

class Initializer(ABC):
    """
    Abstract class to be implemented for each datasource, initializing
    required tables.
    """
    def initialize_tables(self):
        pass
