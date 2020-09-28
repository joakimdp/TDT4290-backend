import pyodbc


class DbInserter():
    """
    Class for inserting data to a table in the database.
    """

    def __init__(self, id, time, coords_utm, coords_latlng):
        self.id = id
        self.time = time
        self.coords_utm = coords_utm
        self.coords_latlng = coords_latlng

    def connect(self):
        # TODO: implement
