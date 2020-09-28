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
        server = 'tcp:avalanche-server.database.windows.net,1433'
        database = 'avalanche-db'
        username = 'admin_user'
        password = '[password]'
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                              server+';DATABASE='+database+';UID='+username+';PWD=' + password)
        cursor = cnxn.cursor()

        cursor.execute("SELECT @@version;")
        row = cursor.fetchone()
        while row:
            print(row[0])
            row = cursor.fetchone()
