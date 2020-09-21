#import apis.fetcher as fetcher
import requests
from pandas.io.json import json_normalize
import pandas as pd
import pyodbc


class RegobsFetcher():
    def fetch(self):
        result = requests.get(
            "https://api.nve.no/hydrology/regobs/v3.2.0/Odata.svc/AvalancheObs/?$filter=AvalancheTriggerTID eq 21 or AvalancheTriggerTID eq 26 or AvalancheTriggerTID eq 27&$format=json")
        print(result.status_code)

        json = result.json()['d']['results']

        # for element in json:
        #    print(element['RegID'])

        df = pd.DataFrame.from_dict(json_normalize(json), orient='columns')

        print(df)

        server = 'tcp:avalanche-server.database.windows.net,1433'
        database = 'avalanche-db'
        username = 'admin_user'
        password = '3S#@7aa2tH'
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                              server+';DATABASE='+database+';UID='+username+';PWD=' + password)
        cursor = cnxn.cursor()

        cursor.execute("SELECT @@version;")
        row = cursor.fetchone()
        while row:
            print(row[0])
            row = cursor.fetchone()


if __name__ == "__main__":
    regObsFetcher = RegobsFetcher()

    regObsFetcher.fetch()
