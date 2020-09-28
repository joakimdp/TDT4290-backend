import apis.fetcher as fetcher
import requests
import pandas as pd
from urllib3.exceptions import MaxRetryError
from urllib3 import Retry
from requests.adapters import HTTPAdapter


class RegobsFetcher(fetcher.Fetcher):

    def __init__(self):
        super().__init__()
        self.__avalanche_obs_url = "https://api.nve.no/hydrology/regobs/v3.2.0/Odata.svc/AvalancheObs/?$filter=AvalancheTriggerTID eq 21 or AvalancheTriggerTID eq 26 or AvalancheTriggerTID eq 27&$format=json"
        self.__incident_url = "https://api.nve.no/hydrology/regobs/v3.2.0/Odata.svc/Incident/?$filter=GeoHazardTID eq 10 and ( DamageExtentTID eq 27 or DamageExtentTID eq 28 or DamageExtentTID eq 29 or DamageExtentTID eq 30 or DamageExtentTID eq 40 )&$format=json"

    def fetch(self) -> pd.DataFrame:
        # Get data from AvalancheObs and Incident
        avalanche_obs = self.__fetch_from_api(self.__avalanche_obs_url)
        incident = self.__fetch_from_api(__incident_url)

        self.regobs_df = pd.merge(avalanche_obs, incident, on=[
                                  'RegID'], how='outer')
        # Combine identical columns
        self.__combine_columns(
            'Registration.__deferred.uri', 'RegistrationUrl')
        self.__combine_columns('UsageFlagTID')
        self.__combine_columns('Comment')
        self.__combine_columns('__metadata.id')
        self.__combine_columns('__metadata.uri')
        self.__combine_columns('__metadata.type')

        # Get ObsLocation data
        self.__get_obs_location_data()

        return self.regobs_df

    def __fetch_from_api(self, url: str) -> pd.DataFrame:
        data = requests.get(url)
        json = data.json()['d']['results']
        data_frame = pd.DataFrame.from_dict(
            pd.json_normalize(json), orient='columns')

        if len(data_frame) != 1000:
            return data_frame

        # If the dataset contains more than 1000 records, iterate over each new set and append to data_frame
        last_id = json[-1]['RegID']
        next_data_frame = data_frame

        while len(next_data_frame) == 1000:
            next_data = requests.get(url + '&$skiptoken=' + str(last_id))
            next_json = next_data.json()['d']['results']
            last_id = next_json[-1]['RegID']
            next_data_frame = pd.DataFrame.from_dict(
                pd.json_normalize(next_json), orient='columns')
            data_frame = data_frame.append(next_data_frame)
        return data_frame

    def __combine_columns(self, column_name: str, new_name: str = None) -> None:
        if new_name == None:
            new_name = column_name

        self.regobs_df[new_name] = self.regobs_df[column_name +
                                                  '_x'].combine_first(self.regobs_df[column_name + '_y'])
        self.regobs_df = self.regobs_df.drop(
            columns=[column_name + '_x', column_name + '_y'])

    def __get_obs_location_data(self) -> None:
        utm_east = []
        utm_north = []

        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1,
                        status_forcelist=[502, 503, 504])
        s.mount('http://', HTTPAdapter(max_retries=retries))

        for index, row in self.regobs_df.iterrows():
            reg_id = row['RegID']

            try:
                obs_location = s.get(
                    'http://api.nve.no/hydrology/RegObs/v3.2.0/OData.svc/Registration(' + str(reg_id) + ')/ObsLocation?$format=json').json()['d']
                utm_east.append(obs_location['UTMEast'])
                utm_north.append(obs_location['UTMNorth'])
            except MaxRetryError as e:
                utm_east.append(None)
                utm_north.append(None)

            print('RegID: ', row['RegID'])
            print('East: ', utm_east[index], ' - North: ', utm_north[index])

        self.regobs_df['UTMEast'] = utm_east
        self.regobs_df['UTMNorth'] = utm_north
