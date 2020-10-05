import apis.fetcher as fetcher
import requests
import pandas as pd
from urllib3.exceptions import MaxRetryError
from urllib3 import Retry
from requests.adapters import HTTPAdapter


class RegobsFetcher(fetcher.Fetcher):

    __avalanche_obs_url = "https://api.nve.no/hydrology/regobs/v3.2.0/Odata.svc/AvalancheObs/?$filter=AvalancheTriggerTID eq 21 or AvalancheTriggerTID eq 26 or AvalancheTriggerTID eq 27&$format=json"
    __incident_url = "https://api.nve.no/hydrology/regobs/v3.2.0/Odata.svc/Incident/?$filter=GeoHazardTID eq 10 and ( DamageExtentTID eq 27 or DamageExtentTID eq 28 or DamageExtentTID eq 29 or DamageExtentTID eq 30 or DamageExtentTID eq 40 )&$format=json"

    def fetch(self) -> pd.DataFrame:
        # Get data from AvalancheObs and Incident
        print('Fetching AvalanceObs..')
        avalanche_obs = self.__fetch_from_api(
            RegobsFetcher.__avalanche_obs_url)
        print('Fetching Incident..')
        incident = self.__fetch_from_api(RegobsFetcher.__incident_url)

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
        print('Fetching additional data..')
        self.__get_additional_data()

        print('All data fetched from RegObs.')
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

    def __get_additional_data(self) -> None:
        # obs_location data
        utm_east = []
        utm_north = []

        # registration data
        dt_obs_time = []
        dt_reg_time = []
        deleted_date = []
        dt_change_time = []

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

            try:
                registration = s.get(
                    'http://api.nve.no/hydrology/RegObs/v3.2.0/OData.svc/Registration(' + str(reg_id) + ')?$format=json').json()['d']
                dt_obs_time.append(registration['DtObsTime'])
                dt_reg_time.append(registration['DtRegTime'])
                deleted_date.append(registration['DeletedDate'])
                dt_change_time.append(registration['DtChangeTime'])
            except MaxRetryError as e:
                dt_obs_time.append(None)
                dt_reg_time.append(None)
                deleted_date.append(None)
                dt_change_time.append(None)

        self.regobs_df['UTMEast'] = utm_east
        self.regobs_df['UTMNorth'] = utm_north

        self.regobs_df['DtObsTime'] = dt_obs_time
        self.regobs_df['DtRegTime'] = dt_reg_time
        self.regobs_df['DeletedDate'] = deleted_date
        self.regobs_df['DtChangeTime'] = dt_change_time
