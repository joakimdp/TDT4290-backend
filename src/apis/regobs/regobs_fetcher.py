import apis.fetcher as fetcher
import requests
import pandas as pd


class RegobsFetcher(fetcher.Fetcher):
    def fetch(self) -> pd.DataFrame:
        # TODO: move urls
        avalanche_obs_url = "https://api.nve.no/hydrology/regobs/v3.2.0/Odata.svc/AvalancheObs/?$filter=AvalancheTriggerTID eq 21 or AvalancheTriggerTID eq 26 or AvalancheTriggerTID eq 27&$format=json"
        incident_url = "https://api.nve.no/hydrology/regobs/v3.2.0/Odata.svc/Incident/?$filter=GeoHazardTID eq 10 and ( DamageExtentTID eq 27 or DamageExtentTID eq 28 or DamageExtentTID eq 29 or DamageExtentTID eq 30 or DamageExtentTID eq 40 )&$format=json"

        avalanche_obs = self.__fetch_from_api(avalanche_obs_url)
        incident = self.__fetch_from_api(incident_url)

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
        return self.regobs_df

    def __combine_columns(self, column_name: str, new_name: str = None):
        if new_name == None:
            new_name = column_name

        print(new_name)
        self.regobs_df[new_name] = self.regobs_df[column_name + '_x'].combine_first(
            self.regobs_df[column_name + '_y'])
        self.regobs_df = self.regobs_df.drop(
            columns=[column_name + '_x', column_name + '_y'])

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
