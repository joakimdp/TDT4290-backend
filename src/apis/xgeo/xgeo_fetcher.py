import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict, List, Tuple

import aiohttp
import pandas as pd
import apis.fetcher as fetcher
from util.async_wrappers import gather_with_concurrency
from util.avalanche_incident import AvalancheIncident


class XgeoFetcher(fetcher.Fetcher):
    DATA_CODES = (("sdfsw3d", "snow_depth_3_days"),
                  ("sdfsw", "snow_depth"),
                  ("rr", "rainfall"),
                  ("tm", "temperature"),
                  ("windSpeed10m24h06", "wind_speed"),
                  ("windDirection10m24h06", "wind_direction"))
    DAYS_EARLIER = 10

    def __init__(self):
        self.incident_count = 1

    def fetch(self, incidents: List[AvalancheIncident]) -> (
        Dict[int, pd.DataFrame]
    ):
        """
        Returns a dictionary where the key is the incident id and the
        value is a dataframe containing relevant data from the xgeo
        api. The format of the dataframe is specified in
        fetch_for_incident
        """

        loop = asyncio.get_event_loop()
        df_dicts = loop.run_until_complete(gather_with_concurrency(40, *(
            self.fetch_for_incident(incident)
            for incident in incidents
        )))

        dataframe_dict = {}
        for dd in df_dicts:
            dataframe_dict.update(dd)

        return dataframe_dict

    async def fetch_for_incident(self, incident: AvalancheIncident) -> (
        Dict[int, pd.DataFrame]
    ):
        """
        Fetches data and returns a dict mapping the avalanche
        incident's ID to a pandas dataframe containing data
        for the avalanche_incident. For the dataframe, the indexes are
        dates for DAYS_EARLIER number of days before the incidents.

        The dataframe will contain one column with data for each tuple
        in DATA_CODES
        """

        if (self.incident_count % 300 == 0):
            await asyncio.sleep(61)
        self.incident_count += 1

        async with aiohttp.ClientSession() as s:
            code_data = await asyncio.gather(*(
                self.fetch_for_code(s, incident, code)
                for code in type(self).DATA_CODES
            ))

            indices = type(self).generate_date_indices(incident.time)
            data = {}
            for cd in code_data:
                data.update(cd)

            return {incident.id: pd.DataFrame(data=data, index=indices)}

    async def fetch_for_code(
        self,
        s: aiohttp.ClientSession,
        incident: AvalancheIncident,
        code: Tuple[str, str]
    ) -> Dict[str, Any]:
        url = type(self).create_url(
            timestamp=incident.time,
            utm_x=incident.coords_utm[0],
            utm_y=incident.coords_utm[1],
            data_code=code[0],
            days_earlier=type(self).DAYS_EARLIER
        )

        return {code[1]: type(self).convert_json_response_to_value_list(
            await self.fetch_url(s, url))}

    async def fetch_url(self, s: aiohttp.ClientSession, url: str) -> (
        Dict[str, Any]
    ):
        # TODO: Clean up this retry hack
        for i in range(5):
            try:
                async with s.get(url) as response:
                    return await response.json()
            except Exception as e:
                logging.exception(
                    f'Exception raised for incident: {self.incident_count} with url: {url}')
                if i == 4:
                    raise e

    @staticmethod
    def create_url(timestamp, utm_x, utm_y, data_code, days_earlier):
        """
        Creates an url to be sent to the API with the given parameters.
        Here, data_code is the code used for getting the right value
        from the API. E.g "rr"=rainfall.
        """
        api_format_string = (
            'http://h-web01.nve.no/chartserver/ShowData.aspx'
            '?req=getchart&ver=1.0&vfmt=json&time={};{}&chs=10x10'
            '&lang=no&chlf=none&chsl=0;+0&chhl=2|0|2&timeo=-06:00&app=3d'
            '&chd=ds=hgts,da=29,id={};{};{},cht=line,mth=inst'
            '&nocache=0.201871693486398'
        )
        end_date = timestamp
        start_date = timestamp - timedelta(days=days_earlier)

        return api_format_string.format(
            XgeoFetcher.create_time_string(start_date),
            XgeoFetcher.create_time_string(end_date),
            utm_x,
            utm_y,
            data_code
        )

    @staticmethod
    def create_time_string(timestamp):
        """Creates the time string xgeo requires from a
        timestamp object"""
        return timestamp.strftime("%Y%m%d") + "T0000"

    @staticmethod
    def convert_json_response_to_value_list(json_response_dict):
        """
        Converts a json-response from the api to a list containing just
        the value points inside the SeriesPoints key.
        """
        series_points = json_response_dict[0]["SeriesPoints"]
        if not series_points:
            return [None for x in range(XgeoFetcher.DAYS_EARLIER + 1)]

        return list(map(lambda s_p: s_p["Value"], series_points))

    @staticmethod
    def generate_date_indices(start_date):
        """
        Input is a json response and output is a list of dates for
        this response in human readable format.
        """
        dates = [
            start_date - timedelta(x)
            for x in range(XgeoFetcher.DAYS_EARLIER + 1)
        ]
        dates.reverse()
        return [date.strftime('%Y-%m-%d') for date in dates]
