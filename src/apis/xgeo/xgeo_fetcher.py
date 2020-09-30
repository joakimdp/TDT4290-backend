import requests
from datetime import timedelta
import apis.fetcher as fetcher
import pandas as pd
from datetime import datetime, timedelta
import re


class XgeoFetcher(fetcher.Fetcher):
    DATA_CODE_LIST = (("sdfsw3d", "snow_depth_3_days"),
                      ("sdfsw", "snow_depth"),
                      ("rr", "rainfall"),
                      ("tm", "temperature"),
                      ("windSpeed10m24h06", "wind_speed"),
                      ("windDirection10m24h06", "wind_direction"))
    DAYS_EARLIER = 10

    def create_time_string(timestamp):
        """ Creates the time string xgeo requires from a timestamp object """
        return timestamp.strftime("%Y%m%d") + "T0000"

    def create_url(timestamp, utm_x, utm_y, data_code, days_earlier):
        """
        Creates an url to be sent to the API with the given parameters.
        Here, data_code is the code used for getting the right value
        from the API. E.g "rr"=rainfall.
        """
        api_format_string = "http://h-web01.nve.no/chartserver/ShowData.aspx?req=getchart&ver=1.0&vfmt=json&time={};{}&chs=10x10&lang=no&chlf=none&chsl=0;+0&chhl=2|0|2&timeo=-06:00&app=3d&chd=ds=hgts,da=29,id={};{};{},cht=line,mth=inst&nocache=0.201871693486398"
        end_date = timestamp
        start_date = timestamp - timedelta(days=days_earlier)

        return api_format_string.format(
            XgeoFetcher.create_time_string(start_date),
            XgeoFetcher.create_time_string(end_date),
            utm_x,
            utm_y,
            data_code
        )

    def fetch_data_for_data_code(avalanche_incident, data_code):
        api_url = XgeoFetcher.create_url(
            timestamp=avalanche_incident.time,
            utm_x=avalanche_incident.coords_utm[0],
            utm_y=avalanche_incident.coords_utm[1],
            data_code=data_code,
            days_earlier=XgeoFetcher.DAYS_EARLIER)

        return requests.get(api_url).json()

    def convert_json_response_to_value_list(json_response_dict):
        """
        Converts a json-response from the api to a list containing just
        the value points inside the SeriesPoints key.
        """
        series_points = json_response_dict[0]["SeriesPoints"]
        if not series_points:
            return [None for x in range(XgeoFetcher.DAYS_EARLIER + 1)]

        return list(map(lambda s_p: s_p["Value"], series_points))

    def generate_date_indices(start_date):
        """
        Input is a json response and output is a list of dates for
        this response in human readable format.
        """
        dates = [start_date - timedelta(x) for x in range(XgeoFetcher.DAYS_EARLIER + 1)]
        dates.reverse()
        return [date.strftime('%Y-%m-%d') for date in dates]

    def fetch_data_for_avalanche_incident(avalanche_incident):
        """
        Fetches data and returns a pandas dataframe containing data
        for the avalanche_incident. For the dataframe, the indexes are
        dates for DAYS_EARLIER number of days before the incidents.

        The dataframe will contain one column with data for each tuple
        in DATA_CODE_LIST
        """
        xgeo_data_dict = {}
        indices = XgeoFetcher.generate_date_indices(avalanche_incident.time)

        for data_code_tuple in XgeoFetcher.DATA_CODE_LIST:
            data_code = data_code_tuple[0]
            data_code_name = data_code_tuple[1]

            response = XgeoFetcher.fetch_data_for_data_code(avalanche_incident, data_code)
            xgeo_data_dict[data_code_name] = XgeoFetcher.convert_json_response_to_value_list(response)

        return pd.DataFrame(data=xgeo_data_dict, index=indices)

    def fetch(self, avalanche_incident_list):
        """
        Returns a dictionary where the key is the incident id and the
        value is a dataframe containing relevant data from the xgeo
        api. The format of the dataframe is spesified in
        fetch_data_for_avalanche_incident
        """
        dataframe_dict = {}

        for avalanche_incident in avalanche_incident_list:
            dataframe = XgeoFetcher.fetch_data_for_avalanche_incident(avalanche_incident)
            dataframe_dict[avalanche_incident.id] = dataframe

        return dataframe_dict
