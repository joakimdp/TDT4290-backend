import requests
from datetime import timedelta
import apis.fetcher as fetcher


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

    def fetch_data_for_avalanche_incident(avalanche_incident):
        """
        Returns a dictionary containing id of avalanche_incident and
        data for each datacode in DATA_CODE_LIST of the form:

        data_code_name: raw_json_response

        """
        incident_xgeo_data = {}
        incident_xgeo_data["id"] = avalanche_incident.id
        for data_code_tuple in XgeoFetcher.DATA_CODE_LIST:
            data_code = data_code_tuple[0]
            data_code_name = data_code_tuple[1]

            response = XgeoFetcher.fetch_data_for_data_code(avalanche_incident, data_code)
            incident_xgeo_data[data_code_name] = response

        return incident_xgeo_data

    def fetch(self, avalanche_incident_list):
        """
        Returns a list of dictionaries containing data from the
        requests in the following form:
        [
          {
            id: id_of_avalanche_incident
            data_code_name_1: json_response_1
            data_code_name_2: json_response_2
            ...
            data_code_name_n: json_response_n
          },
          ...
        ]
        """
        raw_data = []
        for avalanche_incident in avalanche_incident_list:
            # Create dictionary for storing the data
            avalanche_incident_xgeo_data = XgeoFetcher.fetch_data_for_avalanche_incident(avalanche_incident)
            raw_data.append(avalanche_incident_xgeo_data)

        return raw_data
