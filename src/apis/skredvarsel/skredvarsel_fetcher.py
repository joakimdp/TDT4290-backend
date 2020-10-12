from apis.fetcher import Fetcher
import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib3.exceptions import MaxRetryError

class SkredvarselFetcher(Fetcher):
    def create_url(lat, lon, date):
        api_format_sting = "https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/api/AvalancheWarningByCoordinates/Detail/{lat}/{lon}/1/{date_start}/{date_stop}/"
        return api_format_sting.format(lat=lat, lon=lon, date_start=date, date_stop=date)

    def fetch_data_from_skredvarsel(avalanche_incident, request_session):

        if isinstance(avalanche_incident.time, str):
            date = str(datetime.datetime.strptime(avalanche_incident.time, '%Y-%m-%d %H:%M:%S.%f').date())
        else:
            date = avalanche_incident.time.strftime('%Y-%m-%d')

        utm_north = avalanche_incident.coords_utm[1]
        if(utm_north<0):
            return None
        lat = avalanche_incident.coords_latlng[0]
        lon = avalanche_incident.coords_latlng[1]
        if lat == None or lon == None or date == None:
           print("Hallohalloi")
           return None
        api_url = SkredvarselFetcher.create_url(lat, lon, date)
        print(api_url)
        #neste gang: lage if for i tilfelle date, lot, lon ikke er gyldige, eller null
        response = request_session.get(api_url).json()
        return response
        

    def add_avalanche_id(response, avalanche_incident):
        data_dict = {}
        if response == None:
            print("Denne requesten er Nonetype")
            return None
        data_dict.update(response[0])
        data_dict.update({"id": avalanche_incident.id})
        return data_dict


    def fetch(self, avalanche_incident_list):
        raw_data = []
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502,503,504])
        s.mount('https://', HTTPAdapter(max_retries=retries))
        for avalanche_incident in avalanche_incident_list:
            response = SkredvarselFetcher.fetch_data_from_skredvarsel(avalanche_incident, s)
            response_with_id = SkredvarselFetcher.add_avalanche_id(response, avalanche_incident)
            if(response_with_id == None):
                continue
            raw_data.append(response_with_id)
        return raw_data
            
        



