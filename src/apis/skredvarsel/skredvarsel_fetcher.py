from apis.fetcher import Fetcher
import requests

class SkredvarselFetcher(Fetcher):
    def create_url(lat, lon, date):
        api_format_sting = "https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/api/AvalancheWarningByCoordinates/Detail/{}/{}/1/{}/{}/"
        return api_format_sting.format(lat, lon, date, date)

    def fetch_data_from_skredvarsel(avalanche_incident):
        api_url = SkredvarselFetcher.create_url(
        date = avalanche_incident.time,
        lat = avalanche_incident.coords_latlng[0],
        lon = avalanche_incident.coords_latlng[1])
        return requests.get(api_url).json()


    def fetch(self, avalanche_incident_list):
        raw_data = {}
        for avalanche_incident in avalanche_incident_list:
            response = SkredvarselFetcher.fetch_data_from_skredvarsel(avalanche_incident)
            raw_data[avalanche_incident.id] = response[0]
            
        return raw_data
            
        



