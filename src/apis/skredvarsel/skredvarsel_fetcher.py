from apis.fetcher as fetcher
import requests
from 

class SkredvarselFetcher(fetcher.Fetcher):
    def create_url(lat, lon, date):
        api_format_sting = "https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/api/AvalancheWarningByCoordinates/Simple/{}/{}/1/{}"
     return api_format_sting.format(lat, lon, date)

    def fetch_data_from_skredvarsel(avalanche_incident):
        api_url = SkredvarselFetcher.create_url(
        date = avalanche_incident.time,
        lat = avalanche_incident.coords_latlong[0],
        lon = avalanche_incident.coords_lat_long[1])
     return request.get(create_url(api_url).json()

     def fetch_data_for_avalanche_incidents(avalanche_incident):
         incident_skredvarsel_data = {}
         incident_skredvarsel_data["id"] = avalanche_incident.id
         response = SkredvarselFetcher.fetch_data_from_skredvarsel(avalanche_incident)
         incident_skredvarsel_data["json-data"] = response

        return incident_skredvarsel_data

    def fetch(self, avalanche_incident_list):
        raw_data = []
        for avalanche_incident in avalanche_incident_list:
          avalanche_incident_skredvarsel_data = SkredvarselFetcher.fetch_data_for_avalanche_incidents(avalanche_incident)
          raw_data.append(avalanche_incident_skredvarsel_data)
        return raw_data
            
        



