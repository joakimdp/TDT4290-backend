from apis.fetcher as fetcher
import requests
from 

class SkredvarselFetcher(fetcher.Fetcher):
    def fetch(self, avalanche_incident_list):
        response = []
        for incident in avalanche_incident_list:
            time = incident.time
            lat = incident.lat
            lon = incident.long
            #url = "https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/api/AvalancheWarningByCoordinates/Simple/" + lat + "/" + lon + "/" + "/1/" + time
            urlTest = "https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/api/AvalancheWarningByCoordinates/Detail/61.608466699269655/8.5643806640625/1/2020-01-01/"
            response.append(requests.get(url).json())
            print(response)
        return response
            
        



