import requests
from datetime import datetime
import re
import apis.fetcher as fetcher


class XgeoFetcher(fetcher.Fetcher):
    def fetch(self, avalanche_incident_list):
        api_url = "http://h-web01.nve.no/chartserver/ShowData.aspx?req=getchart&ver=1.0&vfmt=json&time=20200915T1000;20200923T1000&chs=10x10&lang=no&chlf=none&chsl=0;+0&chhl=2|0|2&timeo=-06:00&app=3d&chd=ds=hgts,da=29,id=453276;7386451;sdfsw3d,cht=line,mth=inst&nocache=0.201871693486398"
        results = requests.get(api_url).json()[0]['SeriesPoints']
        data = []
        for day in results:
            timestamp = int(re.split(r'\(|\)', day['Key'])[1]) / 1000
            value = day['Value']
            time_string = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            data.append((time_string, value))

        return data
