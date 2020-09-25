import unittest
from datetime import date
from apis.xgeo.xgeo_fetcher import XgeoFetcher
from util.avalanche_incident import AvalancheIncident


class TestXgeoFetcher(unittest.TestCase):
    def test_create_time_string(self):
        timestamp = date.fromisoformat("2019-12-03")
        time_string = XgeoFetcher.create_time_string(timestamp)
        self.assertEqual(time_string, "20191203T0000")

    def test_create_url(self):
        self.maxDiff = None
        timestamp = date.fromisoformat("2019-02-20")
        utm_x = 179633
        utm_y = 6782269
        data_code = "rr"
        days_earlier = 10
        api_url = XgeoFetcher.create_url(timestamp, utm_x, utm_y, data_code, days_earlier)
        expected_api_url = "http://h-web01.nve.no/chartserver/ShowData.aspx?req=getchart&ver=1.0&vfmt=json&time=20190210T0000;20190220T0000&chs=10x10&lang=no&chlf=none&chsl=0;+0&chhl=2|0|2&timeo=-06:00&app=3d&chd=ds=hgts,da=29,id=179633;6782269;rr,cht=line,mth=inst&nocache=0.201871693486398"
        self.assertEqual(api_url, expected_api_url)

    def test_create_url_date_different_month(self):
        self.maxDiff = None
        timestamp = date.fromisoformat("2019-02-01")
        utm_x = 179633
        utm_y = 6782269
        data_code = "rr"
        days_earlier = 10
        api_url = XgeoFetcher.create_url(timestamp, utm_x, utm_y, data_code, days_earlier)

        # 10 days earlier from 2020-02-01 should be 2020-01-22
        expected_api_url = "http://h-web01.nve.no/chartserver/ShowData.aspx?req=getchart&ver=1.0&vfmt=json&time=20190122T0000;20190201T0000&chs=10x10&lang=no&chlf=none&chsl=0;+0&chhl=2|0|2&timeo=-06:00&app=3d&chd=ds=hgts,da=29,id=179633;6782269;rr,cht=line,mth=inst&nocache=0.201871693486398"
        self.assertEqual(api_url, expected_api_url)

    def test_fetch_data_for_data_code(self):
        avalanche_incident = AvalancheIncident(
            id=1,
            time=date.fromisoformat("2019-02-20"),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        )
        data_code = "rr"

        response = XgeoFetcher.fetch_data_for_data_code(avalanche_incident, data_code)
        self.assertEquals(response[0]["LegendText"], "179633;6782269 (519 moh.), Døgnnedbør v2.0")
        self.assertTrue(len(response[0]["SeriesPoints"]) != 0)

    def test_fetch_data_for_avalanche_incident(self):
        avalanche_incident = AvalancheIncident(
            id=1,
            time=date.fromisoformat("2019-02-20"),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        )
        incident_xgeo_data = XgeoFetcher.fetch_data_for_avalanche_incident(avalanche_incident)
        self.assertEquals(incident_xgeo_data["id"], 1)
        self.assertEquals(incident_xgeo_data["rainfall"][0]["LegendText"], "179633;6782269 (519 moh.), Døgnnedbør v2.0")
        self.assertTrue(len(incident_xgeo_data["snow_depth"][0]["SeriesPoints"]) != 0)

    def test_fetch(self):
        avalanche_incident_list = []
        avalanche_incident_list.append(AvalancheIncident(
            id=1,
            time=date.fromisoformat("2019-02-20"),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        ))

        avalanche_incident_list.append(AvalancheIncident(
            id=2,
            time=date.fromisoformat("2019-02-20"),
            coords_utm=(109190, 6725372),
            coords_latlng=(60.474065, 7.882042)
        ))

        raw_data = XgeoFetcher().fetch(avalanche_incident_list)

        self.assertEqual(raw_data[0]["id"], 1)
        self.assertEqual(raw_data[1]["id"], 2)

        self.assertEqual(raw_data[0]["rainfall"][0]["LegendText"], "179633;6782269 (519 moh.), Døgnnedbør v2.0")
        self.assertEqual(raw_data[1]["rainfall"][0]["LegendText"], "109190;6725372 (1229 moh.), Døgnnedbør v2.0")
