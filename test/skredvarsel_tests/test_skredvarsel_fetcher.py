import unittest
from datetime import date
from src.apis.skredvarsel.skredvarsel_fetcher import SkredvarselFetcher

class TestSkredvarselFetcher(unittest.TestCase):
    
    def test_create_url(self):
        date = "2020-01-05"
        lat = 61.62413454082607
        lan = 8.470996875
        api_url = SkredvarselFetcher.create_url(lat, lon, date)
        expected_api_url = "https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/api/AvalancheWarningByCoordinates/Detail/61.62413454082607/8.470996875/1/2020-01-05/"
        self.assertEqual(api_url, expected_api_url)

    