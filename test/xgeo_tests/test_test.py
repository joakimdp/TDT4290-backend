import unittest
import apis.xgeo.xgeo_fetcher as x


class TestStringMethods(unittest.TestCase):
    def test_xgeo_fetcher(self):
        result = x.XgeoFetcher().fetch(1)
        self.assertEqual(result[0][0], "2020-09-15")
