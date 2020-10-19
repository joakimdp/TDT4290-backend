import unittest
import pandas as pd
import numpy as np
import pandas.testing as pd_testing

from apis.regobs.regobs_fetcher import RegobsFetcher
from apis.regobs.regobs_processor import RegobsProcessor


class TestRegobsFetcher(unittest.TestCase):

    def test_fetch_from_api(self):
        url = "https://api.nve.no/hydrology/regobs/v3.2.0/Odata.svc/AvalancheObs/?$filter=AvalancheTriggerTID eq 21 or AvalancheTriggerTID eq 26 or AvalancheTriggerTID eq 27&$format=json"

        regobs_fetcher = RegobsFetcher()

        dataframe = regobs_fetcher._RegobsFetcher__fetch_from_api(url)

        print(dataframe)
        self.assertEqual(len(dataframe.columns), 26)
        self.assertTrue('RegID' in dataframe.columns)
