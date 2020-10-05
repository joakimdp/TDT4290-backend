import unittest
import datetime as dt
import pandas as pd
import pandas.testing as pd_testing
import numpy as np
from apis.frost.frost_fetcher import FrostFetcher
from util.avalanche_incident import AvalancheIncident


class TestFrostFetcher(unittest.TestCase):
    def assertDataFrameEqual(self, a, b, msg=None):
        try:
            pd_testing.assert_frame_equal(a, b, check_column_type=False, check_index_type=False, check_dtype=False)
        except AssertionError as e:
            raise self.failureException(str(e)) from e

    def setUp(self):
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataFrameEqual)

    def test_fetch_single_incident(self):
        incidents = (AvalancheIncident(
            342,
            dt.datetime(2020, 5, 3),
            (0, 0),
            (59, 10)
        ),)
        fetcher = FrostFetcher()
        result = fetcher.fetch(incidents)
        sources = result['frost_sources']
        observations = result['frost_observations']

        # Sort and trim the dataframes for easier testing
        sources = sources.head(3).reset_index(drop=True)
        observations = observations.sort_values(
            ['element', 'source', 'time', 'value', 'time_offset'],
            ascending=[False, True, True, False, False]
        ).head(3).reset_index(drop=True)

        expected_sources = pd.DataFrame([
            [
                'SN30000',
                'SensorSystem',
                'LARVIK',
                'Larvik',
                'Norge',
                'NO',
                None,
                59.0572,
                10.0667,
                28,
                dt.datetime.fromisoformat('1883-06-01T00:00:00.000'),
                None,
                'VESTFOLD OG TELEMARK',
                38,
                'LARVIK',
                3805,
                "['LARVIK KOMMUNE']",
                None,
                None,
                None,
                '0-578-0-30000'
            ],
            [
                'SN27780',
                'SensorSystem',
                'TJØLLING',
                'Tjølling',
                'Norge',
                'NO',
                None,
                59.0467,
                10.125,
                19,
                dt.datetime.fromisoformat('2005-01-01T00:00:00.000'),
                None,
                'VESTFOLD OG TELEMARK',
                38,
                'LARVIK',
                3805,
                "['NIBIO']",
                "['1']",
                None,
                None,
                '0-578-0-27780'
            ],
            [
                'SN30249',
                'SensorSystem',
                'LANGANGEN',
                # God damn stray space
                'Langangen ',
                'Norge',
                'NO',
                None,
                59.0875,
                9.802,
                5,
                dt.datetime.fromisoformat('2015-03-18T00:00:00.000'),
                None,
                'VESTFOLD OG TELEMARK',
                38,
                'PORSGRUNN',
                3806,
                "['PORSGRUNN KOMMUNE']",
                "['501151102']",
                None,
                None,
                '0-578-0-30249'
            ]
        ], columns=FrostFetcher.sources_headers).reset_index(drop=True)
        
        expected_observations = pd.DataFrame([
            [
                'SN27630',
                'sum(precipitation_amount P1D)',
                dt.datetime.fromisoformat('2020-05-01T00:00:00.000'),
                342,
                15.54413153464,
                20.4,
                None,
                'mm',
                None,
                None,
                None,
                None,
                'PT6H',
                'P1D',
                0,
                'C',
                '2',
                0,
                None,
                None
            ],
            [
                'SN27630',
                'sum(precipitation_amount P1D)',
                dt.datetime.fromisoformat('2020-05-01T00:00:00.000'),
                342,
                15.54413153464,
                16.9,
                None,
                'mm',
                None,
                None,
                None,
                None,
                'PT18H',
                'P1D',
                0,
                'C',
                '2',
                2,
                None,
                None
            ],
            [
                'SN27630',
                'sum(precipitation_amount P1D)',
                dt.datetime.fromisoformat('2020-05-02T00:00:00.000'),
                342,
                15.54413153464,
                0.0,
                None,
                'mm',
                None,
                None,
                None,
                None,
                'PT6H',
                'P1D',
                0,
                'C',
                '2',
                0,
                None,
                None
            ]
        ], columns=FrostFetcher.observations_headers).reset_index(drop=True)

        self.assertEqual(sources, expected_sources)
        self.assertEqual(observations, expected_observations)
