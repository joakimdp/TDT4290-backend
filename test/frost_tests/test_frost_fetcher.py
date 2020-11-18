import datetime as dt
import asyncio
import logging

import numpy as np
import pandas as pd
import pandas.testing as pd_testing
import aiohttp
from apis.frost.frost_fetcher import FrostFetcher
from util.avalanche_incident import AvalancheIncident
from util.testing import AvalancheTestCase


class TestFrostFetcher(AvalancheTestCase):
    def setUp(self):
        self.fetcher = FrostFetcher()

    def test_fetch_sources_for_incident_and_element(self):
        async def helper():
            async with aiohttp.ClientSession(
                auth=FrostFetcher._FrostFetcher__frost_auth
            ) as s:
                return await self.fetcher.fetch_sources(
                    s,
                    'sum(precipitation_amount P1D)',
                    59,
                    10,
                    '2020-05-01',
                    '2020-05-04'
                )

        loop = asyncio.get_event_loop()
        actual = loop.run_until_complete(helper())

        expected = [
            {
                "@type": "SensorSystem",
                "id": "SN30000",
                "name": "LARVIK",
                "shortName": "Larvik",
                "country": "Norge",
                "countryCode": "NO",
                "geometry": {
                    "@type": "Point",
                    "coordinates": [
                        10.0667,
                        59.0572
                    ],
                    "nearest": False
                },
                "distance": 7.4176453769,
                "masl": 28,
                "validFrom": "1883-06-01T00:00:00.000Z",
                "county": "VESTFOLD OG TELEMARK",
                "countyId": 38,
                "municipality": "LARVIK",
                "municipalityId": 3805,
                "stationHolders": [
                    "LARVIK KOMMUNE"
                ],
                "wigosId": "0-578-0-30000"
            },
            {
                "@type": "SensorSystem",
                "id": "SN27780",
                "name": "TJØLLING",
                "shortName": "Tjølling",
                "country": "Norge",
                "countryCode": "NO",
                "geometry": {
                    "@type": "Point",
                    "coordinates": [
                        10.125,
                        59.0467
                    ],
                    "nearest": False
                },
                "distance": 8.83984723662,
                "masl": 19,
                "validFrom": "2005-01-01T00:00:00.000Z",
                "county": "VESTFOLD OG TELEMARK",
                "countyId": 38,
                "municipality": "LARVIK",
                "municipalityId": 3805,
                "stationHolders": [
                    "NIBIO"
                ],
                "externalIds": [
                    "1"
                ],
                "wigosId": "0-578-0-27780"
            },
            {
                "@type": "SensorSystem",
                "id": "SN30249",
                "name": "LANGANGEN",
                # Gotta love them stray spaces
                "shortName": "Langangen ",
                "country": "Norge",
                "countryCode": "NO",
                "geometry": {
                    "@type": "Point",
                    "coordinates": [
                        9.802,
                        59.0875
                    ],
                    "nearest": False
                },
                "distance": 14.93048981377,
                "masl": 5,
                "validFrom": "2015-03-18T00:00:00.000Z",
                "county": "VESTFOLD OG TELEMARK",
                "countyId": 38,
                "municipality": "PORSGRUNN",
                "municipalityId": 3806,
                "stationHolders": [
                    "PORSGRUNN KOMMUNE"
                ],
                "externalIds": [
                    "501151102"
                ],
                "wigosId": "0-578-0-30249"
            },
            {
                "@type": "SensorSystem",
                "id": "SN27630",
                "name": "SANDEFJORD - ENGA",
                # And another one
                "shortName": "Enga ",
                "country": "Norge",
                "countryCode": "NO",
                "geometry": {
                    "@type": "Point",
                    "coordinates": [
                        10.2147,
                        59.0857
                    ],
                    "nearest": False
                },
                "distance": 15.54413153464,
                "masl": 4,
                "validFrom": "2013-06-10T00:00:00.000Z",
                "county": "VESTFOLD OG TELEMARK",
                "countyId": 38,
                "municipality": "SANDEFJORD",
                "municipalityId": 3804,
                "stationHolders": [
                    "SANDEFJORD KOMMUNE"
                ],
                "externalIds": [
                    "510121055"
                ],
                "wigosId": "0-578-0-27630"
            },
            {
                "@type": "SensorSystem",
                "id": "SN30248",
                "name": "BREVIK",
                "shortName": "Brevik",
                "country": "Norge",
                "countryCode": "NO",
                "geometry": {
                    "@type": "Point",
                    "coordinates": [
                        9.7014,
                        59.0537
                    ],
                    "nearest": False
                },
                "distance": 18.10064270813,
                "masl": 3,
                "validFrom": "2015-03-31T00:00:00.000Z",
                "county": "VESTFOLD OG TELEMARK",
                "countyId": 38,
                "municipality": "PORSGRUNN",
                "municipalityId": 3806,
                "stationHolders": [
                    "PORSGRUNN KOMMUNE"
                ],
                "externalIds": [
                    "501151103"
                ],
                "wigosId": "0-578-0-30248"
            }
        ]

        self.assertEqual(actual, expected)

    def test_fetch_observations_for_incident(self):
        incidents = (AvalancheIncident(
            342,
            dt.datetime(2020, 5, 3),
            (0, 0),
            (59, 10)
        ))

        async def helper():
            async with aiohttp.ClientSession(
                auth=FrostFetcher._FrostFetcher__frost_auth
            ) as s:
                return await self.fetcher.fetch_observations(
                    s,
                    incidents,
                    'SN30000',
                    '2020-05-01',
                    '2020-05-04',
                    'sum(precipitation_amount P1D)',
                    7.4176453769
                )

        loop = asyncio.get_event_loop()
        actual = loop.run_until_complete(helper())

        expected = pd.DataFrame([
            [
                'SN30000',
                'sum(precipitation_amount P1D)',
                dt.datetime(2020, 5, 1),
                342,
                7.4176453769,
                19,
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
                'SN30000',
                'sum(precipitation_amount P1D)',
                dt.datetime(2020, 5, 2),
                342,
                7.4176453769,
                0,
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
                'SN30000',
                'sum(precipitation_amount P1D)',
                dt.datetime(2020, 5, 3),
                342,
                7.4176453769,
                0,
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
        ], columns=FrostFetcher.observations_headers)

        self.assertEqual(actual, expected)

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
                # See comment in frost_initializer.py
                # dt.datetime.fromisoformat('1883-06-01T00:00:00.000'),
                # None,
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
                # See comment in frost_initializer.py
                # dt.datetime.fromisoformat('2005-01-01T00:00:00.000'),
                # None,
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
                # See comment in frost_initializer.py
                # dt.datetime.fromisoformat('2015-03-18T00:00:00.000'),
                # None,
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
