import unittest
import asyncio
from datetime import date
import aiohttp
from apis.xgeo.xgeo_fetcher import XgeoFetcher
from util.avalanche_incident import AvalancheIncident


class TestXgeoFetcher(unittest.TestCase):
    def test_create_time_string(self):
        timestamp = date.fromisoformat('2019-12-03')
        time_string = XgeoFetcher.create_time_string(timestamp)
        self.assertEqual(time_string, '20191203T0000')

    def test_create_url(self):
        self.maxDiff = None
        timestamp = date.fromisoformat('2019-02-20')
        utm_x = 179633
        utm_y = 6782269
        data_code = 'rr'
        days_earlier = 10
        url = XgeoFetcher.create_url(
            timestamp,
            utm_x,
            utm_y,
            data_code,
            days_earlier
        )
        expected_url = (
            'http://h-web01.nve.no/chartserver/ShowData.aspx?req=getchart'
            '&ver=1.0&vfmt=json&time=20190210T0000;20190220T0000'
            '&chs=10x10&lang=no&chlf=none&chsl=0;+0&chhl=2|0|2&timeo=-06:00'
            '&app=3d&chd=ds=hgts,da=29,id=179633;6782269;rr,cht=line,mth=inst'
            '&nocache=0.201871693486398'
        )
        self.assertEqual(url, expected_url)

    def test_create_url_date_different_month(self):
        self.maxDiff = None
        timestamp = date.fromisoformat('2019-02-01')
        utm_x = 179633
        utm_y = 6782269
        data_code = 'rr'
        days_earlier = 10
        url = XgeoFetcher.create_url(
            timestamp,
            utm_x,
            utm_y,
            data_code,
            days_earlier
        )

        # 10 days earlier from 2020-02-01 should be 2020-01-22
        expected_url = (
            'http://h-web01.nve.no/chartserver/ShowData.aspx?req=getchart'
            '&ver=1.0&vfmt=json&time=20190122T0000;20190201T0000'
            '&chs=10x10&lang=no&chlf=none&chsl=0;+0&chhl=2|0|2&timeo=-06:00'
            '&app=3d&chd=ds=hgts,da=29,id=179633;6782269;rr,cht=line,mth=inst'
            '&nocache=0.201871693486398'
        )
        self.assertEqual(url, expected_url)

    def test_fetch_url(self):
        timestamp = date.fromisoformat('2019-02-20')
        utm_x = 179633
        utm_y = 6782269
        data_code = 'rr'
        days_earlier = 10
        url = XgeoFetcher.create_url(
            timestamp,
            utm_x,
            utm_y,
            data_code,
            days_earlier
        )

        async def fetch_url_with_session():
            async with aiohttp.ClientSession() as s:
                return await XgeoFetcher().fetch_url(s, url)

        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(fetch_url_with_session())

        self.assertEquals(
            response[0]['LegendText'],
            '179633;6782269 (519 moh.), Døgnnedbør v2.0'
        )
        self.assertTrue(len(response[0]['SeriesPoints']) != 0)

    def test_fetch_for_code(self):
        incident = AvalancheIncident(
            id=1,
            time=date.fromisoformat('2019-02-20'),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        )
        code = ('rr', 'rainfall')

        async def fetch_with_session():
            async with aiohttp.ClientSession() as s:
                return await XgeoFetcher().fetch_for_code(
                    s,
                    incident,
                    code
                )

        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(fetch_with_session())
        response = list(response.values())[0]
        expected = [2.799999952316284, 0.699999988079071,
                    0.10000000149011612, 0.10000000149011612,
                    0, 0, 0, 0, 0, 0, 0]

        self.assertEquals(response, expected)

    def test_convert_json_response_to_value_list(self):
        example_json_response = [{
            'LegendText': '179633;6782269 (519 moh.), Døgntemperatur v2.0',
            'SeriesPoints': [
                {
                    'Key': '/Date(1549753200000)/',
                    'Value': -2.6500000953674316,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1549839600000)/',
                    'Value': -5.050000190734863,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1549926000000)/',
                    'Value': -5.25,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1550012400000)/',
                    'Value': -3.6500000953674316,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1550098800000)/',
                    'Value': 1.0499999523162842,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1550185200000)/',
                    'Value': 5.150000095367432,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1550271600000)/',
                    'Value': -0.6499999761581421,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1550358000000)/',
                    'Value': 0.44999998807907104,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1550444400000)/',
                    'Value': 0.05000000074505806,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1550530800000)/',
                    'Value': 2.549999952316284,
                    'CorrectionMark': 0
                },
                {
                    'Key': '/Date(1550617200000)/',
                    'Value': 0.8500000238418579,
                    'CorrectionMark': 0
                }
            ],
            'Statistics': []
        }]
        converted_response = XgeoFetcher().convert_json_response_to_value_list(
            example_json_response
        )
        expected_converted_response = [
            -2.6500000953674316,
            -5.050000190734863,
            -5.25,
            -3.6500000953674316,
            1.0499999523162842,
            5.150000095367432,
            -0.6499999761581421,
            0.44999998807907104,
            0.05000000074505806,
            2.549999952316284,
            0.8500000238418579
        ]
        self.assertEqual(converted_response, expected_converted_response)

    def test_convert_json_response_to_value_list_empty_response(self):
        example_json_response = [{
            'LegendText': '179633;6782269 (519 moh.), Døgntemperatur v2.0',
            'SeriesPoints': [],
            'Statistics': []
        }]
        converted_response = XgeoFetcher().convert_json_response_to_value_list(
            example_json_response
        )
        expected_converted_response = [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None
        ]
        self.assertEqual(converted_response, expected_converted_response)

    def test_generate_date_indices(self):
        start_date = date.fromisoformat('2019-02-20')

        indices = XgeoFetcher.generate_date_indices(start_date)
        expected_indices = [
            '2019-02-10', '2019-02-11', '2019-02-12', '2019-02-13',
            '2019-02-14', '2019-02-15', '2019-02-16', '2019-02-17',
            '2019-02-18', '2019-02-19', '2019-02-20'
        ]
        self.assertEquals(indices, expected_indices)

    def test_fetch_for_incident(self):
        incident = AvalancheIncident(
            id=1,
            time=date.fromisoformat('2019-02-20'),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        )
        loop = asyncio.get_event_loop()
        dataframe_dict = loop.run_until_complete(
            XgeoFetcher().fetch_for_incident(incident)
        )
        # There should be only one value in the returned response
        dataframe = list(dataframe_dict.values())[0]

        # Get the column names of the dataframe from XgeoFetcher
        column_names = list(map(lambda data_code: data_code[1],
                                XgeoFetcher.DATA_CODES))

        first_column_name = column_names[0]

        self.assertEqual(dataframe.index[0], '2019-02-10')
        self.assertIn(first_column_name, dataframe.columns)
        self.assertTrue(len(dataframe[first_column_name]) != 0)

    def test_fetch(self):
        incidents = []
        incidents.append(AvalancheIncident(
            id=1,
            time=date.fromisoformat('2019-02-20'),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        ))

        incidents.append(AvalancheIncident(
            id=2,
            time=date.fromisoformat('2019-02-20'),
            coords_utm=(109190, 6725372),
            coords_latlng=(60.474065, 7.882042)
        ))

        dataframe_dict = XgeoFetcher().fetch(incidents)

        self.assertEqual(dataframe_dict[1].index[0], '2019-02-10')
        self.assertEqual(dataframe_dict[1].index[1], '2019-02-11')

        self.assertEqual(dataframe_dict[2].index[0], '2019-02-10')
        self.assertEqual(dataframe_dict[2].index[1], '2019-02-11')

        # Get the column names of the dataframe from XgeoFetcher
        column_names = list(map(lambda data_code: data_code[1],
                                XgeoFetcher.DATA_CODES))

        first_column_name = column_names[0]
        second_column_name = column_names[1]
        self.assertIn(first_column_name, dataframe_dict[1].columns)
        self.assertIn(first_column_name, dataframe_dict[2].columns)
        self.assertIn(second_column_name, dataframe_dict[1].columns)
        self.assertIn(second_column_name, dataframe_dict[2].columns)
