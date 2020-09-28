import unittest
from apis.xgeo.xgeo_processor import XgeoProcessor
import pandas as pd


class TestXgeoProcessor(unittest.TestCase):
    def test_convert_dataframe_to_correct_format(self):
        example_indices = ["2019-02-10", "2019-02-11", "2019-02-12", "2019-02-13", "2019-02-14", "2019-02-15", "2019-02-16", "2019-02-17", "2019-02-18", "2019-02-19", "2019-02-20"]

        example_xgeo_data_dict = {
            "wind_direction": [1, 0, 1, 1, 2, 1, 2, 1, 2, 3, 1],
            "wind_speed": [1.2, 1.6, 4.1, 4.3, 3.5, 2.1, 2.3, 1.8, 2.0, 2.5, 3.0],
            "temperature": [-2.65, -5.05, -5.25, -3.65, 1.05, 5.15, -0.65, 0.45, 0.05, 2.55, 0.85],
            "rainfall": [2.8, 0.7, 0.1, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "snow_depth": [2.4, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "snow_depth_3_days": [24.700001, 11.900000, 3.000000, 0.600000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000]
        }
        dataframe = pd.DataFrame(data=example_xgeo_data_dict, index=example_indices)
        database_rows = XgeoProcessor.convert_dataframe_to_correct_format(1, dataframe)

        self.assertEqual(len(database_rows), 11)
        self.assertEqual(database_rows["id"][0], 1)
        self.assertEqual(database_rows["id"][10], 1)
        self.assertEqual(database_rows["date"][0], "2019-02-10")

    def test_process(self):
        example_indices = ["2019-02-10", "2019-02-11", "2019-02-12", "2019-02-13", "2019-02-14", "2019-02-15", "2019-02-16", "2019-02-17", "2019-02-18", "2019-02-19", "2019-02-20"]

        example_xgeo_data_dict = {
            "wind_direction": [1, 0, 1, 1, 2, 1, 2, 1, 2, 3, 1],
            "wind_speed": [1.2, 1.6, 4.1, 4.3, 3.5, 2.1, 2.3, 1.8, 2.0, 2.5, 3.0],
            "temperature": [-2.65, -5.05, -5.25, -3.65, 1.05, 5.15, -0.65, 0.45, 0.05, 2.55, 0.85],
            "rainfall": [2.8, 0.7, 0.1, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "snow_depth": [2.4, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "snow_depth_3_days": [24.700001, 11.900000, 3.000000, 0.600000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000]
        }

        dataframe = pd.DataFrame(data=example_xgeo_data_dict, index=example_indices)
        dataframe_dict = {1: dataframe.copy(), 2: dataframe.copy()}
        database_rows = XgeoProcessor().process(dataframe_dict)

        self.assertIn("id", database_rows.columns)
        self.assertIn("date", database_rows.columns)
        self.assertTrue(len(database_rows.columns) > 2)
