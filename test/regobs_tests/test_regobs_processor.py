import unittest
from apis.regobs.regobs_processor import RegobsProcessor
import pandas as pd
import numpy as np
import pandas.testing as pd_testing


class TestRegobsProcessor(unittest.TestCase):
    def assertDataFrameEqual(self, a, b, msg):
        try:
            pd_testing.assert_frame_equal(a, b)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def setUp(self):
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataFrameEqual)

    def test_process(self):
        example_df = pd.DataFrame(
            [["2134", 340000, 5710000, None, "/Date(1359817740000)/", "/Date(1355817740000)/", None, "/Date(1353817740000)/"],
             ["1234", 340000, 5710000, None, None, np.nan, None, "/Date(1359817740000)/"]],
            columns=[
                "reg_id",
                "utm_east_reg",
                "utm_north_reg",
                "deleted_date",
                "dt_avalanche_time",
                "dt_end_time",
                "dt_obs_time",
                "dt_reg_time"]
        )

        processor = RegobsProcessor()
        processed_df = processor.process(example_df)

        self.assertEqual(processed_df.loc[0]["lat"], 51.518406544328315)
        self.assertEqual(processed_df.loc[0]["lng"], 12.693877485718236)

        self.assertEqual(processed_df.loc[0]["time"].year, 2012)
        self.assertEqual(processed_df.loc[1]["time"].year, 2013)

    def test_deleted_row(self):
        example_df = pd.DataFrame(
            [["2134", 340000, 5710000, "/Date(1359817740000)/", "/Date(1359817740000)/", "/Date(1355817740000)/", None, "/Date(1353817740000)/"],
             ["1234", 340000, 5710000, None, None, np.nan, None, "/Date(1359817740000)/"]],
            columns=[
                "reg_id",
                "utm_east_reg",
                "utm_north_reg",
                "deleted_date",
                "dt_avalanche_time",
                "dt_end_time",
                "dt_obs_time",
                "dt_reg_time"]
        )

        processor = RegobsProcessor()
        processed_df = processor.process(example_df)

        self.assertEqual(processed_df.size, 11)
        self.assertEqual(processed_df.iloc[0]["reg_id"], "1234")

    def test_convert_posix_to_datetime(self):
        example_time_string = "/Date(1359817740000)/"
        datetime_object = RegobsProcessor._RegobsProcessor__convert_posix_to_datetime(
            example_time_string)
        self.assertEqual(datetime_object.year, 2013)
        self.assertEqual(datetime_object.month, 2)

    def test_get_timestamp_from_row(self):
        row = {
            "dt_avalanche_time": "/Date(1359817740000)/",
            "dt_end_time": "/Date(1355817740000)/",
            "dt_obs_time": "/Date(1353817740000)/",
            "dt_reg_time": "/Date(135217740000)/"
        }

        datetime_object = RegobsProcessor._RegobsProcessor__get_timestamp_from_row(
            row)

        self.assertEqual(datetime_object.year, 1974)
        self.assertEqual(datetime_object.month, 4)
