import unittest
from apis.regobs.regobs_processor import RegobsProcessor
import pandas as pd
import pandas.testing as pd_testing

class TestRegobsProcessor(unittest.TestCase):

    def assertDataFrameEqual(self, a, b, msg):
        try:
            pd_testing.assert_frame_equal(a,b)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def setUp(self):
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataFrameEqual)
    
    def test_process(self):
        example_df = pd.DataFrame([["2134", 340000, 5710000], ["1234", 340000, 5710000]], columns=["RegID", "UTMEast", "UTMNorth"])
        processor = RegobsProcessor(example_df)
        test = processor.process()

        fasit = pd.DataFrame([["2134", 340000, 5710000, 12.693877485734069, 51.51842960597545], ["1234", 340000, 5710000, 12.693877485734069, 51.51842960597545]], columns=["RegID", "UTMEast", "UTMNorth", "Lon", "Lat"])
        self.assertEqual(test, fasit)
    