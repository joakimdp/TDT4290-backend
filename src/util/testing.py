import unittest
import pandas as pd
import pandas.testing as pd_testing


class AvalancheTestCase(unittest.TestCase):
    def assertDataFrameEqual(self, a, b, msg=None):
        try:
            pd_testing.assert_frame_equal(
                a, b,
                check_column_type=False,
                check_index_type=False,
                check_dtype=False
            )
        except AssertionError as e:
            raise self.failureException(str(e)) from e

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataFrameEqual)
