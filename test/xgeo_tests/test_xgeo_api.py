import unittest
from datetime import date
from apis.xgeo.xgeo import Xgeo
from util.avalanche_incident import AvalancheIncident


class TestXgeoApi(unittest.TestCase):
    def test_get_data(self):
        avalanche_incident_list = []
        avalanche_incident_list.append(AvalancheIncident(
            id=1,
            time=date.fromisoformat("2019-02-20"),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        ))

        avalanche_incident_list.append(AvalancheIncident(
            id=2,
            time=date.fromisoformat("2019-02-20"),
            coords_utm=(109190, 6725372),
            coords_latlng=(60.474065, 7.882042)
        ))

        dataframe_dict = Xgeo().get_data(avalanche_incident_list)
        database_rows = dataframe_dict["xgeo_data"]

        self.assertIn("id", database_rows.columns)
        self.assertIn("date", database_rows.columns)
        self.assertTrue(len(database_rows.columns) > 2)
