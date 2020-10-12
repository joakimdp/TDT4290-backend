import unittest
from src.apis.skredvarsel.skredvarsel import Skredvarsel
from util.avalanche_incident import AvalancheIncident
from datetime import date

class TestSkredvarsel(unittest.TestCase):

    def test_skredvarsel(self):
        avalanche_incident_list = []
        avalanche_incident_list.append(AvalancheIncident(
            id=1,
            time=date.fromisoformat("2019-02-20"),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        ))

        avalanche_incident_list.append(AvalancheIncident(
            id=2,
            time=date.fromisoformat("2019-02-25"),
            coords_utm=(109190, 6725372),
            coords_latlng=(60.474065, 7.882042)
        ))

        print(Skredvarsel().get_data(avalanche_incident_list))
        dataframe_dict = Skredvarsel().get_data(avalanche_incident_list)
        database_rows = dataframe_dict["skredvarsel_data"]
        self.assertIn("reg_id", database_rows.columns)
        self.assertIn("danger_level", database_rows.columns)
        self.assertTrue(len(database_rows.columns) > 2)

        
        
