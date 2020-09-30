import unittest
from datetime import date
from util.avalanche_incident import AvalancheIncident


class TestAvalancheIncident(unittest.TestCase):
    def test_representation(self):
        avalanche_incident_list = []
        avalanche_incident_list.append(AvalancheIncident(
            id=1,
            time=date.fromisoformat("2019-02-20"),
            coords_utm=(179633, 6782269),
            coords_latlng=(61.044338, 9.062769)
        ))

        converted_to_string = str(avalanche_incident_list)

        self.assertEqual(converted_to_string, "[AvalancheIncident with id=1, time=2019-02-20, coords_utm=(179633, 6782269), coords_latlng=(61.044338, 9.062769)]")
