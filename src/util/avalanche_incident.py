import pandas as pd


class AvalancheIncident():
    """
    Dataclass for storing information about an avalanche incident.
    Primarily to be used together with APIs for getting additional
    information about an avalanche.
    """
    def __init__(self, id, time, coords_utm, coords_latlng):
        self.id = id
        self.time = time
        self.coords_utm = coords_utm
        self.coords_latlng = coords_latlng


def create_avalanche_incident_list(df: pd.DataFrame):
    aval_objects = []
    
    for index, row in df.iterrows():
        aval_objects.append(AvalancheIncident(row["reg_id"], row["dt_reg_time"], (row["utm_east_reg"],row["utm_north_reg"]), (row["lat"], row["lng"])))
    
    return aval_objects