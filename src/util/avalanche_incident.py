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
