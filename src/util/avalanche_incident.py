# TODO: Remove when Python 3.10 is deployed
from __future__ import annotations
from typing import List, Tuple
import datetime as dt
import pandas as pd


class AvalancheIncident():
    """
    Dataclass for storing information about an avalanche incident.
    Primarily to be used together with APIs for getting additional
    information about an avalanche.
    """

    def __init__(
        self,
        id: int,
        time: dt.datetime,
        coords_utm: Tuple[int, int],
        coords_latlng: Tuple[float, float]
    ):
        self.id = id
        self.time = time
        self.coords_utm = coords_utm
        self.coords_latlng = coords_latlng

    def __repr__(self):
        return (
            f'AvalancheIncident with id={self.id}, time={self.time}, '
            f'coords_utm={self.coords_utm}, coords_latlng={self.coords_latlng}'
        )

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List[AvalancheIncident]:
        aval_objects = []

        for index, row in df.iterrows():
            aval_objects.append(cls(
                row['reg_id'],
                row['time'],
                (row['utm_east_reg'], row['utm_north_reg']),
                (row['lat'], row['lng'])))

        return aval_objects
