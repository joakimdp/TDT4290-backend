from typing import List, Dict
import pandas as pd
import apis.api as api
from apis.frost.frost_initializer import Base, FrostSource, FrostObservation
from apis.frost.frost_fetcher import FrostFetcher
from apis.frost.frost_processor import FrostProcessor
from util.avalanche_incident import AvalancheIncident


class Frost(api.Api):
    def get_data(self, incidents: List[AvalancheIncident]) -> Dict[str, Base]:
        temp = FrostProcessor().process(FrostFetcher().fetch(incidents))
        return {
            'frost_sources': temp['frost_sources'],
            'frost_observations': temp['frost_observations']
        }
