from typing import List, Dict
import apis.api as api
from apis.frost.frost_initializer import Base, FrostSources, FrostObservations
from apis.frost.frost_fetcher import FrostFetcher
from apis.frost.frost_processor import FrostProcessor


class Frost(api.API):
    def get_data(self, incidents: List[AvalanceIncident]) -> Dict[str, Base]:
        return FrostProcessor.process(FrostFetcher().fetch(incidents))
