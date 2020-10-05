from typing import Dict
from sqlalchemy import Column, Integer, DateTime, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.base import Engine
import apis.initializer as initializer


Base = declarative_base()


class FrostSources(Base):
    __tablename__ = 'frost_sources'
    id = Column(String(20))
    type = Column(String(50))
    name = Column(String(255))
    short_name = Column(String(255))
    country = Column(String(100))
    country_code = Column(String(10))
    wmo_id = Column(String(50))
    latitude = Column(Float())
    longitude = Column(Float())
    masl = Column(String(10))
    valid_from = Column(DateTime())
    valid_to = Column(DateTime())
    county = Column(String(100))
    county_id = Column(String(10))
    municipality = Column(String(100))
    municipality_id = Column(String(10))
    station_holders = Column(Text())
    external_ids = Column(Text())
    icao_codes = Column(String(255))
    ship_codes = Column(String(255))
    wigos_id = Column(String(50))


class FrostObservations(Base):
    __tablename__ = 'frost_observations'
    source = Column(String(20))
    element = Column(String(50))
    time = Column(DateTime())
    reg_id = Column(Integer())
    distance = Column(Float())
    value = Column(String(20))
    orig_value = Column(String(20))
    unit = Column(String(20))
    code_table = Column(String(50))
    level_type = Column(String(50))
    level_unit = Column(String(20))
    level_series_id = Column(String(20))
    performance_category = Column(String(5))
    exposure_category = Column(String(5))
    quality_code = Column(String(5))
    control_info = Column(String(50))
    data_version = Column(String(20))


class FrostInitializer(initializer.Initializer):
    def __init__(self, engine: Engine):
        self.engine = engine

    def initialize_tables(self):
        FrostSources.metadata.create_all(self.engine)
        FrostObservations.metadata.create_all(self.engine)
