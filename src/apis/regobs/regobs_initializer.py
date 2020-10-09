import apis.initializer as initializer
from sqlalchemy import create_engine
import pyodbc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.types import Date, Float, Integer, String
from sqlalchemy.engine.base import Engine

Base = declarative_base()


class RegobsData(Base):
    __tablename__ = 'regobs_data'
    reg_id = Column(Integer, primary_key=True)
    aspect = Column(Integer)
    height_start_zone = Column(Integer)
    height_stop_zone = Column(Integer)
    destructive_size_tid = Column(Integer)
    avalanche_trigger_tid = Column(Integer)
    avalanche_tid = Column(Integer)
    terrain_start_zone_tid = Column(Integer)
    utm_zone_stop = Column(Integer)
    utm_east_stop = Column(Integer)
    utm_north_stop = Column(Integer)
    DtAvalancheTime = Column(Date)
    snow_line = Column(Integer)
    utm_zone_start = Column(Integer)
    utm_east_start = Column(Integer)
    utm_north_start = Column(Integer)
    valid_exposition = Column(Integer)
    aval_cause_tid = Column(Integer)
    fracture_heigth = Column(Integer)
    fracture_width = Column(Integer)
    trajectory = Column(Integer)
    geo_hazard_tid = Column(Integer)
    activity_influenced_tid = Column(Integer)
    damage_extent_tid = Column(Integer)
    forecast_accurate_tid = Column(Integer)
    dt_end_time = Column(Date)
    incident_header = Column(String)
    incident_ingress = Column(String)
    incident_text = Column(String)
    sensitive_text = Column(String)
    incident_url = Column(String)
    registration_url = Column(String)
    usage_flag_tid = Column(Integer)
    comment = Column(String)
    metadata_id = Column(Integer)
    metadata_uri = Column(String)
    metadata_type = Column(String)
    utm_east_reg = Column(Integer)
    utm_north_reg = Column(Integer)
    lat = Column(Float)
    lng = Column(Float)
    dt_obs_time = Column(Date)
    dt_reg_time = Column(Date)
    deleted_date = Column(Date)
    dt_change_time = Column(Date)
    utm_east_prioritized = Column(Integer)
    utm_north_prioritized = Column(Integer)
    forecast_region = Column(Integer)


class RegobsInitializer(initializer.Initializer):

    def __init__(self, engine: Engine):
        self.engine = engine

    def initialize_tables(self):
        RegobsData.metadata.create_all(self.engine)
