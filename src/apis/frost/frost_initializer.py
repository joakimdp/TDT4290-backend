from typing import Dict
from sqlalchemy.schema import ForeignKey, Column
from sqlalchemy.types import Integer, DateTime, Float, String, Text
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.base import Engine
import apis.initializer as initializer


Base = declarative_base()


class FrostSource(Base):
    __tablename__ = 'frost_sources'
    id = Column(String(20), primary_key=True, nullable=False)
    type = Column(String(50))
    name = Column(String(255))
    short_name = Column(String(255))
    country = Column(String(100))
    country_code = Column(String(10))
    wmo_id = Column(String(50))
    latitude = Column(Float())
    longitude = Column(Float())
    masl = Column(String(10))
    # Commented out due to the ODBC driver not respecting the DATETIME2 type
    # valid_from = Column(DATETIME2())
    # valid_to = Column(DateTime())
    county = Column(String(100))
    county_id = Column(String(10))
    municipality = Column(String(100))
    municipality_id = Column(String(10))
    station_holders = Column(Text())
    external_ids = Column(Text())
    icao_codes = Column(String(255))
    ship_codes = Column(String(255))
    wigos_id = Column(String(50))

    # TODO: Add relationship
    # observations = relationship(
    #     'FrostObservation',
    #     order_by="FrostObservation.time",
    #     back_populates='source'
    # )


class FrostObservation(Base):
    __tablename__ = 'frost_observations'
    source_id = Column(
        String(20),
        # TODO: Add FK constraint
        # ForeignKey('frost_sources.id'),
        primary_key=True,
        nullable=False
    )
    element = Column(String(50), primary_key=True, nullable=False)
    time = Column(DateTime(), primary_key=True, nullable=False)
    reg_id = Column(
        Integer(),
        # TODO: Add FK constraint
        # ForeignKey('regobs_data.reg_id'),
        primary_key=True,
        nullable=False
    )
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

    # TODO: Add relationships
    # source = relationship('FrostSource', back_populates='observations')
    # reg = relationship('RegobsData', back_populates='frost_observations')


class FrostInitializer(initializer.Initializer):
    def __init__(self, engine: Engine):
        self.engine = engine

    def initialize_tables(self):
        FrostSource.metadata.create_all(self.engine)
        FrostObservation.metadata.create_all(self.engine)
