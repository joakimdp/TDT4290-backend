import apis.initializer as initializer
from sqlalchemy import Column, Integer, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.base import Engine

Base = declarative_base()


class XgeoData(Base):
    __tablename__ = 'xgeo_data'
    id = Column(Integer, primary_key=True)
    date = Column(Date, primary_key=True)
    snow_depth_3_days = Column(Float)
    snow_depth = Column(Float)
    rainfall = Column(Float)
    temperature = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(Integer)


class XgeoInitializer(initializer.Initializer):
    def __init__(self, engine: Engine):
        self.engine = engine

    def initialize_tables(self):
        XgeoData.metadata.create_all(self.engine)
