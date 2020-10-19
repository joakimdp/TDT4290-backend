import apis.initializer as initializer
from sqlalchemy import Column, Integer, Date, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.base import Engine

Base = declarative_base()


class SkredvarselData(Base):
    __tablename__ = 'skredvarsel_data'
    reg_id = Column(Integer, primary_key=True)
    previous_warning_reg_id = Column(Integer)
    danger_level_name = Column(String)
    utm_zone = Column(Integer)
    utm_east = Column(Integer)
    utm_north = Column(Integer)
    author = Column(String)
    avalanche_danger = Column(String)
    emergency_warning = Column(String)
    snow_surface = Column(String)
    current_weak_layers = Column(String)
    latest_avalanche_activity = Column(String)
    registartion_id = Column(Integer)
    region_id = Column(Integer)
    region_name = Column(String)
    region_type_id = Column(Integer)
    region_type_name = Column(String)
    danger_level = Column(Integer)
    valid_from = Column(Date)
    valid_to = Column(Date)
    next_warning_time = Column(Date)
    publish_time = Column(Date)
    main_text = Column(String)
    lang_key = Column(Integer)
    avalanche_problem_id = Column(Integer)
    avalanche_ext_id = Column(Integer)
    avalanche_ext_name = Column(String)
    aval_cause_id = Column(Integer)
    aval_cause_name = Column(String)
    aval_probability_id = Column(Integer)
    aval_probability_name = Column(String)
    aval_trigger_simple_id = Column(Integer)
    aval_trigger_simple_name = Column(String)
    destructive_size_ext_id = Column(Integer)
    destructive_size_ext_name = Column(String)
    aval_propagation_id = Column(Integer)
    aval_propagation_name = Column(String)
    avalanche_type_id = Column(Integer)
    avalanche_type_name = Column(String)
    avalanche_problem_type_id = Column(Integer)
    avalanche_problem_type_name = Column(String)
    valid_expositions = Column(Integer)
    exposed_heigth_1 = Column(Integer)
    exposed_heigth_2 = Column(Integer)
    exposed_height_fill = Column(Integer)


class SkredvarselInitializer(initializer.Initializer):
    def __init__(self, engine: Engine):
        self.engine = engine

    def initialize_tables(self):
        SkredvarselData.metadata.create_all(self.engine)
