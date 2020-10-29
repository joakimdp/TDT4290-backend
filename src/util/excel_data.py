import pandas as pd
import numpy as np
import csv
from itertools import islice

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.types import Date, Float, Integer, String

Base = declarative_base()


class ExcelData(Base):
    __tablename__ = 'excel_data'
    index = Column(Integer, primary_key=True)
    dato = Column(Date)
    døde = Column(Integer)
    skredtatte = Column(Integer)
    aktivitet = Column(String)
    utløst = Column(String)
    sted = Column(String)
    kommune = Column(String)
    fylke = Column(String)
    regobs_regid = Column(Integer)
    varsom_nyhet_eller_rapport = Column(String)
    beskrivelse = Column(String)
    involvert = Column(String)
    skadet_eller_omkommet = Column(String)
    totalt_begravd = Column(String)
    delvis_begravd = Column(String)
    tatt_men_ikke_begravd = Column(String)
    nære_på_men_ikke_tatt = Column(String)
    najonalitet = Column(String)
    alder = Column(String)
    skredutstyr = Column(String)
    middels_til_mye_erfaring = Column(String)
    type_skred = Column(String)
    skredstørrelse = Column(Integer)
    guide = Column(String)


def process_excel_data():
    filename = './excel_data/excel_data.csv'

    # Get column names
    with open(filename, 'rt', encoding='utf-8-sig') as f:
        reader = csv.reader(islice(f, 5, 16), delimiter=';')
        i = next(reader)
        columns = [x.replace('\n', '\r\n') for x in i if x]

    df = pd.read_csv(filename, sep=';', skiprows=3, usecols=columns)

    # Remove invalid records
    df = df[df['Dato'].notna()]

    # Lazy formatting of column names
    df.columns = [x.replace('\r\n', '').replace(' ', '_').replace('/', '_eller_').replace(',', '').replace('(-årene)', '').replace('-', '').replace('.no', '').lower() for x in columns]
    df.rename(columns={'delvisbegravd': 'delvis_begravd'}, inplace=True)

    df.index.names = ['index']

    return df
