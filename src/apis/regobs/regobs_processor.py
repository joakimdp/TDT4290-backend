import apis.processor as processor
import pandas as pd
import math
import util.utm_converter as utm_converter

class RegobsProcessor(processor.Processor):

    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def process(self):
        '''
        Add columns with latlong coordinates
        '''
        lat = []
        lon = []

        for index, row in self.df.iterrows():
            utmEast = int(row["UTMEast"])
            utmNorth = int(row["UTMNorth"])

            coor = utm_converter.convert(utmEast, utmNorth)

            lon.append(float(coor[1]))
            lat.append(float(coor[0]))

        self.df["Lon"] = lon
        self.df["Lat"] = lat

        return self.df

 