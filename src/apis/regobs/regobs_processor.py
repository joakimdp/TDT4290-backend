import apis.processor as processor
import pandas as pd
import math
import util.utm_converter as utm_converter


class RegobsProcessor(processor.Processor):

    def process(self, df: pd.DataFrame) -> pd.DataFrame:

        df.rename(columns={
            'RegID': 'reg_id',
            'Aspect': 'aspect',
            'HeigthStartZone': 'height_start_zone',
            'HeigthStopZone': 'height_stop_zone',
            'DestructiveSizeTID': 'destructive_size_tid',
            'AvalancheTriggerTID': 'avalanche_trigger_tid',
            'AvalancheTID': 'avalanche_tid',
            'TerrainStartZoneTID': 'terrain_start_zone_tid',
            'UTMZoneStop': 'utm_zone_stop',
            'UTMEastStop': 'utm_east_stop',
            'UTMNorthStop': 'utm_north_stop',
            'DtAvalancheTime': 'dt_avalanche_time',
            'SnowLine': 'snow_line',
            'UTMEastStart': 'utm_east_start',
            'UTMNorthStart': 'utm_north_Start',
            'ValidExposition': 'valid_exposition',
            'AvalCauseTID': 'aval_cause_tid',
            'FractureHeigth': 'fracture_height',
            'FractureWidth': 'fracture_width',
            'Trajectory': 'trajectory',
            'GeoHazardTID': 'geo_hazard_tid',
            'ActivityInfluencedTID': 'activity_influenced_tid',
            'DamageExtentTID': 'damage_extent_tid',
            'ForecastAccurateTID': 'forecast_accurate_tid',
            'DtEndTime': 'dt_end_time',
            'IncidentHeader': 'incident_header',
            'IncidentIngress': 'incident_ingress',
            'IncidentText': 'incident_text',
            'SensitiveText': 'sensitive_text',
            'IncidentURLs.__deferred.uri': 'incident_url',
            'RegistrationUrl': 'registration_url',
            'UsageFlagTID': 'usage_flag_tid',
            'Comment': 'comment',
            '__metadata.id': 'metadata_id',
            '__metadata.uri': 'metadata_uri',
            '__metadata.type': 'metadata_type',
            'UTMEast': 'utm_east_reg',
            'UTMNorth': 'utm_north_reg'
        }, inplace=True)

        '''
        Add columns with latlong coordinates
        '''
        lat = []
        lng = []

        for index, row in df.iterrows():
            utmEast = int(row["utm_east_reg"])
            utmNorth = int(row["utm_north_reg"])

            coor = utm_converter.convert(utmEast, utmNorth)

            lng.append(float(coor[1]))
            lat.append(float(coor[0]))

        df["lng"] = lng
        df["lat"] = lat

        return df
