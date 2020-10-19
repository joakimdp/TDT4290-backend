import apis.processor as processor
import pandas as pd
import util.utm_converter as utm_converter
from datetime import datetime
import re
from typing import List


class RegobsProcessor(processor.Processor):
    TIMESTAMPS_COLUMNS = [
        "dt_avalanche_time",
        "dt_end_time",
        "dt_obs_time",
        "dt_reg_time"
    ]

    @staticmethod
    def __convert_posix_to_datetime(time_string) -> datetime:
        posix_time = int(re.split(r'\(|\)', time_string)[1]) / 1000
        return datetime.fromtimestamp(posix_time)

    @staticmethod
    def __get_timestamp_from_row(row) -> List[datetime]:
        """
        Input is a dataframe-row. Output is the earliest timestamp of
        the row for alle columns in TIMESTAMPS_COLUMNS
        """
        timestamps_for_row = []

        for column in RegobsProcessor.TIMESTAMPS_COLUMNS:
            timestamp = row[column]
            if (row[column] and isinstance(row[column], str)):
                converted_timestamp = RegobsProcessor.__convert_posix_to_datetime(
                    timestamp)
                timestamps_for_row.append(converted_timestamp)

        return sorted(timestamps_for_row)[0]

    def __append_prioritized_utm_coordinates(self, df: pd.DataFrame) -> None:
        prioritized_utm_north = []
        prioritized_utm_east = []

        prioritization_order = [('utm_east_start', 'utm_north_start'),
                                ('utm_east_stop', 'utm_north_stop'), ('utm_east_reg', 'utm_north_reg')]

        for index, row in df.iterrows():
            for utm_tuple in prioritization_order:
                utm_east = row[utm_tuple[0]]
                utm_north = row[utm_tuple[1]]

                if utm_east != None and utm_north != None:
                    # TODO: remove when data is properly filtered
                    if utm_east > -1000000 or utm_north > -1000000:
                        prioritized_utm_east.append(utm_east)
                        prioritized_utm_north.append(utm_north)
                        break

            if len(prioritized_utm_east) < (index + 1):
                prioritized_utm_east.append(0)
                prioritized_utm_north.append(0)

        df['utm_east_prioritized'] = prioritized_utm_east
        df['utm_north_prioritized'] = prioritized_utm_north
        return df

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
            'ForecastRegion': 'forecast_region',
            'DtAvalancheTime': 'dt_avalanche_time',
            'SnowLine': 'snow_line',
            'UTMEastStart': 'utm_east_start',
            'UTMNorthStart': 'utm_north_start',
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
            'UTMNorth': 'utm_north_reg',
            'DtObsTime': 'dt_obs_time',
            'DtRegTime': 'dt_reg_time',
            'DeletedDate': 'deleted_date',
            'DtChangeTime': 'dt_change_time'
        }, inplace=True)

        # Append prioritized utm coordinates columns
        df = self.__append_prioritized_utm_coordinates(df)
        # Remove deleted registrations
        df = df[df['deleted_date'].isna()].copy()

        # Add lat, lng and time variables
        lat = []
        lng = []
        time = []

        for index, row in df.iterrows():
            utmEast = int(row["utm_east_prioritized"])
            utmNorth = int(row["utm_north_prioritized"])
            if utmEast < 0 or utmNorth < 0:
                coor = (None, None)
            coor = utm_converter.convert(utmEast, utmNorth)

            lat.append(float(coor[0]))
            lng.append(float(coor[1]))

            time.append(RegobsProcessor.__get_timestamp_from_row(row))

        df["lat"] = lat
        df["lng"] = lng
        df["time"] = time

        #df.set_index('reg_id', inplace=True)

        return df
