import datetime as dt
from typing import List, Dict, Any
import requests
import pandas as pd
from decouple import config
import apis.fetcher as fetcher
from util.avalanche_incident import AvalancheIncident


class FrostFetcher(fetcher.Fetcher):
    __api_version = 'v0'
    __response_format = 'jsonld'
    __api_base_uri = 'https://frost.met.no'
    __sources_uri = (
        f'{__api_base_uri}/sources/{__api_version}.{__response_format}'
        '?elements={0}&geometry=nearest(POINT({1} {2}))'
        '&nearestmaxcount={3}&validtime={4}/{5}'
    )
    __observations_base_uri = f'{__api_base_uri}/observations'
    __observations_uri = (
        f'{__observations_base_uri}/{__api_version}.{__response_format}'
        '?sources={0}&referencetime={1}/{2}&elements={3}'
    )
    # Currently unnecessary, but may be useful at some point
    # __time_series_uri = (
    #     f'{__observations_base_uri}/availableTimeSeries/'
    #     f'{__api_version}.{__response_format}'
    #     '?sources={0}&referencetime={1}/{2}&elements={3}'

    __time_format = '%Y-%m-%d'

    sources_headers = (
        'id',
        'type',
        'name',
        'short_name',
        'country',
        'country_code',
        'wmo_id',
        'latitude',
        'longitude',
        'masl',
        'valid_from',
        'valid_to',
        'county',
        'county_id',
        'municipality',
        'municipality_id',
        'station_holders',
        'external_ids',
        'icao_codes',
        'ship_codes',
        'wigos_id'
    )
    observations_headers = (
        'source',
        'element',
        'time',
        'reg_id',
        'distance',
        'value',
        'orig_value',
        'unit',
        'code_table',
        'level_type',
        'level_unit',
        'level_value',
        'time_offset',
        'time_resolution',
        'time_series_id',
        'performance_category',
        'exposure_category',
        'quality_code',
        'control_info',
        'data_version'
    )

    elements = [
        'sum(precipitation_amount P1D)',
        'over_time(precipitation_type P1D)',
        'mean(wind_speed P1D)',
        # TODO: calculate mean for day
        # 'mean(wind_from_direction PT1H)',
        'best_estimate_mean(air_temperature P1D)',
        'mean(cloud_area_fraction P1D)'
    ]
    nearestmaxcount = 5
    days_before = 2

    def fetch(self, incidents: List[AvalancheIncident]) -> (
            Dict[str, pd.DataFrame]
    ):
        sources_df = pd.DataFrame(columns=type(self).sources_headers)
        observations_df = pd.DataFrame(columns=type(self).observations_headers)

        s = requests.Session()
        s.auth = (config('FROST_CID'), '')

        # For each incident ...
        for incident in incidents:
            interval_start = (
                incident.time - dt.timedelta(days=type(self).days_before)
            ).strftime(type(self).__time_format)
            interval_end = (incident.time + dt.timedelta(days=1)).strftime(
                type(self).__time_format
            )
            # print('Interval start:', interval_start)
            # print('Interval end:', interval_end)

            # and for each element ...
            for element in type(self).elements:
                # get the n closest sources providing the element ...
                sources_response = self.__fetch_sources(
                    s,
                    element,
                    incident.coords_latlng[0],
                    incident.coords_latlng[1],
                    interval_start,
                    interval_end
                )

                # and store them in a table.
                source_distances = {}
                for source in sources_response:
                    source_distances[source['id']] = source['distance']
                    source_row = self.__create_source_row(source)
                    sources_df = sources_df.append(
                        source_row,
                        ignore_index=True
                    )
                # print(source_distances)

                # Then fetch available observations for each source...
                source_ids = sorted(source_distances.keys())
                for source in source_ids:
                    # print('Getting,', element, 'obs for source:', source)
                    obs_response = self.__fetch_observations(
                        s,
                        source,
                        interval_start,
                        interval_end,
                        element
                    )

                    if obs_response is None:
                        continue

                    # print('Source', source, 'has data for element', element)

                    # and store them in another table.
                    obs_df = self.__create_compound_obs_df(
                        obs_response,
                        source,
                        incident.id,
                        source_distances[source]
                    )
                    observations_df = observations_df.append(
                        obs_df,
                        ignore_index=True
                    )

        sources_df.drop_duplicates(inplace=True)
        observations_df.drop_duplicates(inplace=True)

        return {
            'frost_sources': sources_df,
            'frost_observations': observations_df
        }

    def __fetch_sources(
        self,
        s: requests.Session,
        el: str,
        lat: float,
        long: float,
        start: str,
        end: str
    ) -> Dict[str, Any]:
        sources_response = s.get(type(self).__sources_uri.format(
            el,
            long,
            lat,
            type(self).nearestmaxcount,
            start,
            end
        )).json()['data']

        return sources_response

    def __create_source_row(self, src: Dict[str, Any]) -> pd.DataFrame:
        source_row = pd.DataFrame([[
            src.get('id'),
            src.get('@type'),
            src.get('name'),
            src.get('shortName'),
            src.get('country'),
            src.get('countryCode'),
            src.get('wmoId'),
            src['geometry']['coordinates'][1] if (
                src.get('geometry') is not None) else None,
            src['geometry']['coordinates'][0] if (
                src.get('geometry') is not None) else None,
            src.get('masl'),
            dt.datetime.fromisoformat(
                src.get('validFrom')[:-1]
            ),
            dt.datetime.fromisoformat(
                src.get('validTo')[:-1]
            ) if src.get('validTo') is not None else None,
            src.get('county'),
            src.get('countyId'),
            src.get('municipality'),
            src.get('municipalityId'),
            (str(src.get('stationHolders'))
                if src.get('stationHolders') is not None
                else None),
            (str(src.get('externalIds'))
                if src.get('externalIds') is not None
                else None),
            (str(src.get('icaoCodes'))
                if src.get('icaoCodes') is not None
                else None),
            (str(src.get('shipCodes'))
                if src.get('shipCodes') is not None
                else None),
            src.get('wigosId')
        ]], columns=type(self).sources_headers)

        return source_row

    def __fetch_observations(
        self,
        s: requests.Session,
        src: str,
        start: str,
        end: str,
        el: str
    ) -> Dict[str, Any]:
        obs_response = s.get(type(self).__observations_uri.format(
            src,
            start,
            end,
            el
        )).json().get('data')

        return obs_response

    def __create_observation_row(
        self,
        src: str,
        obs: Dict[str, Any],
        obs_at_time: Dict[str, Any],
        id_: int,
        distance: float
    ) -> pd.DataFrame:
        obs_row = pd.DataFrame([[
            src,
            obs.get('elementId'),
            dt.datetime.fromisoformat(
                obs_at_time.get('referenceTime')[:-1]
            ),
            id_,
            distance,
            obs.get('value'),
            obs.get('origValue'),
            obs.get('unit'),
            obs.get('codeTable'),
            obs['level']['levelType'] if (
                obs.get('level') is not None) else (
                    None),
            obs['level']['unit'] if (
                obs.get('level') is not None) else (
                    None),
            obs['level']['value'] if (
                obs.get('level') is not None) else (
                    None),
            obs.get('timeOffset'),
            obs.get('timeResolution'),
            obs.get('timeSeriesId'),
            obs.get('performanceCategory'),
            obs.get('exposureCategory'),
            obs.get('qualityCode'),
            obs.get('controlInfo'),
            obs.get('dataVersion')
        ]], columns=type(self).observations_headers)

        return obs_row

    def __create_compound_obs_df(
        self,
        obs_response: Dict[str, Any],
        src: str,
        reg_id: int,
        distance: float
    ) -> pd.DataFrame:
        observations_df = pd.DataFrame(columns=type(self).observations_headers)
        for obs_at_time in obs_response:
            for obs in obs_at_time['observations']:
                obs_row = self.__create_observation_row(
                    src,
                    obs,
                    obs_at_time,
                    reg_id,
                    distance
                )
                observations_df = observations_df.append(
                    obs_row,
                    ignore_index=True
                )

        return observations_df
