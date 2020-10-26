import asyncio
import logging
import datetime as dt
from typing import List, Dict, Any
import aiohttp
import pandas as pd
from decouple import config
import apis.fetcher as fetcher
from util.avalanche_incident import AvalancheIncident
from util.async_wrappers import gather_with_concurrency


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

    __frost_auth = aiohttp.BasicAuth(config('FROST_CID'))

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

        loop = asyncio.get_event_loop()
        # Fetch for all incidents
        dfs = loop.run_until_complete(gather_with_concurrency(10, *(
            self.fetch_for_incident(incident) for incident in incidents
        )))

        # Convert the result into a usable structure
        dfs2 = {'sources': [], 'observations': []}
        for ds in dfs:
            for d in ds:
                for key, value in d.items():
                    dfs2[key].append(value)

        for sources in dfs2['sources']:
            sources_df = sources_df.append(sources, ignore_index=True)

        for observations in dfs2['observations']:
            observations_df = observations_df.append(
                observations,
                ignore_index=True
            )

        return {
            'frost_sources': sources_df,
            'frost_observations': observations_df
        }

    async def fetch_for_incident(
        self,
        incident: AvalancheIncident
    ) -> (
        List[Dict[str, pd.DataFrame]]
    ):
        interval_start = (
            incident.time - dt.timedelta(days=type(self).days_before)
        ).strftime(type(self).__time_format)
        interval_end = (incident.time + dt.timedelta(days=1)).strftime(
            type(self).__time_format
        )

        # Fetch for all elements
        return await asyncio.gather(*(
            self.fetch_for_element(
                incident,
                element,
                interval_start,
                interval_end
            ) for element in type(self).elements
        ))

    async def fetch_for_element(
        self,
        incident: AvalancheIncident,
        element: str,
        start: str,
        end: str
    ) -> Dict[str, pd.DataFrame]:
        async with aiohttp.ClientSession(auth=type(self).__frost_auth) as s:
            sources_response = await self.fetch_sources(
                s,
                element,
                incident.coords_latlng[0],
                incident.coords_latlng[1],
                start,
                end
            )

            source_distances = {}
            sources_df = pd.DataFrame(columns=type(self).sources_headers)
            for source in sources_response:
                source_distances[source['id']] = source['distance']
                sources_df = sources_df.append(
                    self.__create_source_row(source),
                    ignore_index=True
                )

            # Fetch for all sources
            observations_dfs = await gather_with_concurrency(5, *(
                self.fetch_observations(
                    s,
                    incident,
                    source,
                    start,
                    end,
                    element,
                    source_distances[source]
                ) for source in source_distances.keys()
            ))

            observations_df = pd.DataFrame(
                columns=type(self).observations_headers
            )
            for obs_df in observations_dfs:
                observations_df = observations_df.append(
                    obs_df,
                    ignore_index=True
                )

            return {
                'sources': sources_df,
                'observations': observations_df
            }

    async def fetch_sources(
        self,
        s: aiohttp.ClientSession,
        element: str,
        latitude: float,
        longitude: float,
        start: str,
        end: str
    ) -> Dict[str, Any]:
        url = type(self).__sources_uri.format(
            element,
            longitude,
            latitude,
            type(self).nearestmaxcount,
            start,
            end
        )

        # TODO: Clean up this retry hack
        for i in range(5):
            async with s.get(url) as response:
                try:
                    result = (await response.json(content_type=None))
                    return result.get('data')
                except Exception as e:
                    logging.exception(f'Exception raised for url {url}')
                    logging.critical(f'Response was:\n{await response.text()}')
                    if i == 4:
                        raise e

    async def fetch_observations(
        self,
        s: aiohttp.ClientSession,
        incident: AvalancheIncident,
        source: str,
        start: str,
        end: str,
        element: str,
        distance: float
    ) -> pd.DataFrame:
        url = type(self).__observations_uri.format(source, start, end, element)

        # TODO: Clean up this retry hack
        for i in range(5):
            async with s.get(url) as response:
                try:
                    obs = (await response.json(content_type=None)).get('data')

                    if obs is None:
                        return None

                    return self.__create_obs_df(
                        obs,
                        source,
                        incident.id,
                        distance
                    )
                except Exception as e:
                    logging.exception(f'Exception raised for url {url}')
                    logging.critical(f'Response was:\n{await response.text()}')
                    if i == 4:
                        raise e

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

    def __create_obs_df(
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
