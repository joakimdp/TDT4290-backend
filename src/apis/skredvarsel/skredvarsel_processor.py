import apis.processor as processor
import pandas as pd
import json
from typing import List
from util.avalanche_incident import AvalancheIncident


class SkredvarselProcessor(processor.Processor):
    def process(self, raw_data: List[AvalancheIncident]) -> pd.DataFrame:
        for varsling in raw_data:
            varsling.pop("CountyList")
            varsling.pop("MunicipalityList")
            varsling.pop("MountainWeather")
            varsling.pop("AvalancheAdvices")
            varsling.pop("LatestObservations")
            avalanche_problems_list = varsling.pop("AvalancheProblems")

            if (avalanche_problems_list == None):
                avalanche_problems = {}

            else:
                avalanche_problems = {}
                avalanche_problems = avalanche_problems_list[0]
                varsling.update(avalanche_problems)

        df = pd.DataFrame(raw_data)
        df.rename(columns={
            'PreviousWarningRegId': 'previous_warning_reg_id',
            'DangerLevelName': 'danger_level_name',
            'UtmZone': 'utm_zone',
            'UtmEast': 'utm_east',
            'UtmNorth': 'utm_north',
            'Author': 'author',
            'AvalancheDanger': 'avalanche_danger',
            'EmergencyWarning': 'emergency_warning',
            'SnowSurface': 'snow_surface',
            'CurrentWeaklayers': 'current_weak_layers',
            'LatestAvalancheActivity': 'latest_avalanche_activity',
            'RegId': 'registration_id',
            'RegionId': 'region_id',
            'RegionName': 'region_name',
            'RegionTypeId': 'region_type_id',
            'RegionTypeName': 'region_type_name',
            'DangerLevel': 'danger_level',
            'ValidFrom': 'valid_from',
            'ValidTo': 'valid_to',
            'NextWarningTime': 'next_warning_time',
            'PublishTime': 'publish_time',
            'MainText': 'main_text',
            'LangKey': 'lang_key',
            'id': 'reg_id',
            'AvalancheProblemId': 'avalanche_problem_id',
            'AvalancheExtId': 'avalanche_ext_id',
            'AvalancheExtName': 'avalanche_ext_name',
            'AvalCauseId': 'aval_cause_id',
            'AvalCauseName': 'aval_cause_name',
            'AvalProbabilityId': 'aval_probability_id',
            'AvalProbabilityName': 'aval_probability_name',
            'AvalTriggerSimpleId': 'aval_trigger_simple_id',
            'AvalTriggerSimpleName': 'aval_trigger_simple_name',
            'DestructiveSizeExtId': 'destructive_size_ext_id',
            'DestructiveSizeExtName': 'destructive_size_ext_name',
            'AvalPropagationId': 'aval_propagation_id',
            'AvalPropagationName': 'aval_propagation_name',
            'AvalancheTypeId': 'avalanche_type_id',
            'AvalancheTypeName': 'avalanche_type_name',
            'AvalancheProblemTypeId': 'avalanche_problem_type_id',
            'AvalancheProblemTypeName': 'avalanche_problem_type_name',
            'ValidExpositions': 'valid_expositions',
            'ExposedHeight1': 'exposed_heigth_1',
            'ExposedHeight2': 'exposed_heigth_2',
            'ExposedHeightFill': 'exposed_height_fill'
        }, inplace=True)

        #df.set_index('reg_id', inplace=True)

        return df
