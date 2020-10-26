import json
from datetime import date

from apis.skredvarsel.skredvarsel_fetcher import SkredvarselFetcher
from apis.skredvarsel.skredvarsel_processor import SkredvarselProcessor
from util.avalanche_incident import AvalancheIncident
from util.testing import AvalancheTestCase


class TestSkredvarselProcessor(AvalancheTestCase):
    def test_process(self):

        example_skredvarsel_data = [
            {
                'CountyList': [
                    {
                        'Id': '03',
                        'Name': 'Oslo'
                    },
                    {
                        'Id': '30',
                        'Name': 'Viken'
                    },
                    {
                        'Id': '34',
                        'Name': 'Innlandet'
                    }
                ],
                'MunicipalityList': [
                    {
                        'Id': '0301',
                        'Name': 'Oslo'
                    },
                    {
                        'Id': '3007',
                        'Name': 'Ringerike'
                    },
                    {
                        'Id': '3031',
                        'Name': 'Nittedal'
                    },
                    {
                        'Id': '3035',
                        'Name': 'Eidsvoll'
                    },
                    {
                        'Id': '3036',
                        'Name': 'Nannestad'
                    },
                    {
                        'Id': '3037',
                        'Name': 'Hurdal'
                    },
                    {
                        'Id': '3039',
                        'Name': 'Flå'
                    },
                    {
                        'Id': '3040',
                        'Name': 'Nesbyen'
                    },
                    {
                        'Id': '3041',
                        'Name': 'Gol'
                    },
                    {
                        'Id': '3042',
                        'Name': 'Hemsedal'
                    },
                    {
                        'Id': '3053',
                        'Name': 'Jevnaker'
                    },
                    {
                        'Id': '3054',
                        'Name': 'Lunner'
                    },
                    {
                        'Id': '3405',
                        'Name': 'Lillehammer'
                    },
                    {
                        'Id': '3407',
                        'Name': 'Gjøvik'
                    },
                    {
                        'Id': '3411',
                        'Name': 'Ringsaker'
                    },
                    {
                        'Id': '3413',
                        'Name': 'Stange'
                    },
                    {
                        'Id': '3423',
                        'Name': 'Stor-Elvdal'
                    },
                    {
                        'Id': '3429',
                        'Name': 'Folldal'
                    },
                    {
                        'Id': '3435',
                        'Name': 'Vågå'
                    },
                    {
                        'Id': '3436',
                        'Name': 'Nord-Fron'
                    },
                    {
                        'Id': '3437',
                        'Name': 'Sel'
                    },
                    {
                        'Id': '3438',
                        'Name': 'Sør-Fron'
                    },
                    {
                        'Id': '3439',
                        'Name': 'Ringebu'
                    },
                    {
                        'Id': '3440',
                        'Name': 'Øyer'
                    },
                    {
                        'Id': '3441',
                        'Name': 'Gausdal'
                    },
                    {
                        'Id': '3442',
                        'Name': 'Østre Toten'
                    },
                    {
                        'Id': '3443',
                        'Name': 'Vestre Toten'
                    },
                    {
                        'Id': '3446',
                        'Name': 'Gran'
                    },
                    {
                        'Id': '3447',
                        'Name': 'Søndre Land'
                    },
                    {
                        'Id': '3448',
                        'Name': 'Nordre Land'
                    },
                    {
                        'Id': '3449',
                        'Name': 'Sør-Aurdal'
                    },
                    {
                        'Id': '3450',
                        'Name': 'Etnedal'
                    },
                    {
                        'Id': '3451',
                        'Name': 'Nord-Aurdal'
                    },
                    {
                        'Id': '3452',
                        'Name': 'Vestre Slidre'
                    },
                    {
                        'Id': '3453',
                        'Name': 'Øystre Slidre'
                    },
                    {
                        'Id': '3454',
                        'Name': 'Vang'
                    }
                ],
                'PreviousWarningRegId': None,
                'DangerLevelName': None,
                'UtmZone': 0,
                'UtmEast': 0,
                'UtmNorth': 0,
                'Author': None,
                'AvalancheDanger': None,
                'EmergencyWarning': None,
                'SnowSurface': None,
                'CurrentWeaklayers': None,
                'LatestAvalancheActivity': None,
                'LatestObservations': None,
                'MountainWeather': None,
                'AvalancheProblems': None,
                'AvalancheAdvices': None,
                'RegId': 0,
                'RegionId': 3042,
                'RegionName': 'Oppland sør',
                'RegionTypeId': 20,
                'RegionTypeName': 'B',
                'DangerLevel': '0',
                'ValidFrom': '2019-02-20T00:00:00',
                'ValidTo': '2019-02-20T23:59:59',
                'NextWarningTime': '2019-02-20T17:00:00',
                'PublishTime': '2019-02-20T00:00:00',
                'MainText': 'Ikke vurdert',
                'LangKey': 1,
                'id': 1
            },
            {
                'CountyList': [
                    {
                        'Id': '30',
                        'Name': 'Viken'
                    },
                    {
                        'Id': '34',
                        'Name': 'Innlandet'
                    },
                    {
                        'Id': '46',
                        'Name': 'Vestland'
                    }
                ],
                'MunicipalityList': [
                    {
                        'Id': '3040',
                        'Name': 'Nesbyen'
                    },
                    {
                        'Id': '3041',
                        'Name': 'Gol'
                    },
                    {
                        'Id': '3042',
                        'Name': 'Hemsedal'
                    },
                    {
                        'Id': '3043',
                        'Name': 'Ål'
                    },
                    {
                        'Id': '3044',
                        'Name': 'Hol'
                    },
                    {
                        'Id': '3052',
                        'Name': 'Nore og Uvdal'
                    },
                    {
                        'Id': '3449',
                        'Name': 'Sør-Aurdal'
                    },
                    {
                        'Id': '3451',
                        'Name': 'Nord-Aurdal'
                    },
                    {
                        'Id': '3452',
                        'Name': 'Vestre Slidre'
                    },
                    {
                        'Id': '3454',
                        'Name': 'Vang'
                    },
                    {
                        'Id': '4619',
                        'Name': 'Eidfjord'
                    },
                    {
                        'Id': '4620',
                        'Name': 'Ulvik'
                    },
                    {
                        'Id': '4641',
                        'Name': 'Aurland'
                    },
                    {
                        'Id': '4642',
                        'Name': 'Lærdal'
                    }
                ],
                'PreviousWarningRegId': 182744,
                'DangerLevelName': '1 Liten',
                'UtmZone': 33,
                'UtmEast': 150188,
                'UtmNorth': 6763814,
                'Author': 'Ronny@NVE',
                'AvalancheDanger': 'Stigande temperatur svekkar bindingar i snødekket. I høgda må ein vere merksam på at det finst vedvarande svakt lag kantkorn. Ein må vere varsom der snødekket er tynt eller blir mjukt i løpet av dagen.\n\nEin kan òg forvente at det kan gå enkelte våte laussnøskred i bratte sørvendte fjellsider der snøoverflata er våt og mjuk.\n\n\n',
                'EmergencyWarning': 'Ikke gitt',
                'SnowSurface': 'Snøoverflata er prega av mildvêr og plussgrader til 1800 moh. Snødekket er fuktig til bakken opp til 1200 moh, og den eldre fokksnøen framstår som kompakt med lite lagdelingar. Snødekket er generelt jevnt fordelt, mest i heng mot NØ-SØ. Ryggar og toppar er avblesne og utan snø. ',
                'CurrentWeaklayers': 'Det har vore observert vedvarande svake lag kantkorn og nedsnødd rim i snødekket. Mildvêret siste tida har truleg nøytralisert desse laga til om lag 1200 moh. Over denne høgda må ein framleis vere merksam på at det kan vere svake lag i snødekket',
                'LatestAvalancheActivity': None,
                'LatestObservations': 'Sundag føremiddag er det lett skya og 2 C på toppen av Hemsedal skisenter.',
                'MountainWeather': {
                    'LastSavedTime': '2019-02-25T08:42:53.99',
                    'CloudCoverId': 20,
                    'CloudCoverName': 'Delvis skyet',
                    'Comment': 'Sterkest vind nord og vest i regionen.  ',
                    'MeasurementTypes': [
                        {
                            'Id': 10,
                            'Name': 'Nedbør',
                            'SortOrder': 10,
                            'MeasurementSubTypes': [
                                {
                                    'Id': 60,
                                    'Name': 'Mest utsatt \r\nområde',
                                    'SortOrder': 20,
                                    'Value': '1'
                                },
                                {
                                    'Id': 70,
                                    'Name': 'Gjennomsnitt',
                                    'SortOrder': 10,
                                    'Value': '0'
                                }
                            ]
                        },
                        {
                            'Id': 20,
                            'Name': 'Vind',
                            'SortOrder': 20,
                            'MeasurementSubTypes': [
                                {
                                    'Id': 20,
                                    'Name': 'Styrke',
                                    'SortOrder': 20,
                                    'Value': 'Liten kuling'
                                },
                                {
                                    'Id': 50,
                                    'Name': 'Retning',
                                    'SortOrder': 50,
                                    'Value': 'W'
                                }
                            ]
                        },
                        {
                            'Id': 30,
                            'Name': 'Vindendring til',
                            'SortOrder': 30,
                            'MeasurementSubTypes': [
                                {
                                    'Id': 20,
                                    'Name': 'Styrke',
                                    'SortOrder': 20,
                                    'Value': None
                                },
                                {
                                    'Id': 50,
                                    'Name': 'Retning',
                                    'SortOrder': 50,
                                    'Value': None
                                },
                                {
                                    'Id': 100,
                                    'Name': 'Tidsperiode start',
                                    'SortOrder': 100,
                                    'Value': None
                                },
                                {
                                    'Id': 110,
                                    'Name': 'Tidsperiode slutt',
                                    'SortOrder': 110,
                                    'Value': None
                                }
                            ]
                        },
                        {
                            'Id': 40,
                            'Name': 'Temperatur',
                            'SortOrder': 40,
                            'MeasurementSubTypes': [
                                {
                                    'Id': 30,
                                    'Name': 'Min',
                                    'SortOrder': 30,
                                    'Value': '-5'
                                },
                                {
                                    'Id': 40,
                                    'Name': 'Maks',
                                    'SortOrder': 40,
                                    'Value': '3'
                                },
                                {
                                    'Id': 90,
                                    'Name': 'moh',
                                    'SortOrder': 90,
                                    'Value': '1400'
                                }
                            ]
                        },
                        {
                            'Id': 50,
                            'Name': 'Nullgradersgrense',
                            'SortOrder': 50,
                            'MeasurementSubTypes': [
                                {
                                    'Id': 90,
                                    'Name': 'moh',
                                    'SortOrder': 90,
                                    'Value': '1500'
                                },
                                {
                                    'Id': 100,
                                    'Name': 'Tidsperiode start',
                                    'SortOrder': 100,
                                    'Value': '12'
                                },
                                {
                                    'Id': 110,
                                    'Name': 'Tidsperiode slutt',
                                    'SortOrder': 110,
                                    'Value': '18'
                                }
                            ]
                        }
                    ]
                },
                'AvalancheProblems': [
                    {
                        'AvalancheProblemId': 1,
                        'AvalancheExtId': 20,
                        'AvalancheExtName': 'Tørre flakskred',
                        'AvalCauseId': 18,
                        'AvalCauseName': 'Kantkornet snø over skarelag',
                        'AvalProbabilityId': 2,
                        'AvalProbabilityName': 'Lite sannsynlig ',
                        'AvalTriggerSimpleId': 10,
                        'AvalTriggerSimpleName': 'Stor tilleggsbelastning',
                        'DestructiveSizeExtId': 2,
                        'DestructiveSizeExtName': '2 - Middels',
                        'AvalPropagationId': 1,
                        'AvalPropagationName': 'Få bratte heng',
                        'AvalancheTypeId': 10,
                        'AvalancheTypeName': 'Flakskred',
                        'AvalancheProblemTypeId': 30,
                        'AvalancheProblemTypeName': 'Vedvarende svakt lag (flakskred)',
                        'ValidExpositions': '11111111',
                        'ExposedHeight1': 1000,
                        'ExposedHeight2': 0,
                        'ExposedHeightFill': 1
                    },
                    {
                        'AvalancheProblemId': 2,
                        'AvalancheExtId': 15,
                        'AvalancheExtName': 'Våte løssnøskred ',
                        'AvalCauseId': 24,
                        'AvalCauseName': 'Ubunden snø',
                        'AvalProbabilityId': 3,
                        'AvalProbabilityName': 'Mulig ',
                        'AvalTriggerSimpleId': 21,
                        'AvalTriggerSimpleName': 'Liten tilleggsbelastning',
                        'DestructiveSizeExtId': 1,
                        'DestructiveSizeExtName': '1 - Små',
                        'AvalPropagationId': 2,
                        'AvalPropagationName': 'Noen bratte heng',
                        'AvalancheTypeId': 20,
                        'AvalancheTypeName': 'Løssnøskred',
                        'AvalancheProblemTypeId': 5,
                        'AvalancheProblemTypeName': 'Våt snø (løssnøskred)',
                        'ValidExpositions': '00111110',
                        'ExposedHeight1': 800,
                        'ExposedHeight2': 0,
                        'ExposedHeightFill': 1
                    }
                ],
                'AvalancheAdvices': [
                    {
                        'AdviceID': 0,
                        'ImageUrl': 'https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/Images/AvalancheAdvice/.jpg',
                        'Text': 'Kun enkelte spesielt utsatte områder er skredutsatt. Vær varsom der skredproblemet er å finne i kombinasjon med terrengfeller.'
                    },
                    {
                        'AdviceID': 1,
                        'ImageUrl': 'https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/Images/AvalancheAdvice/.jpg',
                        'Text': 'Vær varsom der skredproblemet er å finne i kombinasjon med terrengfeller. Kun enkelte spesielt utsatte områder er skredutsatt.'
                    }
                ],
                'RegId': 182885,
                'RegionId': 3032,
                'RegionName': 'Hallingdal',
                'RegionTypeId': 10,
                'RegionTypeName': 'A',
                'DangerLevel': '1',
                'ValidFrom': '2019-02-25T00:00:00',
                'ValidTo': '2019-02-25T23:59:59',
                'NextWarningTime': '2019-02-25T16:00:00',
                'PublishTime': '2019-02-25T08:42:53.477',
                'MainText': 'Generelt stabile forhold. Vedvarande svakt lag i høgfjellet kan påverkast der snødekket er tynt.',
                'LangKey': 1,
                'id': 2
            }
        ]

        processed_data = SkredvarselProcessor().process(example_skredvarsel_data)
        self.assertIn('reg_id', processed_data.columns)
        self.assertIn('danger_level', processed_data.columns)
