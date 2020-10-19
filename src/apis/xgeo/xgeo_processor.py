import apis.processor as processor
import pandas as pd


class XgeoProcessor(processor.Processor):
    @staticmethod
    def convert_dataframe_to_correct_format(id, dataframe):
        number_of_rows = len(dataframe.index)
        ids = [id for x in range(number_of_rows)]
        dates = dataframe.index.copy()

        database_rows = dataframe.reset_index(drop=True, inplace=False)
        database_rows["id"] = ids
        database_rows["date"] = dates

        return database_rows

    def process(self, dataframe_dict):
        """
        Takes as input a dictionary where the key is an id referencing
        an incident and the value is a dataframe containing xgeo_data
        related to the incident.

        Output a set of pandas dataframe objects which are ready to be
        put into the database.
        """
        output_rows = pd.DataFrame()
        for key, value in dataframe_dict.items():
            formatted_dataframe = XgeoProcessor.convert_dataframe_to_correct_format(
                key, value)
            output_rows = output_rows.append(formatted_dataframe)

        output_rows.rename(columns={'id': 'reg_id'}, inplace=True)
        return output_rows
