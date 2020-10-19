
from apis.regobs.regobs import Regobs
from apis.regobs.regobs_initializer import RegobsData, RegobsInitializer
from apis.xgeo.xgeo_initializer import XgeoData, XgeoInitializer
from apis.xgeo.xgeo import Xgeo
from apis.frost.frost import Frost
from db_manager import DbManager
from decouple import config
from util.avalanche_incident import create_avalanche_incident_list
from util.dataframe_difference import dataframe_difference
from util.main_utils import *
from util.csv import to_csv, read_csv
from apis.skredvarsel.skredvarsel import Skredvarsel
from apis.skredvarsel.skredvarsel_initializer import SkredvarselData, SkredvarselInitializer
from apis.frost.frost_initializer import FrostInitializer, FrostObservation


def main():
    # Which APIs to append data to
    api_list = [Skredvarsel(), Xgeo(), Frost()]
    # Which APIs to remove deleted data from
    delete_removed_api_list = [RegobsData,
                               SkredvarselData, XgeoData, FrostObservation]
    # Which tables to initialize in database (on force update)
    initializer_class_list = [
        RegobsInitializer, SkredvarselInitializer, XgeoInitializer, FrostInitializer]

    # Handle command line arguments
    force_update = parse_command_line_arguments()

    # Create engine and db_inserter
    engine = create_db_connection()
    db_manager = DbManager(engine)

    print('Fetching regobs data..')
    fetch_regobs = False
    # Fetch regobs data from api
    if fetch_regobs:
        api_data = Regobs().get_data()
        to_csv(api_data, 'csv_files/regobs.csv')

    # Load regobs data from csv file (can be useful for debugging or testing incremental update)
    else:
        api_data = read_csv('csv_files/regobs.csv')

    # Incremental update. Only update added, updated or deleted rows in database tables.
    if not force_update:
        # Specify that the dataframe should be appended to the existing data in the database tables
        if_table_exists_in_database = 'append'

        # Query current data in database
        print('Querying regobs table from database..')
        db_data = db_manager.query_all_data_from_table('regobs_data', 'reg_id')

        # Compare current database data with new api data
        # Rows to delete from all tables
        print('Comparing dataframes..')
        deleted_rows = dataframe_difference(
            db_data, api_data, ['reg_id', 'dt_change_time'])

        # Rows to add to all tables
        new_rows = dataframe_difference(
            api_data, db_data, ['reg_id', 'dt_change_time'])

        deleted_reg_ids = list(deleted_rows['reg_id'])

        deleted_reg_ids = [int(x) for x in deleted_reg_ids]
        print('reg_ids for deleted rows:', deleted_reg_ids)

        if deleted_reg_ids:
            # Delete removed rows from api's
            for data_class in delete_removed_api_list:
                print('Deleting removed rows for:', str(data_class))
                db_manager.delete_rows_with_reg_id(deleted_reg_ids, data_class)
        else:
            print('No rows to delete from api tables')

        if not new_rows.empty:
            print('Number of new regobs rows: ', len(new_rows))
            avalanche_incident_list = create_avalanche_incident_list(new_rows)

            # Append new rows to regobs table
            insert_regobs_data_to_database(new_rows, db_manager, 'append')

        else:
            avalanche_incident_list = []

    # Initialize database and load all data
    elif force_update:
        if_table_exists_in_database = 'replace'

        avalanche_incident_list = create_avalanche_incident_list(
            api_data)

        print('Initializing tables')
        initialize_tables(initializer_class_list, engine)

        insert_regobs_data_to_database(api_data, db_manager, 'replace')

    if not avalanche_incident_list:
        print('No data to add to api tables')
        return

    api_table_dict = get_table_dict_for_apis_in_list(
        api_list, avalanche_incident_list)

    insert_data_for_table_dict(
        api_table_dict, db_manager, if_table_exists_in_database)


if __name__ == '__main__':
    main()
