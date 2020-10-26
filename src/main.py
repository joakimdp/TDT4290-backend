#!/usr/bin/env python3

import logging
import ast
import configparser
from decouple import config
from sqlalchemy import exc
from db_manager import DbManager

# Utilities
from util.avalanche_incident import create_avalanche_incident_list
from util.dataframe_difference import dataframe_difference
from util.main_utils import *
from util.csv import to_csv, read_csv
from util.logging import setup_logging


def main():
    # See configuration.ini for details
    fetch_regobs, api_fetch_list, api_delete_list, api_initialize_list = load_configuration()

    # Handle command line arguments
    force_update = parse_command_line_arguments()

    logging.info(
        'Application started with force_update={}'.format(force_update))

    # Create engine and db_inserter
    try:
        engine = create_db_connection()
    except Exception as e:
        logging.exception('Cannot connect to the database')
        raise e

    db_manager = DbManager(engine)

    logging.info('Fetching RegObs data..')
    # Fetch regobs data from api
    if fetch_regobs:
        try:
            api_data = Regobs().get_data()
        except Exception as e:
            logging.exception('Cannot fetch RegObs data')
            raise e

        to_csv(api_data, 'csv_files/regobs.csv')

    # Load regobs data from csv file (can be useful for debugging or testing incremental update)
    else:
        api_data = read_csv('csv_files/regobs.csv')

    # Incremental update. Only update added, updated or deleted records in database tables.
    if not force_update:
        # Specify that the dataframe should be appended to the existing data in the database tables
        if_table_exists_in_database = 'append'

        # Query current data in database
        logging.info('Querying regobs table from database..')
        try:
            db_data = db_manager.query_all_data_from_table(
                'regobs_data', 'reg_id')
        except exc.NoSuchTableError as e:
            logging.exception(
                'The table regobs_data does not exist in the database. Run the application with --force-update command line parameter to initialize all tables and fetch all data.')
            raise e
        except Exception as e:
            logging.exception('Cannot query RegObs data from database')
            raise e

        # Compare current database data with new api data
        # Rows to delete from all tables
        logging.info(
            'Comparing dataframes to determine which rows are added or removed..')
        deleted_rows = dataframe_difference(
            db_data, api_data, ['reg_id', 'dt_change_time'])

        # Rows to add to all tables
        new_rows = dataframe_difference(
            api_data, db_data, ['reg_id', 'dt_change_time'])

        deleted_reg_ids = list(deleted_rows['reg_id'])

        deleted_reg_ids = [int(x) for x in deleted_reg_ids]
        logging.info('Records with the following reg_ids will be deleted from the database: {}'.format(
            deleted_reg_ids))

        if deleted_reg_ids:
            # Delete removed rows from api's
            try:
                for data_class in api_delete_list:
                    logging.info(
                        'Deleting removed records for: {}'.format(data_class.__name__))
                    db_manager.delete_rows_with_reg_id(
                        deleted_reg_ids, data_class)
            except Exception as e:
                logging.exception(
                    'Cannot delete removed records from database table')
                raise e
        else:
            logging.info(
                'There are no deleted records to remove from the database')

        if not new_rows.empty:
            logging.info(
                'Number of new records to add: {}'.format(len(new_rows)))

            try:
                avalanche_incident_list = create_avalanche_incident_list(
                    new_rows)
            except Exception as e:
                logging.exception(
                    'Cannot create avalanche_incident_list from regobs data')
                raise e

            # Append new rows to regobs table
            try:
                insert_regobs_data_to_database(new_rows, db_manager, 'append')
            except Exception as e:
                logging.exception(
                    'Cannot append RegObs data to database table')
                raise e

        else:
            avalanche_incident_list = []

    # Initialize database and load all data
    elif force_update:
        # Specify that the dataframe should replace existing data in the database table
        if_table_exists_in_database = 'replace'

        try:
            avalanche_incident_list = create_avalanche_incident_list(
                api_data)
        except Exception as e:
            logging.exception(
                'Cannot create avalanche_incident_list from regobs data')
            raise e

        logging.info('Initializing database tables..')
        try:
            initialize_tables(api_initialize_list, engine)
        except Exception as e:
            logging.exception(
                'Cannot initialize tables in database')
            raise e

        try:
            insert_regobs_data_to_database(api_data, db_manager, 'replace')
        except Exception as e:
            logging.exception(
                'Cannot add RegObs data to database table')
            raise e

    if not avalanche_incident_list:
        logging.info('There is no new records to add to the database')
        logging.info('The application terminated successfully')
        return

    api_table_dict = get_table_dict_for_apis_in_list(
        api_fetch_list, avalanche_incident_list)

    # Set new database connection
    db_manager.engine = create_db_connection()

    try:
        insert_data_for_table_dict(
            api_table_dict, db_manager, if_table_exists_in_database)
    except Exception as e:
        logging.exception(
            'Cannot add API data to database table')
        raise e

    logging.info('The application terminated successfully')


if __name__ == '__main__':
    # Setup application logging
    logging = setup_logging()

    main()
