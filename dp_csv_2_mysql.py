# Load csv file to mysql database.
# Data is inserted using mysql.connector.
# Making use of pandas to read csv file.
########################################################################################################################
import datetime
import json
import logging
import sys
import traceback

import mysql.connector as msql
import pandas as pd
from mysql.connector import Error
import os

########################################################################################################################

########################################################################################################################
# Function to set the logging configuration.


def set_logging():
    logging.basicConfig(filename='info.log', level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')


########################################################################################################################
# Function to get schema file from the user


def get_config_schema(schema_file):
    # Need to add validation
    try:
        with open(schema_file, 'r', encoding='utf8') as schema_file:
            schema_dict = json.load(schema_file)
            logging.info("Opening schema configuration file ")
        return schema_dict
    except FileNotFoundError:
        logging.error("Schema file is not found")


#######################################################################################################################
# Function to get database config file from the user


def get_config_db(database_config):
    # Need to add validation
    try:
        with open(database_config) as json_file:
            conn_parameters = json.load(json_file)
            logging.info("Opening database configuration file")
        return conn_parameters
    except FileNotFoundError:
        logging.error("Database config file is not found")


########################################################################################################################
# read the csv file and load into dataframe.


def extract_csv(datafile, schema):
    try:
        logging.info("Reading CSV file.........")
        skip_rows = schema["skip_rows"]
        start_col = schema["start_column"]
        end_col = schema["end_column"]
        df = pd.read_csv(datafile, skiprows=int(skip_rows), usecols=range(int(start_col), int(end_col)))
        return df
    except FileNotFoundError:
        logging.error("File Not Found in the location provided")
    except Exception as e:
        logging.error(traceback.format_exc())


########################################################################################################################
# Establish a connection to the database


def connect_mysql(connection_parameters):
    connect_obj = None
    try:
        logging.info("Connecting to mysql database")
        connect_obj = msql.connect(**connection_parameters)
        logging.info("Connected successfully")

    except Error as err:
        logging.error("Error in connecting to MySQL", err)
        connect_obj = None
    return connect_obj


########################################################################################################################
# Perform not null validation and data type validation


def validate_data(dataframe, schema):
    df = dataframe
    logging.info("Validating not null and date type values")

    def is_date(date_text, date_format):
        try:
            if pd.isnull(date_text) is not True:
                datetime.datetime.strptime(date_text, date_format)
                return True
        except ValueError:
            return False

    def validate_rows(row, index):
        #print(row)
        invalid_row = None
        row_errors = []
        for field in schema["fields"]:
            try:
                # Null Check

                if str.lower(field["not_null"]) == "y" and pd.isnull(row[field["source_field_name"]]) is True:
                    row_errors.append(
                        {"error": "null found in not null field", "field_name": field["source_field_name"]})
                # Date Check

                if str.lower(field["data_type"]) == "date" and is_date(row[field["source_field_name"]],
                                                                       field["format"]) is False:
                    row_errors.append({"error": "invalid date found", "field_name": field["source_field_name"]})

                if len(row_errors) > 0:
                    invalid_row = {"row": ','.join((row.to_string()).split('\n')), "errors": row_errors,
                                   "index": index}
            except Error as err:
                logging.error(err)
        return invalid_row

    invalid_rows = (z for z in (validate_rows(row, ind) for ind, row in dataframe.iterrows()) if z is not None)
    return invalid_rows


########################################################################################################################
# Function to cleanse the dataframe


def cleanse_data(dataframe, error_dict, schema):
    for e in error_dict:
        dataframe.drop(e["index"], inplace=True)
    print(dataframe)
    for field in schema["fields"]:
        if field["data_type"] == 'date':
            dataframe[field["source_field_name"]] = pd.to_datetime(dataframe[field["source_field_name"]])
            dataframe[field["source_field_name"]] = dataframe[field["source_field_name"]].dt.date
    return dataframe


########################################################################################################################
# Load the dataframe into mysql table


def load_database(dataframe, schema, conn_obj):
    mycursor = conn_obj.cursor()
    table_name = schema["target_table_name"]
    columns = ''
    for field in schema["fields"]:
        if field["data_type"] == 'string':
            field["data_type"] = 'varchar(40)'
        if field['data_type'] == 'float':
            field['data_type'] = f'float({field["precision"]},{field["scale"]})'
        columns = columns + field["target_field_name"] + "  " + field["data_type"]
        if field != schema["fields"][-1]:
            columns = columns + ","

    try:
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        logging.info("Creating table.............")
        print(create_table_sql)
        mycursor.execute(create_table_sql)
    except Exception as e:
        print(e)
    columns_insert = ''
    place_holder = ''
    for field in schema["fields"]:
        columns_insert = columns_insert + field["target_field_name"]
        place_holder = place_holder + '%s'
        if field != schema["fields"][-1]:
            columns_insert = columns_insert + ','
            place_holder = place_holder + ','

    dataframe = dataframe.where(dataframe.notnull(), None)

    file_handler = open("checkpoint_file.txt","a")
    try:
        DB_BATCH_SIZE = 10000
        no_of_records = 0
        values = []
        for i, row in dataframe.iterrows():
            if no_of_records < DB_BATCH_SIZE:
                values.append(tuple(row))
                no_of_records = no_of_records + 1
                if not i == len(dataframe.index) - 1:
                    continue

            ######Call the database insert#######
            #print(f" {len(values)} records inserted.")

            insert_table_sql = f'INSERT INTO {table_name} ({columns_insert}) VALUES ({place_holder})'
            logging.info(f"{len(values)} records inserted.\n")
            mycursor.executemany(insert_table_sql, values)
            conn_obj.commit()
            file_handler.write(f"{len(values)} records inserted.\n")
            ######################################
            #if i<20:
            #    mycursor.close()
            #   conn_obj.close()
            values = [tuple(row)]
            no_of_records = 1
            batch_first_record = i
    except Exception as e:
        logging.error(f"The error {e} occurred processing the records after the index {batch_first_record}")
    else:
        file_handler.close()
        os.remove("checkpoint_file.txt")
    finally:
        if os.path.isfile("checkpoint_file.txt"):
            file_handler.close()

########################################################################################################################
# Main Function


def main(schema_file, database_config, datafile):
    set_logging()
    schema = get_config_schema(schema_file)
    conn_parameters = get_config_db(database_config)
    #@record_lineage
    dataframe = extract_csv(datafile, schema)
    #@record_lineage
    error_rows = validate_data(dataframe, schema)
    clean_dataframe = cleanse_data(dataframe, error_rows, schema)
    print(clean_dataframe.to_string())
    conn_obj = connect_mysql(conn_parameters)
    load_database(clean_dataframe, schema, conn_obj)


##################Main Idiom############################################

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please pass the following parameters: schema_file , database_config_file, data_file")
        exit(-1)
    main(schema_file=sys.argv[1], database_config=sys.argv[2], datafile=sys.argv[3])
