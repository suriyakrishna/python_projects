
import argparse
import json
import logging
import os
import pandas as pd
import sys
from datetime import datetime
from json import JSONDecodeError


def read_config_file(file_path):
    config = {}
    try:
        with open(file_path, 'r') as conf:
            try:
                config = json.load(conf)
            except JSONDecodeError as e:
                logging.error("JSON Exception: {}".format(e))
                logging.error("EXITING")
                sys.exit(1)
    except FileNotFoundError as f:
        logging.error("File Not Found Exception: {}".format(f))
        logging.error("EXITING")
        sys.exit(1)
    return config


class csv_to_json:
    def __init__(self, input_file_path, output_file_path, column_names=None, delimiter=","):
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        if (isinstance(column_names, list)):
            self.column_names = column_names
        elif (isinstance(column_names, str)):
            self.column_names = list(map(lambda x: x.strip(), column_names.split(",")))
        else:
            self.column_names = column_names
        self.delimiter = delimiter

    def file_exists_check(self):
        return os.path.exists(self.input_file_path)

    def is_file(self):
        return os.path.isfile(self.input_file_path)

    def transform_column_names(self, df):
        columns_dict = {}
        for i in map(lambda x: (x, x.strip().replace(' ', '_').lower()), df.columns):
            columns_dict[i[0]] = i[1]
        return columns_dict

    def convert(self):
        if (self.column_names == None):
            df = pd.read_csv(self.input_file_path, delimiter=self.delimiter, engine="python")
        else:
            df = pd.read_csv(self.input_file_path, delimiter=self.delimiter, names=self.column_names, engine="python")
        df = df.rename(columns=self.transform_column_names(df))
        df.to_json(orient="records", path_or_buf=self.output_file_path)


def main():
    parser = argparse.ArgumentParser(description='CSV to JSON Converter')

    parser.add_argument('-t', '--type', help='Configuration type JSON/ARGS', required=True)
    parser.add_argument('-c', '--confFilePath', help='JSON Configuration File path', default=None)
    parser.add_argument('-i', '--inputFilePath', help='Input CSV File Path', default=None)
    parser.add_argument('-o', '--outputFilePath', help='Output path to store JSON File', default=None)
    parser.add_argument('-d', '--delimiter', help='CSV File delimiter', default=",", type=str)
    parser.add_argument('-l', '--logFilePath', help='Log File Path', default=None)
    parser.add_argument('-n', '--columnNames', help='List of Column Names - For files without header', default=None)

    args = parser.parse_args()
    conf_type = args.type
    conf_file_path = args.confFilePath
    input_csv_file_path = args.inputFilePath
    ouput_json_file_path = args.outputFilePath
    delimiter = args.delimiter
    log_file_path = args.logFilePath
    column_names = args.columnNames

    if (conf_type.lower() == "json" and conf_file_path != None):
        config = read_config_file(conf_file_path)
        config_keys = config.keys()
        delimiter = ","
        column_names = None
        if "input_file" in config_keys:
            input_csv_file_path = config["input_file"]
        else:
            logging.error("input_file paramater not definied in the config JSON file")
            logging.error("EXITING")
            sys.exit()
        if "output_file" in config_keys:
            ouput_json_file_path = config["output_file"]
        else:
            logging.error("output_file paramater not definied in the config JSON file")
            logging.error("EXITING")
            sys.exit()
        if "delimiter" in config_keys:
            delimiter = config["delimiter"]
        if "column_names" in config_keys:
            column_names = config["column_names"]
        if "log_file_path" in config_keys:
            log_file_path = config["log_file_path"]
    elif (conf_type.lower() == "json" and conf_file_path == None):
        logging.error("--confFilePath is required arguments when type argument value is JSON")
        logging.error("EXITING")
        sys.exit(1)
    else:
        if (input_csv_file_path == None or ouput_json_file_path == None):
            logging.error(
                "--inputFilePath and --outputFilePath are required arguments when type argument value is not JSON")
            logging.error("EXITING")
            sys.exit(1)
    
    # Create a custom logger
    logger = logging.getLogger(__name__)
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
    log_file_name = 'csv_to_json_' + str(current_timestamp) + '.logs'
    if log_file_path != None:
        log_file_name = os.path.join(log_file_path, log_file_name)
    f_handler = logging.FileHandler(log_file_name)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    f_handler.setLevel(logging.DEBUG)
    logger.addHandler(f_handler)
    
    logger.info("CONFIGURATION TYPE: {}".format(conf_type))
    if conf_file_path != None:
        logger.info("CONF FILE PATH: '{}'".format(conf_file_path))
    if input_csv_file_path != None:
        logger.info("INPUT CSV FILE PATH: '{}'".format(input_csv_file_path))
    if ouput_json_file_path != None:
        logger.info("OUTPUT JSON FILE PATH: '{}'".format(ouput_json_file_path))
    logger.info("DELIMITER: {}".format(delimiter))
    if log_file_path != None:
        logger.info("LOG FILE PATH: '{}'".format(log_file_path))
    if column_names != None:
        logger.info("LIST OF COLUMN NAMES: {}".format(column_names))

    logger.info("PROCESS STARTED")
    if column_names != None:
        converter = csv_to_json(input_csv_file_path, ouput_json_file_path, delimiter=delimiter, column_names=column_names)
    else:
        converter = csv_to_json(input_csv_file_path, ouput_json_file_path, delimiter=delimiter)

    if (converter.is_file()):
        logger.info("INPUT FILE PATH IS FILE '{}'".format(converter.input_file_path))
    else:
        logger.error("INPUT FILE PATH IS NOT FILE '{}'".format(converter.input_file_path))
        logger.error("EXITING..")
        sys.exit(1)

    if (converter.file_exists_check()):
        logger.info("INPUT FILE EXISTS '{}'".format(converter.input_file_path))
    else:
        logger.error("INPUT FILE DOSEN'T EXISTS '{}'".format(converter.input_file_path))
        logger.error("EXITING..")
        sys.exit(1)

    logger.info("CONVERTING CSV TO JSON")
    converter.convert()
    logger.info("FILE CONVERTED SUCCESSFULLY")
    logger.info("CHECK OUTPUT LOCATION: '{}'".format(converter.output_file_path))


if __name__ == "__main__":
    log_frmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_frmt)
    main()