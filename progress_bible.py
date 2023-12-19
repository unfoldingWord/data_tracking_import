#!/usr/bin/python3

import os
import urllib.request
import json
import pandas as pd
from dotenv import load_dotenv
from silapiimporter import *
import logging

load_dotenv()


class ProgressBibleImport(SILAPIImporter):
    def __init__(self):
        super().__init__()
        self.__logger = self.__init_logger()

    def __init_logger(self):
        this_logger = logging.getLogger()

        if not this_logger.hasHandlers():
            c_handler = logging.StreamHandler()
            if os.getenv('STAGE', False) == 'dev':
                c_handler.setLevel(logging.DEBUG)
                this_logger.setLevel(logging.DEBUG)
            else:
                c_handler.setLevel(logging.INFO)
                this_logger.setLevel(logging.INFO)

            log_format = '%(asctime)s  %(levelname)-8s %(message)s'
            c_format = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
            c_handler.setFormatter(c_format)

            this_logger.addHandler(c_handler)

        return this_logger

    def import_data(self):
        key = os.getenv('PB_KEY')
        secret = os.getenv('PB_SECRET')
        base_url = os.getenv('PB_BASE_URL')

        api_sig = self._create_signature(key, secret)

        url = f"{base_url}/?api_key={key}&api_sig={api_sig}&file=AllAccess.json"

        contents = urllib.request.urlopen(url).read()
        obj_json = json.loads(contents)

        pb_dataframe = pd.DataFrame.from_dict(obj_json['resource'])
        pb_dataframe["pb_id"] = pb_dataframe.index + 1

        try:
            # Setup connection to DB
            user = os.getenv('TDB_USER')
            password = os.getenv('TDB_PASSWORD')
            host = os.getenv('TDB_HOST')
            database = os.getenv('TDB_DB')
            ca_cert = os.getenv('TDB_SSL_CA_FILE')

            engine = self._get_db_connection(host=host, user=user, password=password,
                                             database=database, ca_file=ca_cert)

            self.__logger.debug(f"Connection to host '{host}' for user '{user}' created successfully.")

            # This replaces the existing table with a new one, with the new data
            table = 'progress_bible'
            pb_dataframe.to_sql(name=table, con=engine, if_exists='replace', index=False)

            self.__logger.info(f"Import of {len(pb_dataframe.index)} rows into '{database}.{table}' was successful!")

        except Exception as ex:

            self.__logger.error(f"Connection could not be made due to the following error: \n{ex}")


if __name__ == '__main__':
    obj_pb_importer = ProgressBibleImport()
    obj_pb_importer.import_data()
