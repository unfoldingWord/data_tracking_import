#!/usr/bin/python3

import os
import urllib.request
import json
import pandas as pd
from dotenv import load_dotenv
from silapiimporter import *


class ProgressBibleImport(SILAPIImporter):
    def __init__(self):
        super().__init__()
        self.__logger = self._init_logger()
        load_dotenv()

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
        pb_dataframe.columns = map(str.lower, pb_dataframe.columns)

        try:
            # Setup connection to DB
            database = os.getenv('TDB_DB')

            engine = self._get_db_connection()

            # This replaces the existing table with a new one, with the new data
            table = 'pb_language_data'
            pb_dataframe.to_sql(name=table, con=engine, if_exists='replace', index=False)

            self.__logger.info(f"Import of {len(pb_dataframe.index)} rows into '{database}.{table}' was successful!")

        except Exception as ex:

            self.__logger.error(f"Connection could not be made due to the following error: \n{ex}")


if __name__ == '__main__':
    obj_pb_importer = ProgressBibleImport()
    obj_pb_importer.import_data()
