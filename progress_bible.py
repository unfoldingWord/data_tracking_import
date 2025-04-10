import urllib.request
import json
import pandas as pd
from dotenv import load_dotenv
from silapiimporter import *
from sqlalchemy import text


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
        pb_dataframe['IsProtectedCountry'] = pb_dataframe['IsProtectedCountry'].astype(int)
        pb_dataframe.columns = map(str.lower, pb_dataframe.columns)

        try:
            # Setup connection to DB
            database = os.getenv('TDB_DB')

            engine = self._get_db_connection()

            # Insert or update data using upsert
            table = 'pb_language_data'

            with engine.connect() as conn:
                for index, row in pb_dataframe.iterrows():
                    # Extract columns and build param placeholders
                    columns = list(pb_dataframe.columns)
                    col_str = ', '.join(columns)
                    placeholders = ', '.join([f":{col}" for col in columns])

                    # Build the ON DUPLICATE KEY UPDATE part
                    update_values = ', '.join([f"{col} = VALUES({col})" for col in columns])

                    # Create the parameterized query
                    query = text(f"""
                        INSERT INTO `uw-data-tracking`.{table} ({col_str})
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE {update_values};
                    """)

                    # Create the dictionary of parameters (convert NaNs to None)
                    values_dict = {col: (val if pd.notna(val) else None) for col, val in row.items()}

                    # Execute the query
                    with engine.begin():
                        conn.execute(query, values_dict)

            self.__logger.info(f"Upsert of {len(pb_dataframe.index)} rows into '{database}.{table}' was successful!")

        except Exception as ex:
            self.__logger.error(f"Connection could not be made due to the following error: \n{ex}")


if __name__ == '__main__':
    obj_pb_importer = ProgressBibleImport()
    obj_pb_importer.import_data()
