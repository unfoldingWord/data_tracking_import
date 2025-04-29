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
        pb_dataframe['IsProtectedCountry'] = pb_dataframe['IsProtectedCountry'].astype(int)
        pb_dataframe.columns = map(str.lower, pb_dataframe.columns)

        try:
            # Setup connection to DB
            database = os.getenv('TDB_DB')

            engine = self._get_db_connection()

            # Insert or update data using upsert
            table = 'pb_language_data'

            with engine.connect() as conn:
                num_inserts = 0
                num_updates = 0
                for index, row in pb_dataframe.iterrows():
                    # Extract columns and build param placeholders
                    columns = list(pb_dataframe.columns)
                    primary_key_col = 'languagecode'

                    # Fetch existing row
                    select_query = text(f"""
                        SELECT {', '.join(columns)}
                        FROM `uw-data-tracking`.{table}
                        WHERE {primary_key_col} = :pk
                    """)
                    existing_row = conn.execute(select_query, {"pk": row[primary_key_col]}).fetchone()

                    # Build dict of non-NaN values for current row
                    current_row_dict = {col: (val if pd.notna(val) else None) for col, val in row.items()}

                    update_needed = False
                    if existing_row is None:
                        update_needed = True  # No existing row, need to insert
                        num_inserts = num_inserts + 1
                    else:
                        existing_row_dict = dict(existing_row._mapping)
                        for col in columns:
                            if existing_row_dict.get(col) != current_row_dict.get(col):
                                self.__logger.info(f"Column {col} of row with key {existing_row_dict['languagecode']}"
                                                   f" has changed. previous value: {existing_row_dict.get(col)} "
                                                   f"current value: {current_row_dict.get(col)}")
                                update_needed = True
                                num_updates = num_updates + 1
                                break

                    if update_needed:
                        col_str = ', '.join(columns)
                        placeholders = ', '.join([f":{col}" for col in columns])
                        update_values = ', '.join([f"{col} = VALUES({col})" for col in columns])

                        query = text(f"""
                            INSERT INTO `uw-data-tracking`.{table} ({col_str})
                            VALUES ({placeholders})
                            ON DUPLICATE KEY UPDATE {update_values};
                        """)

                        with engine.begin():
                            conn.execute(query, current_row_dict)
                conn.commit()

            self.__logger.info(f"Inserted {num_inserts} rows and updated {num_updates} rows successfully into '{database}.{table}'!")

        except Exception as ex:
            self.__logger.error(f"Connection could not be made due to the following error: \n{ex}")


if __name__ == '__main__':
    obj_pb_importer = ProgressBibleImport()
    obj_pb_importer.import_data()
