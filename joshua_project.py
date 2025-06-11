import urllib.request
import json
import os
import pandas as pd
from silapiimporter import SILAPIImporter
from dotenv import load_dotenv
from sqlalchemy import text


class JoshuaProjectImport(SILAPIImporter):
    def __init__(self):
        super().__init__()
        self.__logger = self._init_logger()
        load_dotenv()

    def pull_from_api(self):
        # set some important variables
        domain = os.getenv('JP_BASE_URL')
        api_key = os.getenv('JP_KEY')

        limit = 2000
        records = 2000
        page = 1
        jp_full_data = None

        while records == limit:
            url = domain + "/v1/people_groups.json?api_key=" + api_key + "&limit=" + str(limit) + "&page=" + str(page)
            conn = urllib.request.urlopen(url).read()
            jp_json = json.loads(conn)
            records = len(jp_json)
            page += 1
            if jp_full_data is not None:
                jp_full_data += jp_json
            else:
                jp_full_data = jp_json

        for i in range(len(jp_full_data)):
            jp_full_data[i].pop("Resources")

        df = pd.DataFrame(jp_full_data)

        return df

    def import_data(self):
        # Pull data from API
        df = self.pull_from_api()

        # Check for duplicates in the DataFrame
        duplicate_check = df.groupby('PeopleID3ROG3').size().reset_index(name='the_count')
        duplicates = duplicate_check[duplicate_check['the_count'] > 1]

        # If duplicates are found, raise an exception with the first duplicate
        if not duplicates.empty:
            raise Exception(
                f"Duplicate found! The first duplicate row has a 'the_count' value greater than 1: {duplicates.iloc[0]}")

        # Setup DB conn
        engine = self._get_db_connection()

        # Cross-reference with what's already in DB
        cross_ref = pd.read_sql(sql='jp_cross_ref_cntry_codes', con=engine)
        uw_country = pd.read_sql(sql='countries', con=engine)

        full_country_ref = pd.merge(cross_ref, uw_country, left_on='ISO2', right_on='alpha_2_code')
        jp_data = df.merge(full_country_ref, on='ROG3', how='left')
        slim_jp = jp_data[[
            "PeopleID3ROG3", "PeopleID3", "PeopNameInCountry", "english_short_name", "ISO2", "LeastReached",
            "PrimaryLanguageName", "ROL3", "Population", "JPScale", "BibleStatus", "Frontier"
        ]]
        slim_jp = slim_jp.sort_values(by=["english_short_name", "PeopNameInCountry"])
        slim_jp.reset_index(level=0, inplace=True, drop=True)
        slim_jp.rename(columns={"english_short_name": "country_name", "ISO2": "country_code"}, inplace=True)
        slim_jp.columns = map(str.lower, slim_jp.columns)

        try:
            # Insert or update data using explicit INSERT and UPDATE
            table = 'joshua_project_data'
            database = os.getenv('TDB_DB')  # Reintroduce database for logging

            # Iterate through the DataFrame and explicitly insert or update the rows
            with engine.connect() as conn:
                num_inserts = 0
                num_updates = 0
                for index, row in slim_jp.iterrows():
                    columns = list(slim_jp.columns)
                    primary_key_col = 'peopleid3rog3'  # Assuming this is your primary key

                    # Fetch existing row
                    select_query = text(f"""
                        SELECT {', '.join(columns)}
                        FROM `uw-data-tracking`.{table}
                        WHERE {primary_key_col} = :pk
                    """)
                    existing_row = conn.execute(select_query, {"pk": row[primary_key_col]}).fetchone()

                    # Build dict of non-NaN values for current row
                    current_row_dict = {col: (val if pd.notna(val) else None) for col, val in row.items()}

                    # If no existing row, we insert
                    if existing_row is None:
                        num_inserts += 1
                        col_str = ', '.join(columns)
                        placeholders = ', '.join([f":{col}" for col in columns])

                        insert_query = text(f"""
                            INSERT INTO `uw-data-tracking`.{table} ({col_str})
                            VALUES ({placeholders});
                        """)
                        conn.execute(insert_query, current_row_dict)
                    else:
                        # If existing row is found, we check if there's any difference and update
                        update_needed = False
                        existing_row_dict = dict(existing_row._mapping)
                        for col in columns:
                            if existing_row_dict.get(col) != current_row_dict.get(col):
                                self.__logger.info(f"Column {col} of row with key {existing_row_dict['peopleid3rog3']}"
                                                   f" has changed. previous value: {existing_row_dict.get(col)} "
                                                   f"current value: {current_row_dict.get(col)}")
                                update_needed = True
                                num_updates += 1
                                break  # As soon as one difference is found, we update

                        if update_needed:
                            # Create the UPDATE query
                            set_values = ', '.join([f"{col} = :{col}" for col in columns if col != primary_key_col])
                            update_query = text(f"""
                                UPDATE `uw-data-tracking`.{table}
                                SET {set_values}
                                WHERE {primary_key_col} = :peopleid3rog3
                            """)
                            # Explicitly ensure the primary key is included for update
                            current_row_dict[primary_key_col] = row[
                                primary_key_col]  # Include pk in the dictionary for the UPDATE
                            conn.execute(update_query, current_row_dict)
                conn.commit()
                self.__logger.info(“commit complete.”)

            self.__logger.info(
                f"Inserted {num_inserts} rows and updated {num_updates} rows successfully into '{database}.{table}'!")

        except Exception as ex:
            self.__logger.error(f"Error during insert/update: {ex}")


if __name__ == '__main__':
    obj_pb_importer = JoshuaProjectImport()
    obj_pb_importer.import_data()
