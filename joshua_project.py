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
            # Insert or update data using upsert
            table = 'joshua_project_data'
            database = os.getenv('TDB_DB')

            # Iterate through the DataFrame and upsert the rows
            with engine.connect() as conn:
                for index, row in slim_jp.iterrows():
                    columns = list(slim_jp.columns)
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
                    values_dict = {col: (val if pd.notna(val) else None) for col, val in row.items()}
                    with engine.begin():
                        conn.execute(query, values_dict)

            self.__logger.info(f"Upsert of {len(slim_jp.index)} rows into '{database}.{table}' was successful!")

        except Exception as ex:
            self.__logger.error(f"Error during upsert: {ex}")


if __name__ == '__main__':
    obj_pb_importer = JoshuaProjectImport()
    obj_pb_importer.import_data()
