import os

from silapiimporter import *
import urllib.request
import json
import pandas as pd
from dotenv import load_dotenv


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
        database = os.getenv('TDB_DB')

        # Pull data from API
        df = self.pull_from_api()

        # Setup DB conn
        engine = self._get_db_connection()

        # Cross-reference with what's already in DB
        cross_ref = pd.read_sql(sql='jp_cross_ref_cntry_codes', con=engine)
        uw_country = pd.read_sql(sql='country_data', con=engine)

        full_country_ref = pd.merge(cross_ref, uw_country, left_on='ISO2', right_on='alpha_2_code')
        jp_data = df.merge(full_country_ref, on='ROG3', how='left')
        slim_jp = jp_data[
            ["PeopNameInCountry", "english_short_name", "ISO2", "LeastReached", "PrimaryLanguageName", "ROL3",
             "Population"]]
        slim_jp = slim_jp.sort_values(by=["english_short_name", "PeopNameInCountry"])
        slim_jp.reset_index(level=0, inplace=True, drop=True)
        slim_jp.reset_index(level=0, inplace=True)
        slim_jp.rename(columns={'index': 'jp_id'})
        slim_jp.rename(columns={'index': 'jp_id', "english_short_name": "Country_name", "ISO2": "Country_code"},
                       inplace=True)

        try:
            # Enter the result into the DB
            table = 'slim_jp'
            slim_jp.to_sql(name=table, con=engine, if_exists='replace', index=False)

            self.__logger.info(f"Import of {len(df.index)} rows into '{database}.{table}' was successful!")

        except Exception as ex:
            self.__logger.error(f"Connection could not be made due to the following error: \n{ex}")


if __name__ == '__main__':
    obj_pb_importer = JoshuaProjectImport()
    obj_pb_importer.import_data()
