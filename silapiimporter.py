import hmac
from hashlib import sha1
from time import time
from sqlalchemy import create_engine
import os
import logging


class SILAPIImporter:
    def __init__(self):
        self.__logger = logging.getLogger()

    def _init_logger(self):
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

    def _get_db_connection(self):
        port = 3306
        user = os.getenv('TDB_USER')
        password = os.getenv('TDB_PASSWORD')
        host = os.getenv('TDB_HOST')
        database = os.getenv('TDB_DB')
        engine = None
        try:
            engine = create_engine(
                url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
                    user, password, host, port, database
                )
            )
            self.__logger.debug(f"Connection to host '{host}' for user '{user}' created successfully.")

        except Exception as ex:

            self.__logger.error(f"Connection could not be made due to the following error: \n{ex}")

        return engine

    def _create_signature(self, key, secret):
        curr_time = str(int(time()))

        concat = curr_time + key

        # hmac expects byte, python 3.x requires explicit conversion
        concat_b = concat.encode('utf-8')
        secret_b = secret.encode('utf-8')

        h1 = hmac.new(secret_b, concat_b, sha1)
        # h1 is byte, so convert to hex

        api_sig = h1.hexdigest()

        return api_sig

