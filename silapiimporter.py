import hmac
from hashlib import sha1
from time import time
from sqlalchemy import create_engine
# Used by SQLAlchemy
import pymysql


class SILAPIImporter:
    def __init__(self):
        pass

    def _get_db_connection(self, host, user, password, database, ca_file=None):
        port = 3306

        ssl_args = None
        if ca_file:
            ssl_args = {'ssl_ca': ca_file,
                        'ssl_verify_cert': True}

        return create_engine(
            url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
                user, password, host, port, database
            ), connect_args=ssl_args
        )

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

