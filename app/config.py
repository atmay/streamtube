import os
from os.path import join, dirname
from dotenv import load_dotenv
from functools import lru_cache

os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'


class Settings:
    dotenv_path = join(dirname(__file__), '../.env')
    load_dotenv(dotenv_path)

    keyspace: str = os.environ.get('ASTRADB_KEYSPACE')
    db_client_id: str = os.environ.get('ASTRADB_CLIENT_ID')
    db_client_secret: str = os.environ.get('ASTRADB_CLIENT_SECRET')


@lru_cache
def get_settings():
    return
