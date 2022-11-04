import os
import pathlib
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import connection

from app.config import get_settings

settings = get_settings()

BASE_DIRECTORY = pathlib.Path(__file__).resolve().parent
CONNECT_BUNDLE = BASE_DIRECTORY / 'connect_bundle' / 'Archive.zip'

ASTRADB_CLIENT_ID = os.getenv('ASTRADB_CLIENT_ID')
ASTRADB_CLIENT_SECRET = os.getenv('ASTRADB_CLIENT_SECRET')


def get_db_session():
    cloud_config = {
        'secure_connect_bundle': CONNECT_BUNDLE
    }
    auth_provider = PlainTextAuthProvider(ASTRADB_CLIENT_ID, ASTRADB_CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    connection.register_connection(str(session), session=session)
    connection.set_default_connection(str(session))
    return session
