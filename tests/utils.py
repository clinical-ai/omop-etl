import psycopg2
import pytest
from pytest_postgresql import factories
from schema import *

_PG_CONNECTION = {
    "host": "localhost",
    "password": "password",
    "port": 5432,
    "user": "postgres",
}


try:
    conn = psycopg2.connect(**_PG_CONNECTION, connect_timeout=1)
    conn.close()
    PG_AVAILABLE = True
except Exception as ex:
    PG_AVAILABLE = False

postgresql_proc = factories.postgresql_noproc(**_PG_CONNECTION)


skip_if_no_db = pytest.mark.skipif(not PG_AVAILABLE, reason="no postgres db available")

