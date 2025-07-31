# make oracledb work with sqlalchemy 1.4
# https://stackoverflow.com/questions/74093231/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectsoracle-oracledb
import sys
import oracledb
import logging

from airflow.providers.oracle.hooks.oracle import OracleHook
from sqlalchemy import text

# Grab logger for this module
logger = logging.getLogger(__name__)

# força inicialização do sdk oracle (thin)
oracledb.init_oracle_client() # força inicialização do sdk oracle (thin)
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb


# Custom Oracle Hook to handle SDE version and connection
class SDEOracleHook(OracleHook):
    def get_uri(self):
        uri = super().get_uri()

        # Force excluding oracle driver from the URI (SQLAlchemy 1.4 compatibility)
        if uri.startswith("oracle+oracledb://"):
            uri = uri.replace("oracle+oracledb://", "oracle://")
        
        return uri
    
    _sde_version = None

    @property
    def sde_version(self):
        if not self._sde_version:
            engine = self.get_sqlalchemy_engine()

            with engine.connect() as conn:
                _table = 'sde.sde_version'

                if conn.dialect.name == 'oracle':
                    _table = 'sde.version'

                try:
                    major, minor, bugfix = conn.execute(
                        text(f"SELECT major, minor, bugfix FROM {_table}")
                    ).fetchone() # type: ignore

                except Exception as e:
                    logger.exception("Failed to fetch SDE version: %s", e)
                    raise e

            self._sde_version = major, minor, bugfix

        return self._sde_version