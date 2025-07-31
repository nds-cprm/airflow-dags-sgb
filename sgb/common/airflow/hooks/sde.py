# make oracledb work with sqlalchemy 1.4
# https://stackoverflow.com/questions/74093231/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectsoracle-oracledb
import sys
import oracledb

from airflow.providers.oracle.hooks.oracle import OracleHook
from sqlalchemy import text

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
                    self.log.exception("Failed to fetch SDE version: %s", e)
                    raise e

            self._sde_version = major, minor, bugfix

        return self._sde_version

    @property
    def has_sde(self):
        sde_version = self.sde_version

        if sde_version is not None:
            self.log.info("SDE Version: %s", sde_version)
            return True
        
        return False
    
    def get_spatial_table_props(self, schema: str, table: str):
        """
        Returns the properties of a spatial table.
        """
        engine = self.get_sqlalchemy_engine()
        table_props = {}
        geometry_props = []

        if self.has_sde:
            with engine.connect() as conn:
                params = {"table": table, "schema": schema}

                # Primary key (RowID)
                table_props['pk'] = conn.execute(
                    text("SELECT sde.gdb_util.rowid_name(:schema, :table) FROM DUAL"),
                    params
                ).scalar()

                # If a table is not simple, it should not be edited outside ArcGIS.
                table_props['is_simple'] = conn.execute(
                    text("SELECT sde.gdb_util.is_simple(:schema, :table) FROM DUAL"),
                    params
                ).scalar()

                table_props['is_versioned'] = conn.execute(
                    text("SELECT sde.gdb_util.IS_VERSIONED(:schema, :table) FROM DUAL"),
                    params
                ).scalar()

                table_props['is_replicated'] = conn.execute(
                    text("SELECT sde.gdb_util.is_replicated(:schema, :table) FROM DUAL"),
                    params
                ).scalar()

                # geometries
                geometries = [
                    row[0] for row in conn.execute(
                        text("SELECT sde.gdb_util.geometry_columns(:schema, :table) FROM DUAL"),
                        params
                    ).fetchall()
                ]

                verbose_table_name = f"{schema}.{table}"
                geometry_prop = {}
                
                for geometry in geometries:
                    # column_name
                    geometry_prop["name"] = geometry 

                    # SRID
                    geometry_prop['srid'] = conn.execute(
                        text(f"SELECT DISTINCT sde.st_srid({geometry}) FROM {verbose_table_name}")
                    ).scalar()

                    # Geometry type
                    _type = [row[0] for row in conn.execute(
                        text(f"SELECT DISTINCT replace(replace(upper(sde.st_geometrytype({geometry})), 'ST_', ''), 'MULTI', '') FROM {verbose_table_name}")
                    ).fetchall()]

                    assert len(_type) > 0

                    geometry_prop['type'] = _type[0] if len(_type) == 1 else 'GEOMETRYCOLLECTION'

                    # Simple or multipart
                    geometry_prop['is_multi'] = bool(conn.execute(
                        text(f"SELECT max(sde.st_numgeometries({geometry})) FROM {verbose_table_name}")
                    ).scalar() > 1)

                    # Empty geometry
                    geometry_prop['has_empty'] = any([row[0] for row in conn.execute(
                        text(f"SELECT DISTINCT sde.st_isempty({geometry}) FROM {verbose_table_name}")
                    ).fetchall()])

                    # 3d
                    geometry_prop['is_3d'] = any([row[0] for row in conn.execute(
                        text(f"SELECT DISTINCT sde.st_is3d({geometry}) FROM {verbose_table_name}")
                    ).fetchall()])

                    # Measured
                    geometry_prop['is_measured'] = any([row[0] for row in conn.execute(
                        text(f"SELECT DISTINCT sde.st_ismeasured({geometry}) FROM {verbose_table_name}")
                    ).fetchall()])
                    
                    # OGC Simple Feature
                    geometry_prop['is_simple'] = all([row[0] for row in conn.execute(
                        text(f"SELECT DISTINCT sde.st_issimple({geometry}) FROM {verbose_table_name}")
                    ).fetchall()])

                    geometry_props.append(geometry_prop)

            table_props["geometry_columns"] = geometry_props

            # Atestar que só tenha uma coluna de geometria
            assert len(geometries) == 1


        return table_props
            