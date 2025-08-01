# make oracledb work with sqlalchemy 1.4
# https://stackoverflow.com/questions/74093231/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectsoracle-oracledb
import sys
import oracledb

from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.exceptions import AirflowException
from sqlalchemy import text, MetaData, Table, event
from sqlalchemy.types import NullType
from dataclasses import dataclass, field
from typing import List, Optional, Mapping, Any

try:
    from geopandas import GeoDataFrame, read_postgis
except ImportError:
    raise AirflowException("Geopandas library not installed, run: pip install geopandas.")

from ...sqlalchemy.types import STGeometry
from ...utils import str2bool

# força inicialização do sdk oracle (thin)
oracledb.init_oracle_client() # força inicialização do sdk oracle (thin)
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb


@dataclass
class GeometryProps:
    name: str
    srid: int
    type: str
    is_multi: bool
    has_empty: bool
    is_3d: bool
    is_measured: bool
    is_simple: bool

    def get_ogc_sf_geometry_type(self):
        """
        Returns the geometry type in OGC Simple Features format.
        """
        _type = self.type.upper()
        
        if self.is_multi and not self.type == "GEOMETRYCOLLECTION":
            _type =  "MULTI" + _type
        
        if self.is_3d:
            _type += "Z"
        
        if self.is_measured:
            _type += "M"
        
        return _type


@dataclass
class TableProps:
    schema: str
    table: str
    pk: str
    is_simple: bool
    is_versioned: bool
    is_replicated: bool
    geometry_columns: List[GeometryProps] = field(default_factory=list)

    @property
    def verbose_name(self):
        return f"{self.schema}.{self.table}"


# Custom Oracle Hook to handle SDE version and connection
class SDEOracleHook(OracleHook):
    def get_uri(self) -> str:
        uri = super().get_uri()

        # Force excluding oracle driver from the URI (SQLAlchemy 1.4 compatibility)
        if uri.startswith("oracle+oracledb://"):
            uri = uri.replace("oracle+oracledb://", "oracle://")
        
        return uri
    
    _sde_version = None


    @property
    def sde_version(self) -> tuple[int, int, int]:
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
                    raise AirflowException(e)

            self._sde_version = major, minor, bugfix

        return self._sde_version


    @property
    def has_sde(self) -> bool:
        sde_version = self.sde_version

        if sde_version is not None:
            self.log.info("SDE Version: %s", sde_version)
            return True
        
        return False
    

    def get_spatial_table_props(
            self, 
            schema: str, 
            table: str
        ) -> TableProps:
        """
        Returns the properties of a spatial table.
        """
        engine = self.get_sqlalchemy_engine()

        if self.has_sde:
            with engine.connect() as conn:
                params = {"table": table, "schema": schema}
                verbose_table_name = f"{schema}.{table}"

                table_props = TableProps(
                    schema=schema,
                    table=table,
                    # Primary key (RowID)
                    pk=conn.execute(
                        text("SELECT sde.gdb_util.rowid_name(:schema, :table) FROM DUAL"),
                        params
                    ).scalar(), # type: ignore
                    # If a table is not simple, it should not be edited outside ArcGIS.
                    is_simple=str2bool(conn.execute(
                        text("SELECT sde.gdb_util.is_simple(:schema, :table) FROM DUAL"),
                        params
                    ).scalar()), # type: ignore
                    is_versioned=str2bool(conn.execute(
                        text("SELECT sde.gdb_util.IS_VERSIONED(:schema, :table) FROM DUAL"),
                        params
                    ).scalar()), # type: ignore
                    is_replicated=str2bool(conn.execute(
                        text("SELECT sde.gdb_util.is_replicated(:schema, :table) FROM DUAL"),
                        params
                    ).scalar()) # type: ignore
                )

                # geometries
                geometries = [
                    row for row, in conn.execute(
                        text("SELECT sde.gdb_util.geometry_columns(:schema, :table) FROM DUAL"),
                        params
                    ).fetchall() # type: ignore
                ]
                
                for geometry in geometries:
                    geom_prop = GeometryProps(
                        # column_name
                        name=geometry,
                         # SRID
                        srid=conn.execute(
                            text(f"SELECT DISTINCT sde.st_srid({geometry}) FROM {verbose_table_name}")
                        ).scalar(),  # type: ignore
                        # Geometry type (Will be set later)
                        type='GEOMETRYCOLLECTION', 
                        # Is multipart
                        is_multi=bool(conn.execute(
                            text(f"SELECT max(sde.st_numgeometries({geometry})) FROM {verbose_table_name}")
                        ).scalar() > 1), # type: ignore
                        # Empty geometry
                        has_empty=any([row for row, in conn.execute(
                            text(f"SELECT DISTINCT sde.st_isempty({geometry}) FROM {verbose_table_name}")
                        ).fetchall()]), # type: ignore
                        # 3d
                        is_3d=any([row for row, in conn.execute(
                            text(f"SELECT DISTINCT sde.st_is3d({geometry}) FROM {verbose_table_name}")
                        ).fetchall()]), # type: ignore
                        # Is measured
                        is_measured=any([row for row, in conn.execute(
                            text(f"SELECT DISTINCT sde.st_ismeasured({geometry}) FROM {verbose_table_name}")
                        ).fetchall()]),  # type: ignore
                        # OGC Simple Features
                        is_simple=all([row for row, in conn.execute(
                            text(f"SELECT DISTINCT sde.st_issimple({geometry}) FROM {verbose_table_name}")
                        ).fetchall()]) # type: ignore
                    )

                    # Geometry type
                    _type = [row for row, in conn.execute(
                        text(f"SELECT DISTINCT replace(replace(upper(sde.st_geometrytype({geometry})), 'ST_', ''), 'MULTI', '') FROM {verbose_table_name}")
                    ).fetchall()] # type: ignore

                    assert len(_type) > 0

                    geom_prop.type = _type[0] if len(_type) == 1 else 'GEOMETRYCOLLECTION'

                    table_props.geometry_columns.append(geom_prop)

            # Atestar que só tenha uma coluna de geometria
            assert len(geometries) == 1

            return table_props
        
        else:
            raise AirflowException("SDE is not available in this Oracle database.")
        
    
    def get_reflected_table(
            self, 
            schema: str, 
            table: str, 
            geometry_props: Optional[GeometryProps] = None
        ) -> Table:
        """
        Returns the reflected table ORM.
        """
        engine = self.get_sqlalchemy_engine()
        metadata = MetaData(schema=schema)

        if not geometry_props:
            # If no geometry properties are provided, fetch the table properties
            table_props = self.get_spatial_table_props(schema, table)
            geometry_props = table_props.geometry_columns[0]

        # Event handler for reflecting columns, for replace vendor to generalized ones
        @event.listens_for(metadata, "column_reflect")
        def genericize_datatypes(inspector, tablename, column_dict):            
            # Verificar se é uma coluna de geometria ESRI
            if type(column_dict["type"]) is NullType and column_dict["name"] == geometry_props.name:
                column_dict["type"] = STGeometry(
                    geometry_props.get_ogc_sf_geometry_type(), 
                    srid=geometry_props.srid, 
                    spatial_index=False
                )
            # TODO: Checar Numeric com scale=0 e as_decimal=False: < 5 smallint 5 <= x <= 9, int; > 9 bigint
            # elif:
            else:
                # Forçar o tipo genérico para intercâmbio de bancos de dados
                column_dict["type"] = column_dict["type"].as_generic()
                
        # New reflected table, from source
        self.log.info("Reflecting table %s.%s...", schema, table)        
        table_obj = Table(
            table, 
            metadata, 
            autoload_with=engine, 
        )

        # Ignore SRC indexes
        table_obj.indexes = set() 
        self.log.info("Indexes of %s.%s ignored.", schema, table)

        return table_obj    
    

    def get_geopandas_df(
        self, 
        sql, 
        geom_col,
        crs,
        **kwargs
    ) -> GeoDataFrame:
        with self.get_sqlalchemy_engine().raw_connection() as conn: # type: ignore
            return read_postgis(
                sql, 
                con=conn, 
                geom_col=geom_col,
                crs=crs,
                **kwargs
            ) # type: ignore
        

    def get_geopandas_df_by_chunks(self, sql, parameters: list | tuple | Mapping[str, Any] | None = None, **kwargs):
        pass
