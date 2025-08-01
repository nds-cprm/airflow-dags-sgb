from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import BLOB
from geoalchemy2.types import Geometry


# Function to convert ST_Geometry to WKB
class SDEAsBinary(GenericFunction):
    type = BLOB
    name = quoted_name("SDE.ST_ASBINARY", False)
    identifier = "sde_asbinary"
    inherit_cache=True
    
# Function to convert text to ST_Geometry (Useless. Only for set property)
class SDEFromWKB(GenericFunction):
    type = Geometry
    name = quoted_name("SDE.ST_GEOMFROMWKB", False)
    identifier = "sde_fromwkb"
    inherit_cache=True
