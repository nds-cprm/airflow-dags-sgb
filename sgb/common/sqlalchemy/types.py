from geoalchemy2.types import _GISType
from geoalchemy2.elements import WKBElement
from sqlalchemy.dialects.oracle.base import ischema_names

from .functions import SDEAsBinary, SDEFromWKB


# Emulate ESRI Geometry Type
class STGeometry(_GISType):
    name = "shape"
    as_binary = SDEAsBinary.identifier
    from_text = SDEFromWKB.identifier
    ElementType = WKBElement
    cache_ok = False

ischema_names['ST_GEOMETRY'] = STGeometry
