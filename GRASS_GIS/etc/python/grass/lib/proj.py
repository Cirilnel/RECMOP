r"""Wrapper for gprojects.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_gproj.8.4 -IC:/osgeo4w/include C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gprojects.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h -o OBJ.x86_64-w64-mingw32/proj.py

Do not modify this file.
"""

__docformat__ = "restructuredtext"

# Begin preamble for Python

from .ctypes_preamble import *
from .ctypes_preamble import _variadic_function

# End preamble

_libs = {}
_libdirs = []

# Begin loader

from .ctypes_loader import *

# End loader

add_library_search_dirs([])

# Begin libraries
_libs["grass_gproj.8.4"] = load_library("grass_gproj.8.4")

# 1 libraries
# End libraries

# No modules

# C:/osgeo4w/include/proj.h: 224
class struct_PJconsts(Structure):
    pass

PJ = struct_PJconsts# C:/osgeo4w/include/proj.h: 225

PJ_WKT2_2015 = 0# C:/osgeo4w/include/proj.h: 911

PJ_WKT2_2015_SIMPLIFIED = (PJ_WKT2_2015 + 1)# C:/osgeo4w/include/proj.h: 911

PJ_WKT2_2019 = (PJ_WKT2_2015_SIMPLIFIED + 1)# C:/osgeo4w/include/proj.h: 911

PJ_WKT2_2018 = PJ_WKT2_2019# C:/osgeo4w/include/proj.h: 911

OGRSpatialReferenceH = POINTER(None)# C:/osgeo4w/include/ogr_srs_api.h: 434

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 71
class struct_pj_info(Structure):
    pass

struct_pj_info.__slots__ = [
    'pj',
    'meters',
    'zone',
    'proj',
    'def',
    'srid',
    'wkt',
]
struct_pj_info._fields_ = [
    ('pj', POINTER(PJ)),
    ('meters', c_double),
    ('zone', c_int),
    ('proj', c_char * int(100)),
    ('def', String),
    ('srid', String),
    ('wkt', String),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 85
class struct_gpj_datum(Structure):
    pass

struct_gpj_datum.__slots__ = [
    'name',
    'longname',
    'ellps',
    'dx',
    'dy',
    'dz',
]
struct_gpj_datum._fields_ = [
    ('name', String),
    ('longname', String),
    ('ellps', String),
    ('dx', c_double),
    ('dy', c_double),
    ('dz', c_double),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 91
class struct_gpj_datum_transform_list(Structure):
    pass

struct_gpj_datum_transform_list.__slots__ = [
    'count',
    'params',
    'where_used',
    'comment',
    'next',
]
struct_gpj_datum_transform_list._fields_ = [
    ('count', c_int),
    ('params', String),
    ('where_used', String),
    ('comment', String),
    ('next', POINTER(struct_gpj_datum_transform_list)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 107
class struct_gpj_ellps(Structure):
    pass

struct_gpj_ellps.__slots__ = [
    'name',
    'longname',
    'a',
    'es',
    'rf',
]
struct_gpj_ellps._fields_ = [
    ('name', String),
    ('longname', String),
    ('a', c_double),
    ('es', c_double),
    ('rf', c_double),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 5
if _libs["grass_gproj.8.4"].has("GPJ_init_transform", "cdecl"):
    GPJ_init_transform = _libs["grass_gproj.8.4"].get("GPJ_init_transform", "cdecl")
    GPJ_init_transform.argtypes = [POINTER(struct_pj_info), POINTER(struct_pj_info), POINTER(struct_pj_info)]
    GPJ_init_transform.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 7
if _libs["grass_gproj.8.4"].has("GPJ_transform", "cdecl"):
    GPJ_transform = _libs["grass_gproj.8.4"].get("GPJ_transform", "cdecl")
    GPJ_transform.argtypes = [POINTER(struct_pj_info), POINTER(struct_pj_info), POINTER(struct_pj_info), c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    GPJ_transform.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 9
if _libs["grass_gproj.8.4"].has("GPJ_transform_array", "cdecl"):
    GPJ_transform_array = _libs["grass_gproj.8.4"].get("GPJ_transform_array", "cdecl")
    GPJ_transform_array.argtypes = [POINTER(struct_pj_info), POINTER(struct_pj_info), POINTER(struct_pj_info), c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double), c_int]
    GPJ_transform_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 14
if _libs["grass_gproj.8.4"].has("pj_do_proj", "cdecl"):
    pj_do_proj = _libs["grass_gproj.8.4"].get("pj_do_proj", "cdecl")
    pj_do_proj.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(struct_pj_info), POINTER(struct_pj_info)]
    pj_do_proj.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 16
if _libs["grass_gproj.8.4"].has("pj_do_transform", "cdecl"):
    pj_do_transform = _libs["grass_gproj.8.4"].get("pj_do_transform", "cdecl")
    pj_do_transform.argtypes = [c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(struct_pj_info), POINTER(struct_pj_info)]
    pj_do_transform.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 21
class struct_Key_Value(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 21
if _libs["grass_gproj.8.4"].has("pj_get_kv", "cdecl"):
    pj_get_kv = _libs["grass_gproj.8.4"].get("pj_get_kv", "cdecl")
    pj_get_kv.argtypes = [POINTER(struct_pj_info), POINTER(struct_Key_Value), POINTER(struct_Key_Value)]
    pj_get_kv.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 23
if _libs["grass_gproj.8.4"].has("pj_get_string", "cdecl"):
    pj_get_string = _libs["grass_gproj.8.4"].get("pj_get_string", "cdecl")
    pj_get_string.argtypes = [POINTER(struct_pj_info), String]
    pj_get_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 28
if _libs["grass_gproj.8.4"].has("set_proj_share", "cdecl"):
    set_proj_share = _libs["grass_gproj.8.4"].get("set_proj_share", "cdecl")
    set_proj_share.argtypes = [String]
    set_proj_share.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 29
if _libs["grass_gproj.8.4"].has("pj_print_proj_params", "cdecl"):
    pj_print_proj_params = _libs["grass_gproj.8.4"].get("pj_print_proj_params", "cdecl")
    pj_print_proj_params.argtypes = [POINTER(struct_pj_info), POINTER(struct_pj_info)]
    pj_print_proj_params.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 32
if _libs["grass_gproj.8.4"].has("GPJ_grass_to_wkt", "cdecl"):
    GPJ_grass_to_wkt = _libs["grass_gproj.8.4"].get("GPJ_grass_to_wkt", "cdecl")
    GPJ_grass_to_wkt.argtypes = [POINTER(struct_Key_Value), POINTER(struct_Key_Value), c_int, c_int]
    if sizeof(c_int) == sizeof(c_void_p):
        GPJ_grass_to_wkt.restype = ReturnString
    else:
        GPJ_grass_to_wkt.restype = String
        GPJ_grass_to_wkt.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 34
if _libs["grass_gproj.8.4"].has("GPJ_grass_to_wkt2", "cdecl"):
    GPJ_grass_to_wkt2 = _libs["grass_gproj.8.4"].get("GPJ_grass_to_wkt2", "cdecl")
    GPJ_grass_to_wkt2.argtypes = [POINTER(struct_Key_Value), POINTER(struct_Key_Value), POINTER(struct_Key_Value), c_int, c_int]
    if sizeof(c_int) == sizeof(c_void_p):
        GPJ_grass_to_wkt2.restype = ReturnString
    else:
        GPJ_grass_to_wkt2.restype = String
        GPJ_grass_to_wkt2.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 37
if _libs["grass_gproj.8.4"].has("GPJ_grass_to_osr", "cdecl"):
    GPJ_grass_to_osr = _libs["grass_gproj.8.4"].get("GPJ_grass_to_osr", "cdecl")
    GPJ_grass_to_osr.argtypes = [POINTER(struct_Key_Value), POINTER(struct_Key_Value)]
    GPJ_grass_to_osr.restype = OGRSpatialReferenceH

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 39
if _libs["grass_gproj.8.4"].has("GPJ_grass_to_osr2", "cdecl"):
    GPJ_grass_to_osr2 = _libs["grass_gproj.8.4"].get("GPJ_grass_to_osr2", "cdecl")
    GPJ_grass_to_osr2.argtypes = [POINTER(struct_Key_Value), POINTER(struct_Key_Value), POINTER(struct_Key_Value)]
    GPJ_grass_to_osr2.restype = OGRSpatialReferenceH

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 42
if _libs["grass_gproj.8.4"].has("GPJ_set_csv_loc", "cdecl"):
    GPJ_set_csv_loc = _libs["grass_gproj.8.4"].get("GPJ_set_csv_loc", "cdecl")
    GPJ_set_csv_loc.argtypes = [String]
    GPJ_set_csv_loc.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 43
class struct_Cell_head(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 43
if _libs["grass_gproj.8.4"].has("GPJ_osr_to_grass", "cdecl"):
    GPJ_osr_to_grass = _libs["grass_gproj.8.4"].get("GPJ_osr_to_grass", "cdecl")
    GPJ_osr_to_grass.argtypes = [POINTER(struct_Cell_head), POINTER(POINTER(struct_Key_Value)), POINTER(POINTER(struct_Key_Value)), OGRSpatialReferenceH, c_int]
    GPJ_osr_to_grass.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 46
if _libs["grass_gproj.8.4"].has("GPJ_wkt_to_grass", "cdecl"):
    GPJ_wkt_to_grass = _libs["grass_gproj.8.4"].get("GPJ_wkt_to_grass", "cdecl")
    GPJ_wkt_to_grass.argtypes = [POINTER(struct_Cell_head), POINTER(POINTER(struct_Key_Value)), POINTER(POINTER(struct_Key_Value)), String, c_int]
    GPJ_wkt_to_grass.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 50
if _libs["grass_gproj.8.4"].has("GPJ_get_datum_by_name", "cdecl"):
    GPJ_get_datum_by_name = _libs["grass_gproj.8.4"].get("GPJ_get_datum_by_name", "cdecl")
    GPJ_get_datum_by_name.argtypes = [String, POINTER(struct_gpj_datum)]
    GPJ_get_datum_by_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 51
if _libs["grass_gproj.8.4"].has("GPJ_get_default_datum_params_by_name", "cdecl"):
    GPJ_get_default_datum_params_by_name = _libs["grass_gproj.8.4"].get("GPJ_get_default_datum_params_by_name", "cdecl")
    GPJ_get_default_datum_params_by_name.argtypes = [String, POINTER(POINTER(c_char))]
    GPJ_get_default_datum_params_by_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 52
if _libs["grass_gproj.8.4"].has("GPJ_get_datum_params", "cdecl"):
    GPJ_get_datum_params = _libs["grass_gproj.8.4"].get("GPJ_get_datum_params", "cdecl")
    GPJ_get_datum_params.argtypes = [POINTER(POINTER(c_char)), POINTER(POINTER(c_char))]
    GPJ_get_datum_params.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 53
if _libs["grass_gproj.8.4"].has("GPJ__get_datum_params", "cdecl"):
    GPJ__get_datum_params = _libs["grass_gproj.8.4"].get("GPJ__get_datum_params", "cdecl")
    GPJ__get_datum_params.argtypes = [POINTER(struct_Key_Value), POINTER(POINTER(c_char)), POINTER(POINTER(c_char))]
    GPJ__get_datum_params.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 54
if _libs["grass_gproj.8.4"].has("GPJ_free_datum", "cdecl"):
    GPJ_free_datum = _libs["grass_gproj.8.4"].get("GPJ_free_datum", "cdecl")
    GPJ_free_datum.argtypes = [POINTER(struct_gpj_datum)]
    GPJ_free_datum.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 55
if _libs["grass_gproj.8.4"].has("GPJ_get_datum_transform_by_name", "cdecl"):
    GPJ_get_datum_transform_by_name = _libs["grass_gproj.8.4"].get("GPJ_get_datum_transform_by_name", "cdecl")
    GPJ_get_datum_transform_by_name.argtypes = [String]
    GPJ_get_datum_transform_by_name.restype = POINTER(struct_gpj_datum_transform_list)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 56
if _libs["grass_gproj.8.4"].has("GPJ_free_datum_transform", "cdecl"):
    GPJ_free_datum_transform = _libs["grass_gproj.8.4"].get("GPJ_free_datum_transform", "cdecl")
    GPJ_free_datum_transform.argtypes = [POINTER(struct_gpj_datum_transform_list)]
    GPJ_free_datum_transform.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 59
if _libs["grass_gproj.8.4"].has("GPJ_get_ellipsoid_by_name", "cdecl"):
    GPJ_get_ellipsoid_by_name = _libs["grass_gproj.8.4"].get("GPJ_get_ellipsoid_by_name", "cdecl")
    GPJ_get_ellipsoid_by_name.argtypes = [String, POINTER(struct_gpj_ellps)]
    GPJ_get_ellipsoid_by_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 60
if _libs["grass_gproj.8.4"].has("GPJ_get_ellipsoid_params", "cdecl"):
    GPJ_get_ellipsoid_params = _libs["grass_gproj.8.4"].get("GPJ_get_ellipsoid_params", "cdecl")
    GPJ_get_ellipsoid_params.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    GPJ_get_ellipsoid_params.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 61
if _libs["grass_gproj.8.4"].has("GPJ__get_ellipsoid_params", "cdecl"):
    GPJ__get_ellipsoid_params = _libs["grass_gproj.8.4"].get("GPJ__get_ellipsoid_params", "cdecl")
    GPJ__get_ellipsoid_params.argtypes = [POINTER(struct_Key_Value), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    GPJ__get_ellipsoid_params.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 63
if _libs["grass_gproj.8.4"].has("GPJ_free_ellps", "cdecl"):
    GPJ_free_ellps = _libs["grass_gproj.8.4"].get("GPJ_free_ellps", "cdecl")
    GPJ_free_ellps.argtypes = [POINTER(struct_gpj_ellps)]
    GPJ_free_ellps.restype = None

# C:/osgeo4w/include/proj.h: 176
try:
    PROJ_VERSION_MAJOR = 9
except:
    pass

# C:/osgeo4w/include/proj.h: 177
try:
    PROJ_VERSION_MINOR = 5
except:
    pass

# C:/osgeo4w/include/proj.h: 178
try:
    PROJ_VERSION_PATCH = 1
except:
    pass

# C:/osgeo4w/include/proj.h: 182
def PROJ_COMPUTE_VERSION(maj, min, patch):
    return (((maj * 10000) + (min * 100)) + patch)

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 23
try:
    RAD_TO_DEG = 57.295779513082321
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 24
try:
    DEG_TO_RAD = .017453292519943296
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 36
try:
    PROJ_VERSION_NUM = (PROJ_COMPUTE_VERSION (PROJ_VERSION_MAJOR, PROJ_VERSION_MINOR, PROJ_VERSION_PATCH))
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 43
try:
    PJ_WKT2_LATEST = PJ_WKT2_2018
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 64
try:
    ELLIPSOIDTABLE = '/etc/proj/ellipse.table'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 65
try:
    DATUMTABLE = '/etc/proj/datum.table'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 66
try:
    DATUMTRANSFORMTABLE = '/etc/proj/datumtransform.table'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 68
try:
    GRIDDIR = '/etc/proj/nad'
except:
    pass

pj_info = struct_pj_info# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 71

gpj_datum = struct_gpj_datum# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 85

gpj_datum_transform_list = struct_gpj_datum_transform_list# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 91

gpj_ellps = struct_gpj_ellps# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\gprojects.h: 107

Key_Value = struct_Key_Value# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 21

Cell_head = struct_Cell_head# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/gprojects.h: 43

# No inserted files

# No prefix-stripping

