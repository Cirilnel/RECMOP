r"""Wrapper for temporal.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_temporal.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/temporal.h -o OBJ.x86_64-w64-mingw32/temporal.py

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
_libs["grass_temporal.8.4"] = load_library("grass_temporal.8.4")

# 1 libraries
# End libraries

# No modules

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/datetime.h: 26
class struct_DateTime(Structure):
    pass

struct_DateTime.__slots__ = [
    'mode',
    'from',
    'to',
    'fracsec',
    'year',
    'month',
    'day',
    'hour',
    'minute',
    'second',
    'positive',
    'tz',
]
struct_DateTime._fields_ = [
    ('mode', c_int),
    ('from', c_int),
    ('to', c_int),
    ('fracsec', c_int),
    ('year', c_int),
    ('month', c_int),
    ('day', c_int),
    ('hour', c_int),
    ('minute', c_int),
    ('second', c_double),
    ('positive', c_int),
    ('tz', c_int),
]

DateTime = struct_DateTime# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/datetime.h: 26

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 613
class struct_TimeStamp(Structure):
    pass

struct_TimeStamp.__slots__ = [
    'dt',
    'count',
]
struct_TimeStamp._fields_ = [
    ('dt', DateTime * int(2)),
    ('count', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dbmi.h: 287
class struct__db_connection(Structure):
    pass

struct__db_connection.__slots__ = [
    'driverName',
    'hostName',
    'databaseName',
    'schemaName',
    'port',
    'user',
    'password',
    'keycol',
    'group',
]
struct__db_connection._fields_ = [
    ('driverName', String),
    ('hostName', String),
    ('databaseName', String),
    ('schemaName', String),
    ('port', String),
    ('user', String),
    ('password', String),
    ('keycol', String),
    ('group', String),
]

dbConnection = struct__db_connection# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dbmi.h: 287

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 12
if _libs["grass_temporal.8.4"].has("tgis_set_connection", "cdecl"):
    tgis_set_connection = _libs["grass_temporal.8.4"].get("tgis_set_connection", "cdecl")
    tgis_set_connection.argtypes = [POINTER(dbConnection)]
    tgis_set_connection.restype = c_int

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 13
if _libs["grass_temporal.8.4"].has("tgis_get_connection", "cdecl"):
    tgis_get_connection = _libs["grass_temporal.8.4"].get("tgis_get_connection", "cdecl")
    tgis_get_connection.argtypes = [POINTER(dbConnection)]
    tgis_get_connection.restype = c_int

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 14
if _libs["grass_temporal.8.4"].has("tgis_get_default_driver_name", "cdecl"):
    tgis_get_default_driver_name = _libs["grass_temporal.8.4"].get("tgis_get_default_driver_name", "cdecl")
    tgis_get_default_driver_name.argtypes = []
    tgis_get_default_driver_name.restype = c_char_p

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 15
if _libs["grass_temporal.8.4"].has("tgis_get_default_database_name", "cdecl"):
    tgis_get_default_database_name = _libs["grass_temporal.8.4"].get("tgis_get_default_database_name", "cdecl")
    tgis_get_default_database_name.argtypes = []
    if sizeof(c_int) == sizeof(c_void_p):
        tgis_get_default_database_name.restype = ReturnString
    else:
        tgis_get_default_database_name.restype = String
        tgis_get_default_database_name.errcheck = ReturnString

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 16
if _libs["grass_temporal.8.4"].has("tgis_get_driver_name", "cdecl"):
    tgis_get_driver_name = _libs["grass_temporal.8.4"].get("tgis_get_driver_name", "cdecl")
    tgis_get_driver_name.argtypes = []
    if sizeof(c_int) == sizeof(c_void_p):
        tgis_get_driver_name.restype = ReturnString
    else:
        tgis_get_driver_name.restype = String
        tgis_get_driver_name.errcheck = ReturnString

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 17
if _libs["grass_temporal.8.4"].has("tgis_get_database_name", "cdecl"):
    tgis_get_database_name = _libs["grass_temporal.8.4"].get("tgis_get_database_name", "cdecl")
    tgis_get_database_name.argtypes = []
    if sizeof(c_int) == sizeof(c_void_p):
        tgis_get_database_name.restype = ReturnString
    else:
        tgis_get_database_name.restype = String
        tgis_get_database_name.errcheck = ReturnString

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 18
if _libs["grass_temporal.8.4"].has("tgis_get_mapset_driver_name", "cdecl"):
    tgis_get_mapset_driver_name = _libs["grass_temporal.8.4"].get("tgis_get_mapset_driver_name", "cdecl")
    tgis_get_mapset_driver_name.argtypes = [String]
    if sizeof(c_int) == sizeof(c_void_p):
        tgis_get_mapset_driver_name.restype = ReturnString
    else:
        tgis_get_mapset_driver_name.restype = String
        tgis_get_mapset_driver_name.errcheck = ReturnString

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 19
if _libs["grass_temporal.8.4"].has("tgis_get_mapset_database_name", "cdecl"):
    tgis_get_mapset_database_name = _libs["grass_temporal.8.4"].get("tgis_get_mapset_database_name", "cdecl")
    tgis_get_mapset_database_name.argtypes = [String]
    if sizeof(c_int) == sizeof(c_void_p):
        tgis_get_mapset_database_name.restype = ReturnString
    else:
        tgis_get_mapset_database_name.restype = String
        tgis_get_mapset_database_name.errcheck = ReturnString

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 20
if _libs["grass_temporal.8.4"].has("tgis_set_default_connection", "cdecl"):
    tgis_set_default_connection = _libs["grass_temporal.8.4"].get("tgis_set_default_connection", "cdecl")
    tgis_set_default_connection.argtypes = []
    tgis_set_default_connection.restype = c_int

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 46
class struct__tgisMap(Structure):
    pass

struct__tgisMap.__slots__ = [
    'name',
    'mapset',
    'ts',
]
struct__tgisMap._fields_ = [
    ('name', String),
    ('mapset', String),
    ('ts', struct_TimeStamp),
]

tgisMap = struct__tgisMap# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 46

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 67
class struct__tgisMapList(Structure):
    pass

struct__tgisMapList.__slots__ = [
    'values',
    'n_values',
    'alloc_values',
]
struct__tgisMapList._fields_ = [
    ('values', POINTER(POINTER(tgisMap))),
    ('n_values', c_int),
    ('alloc_values', c_int),
]

tgisMapList = struct__tgisMapList# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 67

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 70
if _libs["grass_temporal.8.4"].has("tgis_init_map_list", "cdecl"):
    tgis_init_map_list = _libs["grass_temporal.8.4"].get("tgis_init_map_list", "cdecl")
    tgis_init_map_list.argtypes = [POINTER(tgisMapList)]
    tgis_init_map_list.restype = None

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 71
if _libs["grass_temporal.8.4"].has("tgis_free_map_list", "cdecl"):
    tgis_free_map_list = _libs["grass_temporal.8.4"].get("tgis_free_map_list", "cdecl")
    tgis_free_map_list.argtypes = [POINTER(tgisMapList)]
    tgis_free_map_list.restype = None

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 72
if _libs["grass_temporal.8.4"].has("tgis_new_map_list", "cdecl"):
    tgis_new_map_list = _libs["grass_temporal.8.4"].get("tgis_new_map_list", "cdecl")
    tgis_new_map_list.argtypes = []
    tgis_new_map_list.restype = POINTER(tgisMapList)

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 75
if _libs["grass_temporal.8.4"].has("tgis_map_list_insert", "cdecl"):
    tgis_map_list_insert = _libs["grass_temporal.8.4"].get("tgis_map_list_insert", "cdecl")
    tgis_map_list_insert.argtypes = [POINTER(tgisMapList), String, String, POINTER(struct_TimeStamp)]
    tgis_map_list_insert.restype = None

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 78
if _libs["grass_temporal.8.4"].has("tgis_map_list_add", "cdecl"):
    tgis_map_list_add = _libs["grass_temporal.8.4"].get("tgis_map_list_add", "cdecl")
    tgis_map_list_add.argtypes = [POINTER(tgisMapList), POINTER(tgisMap)]
    tgis_map_list_add.restype = None

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 101
class struct__tgisExtent(Structure):
    pass

struct__tgisExtent.__slots__ = [
    'start',
    'end',
    'has_end',
    'north',
    'south',
    'east',
    'west',
    'top',
    'bottom',
]
struct__tgisExtent._fields_ = [
    ('start', c_double),
    ('end', c_double),
    ('has_end', c_char),
    ('north', c_double),
    ('south', c_double),
    ('east', c_double),
    ('west', c_double),
    ('top', c_double),
    ('bottom', c_double),
]

tgisExtent = struct__tgisExtent# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 101

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 130
class struct__tgisDataset(Structure):
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 125
class struct__tgisDatasetList(Structure):
    pass

struct__tgisDatasetList.__slots__ = [
    'values',
    'n_values',
    'alloc_values',
]
struct__tgisDatasetList._fields_ = [
    ('values', POINTER(POINTER(struct__tgisDataset))),
    ('n_values', c_int),
    ('alloc_values', c_int),
]

tgisDatasetList = struct__tgisDatasetList# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 125

struct__tgisDataset.__slots__ = [
    'name',
    'mapset',
    'creator',
    'creation_time',
    'temporal_type',
    'ts',
    'extent',
    'metadata',
    'dataset_type',
    'is_stds',
    'next',
    'prev',
    'equal',
    'follows',
    'precedes',
    'overlaps',
    'overlapped',
    'during',
    'contains',
    'starts',
    'started',
    'finishes',
    'finished',
    'equivalent',
    'cover',
    'covered',
    'overlap',
    'in',
    'contain',
    'meet',
]
struct__tgisDataset._fields_ = [
    ('name', String),
    ('mapset', String),
    ('creator', String),
    ('creation_time', DateTime),
    ('temporal_type', c_char),
    ('ts', struct_TimeStamp),
    ('extent', tgisExtent),
    ('metadata', POINTER(None)),
    ('dataset_type', c_char),
    ('is_stds', c_char),
    ('next', POINTER(struct__tgisDataset)),
    ('prev', POINTER(struct__tgisDataset)),
    ('equal', tgisDatasetList),
    ('follows', tgisDatasetList),
    ('precedes', tgisDatasetList),
    ('overlaps', tgisDatasetList),
    ('overlapped', tgisDatasetList),
    ('during', tgisDatasetList),
    ('contains', tgisDatasetList),
    ('starts', tgisDatasetList),
    ('started', tgisDatasetList),
    ('finishes', tgisDatasetList),
    ('finished', tgisDatasetList),
    ('equivalent', tgisDatasetList),
    ('cover', tgisDatasetList),
    ('covered', tgisDatasetList),
    ('overlap', tgisDatasetList),
    ('in', tgisDatasetList),
    ('contain', tgisDatasetList),
    ('meet', tgisDatasetList),
]

tgisDataset = struct__tgisDataset# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 175

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 178
for _lib in _libs.values():
    if not _lib.has("tgis_init_dataset_list", "cdecl"):
        continue
    tgis_init_dataset_list = _lib.get("tgis_init_dataset_list", "cdecl")
    tgis_init_dataset_list.argtypes = [POINTER(tgisDatasetList)]
    tgis_init_dataset_list.restype = None
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 179
for _lib in _libs.values():
    if not _lib.has("tgis_free_dataset_list", "cdecl"):
        continue
    tgis_free_dataset_list = _lib.get("tgis_free_dataset_list", "cdecl")
    tgis_free_dataset_list.argtypes = [POINTER(tgisDatasetList)]
    tgis_free_dataset_list.restype = None
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 180
for _lib in _libs.values():
    if not _lib.has("tgis_new_dataset_list", "cdecl"):
        continue
    tgis_new_dataset_list = _lib.get("tgis_new_dataset_list", "cdecl")
    tgis_new_dataset_list.argtypes = []
    tgis_new_dataset_list.restype = POINTER(tgisDatasetList)
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 183
for _lib in _libs.values():
    if not _lib.has("tgis_dataset_list_insert", "cdecl"):
        continue
    tgis_dataset_list_insert = _lib.get("tgis_dataset_list_insert", "cdecl")
    tgis_dataset_list_insert.argtypes = [POINTER(tgisDatasetList), String, String, String, POINTER(DateTime), c_char, POINTER(struct_TimeStamp), POINTER(tgisExtent), POINTER(None), c_char, c_char]
    tgis_dataset_list_insert.restype = None
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 189
for _lib in _libs.values():
    if not _lib.has("tgis_dataset_list_add", "cdecl"):
        continue
    tgis_dataset_list_add = _lib.get("tgis_dataset_list_add", "cdecl")
    tgis_dataset_list_add.argtypes = [POINTER(tgisDataset)]
    tgis_dataset_list_add.restype = None
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 194
for _lib in _libs.values():
    if not _lib.has("tgis_build_topology", "cdecl"):
        continue
    tgis_build_topology = _lib.get("tgis_build_topology", "cdecl")
    tgis_build_topology.argtypes = [POINTER(tgisDatasetList), c_char]
    tgis_build_topology.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 198
for _lib in _libs.values():
    if not _lib.has("tgis_build_topology2", "cdecl"):
        continue
    tgis_build_topology2 = _lib.get("tgis_build_topology2", "cdecl")
    tgis_build_topology2.argtypes = [POINTER(tgisDatasetList), POINTER(tgisDatasetList), c_char]
    tgis_build_topology2.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 206
for _lib in _libs.values():
    if not _lib.has("tgis_create_stds", "cdecl"):
        continue
    tgis_create_stds = _lib.get("tgis_create_stds", "cdecl")
    tgis_create_stds.argtypes = [String, c_char, c_char, String, String, String, String]
    tgis_create_stds.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 211
for _lib in _libs.values():
    if not _lib.has("tgis_modify_stds", "cdecl"):
        continue
    tgis_modify_stds = _lib.get("tgis_modify_stds", "cdecl")
    tgis_modify_stds.argtypes = [String, c_char, String, String, String, String]
    tgis_modify_stds.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 217
for _lib in _libs.values():
    if not _lib.has("tgis_remove_stds", "cdecl"):
        continue
    tgis_remove_stds = _lib.get("tgis_remove_stds", "cdecl")
    tgis_remove_stds.argtypes = [String, c_char, c_char]
    tgis_remove_stds.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 220
for _lib in _libs.values():
    if not _lib.has("tgis_update_stds", "cdecl"):
        continue
    tgis_update_stds = _lib.get("tgis_update_stds", "cdecl")
    tgis_update_stds.argtypes = [String, c_char]
    tgis_update_stds.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 225
for _lib in _libs.values():
    if not _lib.has("tgis_register_map", "cdecl"):
        continue
    tgis_register_map = _lib.get("tgis_register_map", "cdecl")
    tgis_register_map.argtypes = [POINTER(tgisMap), c_char, String]
    tgis_register_map.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 229
for _lib in _libs.values():
    if not _lib.has("tgis_unregister_map", "cdecl"):
        continue
    tgis_unregister_map = _lib.get("tgis_unregister_map", "cdecl")
    tgis_unregister_map.argtypes = [POINTER(tgisMap), c_char, String]
    tgis_unregister_map.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 233
for _lib in _libs.values():
    if not _lib.has("tgis_register_maps", "cdecl"):
        continue
    tgis_register_maps = _lib.get("tgis_register_maps", "cdecl")
    tgis_register_maps.argtypes = [POINTER(tgisMapList), c_char, String]
    tgis_register_maps.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 237
for _lib in _libs.values():
    if not _lib.has("tgis_unregister_maps", "cdecl"):
        continue
    tgis_unregister_maps = _lib.get("tgis_unregister_maps", "cdecl")
    tgis_unregister_maps.argtypes = [POINTER(tgisMapList), c_char, String]
    tgis_unregister_maps.restype = c_int
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 240
for _lib in _libs.values():
    if not _lib.has("tgis_get_registered_maps", "cdecl"):
        continue
    tgis_get_registered_maps = _lib.get("tgis_get_registered_maps", "cdecl")
    tgis_get_registered_maps.argtypes = [String, String, c_char, String, String]
    tgis_get_registered_maps.restype = POINTER(tgisDatasetList)
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 246
for _lib in _libs.values():
    if not _lib.has("tgis_get_registered_stds", "cdecl"):
        continue
    tgis_get_registered_stds = _lib.get("tgis_get_registered_stds", "cdecl")
    tgis_get_registered_stds.argtypes = [String, String, c_char, c_char, String, String]
    tgis_get_registered_stds.restype = POINTER(tgisDatasetList)
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 252
for _lib in _libs.values():
    if not _lib.has("tgis_get_stds_info", "cdecl"):
        continue
    tgis_get_stds_info = _lib.get("tgis_get_stds_info", "cdecl")
    tgis_get_stds_info.argtypes = [String, String, c_char]
    tgis_get_stds_info.restype = POINTER(tgisDataset)
    break

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 8
try:
    TGISDB_DEFAULT_DRIVER = 'sqlite'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 10
try:
    TGISDB_DEFAULT_SQLITE_PATH = 'tgis/sqlite.db'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 28
try:
    TGIS_TYPE_MAP = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 29
try:
    TGIS_TYPE_STDS = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 31
try:
    TGIS_RASTER_MAP = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 32
try:
    TGIS_RASTER3D_MAP = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 33
try:
    TGIS_VECTOR_MAP = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 34
try:
    TGIS_STRDS = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 35
try:
    TGIS_STR3DS = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 36
try:
    TGIS_STVDS = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 38
try:
    TGIS_ABSOLUTE_TIME = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 39
try:
    TGIS_RELATIVE_TIME = 1
except:
    pass

_tgisMap = struct__tgisMap# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 46

_tgisMapList = struct__tgisMapList# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 67

_tgisExtent = struct__tgisExtent# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 101

_tgisDataset = struct__tgisDataset# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 130

_tgisDatasetList = struct__tgisDatasetList# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\temporal.h: 125

# No inserted files

# No prefix-stripping

