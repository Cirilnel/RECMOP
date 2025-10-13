r"""Wrapper for dbmi.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_dbmiclient.8.4 -lgrass_dbmibase.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dbmi.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h -o OBJ.x86_64-w64-mingw32/dbmi.py

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
_libs["grass_dbmiclient.8.4"] = load_library("grass_dbmiclient.8.4")
_libs["grass_dbmibase.8.4"] = load_library("grass_dbmibase.8.4")

# 2 libraries
# End libraries

# No modules

# C:/msys64/mingw64/include/stdio.h: 33
class struct__iobuf(Structure):
    pass

struct__iobuf.__slots__ = [
    '_ptr',
    '_cnt',
    '_base',
    '_flag',
    '_file',
    '_charbuf',
    '_bufsiz',
    '_tmpfname',
]
struct__iobuf._fields_ = [
    ('_ptr', String),
    ('_cnt', c_int),
    ('_base', String),
    ('_flag', c_int),
    ('_file', c_int),
    ('_charbuf', c_int),
    ('_bufsiz', c_int),
    ('_tmpfname', String),
]

FILE = struct__iobuf# C:/msys64/mingw64/include/stdio.h: 47

dbAddress = POINTER(None)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 144

dbToken = c_int# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 145

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 150
class struct__db_string(Structure):
    pass

struct__db_string.__slots__ = [
    'string',
    'nalloc',
]
struct__db_string._fields_ = [
    ('string', String),
    ('nalloc', c_int),
]

dbString = struct__db_string# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 150

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 152
class struct__dbmscap(Structure):
    pass

struct__dbmscap.__slots__ = [
    'driverName',
    'startup',
    'comment',
    'next',
]
struct__dbmscap._fields_ = [
    ('driverName', c_char * int(256)),
    ('startup', c_char * int(256)),
    ('comment', c_char * int(256)),
    ('next', POINTER(struct__dbmscap)),
]

dbDbmscap = struct__dbmscap# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 157

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 163
class struct__db_dirent(Structure):
    pass

struct__db_dirent.__slots__ = [
    'name',
    'isdir',
    'perm',
]
struct__db_dirent._fields_ = [
    ('name', dbString),
    ('isdir', c_int),
    ('perm', c_int),
]

dbDirent = struct__db_dirent# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 163

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 169
class struct__db_driver(Structure):
    pass

struct__db_driver.__slots__ = [
    'dbmscap',
    'send',
    'recv',
    'pid',
]
struct__db_driver._fields_ = [
    ('dbmscap', dbDbmscap),
    ('send', POINTER(FILE)),
    ('recv', POINTER(FILE)),
    ('pid', c_int),
]

dbDriver = struct__db_driver# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 169

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 175
class struct__db_handle(Structure):
    pass

struct__db_handle.__slots__ = [
    'dbName',
    'dbSchema',
]
struct__db_handle._fields_ = [
    ('dbName', dbString),
    ('dbSchema', dbString),
]

dbHandle = struct__db_handle# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 175

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 185
class struct__db_date_time(Structure):
    pass

struct__db_date_time.__slots__ = [
    'current',
    'year',
    'month',
    'day',
    'hour',
    'minute',
    'seconds',
]
struct__db_date_time._fields_ = [
    ('current', c_char),
    ('year', c_int),
    ('month', c_int),
    ('day', c_int),
    ('hour', c_int),
    ('minute', c_int),
    ('seconds', c_double),
]

dbDateTime = struct__db_date_time# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 185

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 193
class struct__db_value(Structure):
    pass

struct__db_value.__slots__ = [
    'isNull',
    'i',
    'd',
    's',
    't',
]
struct__db_value._fields_ = [
    ('isNull', c_char),
    ('i', c_int),
    ('d', c_double),
    ('s', dbString),
    ('t', dbDateTime),
]

dbValue = struct__db_value# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 193

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 210
class struct__db_column(Structure):
    pass

struct__db_column.__slots__ = [
    'columnName',
    'description',
    'sqlDataType',
    'hostDataType',
    'value',
    'dataLen',
    'precision',
    'scale',
    'nullAllowed',
    'hasDefaultValue',
    'useDefaultValue',
    'defaultValue',
    'select',
    'update',
]
struct__db_column._fields_ = [
    ('columnName', dbString),
    ('description', dbString),
    ('sqlDataType', c_int),
    ('hostDataType', c_int),
    ('value', dbValue),
    ('dataLen', c_int),
    ('precision', c_int),
    ('scale', c_int),
    ('nullAllowed', c_char),
    ('hasDefaultValue', c_char),
    ('useDefaultValue', c_char),
    ('defaultValue', dbValue),
    ('select', c_int),
    ('update', c_int),
]

dbColumn = struct__db_column# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 210

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 219
class struct__db_table(Structure):
    pass

struct__db_table.__slots__ = [
    'tableName',
    'description',
    'numColumns',
    'columns',
    'priv_insert',
    'priv_delete',
]
struct__db_table._fields_ = [
    ('tableName', dbString),
    ('description', dbString),
    ('numColumns', c_int),
    ('columns', POINTER(dbColumn)),
    ('priv_insert', c_int),
    ('priv_delete', c_int),
]

dbTable = struct__db_table# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 219

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 228
class struct__db_cursor(Structure):
    pass

struct__db_cursor.__slots__ = [
    'token',
    'driver',
    'table',
    'column_flags',
    'type',
    'mode',
]
struct__db_cursor._fields_ = [
    ('token', dbToken),
    ('driver', POINTER(dbDriver)),
    ('table', POINTER(dbTable)),
    ('column_flags', POINTER(c_short)),
    ('type', c_int),
    ('mode', c_int),
]

dbCursor = struct__db_cursor# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 228

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 236
class struct__db_index(Structure):
    pass

struct__db_index.__slots__ = [
    'indexName',
    'tableName',
    'numColumns',
    'columnNames',
    'unique',
]
struct__db_index._fields_ = [
    ('indexName', dbString),
    ('tableName', dbString),
    ('numColumns', c_int),
    ('columnNames', POINTER(dbString)),
    ('unique', c_char),
]

dbIndex = struct__db_index# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 236

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 244
class struct__db_driver_state(Structure):
    pass

struct__db_driver_state.__slots__ = [
    'dbname',
    'dbschema',
    'open',
    'ncursors',
    'cursor_list',
]
struct__db_driver_state._fields_ = [
    ('dbname', String),
    ('dbschema', String),
    ('open', c_int),
    ('ncursors', c_int),
    ('cursor_list', POINTER(POINTER(dbCursor))),
]

dbDriverState = struct__db_driver_state# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 244

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 250
class struct_anon_7(Structure):
    pass

struct_anon_7.__slots__ = [
    'cat',
    'val',
]
struct_anon_7._fields_ = [
    ('cat', c_int),
    ('val', c_int),
]

dbCatValI = struct_anon_7# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 250

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 256
class union_anon_8(Union):
    pass

union_anon_8.__slots__ = [
    'i',
    'd',
    's',
    't',
]
union_anon_8._fields_ = [
    ('i', c_int),
    ('d', c_double),
    ('s', POINTER(dbString)),
    ('t', POINTER(dbDateTime)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 266
class struct_anon_9(Structure):
    pass

struct_anon_9.__slots__ = [
    'cat',
    'isNull',
    'val',
]
struct_anon_9._fields_ = [
    ('cat', c_int),
    ('isNull', c_int),
    ('val', union_anon_8),
]

dbCatVal = struct_anon_9# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 266

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 274
class struct_anon_10(Structure):
    pass

struct_anon_10.__slots__ = [
    'n_values',
    'alloc',
    'ctype',
    'value',
]
struct_anon_10._fields_ = [
    ('n_values', c_int),
    ('alloc', c_int),
    ('ctype', c_int),
    ('value', POINTER(dbCatVal)),
]

dbCatValArray = struct_anon_10# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 274

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 287
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

dbConnection = struct__db_connection# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 287

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 298
class struct_anon_11(Structure):
    pass

struct_anon_11.__slots__ = [
    'count',
    'alloc',
    'table',
    'key',
    'cat',
    'where',
    'label',
]
struct_anon_11._fields_ = [
    ('count', c_int),
    ('alloc', c_int),
    ('table', String),
    ('key', String),
    ('cat', POINTER(c_int)),
    ('where', POINTER(POINTER(c_char))),
    ('label', POINTER(POINTER(c_char))),
]

dbRclsRule = struct_anon_11# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 298

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 4
if _libs["grass_dbmibase.8.4"].has("db_Cstring_to_lowercase", "cdecl"):
    db_Cstring_to_lowercase = _libs["grass_dbmibase.8.4"].get("db_Cstring_to_lowercase", "cdecl")
    db_Cstring_to_lowercase.argtypes = [String]
    db_Cstring_to_lowercase.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 5
if _libs["grass_dbmibase.8.4"].has("db_Cstring_to_uppercase", "cdecl"):
    db_Cstring_to_uppercase = _libs["grass_dbmibase.8.4"].get("db_Cstring_to_uppercase", "cdecl")
    db_Cstring_to_uppercase.argtypes = [String]
    db_Cstring_to_uppercase.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 6
if _libs["grass_dbmiclient.8.4"].has("db_add_column", "cdecl"):
    db_add_column = _libs["grass_dbmiclient.8.4"].get("db_add_column", "cdecl")
    db_add_column.argtypes = [POINTER(dbDriver), POINTER(dbString), POINTER(dbColumn)]
    db_add_column.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 7
for _lib in _libs.values():
    if not _lib.has("db__add_cursor_to_driver_state", "cdecl"):
        continue
    db__add_cursor_to_driver_state = _lib.get("db__add_cursor_to_driver_state", "cdecl")
    db__add_cursor_to_driver_state.argtypes = [POINTER(dbCursor)]
    db__add_cursor_to_driver_state.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 8
if _libs["grass_dbmibase.8.4"].has("db_alloc_cursor_column_flags", "cdecl"):
    db_alloc_cursor_column_flags = _libs["grass_dbmibase.8.4"].get("db_alloc_cursor_column_flags", "cdecl")
    db_alloc_cursor_column_flags.argtypes = [POINTER(dbCursor)]
    db_alloc_cursor_column_flags.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 9
if _libs["grass_dbmibase.8.4"].has("db_alloc_cursor_table", "cdecl"):
    db_alloc_cursor_table = _libs["grass_dbmibase.8.4"].get("db_alloc_cursor_table", "cdecl")
    db_alloc_cursor_table.argtypes = [POINTER(dbCursor), c_int]
    db_alloc_cursor_table.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 10
if _libs["grass_dbmibase.8.4"].has("db_append_table_column", "cdecl"):
    db_append_table_column = _libs["grass_dbmibase.8.4"].get("db_append_table_column", "cdecl")
    db_append_table_column.argtypes = [POINTER(dbTable), POINTER(dbColumn)]
    db_append_table_column.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 11
if _libs["grass_dbmibase.8.4"].has("db_alloc_dirent_array", "cdecl"):
    db_alloc_dirent_array = _libs["grass_dbmibase.8.4"].get("db_alloc_dirent_array", "cdecl")
    db_alloc_dirent_array.argtypes = [c_int]
    db_alloc_dirent_array.restype = POINTER(dbDirent)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 12
if _libs["grass_dbmibase.8.4"].has("db_alloc_handle_array", "cdecl"):
    db_alloc_handle_array = _libs["grass_dbmibase.8.4"].get("db_alloc_handle_array", "cdecl")
    db_alloc_handle_array.argtypes = [c_int]
    db_alloc_handle_array.restype = POINTER(dbHandle)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 13
if _libs["grass_dbmibase.8.4"].has("db_alloc_index_array", "cdecl"):
    db_alloc_index_array = _libs["grass_dbmibase.8.4"].get("db_alloc_index_array", "cdecl")
    db_alloc_index_array.argtypes = [c_int]
    db_alloc_index_array.restype = POINTER(dbIndex)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 14
if _libs["grass_dbmibase.8.4"].has("db_alloc_index_columns", "cdecl"):
    db_alloc_index_columns = _libs["grass_dbmibase.8.4"].get("db_alloc_index_columns", "cdecl")
    db_alloc_index_columns.argtypes = [POINTER(dbIndex), c_int]
    db_alloc_index_columns.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 15
if _libs["grass_dbmibase.8.4"].has("db_alloc_string_array", "cdecl"):
    db_alloc_string_array = _libs["grass_dbmibase.8.4"].get("db_alloc_string_array", "cdecl")
    db_alloc_string_array.argtypes = [c_int]
    db_alloc_string_array.restype = POINTER(dbString)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 16
if _libs["grass_dbmibase.8.4"].has("db_alloc_table", "cdecl"):
    db_alloc_table = _libs["grass_dbmibase.8.4"].get("db_alloc_table", "cdecl")
    db_alloc_table.argtypes = [c_int]
    db_alloc_table.restype = POINTER(dbTable)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 17
if _libs["grass_dbmibase.8.4"].has("db_append_string", "cdecl"):
    db_append_string = _libs["grass_dbmibase.8.4"].get("db_append_string", "cdecl")
    db_append_string.argtypes = [POINTER(dbString), String]
    db_append_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 18
if _libs["grass_dbmibase.8.4"].has("db_auto_print_errors", "cdecl"):
    db_auto_print_errors = _libs["grass_dbmibase.8.4"].get("db_auto_print_errors", "cdecl")
    db_auto_print_errors.argtypes = [c_int]
    db_auto_print_errors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 19
if _libs["grass_dbmibase.8.4"].has("db_auto_print_protocol_errors", "cdecl"):
    db_auto_print_protocol_errors = _libs["grass_dbmibase.8.4"].get("db_auto_print_protocol_errors", "cdecl")
    db_auto_print_protocol_errors.argtypes = [c_int]
    db_auto_print_protocol_errors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 20
if _libs["grass_dbmiclient.8.4"].has("db_bind_update", "cdecl"):
    db_bind_update = _libs["grass_dbmiclient.8.4"].get("db_bind_update", "cdecl")
    db_bind_update.argtypes = [POINTER(dbCursor)]
    db_bind_update.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 21
if _libs["grass_dbmibase.8.4"].has("db_calloc", "cdecl"):
    db_calloc = _libs["grass_dbmibase.8.4"].get("db_calloc", "cdecl")
    db_calloc.argtypes = [c_int, c_int]
    db_calloc.restype = POINTER(c_ubyte)
    db_calloc.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 22
if _libs["grass_dbmibase.8.4"].has("db_CatValArray_alloc", "cdecl"):
    db_CatValArray_alloc = _libs["grass_dbmibase.8.4"].get("db_CatValArray_alloc", "cdecl")
    db_CatValArray_alloc.argtypes = [POINTER(dbCatValArray), c_int]
    db_CatValArray_alloc.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 23
if _libs["grass_dbmibase.8.4"].has("db_CatValArray_realloc", "cdecl"):
    db_CatValArray_realloc = _libs["grass_dbmibase.8.4"].get("db_CatValArray_realloc", "cdecl")
    db_CatValArray_realloc.argtypes = [POINTER(dbCatValArray), c_int]
    db_CatValArray_realloc.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 24
if _libs["grass_dbmibase.8.4"].has("db_CatValArray_free", "cdecl"):
    db_CatValArray_free = _libs["grass_dbmibase.8.4"].get("db_CatValArray_free", "cdecl")
    db_CatValArray_free.argtypes = [POINTER(dbCatValArray)]
    db_CatValArray_free.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 25
if _libs["grass_dbmibase.8.4"].has("db_CatValArray_init", "cdecl"):
    db_CatValArray_init = _libs["grass_dbmibase.8.4"].get("db_CatValArray_init", "cdecl")
    db_CatValArray_init.argtypes = [POINTER(dbCatValArray)]
    db_CatValArray_init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 26
if _libs["grass_dbmiclient.8.4"].has("db_CatValArray_sort", "cdecl"):
    db_CatValArray_sort = _libs["grass_dbmiclient.8.4"].get("db_CatValArray_sort", "cdecl")
    db_CatValArray_sort.argtypes = [POINTER(dbCatValArray)]
    db_CatValArray_sort.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 27
if _libs["grass_dbmiclient.8.4"].has("db_CatValArray_sort_by_value", "cdecl"):
    db_CatValArray_sort_by_value = _libs["grass_dbmiclient.8.4"].get("db_CatValArray_sort_by_value", "cdecl")
    db_CatValArray_sort_by_value.argtypes = [POINTER(dbCatValArray)]
    db_CatValArray_sort_by_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 28
if _libs["grass_dbmiclient.8.4"].has("db_CatValArray_get_value", "cdecl"):
    db_CatValArray_get_value = _libs["grass_dbmiclient.8.4"].get("db_CatValArray_get_value", "cdecl")
    db_CatValArray_get_value.argtypes = [POINTER(dbCatValArray), c_int, POINTER(POINTER(dbCatVal))]
    db_CatValArray_get_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 29
if _libs["grass_dbmiclient.8.4"].has("db_CatValArray_get_value_int", "cdecl"):
    db_CatValArray_get_value_int = _libs["grass_dbmiclient.8.4"].get("db_CatValArray_get_value_int", "cdecl")
    db_CatValArray_get_value_int.argtypes = [POINTER(dbCatValArray), c_int, POINTER(c_int)]
    db_CatValArray_get_value_int.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 30
if _libs["grass_dbmiclient.8.4"].has("db_CatValArray_get_value_double", "cdecl"):
    db_CatValArray_get_value_double = _libs["grass_dbmiclient.8.4"].get("db_CatValArray_get_value_double", "cdecl")
    db_CatValArray_get_value_double.argtypes = [POINTER(dbCatValArray), c_int, POINTER(c_double)]
    db_CatValArray_get_value_double.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 31
if _libs["grass_dbmibase.8.4"].has("db_char_to_lowercase", "cdecl"):
    db_char_to_lowercase = _libs["grass_dbmibase.8.4"].get("db_char_to_lowercase", "cdecl")
    db_char_to_lowercase.argtypes = [String]
    db_char_to_lowercase.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 32
if _libs["grass_dbmibase.8.4"].has("db_char_to_uppercase", "cdecl"):
    db_char_to_uppercase = _libs["grass_dbmibase.8.4"].get("db_char_to_uppercase", "cdecl")
    db_char_to_uppercase.argtypes = [String]
    db_char_to_uppercase.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 33
if _libs["grass_dbmibase.8.4"].has("db_clear_error", "cdecl"):
    db_clear_error = _libs["grass_dbmibase.8.4"].get("db_clear_error", "cdecl")
    db_clear_error.argtypes = []
    db_clear_error.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 34
if _libs["grass_dbmibase.8.4"].has("db_clone_table", "cdecl"):
    db_clone_table = _libs["grass_dbmibase.8.4"].get("db_clone_table", "cdecl")
    db_clone_table.argtypes = [POINTER(dbTable)]
    db_clone_table.restype = POINTER(dbTable)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 35
for _lib in _libs.values():
    if not _lib.has("db__close_all_cursors", "cdecl"):
        continue
    db__close_all_cursors = _lib.get("db__close_all_cursors", "cdecl")
    db__close_all_cursors.argtypes = []
    db__close_all_cursors.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 36
if _libs["grass_dbmiclient.8.4"].has("db_close_cursor", "cdecl"):
    db_close_cursor = _libs["grass_dbmiclient.8.4"].get("db_close_cursor", "cdecl")
    db_close_cursor.argtypes = [POINTER(dbCursor)]
    db_close_cursor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 37
if _libs["grass_dbmiclient.8.4"].has("db_close_database", "cdecl"):
    db_close_database = _libs["grass_dbmiclient.8.4"].get("db_close_database", "cdecl")
    db_close_database.argtypes = [POINTER(dbDriver)]
    db_close_database.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 38
if _libs["grass_dbmiclient.8.4"].has("db_close_database_shutdown_driver", "cdecl"):
    db_close_database_shutdown_driver = _libs["grass_dbmiclient.8.4"].get("db_close_database_shutdown_driver", "cdecl")
    db_close_database_shutdown_driver.argtypes = [POINTER(dbDriver)]
    db_close_database_shutdown_driver.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 39
if _libs["grass_dbmiclient.8.4"].has("db_column_sqltype", "cdecl"):
    db_column_sqltype = _libs["grass_dbmiclient.8.4"].get("db_column_sqltype", "cdecl")
    db_column_sqltype.argtypes = [POINTER(dbDriver), String, String]
    db_column_sqltype.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 40
if _libs["grass_dbmiclient.8.4"].has("db_column_Ctype", "cdecl"):
    db_column_Ctype = _libs["grass_dbmiclient.8.4"].get("db_column_Ctype", "cdecl")
    db_column_Ctype.argtypes = [POINTER(dbDriver), String, String]
    db_column_Ctype.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 41
if _libs["grass_dbmibase.8.4"].has("db_convert_Cstring_to_column_default_value", "cdecl"):
    db_convert_Cstring_to_column_default_value = _libs["grass_dbmibase.8.4"].get("db_convert_Cstring_to_column_default_value", "cdecl")
    db_convert_Cstring_to_column_default_value.argtypes = [String, POINTER(dbColumn)]
    db_convert_Cstring_to_column_default_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 42
if _libs["grass_dbmibase.8.4"].has("db_convert_Cstring_to_column_value", "cdecl"):
    db_convert_Cstring_to_column_value = _libs["grass_dbmibase.8.4"].get("db_convert_Cstring_to_column_value", "cdecl")
    db_convert_Cstring_to_column_value.argtypes = [String, POINTER(dbColumn)]
    db_convert_Cstring_to_column_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 43
if _libs["grass_dbmibase.8.4"].has("db_convert_Cstring_to_value", "cdecl"):
    db_convert_Cstring_to_value = _libs["grass_dbmibase.8.4"].get("db_convert_Cstring_to_value", "cdecl")
    db_convert_Cstring_to_value.argtypes = [String, c_int, POINTER(dbValue)]
    db_convert_Cstring_to_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 44
if _libs["grass_dbmibase.8.4"].has("db_convert_Cstring_to_value_datetime", "cdecl"):
    db_convert_Cstring_to_value_datetime = _libs["grass_dbmibase.8.4"].get("db_convert_Cstring_to_value_datetime", "cdecl")
    db_convert_Cstring_to_value_datetime.argtypes = [String, c_int, POINTER(dbValue)]
    db_convert_Cstring_to_value_datetime.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 45
if _libs["grass_dbmibase.8.4"].has("db_convert_column_default_value_to_string", "cdecl"):
    db_convert_column_default_value_to_string = _libs["grass_dbmibase.8.4"].get("db_convert_column_default_value_to_string", "cdecl")
    db_convert_column_default_value_to_string.argtypes = [POINTER(dbColumn), POINTER(dbString)]
    db_convert_column_default_value_to_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 46
if _libs["grass_dbmibase.8.4"].has("db_convert_column_value_to_string", "cdecl"):
    db_convert_column_value_to_string = _libs["grass_dbmibase.8.4"].get("db_convert_column_value_to_string", "cdecl")
    db_convert_column_value_to_string.argtypes = [POINTER(dbColumn), POINTER(dbString)]
    db_convert_column_value_to_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 47
if _libs["grass_dbmibase.8.4"].has("db_convert_value_datetime_into_string", "cdecl"):
    db_convert_value_datetime_into_string = _libs["grass_dbmibase.8.4"].get("db_convert_value_datetime_into_string", "cdecl")
    db_convert_value_datetime_into_string.argtypes = [POINTER(dbValue), c_int, POINTER(dbString)]
    db_convert_value_datetime_into_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 48
if _libs["grass_dbmibase.8.4"].has("db_convert_value_to_string", "cdecl"):
    db_convert_value_to_string = _libs["grass_dbmibase.8.4"].get("db_convert_value_to_string", "cdecl")
    db_convert_value_to_string.argtypes = [POINTER(dbValue), c_int, POINTER(dbString)]
    db_convert_value_to_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 49
if _libs["grass_dbmibase.8.4"].has("db_copy_column", "cdecl"):
    db_copy_column = _libs["grass_dbmibase.8.4"].get("db_copy_column", "cdecl")
    db_copy_column.argtypes = [POINTER(dbColumn), POINTER(dbColumn)]
    db_copy_column.restype = POINTER(dbColumn)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 50
if _libs["grass_dbmibase.8.4"].has("db_copy_dbmscap_entry", "cdecl"):
    db_copy_dbmscap_entry = _libs["grass_dbmibase.8.4"].get("db_copy_dbmscap_entry", "cdecl")
    db_copy_dbmscap_entry.argtypes = [POINTER(dbDbmscap), POINTER(dbDbmscap)]
    db_copy_dbmscap_entry.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 51
if _libs["grass_dbmibase.8.4"].has("db_copy_string", "cdecl"):
    db_copy_string = _libs["grass_dbmibase.8.4"].get("db_copy_string", "cdecl")
    db_copy_string.argtypes = [POINTER(dbString), POINTER(dbString)]
    db_copy_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 52
if _libs["grass_dbmibase.8.4"].has("db_table_to_sql", "cdecl"):
    db_table_to_sql = _libs["grass_dbmibase.8.4"].get("db_table_to_sql", "cdecl")
    db_table_to_sql.argtypes = [POINTER(dbTable), POINTER(dbString)]
    db_table_to_sql.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 53
if _libs["grass_dbmiclient.8.4"].has("db_copy_table", "cdecl"):
    db_copy_table = _libs["grass_dbmiclient.8.4"].get("db_copy_table", "cdecl")
    db_copy_table.argtypes = [String, String, String, String, String, String]
    db_copy_table.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 55
if _libs["grass_dbmiclient.8.4"].has("db_copy_table_where", "cdecl"):
    db_copy_table_where = _libs["grass_dbmiclient.8.4"].get("db_copy_table_where", "cdecl")
    db_copy_table_where.argtypes = [String, String, String, String, String, String, String]
    db_copy_table_where.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 57
if _libs["grass_dbmiclient.8.4"].has("db_copy_table_select", "cdecl"):
    db_copy_table_select = _libs["grass_dbmiclient.8.4"].get("db_copy_table_select", "cdecl")
    db_copy_table_select.argtypes = [String, String, String, String, String, String, String]
    db_copy_table_select.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 59
if _libs["grass_dbmiclient.8.4"].has("db_copy_table_by_ints", "cdecl"):
    db_copy_table_by_ints = _libs["grass_dbmiclient.8.4"].get("db_copy_table_by_ints", "cdecl")
    db_copy_table_by_ints.argtypes = [String, String, String, String, String, String, String, POINTER(c_int), c_int]
    db_copy_table_by_ints.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 62
if _libs["grass_dbmibase.8.4"].has("db_copy_value", "cdecl"):
    db_copy_value = _libs["grass_dbmibase.8.4"].get("db_copy_value", "cdecl")
    db_copy_value.argtypes = [POINTER(dbValue), POINTER(dbValue)]
    db_copy_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 63
if _libs["grass_dbmiclient.8.4"].has("db_create_database", "cdecl"):
    db_create_database = _libs["grass_dbmiclient.8.4"].get("db_create_database", "cdecl")
    db_create_database.argtypes = [POINTER(dbDriver), POINTER(dbHandle)]
    db_create_database.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 64
if _libs["grass_dbmiclient.8.4"].has("db_create_index", "cdecl"):
    db_create_index = _libs["grass_dbmiclient.8.4"].get("db_create_index", "cdecl")
    db_create_index.argtypes = [POINTER(dbDriver), POINTER(dbIndex)]
    db_create_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 65
if _libs["grass_dbmiclient.8.4"].has("db_create_index2", "cdecl"):
    db_create_index2 = _libs["grass_dbmiclient.8.4"].get("db_create_index2", "cdecl")
    db_create_index2.argtypes = [POINTER(dbDriver), String, String]
    db_create_index2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 66
if _libs["grass_dbmiclient.8.4"].has("db_create_table", "cdecl"):
    db_create_table = _libs["grass_dbmiclient.8.4"].get("db_create_table", "cdecl")
    db_create_table.argtypes = [POINTER(dbDriver), POINTER(dbTable)]
    db_create_table.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 67
for _lib in _libs.values():
    if not _lib.has("db_d_add_column", "cdecl"):
        continue
    db_d_add_column = _lib.get("db_d_add_column", "cdecl")
    db_d_add_column.argtypes = []
    db_d_add_column.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 68
for _lib in _libs.values():
    if not _lib.has("db_d_bind_update", "cdecl"):
        continue
    db_d_bind_update = _lib.get("db_d_bind_update", "cdecl")
    db_d_bind_update.argtypes = []
    db_d_bind_update.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 69
if _libs["grass_dbmibase.8.4"].has("db_dbmscap_filename", "cdecl"):
    db_dbmscap_filename = _libs["grass_dbmibase.8.4"].get("db_dbmscap_filename", "cdecl")
    db_dbmscap_filename.argtypes = []
    db_dbmscap_filename.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 70
for _lib in _libs.values():
    if not _lib.has("db_d_close_cursor", "cdecl"):
        continue
    db_d_close_cursor = _lib.get("db_d_close_cursor", "cdecl")
    db_d_close_cursor.argtypes = []
    db_d_close_cursor.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 71
for _lib in _libs.values():
    if not _lib.has("db_d_close_database", "cdecl"):
        continue
    db_d_close_database = _lib.get("db_d_close_database", "cdecl")
    db_d_close_database.argtypes = []
    db_d_close_database.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 72
for _lib in _libs.values():
    if not _lib.has("db_d_create_database", "cdecl"):
        continue
    db_d_create_database = _lib.get("db_d_create_database", "cdecl")
    db_d_create_database.argtypes = []
    db_d_create_database.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 73
for _lib in _libs.values():
    if not _lib.has("db_d_create_index", "cdecl"):
        continue
    db_d_create_index = _lib.get("db_d_create_index", "cdecl")
    db_d_create_index.argtypes = []
    db_d_create_index.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 74
for _lib in _libs.values():
    if not _lib.has("db_d_create_table", "cdecl"):
        continue
    db_d_create_table = _lib.get("db_d_create_table", "cdecl")
    db_d_create_table.argtypes = []
    db_d_create_table.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 75
for _lib in _libs.values():
    if not _lib.has("db_d_delete", "cdecl"):
        continue
    db_d_delete = _lib.get("db_d_delete", "cdecl")
    db_d_delete.argtypes = []
    db_d_delete.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 76
for _lib in _libs.values():
    if not _lib.has("db_d_delete_database", "cdecl"):
        continue
    db_d_delete_database = _lib.get("db_d_delete_database", "cdecl")
    db_d_delete_database.argtypes = []
    db_d_delete_database.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 77
for _lib in _libs.values():
    if not _lib.has("db_d_describe_table", "cdecl"):
        continue
    db_d_describe_table = _lib.get("db_d_describe_table", "cdecl")
    db_d_describe_table.argtypes = []
    db_d_describe_table.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 78
for _lib in _libs.values():
    if not _lib.has("db_d_drop_column", "cdecl"):
        continue
    db_d_drop_column = _lib.get("db_d_drop_column", "cdecl")
    db_d_drop_column.argtypes = []
    db_d_drop_column.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 79
for _lib in _libs.values():
    if not _lib.has("db_d_drop_index", "cdecl"):
        continue
    db_d_drop_index = _lib.get("db_d_drop_index", "cdecl")
    db_d_drop_index.argtypes = []
    db_d_drop_index.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 80
for _lib in _libs.values():
    if not _lib.has("db_d_drop_table", "cdecl"):
        continue
    db_d_drop_table = _lib.get("db_d_drop_table", "cdecl")
    db_d_drop_table.argtypes = []
    db_d_drop_table.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 81
if _libs["grass_dbmibase.8.4"].has("db_debug", "cdecl"):
    db_debug = _libs["grass_dbmibase.8.4"].get("db_debug", "cdecl")
    db_debug.argtypes = [String]
    db_debug.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 82
if _libs["grass_dbmibase.8.4"].has("db_debug_off", "cdecl"):
    db_debug_off = _libs["grass_dbmibase.8.4"].get("db_debug_off", "cdecl")
    db_debug_off.argtypes = []
    db_debug_off.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 83
if _libs["grass_dbmibase.8.4"].has("db_debug_on", "cdecl"):
    db_debug_on = _libs["grass_dbmibase.8.4"].get("db_debug_on", "cdecl")
    db_debug_on.argtypes = []
    db_debug_on.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 84
if _libs["grass_dbmiclient.8.4"].has("db_delete", "cdecl"):
    db_delete = _libs["grass_dbmiclient.8.4"].get("db_delete", "cdecl")
    db_delete.argtypes = [POINTER(dbCursor)]
    db_delete.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 85
if _libs["grass_dbmiclient.8.4"].has("db_delete_database", "cdecl"):
    db_delete_database = _libs["grass_dbmiclient.8.4"].get("db_delete_database", "cdecl")
    db_delete_database.argtypes = [POINTER(dbDriver), POINTER(dbHandle)]
    db_delete_database.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 86
if _libs["grass_dbmiclient.8.4"].has("db_delete_table", "cdecl"):
    db_delete_table = _libs["grass_dbmiclient.8.4"].get("db_delete_table", "cdecl")
    db_delete_table.argtypes = [String, String, String]
    db_delete_table.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 87
if _libs["grass_dbmiclient.8.4"].has("db_describe_table", "cdecl"):
    db_describe_table = _libs["grass_dbmiclient.8.4"].get("db_describe_table", "cdecl")
    db_describe_table.argtypes = [POINTER(dbDriver), POINTER(dbString), POINTER(POINTER(dbTable))]
    db_describe_table.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 88
for _lib in _libs.values():
    if not _lib.has("db_d_execute_immediate", "cdecl"):
        continue
    db_d_execute_immediate = _lib.get("db_d_execute_immediate", "cdecl")
    db_d_execute_immediate.argtypes = []
    db_d_execute_immediate.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 89
for _lib in _libs.values():
    if not _lib.has("db_d_begin_transaction", "cdecl"):
        continue
    db_d_begin_transaction = _lib.get("db_d_begin_transaction", "cdecl")
    db_d_begin_transaction.argtypes = []
    db_d_begin_transaction.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 90
for _lib in _libs.values():
    if not _lib.has("db_d_commit_transaction", "cdecl"):
        continue
    db_d_commit_transaction = _lib.get("db_d_commit_transaction", "cdecl")
    db_d_commit_transaction.argtypes = []
    db_d_commit_transaction.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 91
for _lib in _libs.values():
    if not _lib.has("db_d_fetch", "cdecl"):
        continue
    db_d_fetch = _lib.get("db_d_fetch", "cdecl")
    db_d_fetch.argtypes = []
    db_d_fetch.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 92
for _lib in _libs.values():
    if not _lib.has("db_d_find_database", "cdecl"):
        continue
    db_d_find_database = _lib.get("db_d_find_database", "cdecl")
    db_d_find_database.argtypes = []
    db_d_find_database.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 93
for _lib in _libs.values():
    if not _lib.has("db_d_get_num_rows", "cdecl"):
        continue
    db_d_get_num_rows = _lib.get("db_d_get_num_rows", "cdecl")
    db_d_get_num_rows.argtypes = []
    db_d_get_num_rows.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 94
for _lib in _libs.values():
    if not _lib.has("db_d_grant_on_table", "cdecl"):
        continue
    db_d_grant_on_table = _lib.get("db_d_grant_on_table", "cdecl")
    db_d_grant_on_table.argtypes = []
    db_d_grant_on_table.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 95
for _lib in _libs.values():
    if not _lib.has("db_d_insert", "cdecl"):
        continue
    db_d_insert = _lib.get("db_d_insert", "cdecl")
    db_d_insert.argtypes = []
    db_d_insert.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 96
for _lib in _libs.values():
    if not _lib.has("db_d_init_error", "cdecl"):
        continue
    db_d_init_error = _lib.get("db_d_init_error", "cdecl")
    db_d_init_error.argtypes = [String]
    db_d_init_error.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 97
for _lib in _libs.values():
    if _lib.has("db_d_append_error", "cdecl"):
        _func = _lib.get("db_d_append_error", "cdecl")
        _restype = None
        _errcheck = None
        _argtypes = [String]
        db_d_append_error = _variadic_function(_func,_restype,_argtypes,_errcheck)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 98
for _lib in _libs.values():
    if not _lib.has("db_d_report_error", "cdecl"):
        continue
    db_d_report_error = _lib.get("db_d_report_error", "cdecl")
    db_d_report_error.argtypes = []
    db_d_report_error.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 99
if _libs["grass_dbmibase.8.4"].has("db_dirent", "cdecl"):
    db_dirent = _libs["grass_dbmibase.8.4"].get("db_dirent", "cdecl")
    db_dirent.argtypes = [String, POINTER(c_int)]
    db_dirent.restype = POINTER(dbDirent)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 100
for _lib in _libs.values():
    if not _lib.has("db_d_list_databases", "cdecl"):
        continue
    db_d_list_databases = _lib.get("db_d_list_databases", "cdecl")
    db_d_list_databases.argtypes = []
    db_d_list_databases.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 101
for _lib in _libs.values():
    if not _lib.has("db_d_list_indexes", "cdecl"):
        continue
    db_d_list_indexes = _lib.get("db_d_list_indexes", "cdecl")
    db_d_list_indexes.argtypes = []
    db_d_list_indexes.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 102
for _lib in _libs.values():
    if not _lib.has("db_d_list_tables", "cdecl"):
        continue
    db_d_list_tables = _lib.get("db_d_list_tables", "cdecl")
    db_d_list_tables.argtypes = []
    db_d_list_tables.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 103
for _lib in _libs.values():
    if not _lib.has("db_d_open_database", "cdecl"):
        continue
    db_d_open_database = _lib.get("db_d_open_database", "cdecl")
    db_d_open_database.argtypes = []
    db_d_open_database.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 104
for _lib in _libs.values():
    if not _lib.has("db_d_open_insert_cursor", "cdecl"):
        continue
    db_d_open_insert_cursor = _lib.get("db_d_open_insert_cursor", "cdecl")
    db_d_open_insert_cursor.argtypes = []
    db_d_open_insert_cursor.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 105
for _lib in _libs.values():
    if not _lib.has("db_d_open_select_cursor", "cdecl"):
        continue
    db_d_open_select_cursor = _lib.get("db_d_open_select_cursor", "cdecl")
    db_d_open_select_cursor.argtypes = []
    db_d_open_select_cursor.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 106
for _lib in _libs.values():
    if not _lib.has("db_d_open_update_cursor", "cdecl"):
        continue
    db_d_open_update_cursor = _lib.get("db_d_open_update_cursor", "cdecl")
    db_d_open_update_cursor.argtypes = []
    db_d_open_update_cursor.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 107
if _libs["grass_dbmibase.8.4"].has("db_double_quote_string", "cdecl"):
    db_double_quote_string = _libs["grass_dbmibase.8.4"].get("db_double_quote_string", "cdecl")
    db_double_quote_string.argtypes = [POINTER(dbString)]
    db_double_quote_string.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 108
for _lib in _libs.values():
    if not _lib.has("db_driver", "cdecl"):
        continue
    db_driver = _lib.get("db_driver", "cdecl")
    db_driver.argtypes = [c_int, POINTER(POINTER(c_char))]
    db_driver.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 110
for _lib in _libs.values():
    if not _lib.has("db_driver_mkdir", "cdecl"):
        continue
    db_driver_mkdir = _lib.get("db_driver_mkdir", "cdecl")
    db_driver_mkdir.argtypes = [String, c_int, c_int]
    db_driver_mkdir.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 111
if _libs["grass_dbmiclient.8.4"].has("db_drop_column", "cdecl"):
    db_drop_column = _libs["grass_dbmiclient.8.4"].get("db_drop_column", "cdecl")
    db_drop_column.argtypes = [POINTER(dbDriver), POINTER(dbString), POINTER(dbString)]
    db_drop_column.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 112
for _lib in _libs.values():
    if not _lib.has("db__drop_cursor_from_driver_state", "cdecl"):
        continue
    db__drop_cursor_from_driver_state = _lib.get("db__drop_cursor_from_driver_state", "cdecl")
    db__drop_cursor_from_driver_state.argtypes = [POINTER(dbCursor)]
    db__drop_cursor_from_driver_state.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 113
if _libs["grass_dbmiclient.8.4"].has("db_drop_index", "cdecl"):
    db_drop_index = _libs["grass_dbmiclient.8.4"].get("db_drop_index", "cdecl")
    db_drop_index.argtypes = [POINTER(dbDriver), POINTER(dbString)]
    db_drop_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 114
if _libs["grass_dbmiclient.8.4"].has("db_drop_table", "cdecl"):
    db_drop_table = _libs["grass_dbmiclient.8.4"].get("db_drop_table", "cdecl")
    db_drop_table.argtypes = [POINTER(dbDriver), POINTER(dbString)]
    db_drop_table.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 115
if _libs["grass_dbmibase.8.4"].has("db_drop_token", "cdecl"):
    db_drop_token = _libs["grass_dbmibase.8.4"].get("db_drop_token", "cdecl")
    db_drop_token.argtypes = [dbToken]
    db_drop_token.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 116
for _lib in _libs.values():
    if not _lib.has("db_d_update", "cdecl"):
        continue
    db_d_update = _lib.get("db_d_update", "cdecl")
    db_d_update.argtypes = []
    db_d_update.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 117
for _lib in _libs.values():
    if not _lib.has("db_d_version", "cdecl"):
        continue
    db_d_version = _lib.get("db_d_version", "cdecl")
    db_d_version.argtypes = []
    db_d_version.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 118
if _libs["grass_dbmibase.8.4"].has("db_enlarge_string", "cdecl"):
    db_enlarge_string = _libs["grass_dbmibase.8.4"].get("db_enlarge_string", "cdecl")
    db_enlarge_string.argtypes = [POINTER(dbString), c_int]
    db_enlarge_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 119
if _libs["grass_dbmibase.8.4"].has("db_error", "cdecl"):
    db_error = _libs["grass_dbmibase.8.4"].get("db_error", "cdecl")
    db_error.argtypes = [String]
    db_error.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 120
if _libs["grass_dbmiclient.8.4"].has("db_execute_immediate", "cdecl"):
    db_execute_immediate = _libs["grass_dbmiclient.8.4"].get("db_execute_immediate", "cdecl")
    db_execute_immediate.argtypes = [POINTER(dbDriver), POINTER(dbString)]
    db_execute_immediate.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 121
if _libs["grass_dbmiclient.8.4"].has("db_begin_transaction", "cdecl"):
    db_begin_transaction = _libs["grass_dbmiclient.8.4"].get("db_begin_transaction", "cdecl")
    db_begin_transaction.argtypes = [POINTER(dbDriver)]
    db_begin_transaction.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 122
if _libs["grass_dbmiclient.8.4"].has("db_commit_transaction", "cdecl"):
    db_commit_transaction = _libs["grass_dbmiclient.8.4"].get("db_commit_transaction", "cdecl")
    db_commit_transaction.argtypes = [POINTER(dbDriver)]
    db_commit_transaction.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 123
if _libs["grass_dbmiclient.8.4"].has("db_fetch", "cdecl"):
    db_fetch = _libs["grass_dbmiclient.8.4"].get("db_fetch", "cdecl")
    db_fetch.argtypes = [POINTER(dbCursor), c_int, POINTER(c_int)]
    db_fetch.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 124
if _libs["grass_dbmiclient.8.4"].has("db_find_database", "cdecl"):
    db_find_database = _libs["grass_dbmiclient.8.4"].get("db_find_database", "cdecl")
    db_find_database.argtypes = [POINTER(dbDriver), POINTER(dbHandle), POINTER(c_int)]
    db_find_database.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 125
if _libs["grass_dbmibase.8.4"].has("db_find_token", "cdecl"):
    db_find_token = _libs["grass_dbmibase.8.4"].get("db_find_token", "cdecl")
    db_find_token.argtypes = [dbToken]
    db_find_token.restype = dbAddress

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 126
if _libs["grass_dbmibase.8.4"].has("db_free", "cdecl"):
    db_free = _libs["grass_dbmibase.8.4"].get("db_free", "cdecl")
    db_free.argtypes = [POINTER(None)]
    db_free.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 127
if _libs["grass_dbmibase.8.4"].has("db_free_column", "cdecl"):
    db_free_column = _libs["grass_dbmibase.8.4"].get("db_free_column", "cdecl")
    db_free_column.argtypes = [POINTER(dbColumn)]
    db_free_column.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 128
if _libs["grass_dbmibase.8.4"].has("db_free_cursor", "cdecl"):
    db_free_cursor = _libs["grass_dbmibase.8.4"].get("db_free_cursor", "cdecl")
    db_free_cursor.argtypes = [POINTER(dbCursor)]
    db_free_cursor.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 129
if _libs["grass_dbmibase.8.4"].has("db_free_cursor_column_flags", "cdecl"):
    db_free_cursor_column_flags = _libs["grass_dbmibase.8.4"].get("db_free_cursor_column_flags", "cdecl")
    db_free_cursor_column_flags.argtypes = [POINTER(dbCursor)]
    db_free_cursor_column_flags.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 130
if _libs["grass_dbmibase.8.4"].has("db_free_dbmscap", "cdecl"):
    db_free_dbmscap = _libs["grass_dbmibase.8.4"].get("db_free_dbmscap", "cdecl")
    db_free_dbmscap.argtypes = [POINTER(dbDbmscap)]
    db_free_dbmscap.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 131
if _libs["grass_dbmibase.8.4"].has("db_free_dirent_array", "cdecl"):
    db_free_dirent_array = _libs["grass_dbmibase.8.4"].get("db_free_dirent_array", "cdecl")
    db_free_dirent_array.argtypes = [POINTER(dbDirent), c_int]
    db_free_dirent_array.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 132
if _libs["grass_dbmibase.8.4"].has("db_free_handle", "cdecl"):
    db_free_handle = _libs["grass_dbmibase.8.4"].get("db_free_handle", "cdecl")
    db_free_handle.argtypes = [POINTER(dbHandle)]
    db_free_handle.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 133
if _libs["grass_dbmibase.8.4"].has("db_free_handle_array", "cdecl"):
    db_free_handle_array = _libs["grass_dbmibase.8.4"].get("db_free_handle_array", "cdecl")
    db_free_handle_array.argtypes = [POINTER(dbHandle), c_int]
    db_free_handle_array.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 134
if _libs["grass_dbmibase.8.4"].has("db_free_index", "cdecl"):
    db_free_index = _libs["grass_dbmibase.8.4"].get("db_free_index", "cdecl")
    db_free_index.argtypes = [POINTER(dbIndex)]
    db_free_index.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 135
if _libs["grass_dbmibase.8.4"].has("db_free_index_array", "cdecl"):
    db_free_index_array = _libs["grass_dbmibase.8.4"].get("db_free_index_array", "cdecl")
    db_free_index_array.argtypes = [POINTER(dbIndex), c_int]
    db_free_index_array.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 136
if _libs["grass_dbmibase.8.4"].has("db_free_string", "cdecl"):
    db_free_string = _libs["grass_dbmibase.8.4"].get("db_free_string", "cdecl")
    db_free_string.argtypes = [POINTER(dbString)]
    db_free_string.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 137
if _libs["grass_dbmibase.8.4"].has("db_free_string_array", "cdecl"):
    db_free_string_array = _libs["grass_dbmibase.8.4"].get("db_free_string_array", "cdecl")
    db_free_string_array.argtypes = [POINTER(dbString), c_int]
    db_free_string_array.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 138
if _libs["grass_dbmibase.8.4"].has("db_free_table", "cdecl"):
    db_free_table = _libs["grass_dbmibase.8.4"].get("db_free_table", "cdecl")
    db_free_table.argtypes = [POINTER(dbTable)]
    db_free_table.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 139
if _libs["grass_dbmiclient.8.4"].has("db_get_column", "cdecl"):
    db_get_column = _libs["grass_dbmiclient.8.4"].get("db_get_column", "cdecl")
    db_get_column.argtypes = [POINTER(dbDriver), String, String, POINTER(POINTER(dbColumn))]
    db_get_column.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 140
if _libs["grass_dbmibase.8.4"].has("db_get_column_default_value", "cdecl"):
    db_get_column_default_value = _libs["grass_dbmibase.8.4"].get("db_get_column_default_value", "cdecl")
    db_get_column_default_value.argtypes = [POINTER(dbColumn)]
    db_get_column_default_value.restype = POINTER(dbValue)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 141
if _libs["grass_dbmibase.8.4"].has("db_get_column_description", "cdecl"):
    db_get_column_description = _libs["grass_dbmibase.8.4"].get("db_get_column_description", "cdecl")
    db_get_column_description.argtypes = [POINTER(dbColumn)]
    db_get_column_description.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 142
if _libs["grass_dbmibase.8.4"].has("db_get_column_host_type", "cdecl"):
    db_get_column_host_type = _libs["grass_dbmibase.8.4"].get("db_get_column_host_type", "cdecl")
    db_get_column_host_type.argtypes = [POINTER(dbColumn)]
    db_get_column_host_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 143
if _libs["grass_dbmibase.8.4"].has("db_get_column_length", "cdecl"):
    db_get_column_length = _libs["grass_dbmibase.8.4"].get("db_get_column_length", "cdecl")
    db_get_column_length.argtypes = [POINTER(dbColumn)]
    db_get_column_length.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 144
if _libs["grass_dbmibase.8.4"].has("db_get_column_name", "cdecl"):
    db_get_column_name = _libs["grass_dbmibase.8.4"].get("db_get_column_name", "cdecl")
    db_get_column_name.argtypes = [POINTER(dbColumn)]
    db_get_column_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 145
if _libs["grass_dbmibase.8.4"].has("db_get_column_precision", "cdecl"):
    db_get_column_precision = _libs["grass_dbmibase.8.4"].get("db_get_column_precision", "cdecl")
    db_get_column_precision.argtypes = [POINTER(dbColumn)]
    db_get_column_precision.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 146
if _libs["grass_dbmibase.8.4"].has("db_get_column_scale", "cdecl"):
    db_get_column_scale = _libs["grass_dbmibase.8.4"].get("db_get_column_scale", "cdecl")
    db_get_column_scale.argtypes = [POINTER(dbColumn)]
    db_get_column_scale.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 147
if _libs["grass_dbmibase.8.4"].has("db_get_column_select_priv", "cdecl"):
    db_get_column_select_priv = _libs["grass_dbmibase.8.4"].get("db_get_column_select_priv", "cdecl")
    db_get_column_select_priv.argtypes = [POINTER(dbColumn)]
    db_get_column_select_priv.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 148
if _libs["grass_dbmibase.8.4"].has("db_get_column_sqltype", "cdecl"):
    db_get_column_sqltype = _libs["grass_dbmibase.8.4"].get("db_get_column_sqltype", "cdecl")
    db_get_column_sqltype.argtypes = [POINTER(dbColumn)]
    db_get_column_sqltype.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 149
if _libs["grass_dbmibase.8.4"].has("db_get_column_update_priv", "cdecl"):
    db_get_column_update_priv = _libs["grass_dbmibase.8.4"].get("db_get_column_update_priv", "cdecl")
    db_get_column_update_priv.argtypes = [POINTER(dbColumn)]
    db_get_column_update_priv.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 150
if _libs["grass_dbmibase.8.4"].has("db_get_column_value", "cdecl"):
    db_get_column_value = _libs["grass_dbmibase.8.4"].get("db_get_column_value", "cdecl")
    db_get_column_value.argtypes = [POINTER(dbColumn)]
    db_get_column_value.restype = POINTER(dbValue)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 151
if _libs["grass_dbmibase.8.4"].has("db_get_connection", "cdecl"):
    db_get_connection = _libs["grass_dbmibase.8.4"].get("db_get_connection", "cdecl")
    db_get_connection.argtypes = [POINTER(dbConnection)]
    db_get_connection.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 152
if _libs["grass_dbmibase.8.4"].has("db_get_cursor_number_of_columns", "cdecl"):
    db_get_cursor_number_of_columns = _libs["grass_dbmibase.8.4"].get("db_get_cursor_number_of_columns", "cdecl")
    db_get_cursor_number_of_columns.argtypes = [POINTER(dbCursor)]
    db_get_cursor_number_of_columns.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 153
if _libs["grass_dbmibase.8.4"].has("db_get_cursor_table", "cdecl"):
    db_get_cursor_table = _libs["grass_dbmibase.8.4"].get("db_get_cursor_table", "cdecl")
    db_get_cursor_table.argtypes = [POINTER(dbCursor)]
    db_get_cursor_table.restype = POINTER(dbTable)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 154
if _libs["grass_dbmibase.8.4"].has("db_get_cursor_token", "cdecl"):
    db_get_cursor_token = _libs["grass_dbmibase.8.4"].get("db_get_cursor_token", "cdecl")
    db_get_cursor_token.argtypes = [POINTER(dbCursor)]
    db_get_cursor_token.restype = dbToken

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 155
if _libs["grass_dbmibase.8.4"].has("db_get_default_driver_name", "cdecl"):
    db_get_default_driver_name = _libs["grass_dbmibase.8.4"].get("db_get_default_driver_name", "cdecl")
    db_get_default_driver_name.argtypes = []
    db_get_default_driver_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 156
if _libs["grass_dbmibase.8.4"].has("db_get_default_database_name", "cdecl"):
    db_get_default_database_name = _libs["grass_dbmibase.8.4"].get("db_get_default_database_name", "cdecl")
    db_get_default_database_name.argtypes = []
    db_get_default_database_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 157
if _libs["grass_dbmibase.8.4"].has("db_get_default_schema_name", "cdecl"):
    db_get_default_schema_name = _libs["grass_dbmibase.8.4"].get("db_get_default_schema_name", "cdecl")
    db_get_default_schema_name.argtypes = []
    db_get_default_schema_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 158
if _libs["grass_dbmibase.8.4"].has("db_get_default_group_name", "cdecl"):
    db_get_default_group_name = _libs["grass_dbmibase.8.4"].get("db_get_default_group_name", "cdecl")
    db_get_default_group_name.argtypes = []
    db_get_default_group_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 159
for _lib in _libs.values():
    if not _lib.has("db__get_driver_state", "cdecl"):
        continue
    db__get_driver_state = _lib.get("db__get_driver_state", "cdecl")
    db__get_driver_state.argtypes = []
    db__get_driver_state.restype = POINTER(dbDriverState)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 160
if _libs["grass_dbmibase.8.4"].has("db_get_error_code", "cdecl"):
    db_get_error_code = _libs["grass_dbmibase.8.4"].get("db_get_error_code", "cdecl")
    db_get_error_code.argtypes = []
    db_get_error_code.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 161
if _libs["grass_dbmibase.8.4"].has("db_get_error_msg", "cdecl"):
    db_get_error_msg = _libs["grass_dbmibase.8.4"].get("db_get_error_msg", "cdecl")
    db_get_error_msg.argtypes = []
    db_get_error_msg.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 162
if _libs["grass_dbmibase.8.4"].has("db_get_error_who", "cdecl"):
    db_get_error_who = _libs["grass_dbmibase.8.4"].get("db_get_error_who", "cdecl")
    db_get_error_who.argtypes = []
    db_get_error_who.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 163
if _libs["grass_dbmibase.8.4"].has("db_get_handle_dbname", "cdecl"):
    db_get_handle_dbname = _libs["grass_dbmibase.8.4"].get("db_get_handle_dbname", "cdecl")
    db_get_handle_dbname.argtypes = [POINTER(dbHandle)]
    db_get_handle_dbname.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 164
if _libs["grass_dbmibase.8.4"].has("db_get_handle_dbschema", "cdecl"):
    db_get_handle_dbschema = _libs["grass_dbmibase.8.4"].get("db_get_handle_dbschema", "cdecl")
    db_get_handle_dbschema.argtypes = [POINTER(dbHandle)]
    db_get_handle_dbschema.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 165
if _libs["grass_dbmibase.8.4"].has("db_get_index_column_name", "cdecl"):
    db_get_index_column_name = _libs["grass_dbmibase.8.4"].get("db_get_index_column_name", "cdecl")
    db_get_index_column_name.argtypes = [POINTER(dbIndex), c_int]
    db_get_index_column_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 166
if _libs["grass_dbmibase.8.4"].has("db_get_index_name", "cdecl"):
    db_get_index_name = _libs["grass_dbmibase.8.4"].get("db_get_index_name", "cdecl")
    db_get_index_name.argtypes = [POINTER(dbIndex)]
    db_get_index_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 167
if _libs["grass_dbmibase.8.4"].has("db_get_index_number_of_columns", "cdecl"):
    db_get_index_number_of_columns = _libs["grass_dbmibase.8.4"].get("db_get_index_number_of_columns", "cdecl")
    db_get_index_number_of_columns.argtypes = [POINTER(dbIndex)]
    db_get_index_number_of_columns.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 168
if _libs["grass_dbmibase.8.4"].has("db_get_index_table_name", "cdecl"):
    db_get_index_table_name = _libs["grass_dbmibase.8.4"].get("db_get_index_table_name", "cdecl")
    db_get_index_table_name.argtypes = [POINTER(dbIndex)]
    db_get_index_table_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 169
if _libs["grass_dbmiclient.8.4"].has("db_get_num_rows", "cdecl"):
    db_get_num_rows = _libs["grass_dbmiclient.8.4"].get("db_get_num_rows", "cdecl")
    db_get_num_rows.argtypes = [POINTER(dbCursor)]
    db_get_num_rows.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 170
if _libs["grass_dbmibase.8.4"].has("db_get_string", "cdecl"):
    db_get_string = _libs["grass_dbmibase.8.4"].get("db_get_string", "cdecl")
    db_get_string.argtypes = [POINTER(dbString)]
    if sizeof(c_int) == sizeof(c_void_p):
        db_get_string.restype = ReturnString
    else:
        db_get_string.restype = String
        db_get_string.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 171
if _libs["grass_dbmibase.8.4"].has("db_get_table_column", "cdecl"):
    db_get_table_column = _libs["grass_dbmibase.8.4"].get("db_get_table_column", "cdecl")
    db_get_table_column.argtypes = [POINTER(dbTable), c_int]
    db_get_table_column.restype = POINTER(dbColumn)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 172
if _libs["grass_dbmibase.8.4"].has("db_get_table_column_by_name", "cdecl"):
    db_get_table_column_by_name = _libs["grass_dbmibase.8.4"].get("db_get_table_column_by_name", "cdecl")
    db_get_table_column_by_name.argtypes = [POINTER(dbTable), String]
    db_get_table_column_by_name.restype = POINTER(dbColumn)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 173
if _libs["grass_dbmibase.8.4"].has("db_get_table_delete_priv", "cdecl"):
    db_get_table_delete_priv = _libs["grass_dbmibase.8.4"].get("db_get_table_delete_priv", "cdecl")
    db_get_table_delete_priv.argtypes = [POINTER(dbTable)]
    db_get_table_delete_priv.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 174
if _libs["grass_dbmibase.8.4"].has("db_get_table_description", "cdecl"):
    db_get_table_description = _libs["grass_dbmibase.8.4"].get("db_get_table_description", "cdecl")
    db_get_table_description.argtypes = [POINTER(dbTable)]
    db_get_table_description.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 175
if _libs["grass_dbmibase.8.4"].has("db_get_table_insert_priv", "cdecl"):
    db_get_table_insert_priv = _libs["grass_dbmibase.8.4"].get("db_get_table_insert_priv", "cdecl")
    db_get_table_insert_priv.argtypes = [POINTER(dbTable)]
    db_get_table_insert_priv.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 176
if _libs["grass_dbmibase.8.4"].has("db_get_table_name", "cdecl"):
    db_get_table_name = _libs["grass_dbmibase.8.4"].get("db_get_table_name", "cdecl")
    db_get_table_name.argtypes = [POINTER(dbTable)]
    db_get_table_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 177
if _libs["grass_dbmibase.8.4"].has("db_get_table_number_of_columns", "cdecl"):
    db_get_table_number_of_columns = _libs["grass_dbmibase.8.4"].get("db_get_table_number_of_columns", "cdecl")
    db_get_table_number_of_columns.argtypes = [POINTER(dbTable)]
    db_get_table_number_of_columns.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 178
if _libs["grass_dbmiclient.8.4"].has("db_get_table_number_of_rows", "cdecl"):
    db_get_table_number_of_rows = _libs["grass_dbmiclient.8.4"].get("db_get_table_number_of_rows", "cdecl")
    db_get_table_number_of_rows.argtypes = [POINTER(dbDriver), POINTER(dbString)]
    db_get_table_number_of_rows.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 179
if _libs["grass_dbmibase.8.4"].has("db_get_table_select_priv", "cdecl"):
    db_get_table_select_priv = _libs["grass_dbmibase.8.4"].get("db_get_table_select_priv", "cdecl")
    db_get_table_select_priv.argtypes = [POINTER(dbTable)]
    db_get_table_select_priv.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 180
if _libs["grass_dbmibase.8.4"].has("db_get_table_update_priv", "cdecl"):
    db_get_table_update_priv = _libs["grass_dbmibase.8.4"].get("db_get_table_update_priv", "cdecl")
    db_get_table_update_priv.argtypes = [POINTER(dbTable)]
    db_get_table_update_priv.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 181
if _libs["grass_dbmibase.8.4"].has("db_get_value_as_double", "cdecl"):
    db_get_value_as_double = _libs["grass_dbmibase.8.4"].get("db_get_value_as_double", "cdecl")
    db_get_value_as_double.argtypes = [POINTER(dbValue), c_int]
    db_get_value_as_double.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 182
if _libs["grass_dbmibase.8.4"].has("db_get_value_day", "cdecl"):
    db_get_value_day = _libs["grass_dbmibase.8.4"].get("db_get_value_day", "cdecl")
    db_get_value_day.argtypes = [POINTER(dbValue)]
    db_get_value_day.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 183
if _libs["grass_dbmibase.8.4"].has("db_get_value_double", "cdecl"):
    db_get_value_double = _libs["grass_dbmibase.8.4"].get("db_get_value_double", "cdecl")
    db_get_value_double.argtypes = [POINTER(dbValue)]
    db_get_value_double.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 184
if _libs["grass_dbmibase.8.4"].has("db_get_value_hour", "cdecl"):
    db_get_value_hour = _libs["grass_dbmibase.8.4"].get("db_get_value_hour", "cdecl")
    db_get_value_hour.argtypes = [POINTER(dbValue)]
    db_get_value_hour.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 185
if _libs["grass_dbmibase.8.4"].has("db_get_value_int", "cdecl"):
    db_get_value_int = _libs["grass_dbmibase.8.4"].get("db_get_value_int", "cdecl")
    db_get_value_int.argtypes = [POINTER(dbValue)]
    db_get_value_int.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 186
if _libs["grass_dbmibase.8.4"].has("db_get_value_minute", "cdecl"):
    db_get_value_minute = _libs["grass_dbmibase.8.4"].get("db_get_value_minute", "cdecl")
    db_get_value_minute.argtypes = [POINTER(dbValue)]
    db_get_value_minute.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 187
if _libs["grass_dbmibase.8.4"].has("db_get_value_month", "cdecl"):
    db_get_value_month = _libs["grass_dbmibase.8.4"].get("db_get_value_month", "cdecl")
    db_get_value_month.argtypes = [POINTER(dbValue)]
    db_get_value_month.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 188
if _libs["grass_dbmibase.8.4"].has("db_get_value_seconds", "cdecl"):
    db_get_value_seconds = _libs["grass_dbmibase.8.4"].get("db_get_value_seconds", "cdecl")
    db_get_value_seconds.argtypes = [POINTER(dbValue)]
    db_get_value_seconds.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 189
if _libs["grass_dbmibase.8.4"].has("db_get_value_string", "cdecl"):
    db_get_value_string = _libs["grass_dbmibase.8.4"].get("db_get_value_string", "cdecl")
    db_get_value_string.argtypes = [POINTER(dbValue)]
    db_get_value_string.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 190
if _libs["grass_dbmibase.8.4"].has("db_get_value_year", "cdecl"):
    db_get_value_year = _libs["grass_dbmibase.8.4"].get("db_get_value_year", "cdecl")
    db_get_value_year.argtypes = [POINTER(dbValue)]
    db_get_value_year.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 191
if _libs["grass_dbmiclient.8.4"].has("db_grant_on_table", "cdecl"):
    db_grant_on_table = _libs["grass_dbmiclient.8.4"].get("db_grant_on_table", "cdecl")
    db_grant_on_table.argtypes = [POINTER(dbDriver), String, c_int, c_int]
    db_grant_on_table.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 192
if _libs["grass_dbmibase.8.4"].has("db_has_dbms", "cdecl"):
    db_has_dbms = _libs["grass_dbmibase.8.4"].get("db_has_dbms", "cdecl")
    db_has_dbms.argtypes = []
    db_has_dbms.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 193
if _libs["grass_dbmibase.8.4"].has("db_init_column", "cdecl"):
    db_init_column = _libs["grass_dbmibase.8.4"].get("db_init_column", "cdecl")
    db_init_column.argtypes = [POINTER(dbColumn)]
    db_init_column.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 194
if _libs["grass_dbmibase.8.4"].has("db_init_cursor", "cdecl"):
    db_init_cursor = _libs["grass_dbmibase.8.4"].get("db_init_cursor", "cdecl")
    db_init_cursor.argtypes = [POINTER(dbCursor)]
    db_init_cursor.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 195
for _lib in _libs.values():
    if not _lib.has("db__init_driver_state", "cdecl"):
        continue
    db__init_driver_state = _lib.get("db__init_driver_state", "cdecl")
    db__init_driver_state.argtypes = []
    db__init_driver_state.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 196
if _libs["grass_dbmibase.8.4"].has("db_init_handle", "cdecl"):
    db_init_handle = _libs["grass_dbmibase.8.4"].get("db_init_handle", "cdecl")
    db_init_handle.argtypes = [POINTER(dbHandle)]
    db_init_handle.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 197
if _libs["grass_dbmibase.8.4"].has("db_init_index", "cdecl"):
    db_init_index = _libs["grass_dbmibase.8.4"].get("db_init_index", "cdecl")
    db_init_index.argtypes = [POINTER(dbIndex)]
    db_init_index.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 198
if _libs["grass_dbmibase.8.4"].has("db_init_string", "cdecl"):
    db_init_string = _libs["grass_dbmibase.8.4"].get("db_init_string", "cdecl")
    db_init_string.argtypes = [POINTER(dbString)]
    db_init_string.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 199
if _libs["grass_dbmibase.8.4"].has("db_init_table", "cdecl"):
    db_init_table = _libs["grass_dbmibase.8.4"].get("db_init_table", "cdecl")
    db_init_table.argtypes = [POINTER(dbTable)]
    db_init_table.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 200
if _libs["grass_dbmiclient.8.4"].has("db_insert", "cdecl"):
    db_insert = _libs["grass_dbmiclient.8.4"].get("db_insert", "cdecl")
    db_insert.argtypes = [POINTER(dbCursor)]
    db_insert.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 201
if _libs["grass_dbmibase.8.4"].has("db_interval_range", "cdecl"):
    db_interval_range = _libs["grass_dbmibase.8.4"].get("db_interval_range", "cdecl")
    db_interval_range.argtypes = [c_int, POINTER(c_int), POINTER(c_int)]
    db_interval_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 202
if _libs["grass_dbmibase.8.4"].has("db_isdir", "cdecl"):
    db_isdir = _libs["grass_dbmibase.8.4"].get("db_isdir", "cdecl")
    db_isdir.argtypes = [String]
    db_isdir.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 203
if _libs["grass_dbmibase.8.4"].has("db_legal_tablename", "cdecl"):
    db_legal_tablename = _libs["grass_dbmibase.8.4"].get("db_legal_tablename", "cdecl")
    db_legal_tablename.argtypes = [String]
    db_legal_tablename.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 204
if _libs["grass_dbmiclient.8.4"].has("db_list_databases", "cdecl"):
    db_list_databases = _libs["grass_dbmiclient.8.4"].get("db_list_databases", "cdecl")
    db_list_databases.argtypes = [POINTER(dbDriver), POINTER(dbString), c_int, POINTER(POINTER(dbHandle)), POINTER(c_int)]
    db_list_databases.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 205
if _libs["grass_dbmiclient.8.4"].has("db_list_drivers", "cdecl"):
    db_list_drivers = _libs["grass_dbmiclient.8.4"].get("db_list_drivers", "cdecl")
    db_list_drivers.argtypes = []
    db_list_drivers.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 206
if _libs["grass_dbmiclient.8.4"].has("db_list_indexes", "cdecl"):
    db_list_indexes = _libs["grass_dbmiclient.8.4"].get("db_list_indexes", "cdecl")
    db_list_indexes.argtypes = [POINTER(dbDriver), POINTER(dbString), POINTER(POINTER(dbIndex)), POINTER(c_int)]
    db_list_indexes.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 207
if _libs["grass_dbmiclient.8.4"].has("db_list_tables", "cdecl"):
    db_list_tables = _libs["grass_dbmiclient.8.4"].get("db_list_tables", "cdecl")
    db_list_tables.argtypes = [POINTER(dbDriver), POINTER(POINTER(dbString)), POINTER(c_int), c_int]
    db_list_tables.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 208
if _libs["grass_dbmibase.8.4"].has("db_malloc", "cdecl"):
    db_malloc = _libs["grass_dbmibase.8.4"].get("db_malloc", "cdecl")
    db_malloc.argtypes = [c_int]
    db_malloc.restype = POINTER(c_ubyte)
    db_malloc.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 209
for _lib in _libs.values():
    if not _lib.has("db__mark_database_closed", "cdecl"):
        continue
    db__mark_database_closed = _lib.get("db__mark_database_closed", "cdecl")
    db__mark_database_closed.argtypes = []
    db__mark_database_closed.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 210
for _lib in _libs.values():
    if not _lib.has("db__mark_database_open", "cdecl"):
        continue
    db__mark_database_open = _lib.get("db__mark_database_open", "cdecl")
    db__mark_database_open.argtypes = [String, String]
    db__mark_database_open.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 211
if _libs["grass_dbmibase.8.4"].has("db_memory_error", "cdecl"):
    db_memory_error = _libs["grass_dbmibase.8.4"].get("db_memory_error", "cdecl")
    db_memory_error.argtypes = []
    db_memory_error.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 212
if _libs["grass_dbmibase.8.4"].has("db_new_token", "cdecl"):
    db_new_token = _libs["grass_dbmibase.8.4"].get("db_new_token", "cdecl")
    db_new_token.argtypes = [dbAddress]
    db_new_token.restype = dbToken

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 213
if _libs["grass_dbmibase.8.4"].has("db_nocase_compare", "cdecl"):
    db_nocase_compare = _libs["grass_dbmibase.8.4"].get("db_nocase_compare", "cdecl")
    db_nocase_compare.argtypes = [String, String]
    db_nocase_compare.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 214
if _libs["grass_dbmibase.8.4"].has("db_noproc_error", "cdecl"):
    db_noproc_error = _libs["grass_dbmibase.8.4"].get("db_noproc_error", "cdecl")
    db_noproc_error.argtypes = [c_int]
    db_noproc_error.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 215
if _libs["grass_dbmiclient.8.4"].has("db_open_database", "cdecl"):
    db_open_database = _libs["grass_dbmiclient.8.4"].get("db_open_database", "cdecl")
    db_open_database.argtypes = [POINTER(dbDriver), POINTER(dbHandle)]
    db_open_database.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 216
if _libs["grass_dbmiclient.8.4"].has("db_open_insert_cursor", "cdecl"):
    db_open_insert_cursor = _libs["grass_dbmiclient.8.4"].get("db_open_insert_cursor", "cdecl")
    db_open_insert_cursor.argtypes = [POINTER(dbDriver), POINTER(dbCursor)]
    db_open_insert_cursor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 217
if _libs["grass_dbmiclient.8.4"].has("db_open_select_cursor", "cdecl"):
    db_open_select_cursor = _libs["grass_dbmiclient.8.4"].get("db_open_select_cursor", "cdecl")
    db_open_select_cursor.argtypes = [POINTER(dbDriver), POINTER(dbString), POINTER(dbCursor), c_int]
    db_open_select_cursor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 218
if _libs["grass_dbmiclient.8.4"].has("db_open_update_cursor", "cdecl"):
    db_open_update_cursor = _libs["grass_dbmiclient.8.4"].get("db_open_update_cursor", "cdecl")
    db_open_update_cursor.argtypes = [POINTER(dbDriver), POINTER(dbString), POINTER(dbString), POINTER(dbCursor), c_int]
    db_open_update_cursor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 220
if _libs["grass_dbmiclient.8.4"].has("db_print_column_definition", "cdecl"):
    db_print_column_definition = _libs["grass_dbmiclient.8.4"].get("db_print_column_definition", "cdecl")
    db_print_column_definition.argtypes = [POINTER(FILE), POINTER(dbColumn)]
    db_print_column_definition.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 221
if _libs["grass_dbmibase.8.4"].has("db_print_error", "cdecl"):
    db_print_error = _libs["grass_dbmibase.8.4"].get("db_print_error", "cdecl")
    db_print_error.argtypes = []
    db_print_error.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 222
if _libs["grass_dbmibase.8.4"].has("db_print_index", "cdecl"):
    db_print_index = _libs["grass_dbmibase.8.4"].get("db_print_index", "cdecl")
    db_print_index.argtypes = [POINTER(FILE), POINTER(dbIndex)]
    db_print_index.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 223
if _libs["grass_dbmiclient.8.4"].has("db_print_table_definition", "cdecl"):
    db_print_table_definition = _libs["grass_dbmiclient.8.4"].get("db_print_table_definition", "cdecl")
    db_print_table_definition.argtypes = [POINTER(FILE), POINTER(dbTable)]
    db_print_table_definition.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 224
if _libs["grass_dbmibase.8.4"].has("db_procedure_not_implemented", "cdecl"):
    db_procedure_not_implemented = _libs["grass_dbmibase.8.4"].get("db_procedure_not_implemented", "cdecl")
    db_procedure_not_implemented.argtypes = [String]
    db_procedure_not_implemented.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 225
if _libs["grass_dbmibase.8.4"].has("db_protocol_error", "cdecl"):
    db_protocol_error = _libs["grass_dbmibase.8.4"].get("db_protocol_error", "cdecl")
    db_protocol_error.argtypes = []
    db_protocol_error.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 226
if _libs["grass_dbmibase.8.4"].has("db_read_dbmscap", "cdecl"):
    db_read_dbmscap = _libs["grass_dbmibase.8.4"].get("db_read_dbmscap", "cdecl")
    db_read_dbmscap.argtypes = []
    db_read_dbmscap.restype = POINTER(dbDbmscap)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 227
if _libs["grass_dbmibase.8.4"].has("db_realloc", "cdecl"):
    db_realloc = _libs["grass_dbmibase.8.4"].get("db_realloc", "cdecl")
    db_realloc.argtypes = [POINTER(None), c_int]
    db_realloc.restype = POINTER(c_ubyte)
    db_realloc.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 228
if _libs["grass_dbmibase.8.4"].has("db__recv_char", "cdecl"):
    db__recv_char = _libs["grass_dbmibase.8.4"].get("db__recv_char", "cdecl")
    db__recv_char.argtypes = [String]
    db__recv_char.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 229
if _libs["grass_dbmibase.8.4"].has("db__recv_column_default_value", "cdecl"):
    db__recv_column_default_value = _libs["grass_dbmibase.8.4"].get("db__recv_column_default_value", "cdecl")
    db__recv_column_default_value.argtypes = [POINTER(dbColumn)]
    db__recv_column_default_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 230
if _libs["grass_dbmibase.8.4"].has("db__recv_column_definition", "cdecl"):
    db__recv_column_definition = _libs["grass_dbmibase.8.4"].get("db__recv_column_definition", "cdecl")
    db__recv_column_definition.argtypes = [POINTER(dbColumn)]
    db__recv_column_definition.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 231
if _libs["grass_dbmibase.8.4"].has("db__recv_column_value", "cdecl"):
    db__recv_column_value = _libs["grass_dbmibase.8.4"].get("db__recv_column_value", "cdecl")
    db__recv_column_value.argtypes = [POINTER(dbColumn)]
    db__recv_column_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 232
if _libs["grass_dbmibase.8.4"].has("db__recv_datetime", "cdecl"):
    db__recv_datetime = _libs["grass_dbmibase.8.4"].get("db__recv_datetime", "cdecl")
    db__recv_datetime.argtypes = [POINTER(dbDateTime)]
    db__recv_datetime.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 233
if _libs["grass_dbmibase.8.4"].has("db__recv_double", "cdecl"):
    db__recv_double = _libs["grass_dbmibase.8.4"].get("db__recv_double", "cdecl")
    db__recv_double.argtypes = [POINTER(c_double)]
    db__recv_double.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 234
if _libs["grass_dbmibase.8.4"].has("db__recv_double_array", "cdecl"):
    db__recv_double_array = _libs["grass_dbmibase.8.4"].get("db__recv_double_array", "cdecl")
    db__recv_double_array.argtypes = [POINTER(POINTER(c_double)), POINTER(c_int)]
    db__recv_double_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 235
if _libs["grass_dbmibase.8.4"].has("db__recv_float", "cdecl"):
    db__recv_float = _libs["grass_dbmibase.8.4"].get("db__recv_float", "cdecl")
    db__recv_float.argtypes = [POINTER(c_float)]
    db__recv_float.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 236
if _libs["grass_dbmibase.8.4"].has("db__recv_float_array", "cdecl"):
    db__recv_float_array = _libs["grass_dbmibase.8.4"].get("db__recv_float_array", "cdecl")
    db__recv_float_array.argtypes = [POINTER(POINTER(c_float)), POINTER(c_int)]
    db__recv_float_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 237
if _libs["grass_dbmibase.8.4"].has("db__recv_handle", "cdecl"):
    db__recv_handle = _libs["grass_dbmibase.8.4"].get("db__recv_handle", "cdecl")
    db__recv_handle.argtypes = [POINTER(dbHandle)]
    db__recv_handle.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 238
if _libs["grass_dbmibase.8.4"].has("db__recv_index", "cdecl"):
    db__recv_index = _libs["grass_dbmibase.8.4"].get("db__recv_index", "cdecl")
    db__recv_index.argtypes = [POINTER(dbIndex)]
    db__recv_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 239
if _libs["grass_dbmibase.8.4"].has("db__recv_index_array", "cdecl"):
    db__recv_index_array = _libs["grass_dbmibase.8.4"].get("db__recv_index_array", "cdecl")
    db__recv_index_array.argtypes = [POINTER(POINTER(dbIndex)), POINTER(c_int)]
    db__recv_index_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 240
if _libs["grass_dbmibase.8.4"].has("db__recv_int", "cdecl"):
    db__recv_int = _libs["grass_dbmibase.8.4"].get("db__recv_int", "cdecl")
    db__recv_int.argtypes = [POINTER(c_int)]
    db__recv_int.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 241
if _libs["grass_dbmibase.8.4"].has("db__recv_int_array", "cdecl"):
    db__recv_int_array = _libs["grass_dbmibase.8.4"].get("db__recv_int_array", "cdecl")
    db__recv_int_array.argtypes = [POINTER(POINTER(c_int)), POINTER(c_int)]
    db__recv_int_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 242
if _libs["grass_dbmibase.8.4"].has("db__recv_procnum", "cdecl"):
    db__recv_procnum = _libs["grass_dbmibase.8.4"].get("db__recv_procnum", "cdecl")
    db__recv_procnum.argtypes = [POINTER(c_int)]
    db__recv_procnum.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 243
if _libs["grass_dbmibase.8.4"].has("db__recv_return_code", "cdecl"):
    db__recv_return_code = _libs["grass_dbmibase.8.4"].get("db__recv_return_code", "cdecl")
    db__recv_return_code.argtypes = [POINTER(c_int)]
    db__recv_return_code.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 244
if _libs["grass_dbmibase.8.4"].has("db__recv_short", "cdecl"):
    db__recv_short = _libs["grass_dbmibase.8.4"].get("db__recv_short", "cdecl")
    db__recv_short.argtypes = [POINTER(c_short)]
    db__recv_short.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 245
if _libs["grass_dbmibase.8.4"].has("db__recv_short_array", "cdecl"):
    db__recv_short_array = _libs["grass_dbmibase.8.4"].get("db__recv_short_array", "cdecl")
    db__recv_short_array.argtypes = [POINTER(POINTER(c_short)), POINTER(c_int)]
    db__recv_short_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 246
if _libs["grass_dbmibase.8.4"].has("db__recv_string", "cdecl"):
    db__recv_string = _libs["grass_dbmibase.8.4"].get("db__recv_string", "cdecl")
    db__recv_string.argtypes = [POINTER(dbString)]
    db__recv_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 247
if _libs["grass_dbmibase.8.4"].has("db__recv_string_array", "cdecl"):
    db__recv_string_array = _libs["grass_dbmibase.8.4"].get("db__recv_string_array", "cdecl")
    db__recv_string_array.argtypes = [POINTER(POINTER(dbString)), POINTER(c_int)]
    db__recv_string_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 248
if _libs["grass_dbmibase.8.4"].has("db__recv_table_data", "cdecl"):
    db__recv_table_data = _libs["grass_dbmibase.8.4"].get("db__recv_table_data", "cdecl")
    db__recv_table_data.argtypes = [POINTER(dbTable)]
    db__recv_table_data.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 249
if _libs["grass_dbmibase.8.4"].has("db__recv_table_definition", "cdecl"):
    db__recv_table_definition = _libs["grass_dbmibase.8.4"].get("db__recv_table_definition", "cdecl")
    db__recv_table_definition.argtypes = [POINTER(POINTER(dbTable))]
    db__recv_table_definition.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 250
if _libs["grass_dbmibase.8.4"].has("db__recv_token", "cdecl"):
    db__recv_token = _libs["grass_dbmibase.8.4"].get("db__recv_token", "cdecl")
    db__recv_token.argtypes = [POINTER(dbToken)]
    db__recv_token.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 251
if _libs["grass_dbmibase.8.4"].has("db__recv_value", "cdecl"):
    db__recv_value = _libs["grass_dbmibase.8.4"].get("db__recv_value", "cdecl")
    db__recv_value.argtypes = [POINTER(dbValue), c_int]
    db__recv_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 252
if _libs["grass_dbmibase.8.4"].has("db__send_Cstring", "cdecl"):
    db__send_Cstring = _libs["grass_dbmibase.8.4"].get("db__send_Cstring", "cdecl")
    db__send_Cstring.argtypes = [String]
    db__send_Cstring.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 253
if _libs["grass_dbmibase.8.4"].has("db__send_char", "cdecl"):
    db__send_char = _libs["grass_dbmibase.8.4"].get("db__send_char", "cdecl")
    db__send_char.argtypes = [c_int]
    db__send_char.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 254
if _libs["grass_dbmibase.8.4"].has("db__send_column_default_value", "cdecl"):
    db__send_column_default_value = _libs["grass_dbmibase.8.4"].get("db__send_column_default_value", "cdecl")
    db__send_column_default_value.argtypes = [POINTER(dbColumn)]
    db__send_column_default_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 255
if _libs["grass_dbmibase.8.4"].has("db__send_column_definition", "cdecl"):
    db__send_column_definition = _libs["grass_dbmibase.8.4"].get("db__send_column_definition", "cdecl")
    db__send_column_definition.argtypes = [POINTER(dbColumn)]
    db__send_column_definition.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 256
if _libs["grass_dbmibase.8.4"].has("db__send_column_value", "cdecl"):
    db__send_column_value = _libs["grass_dbmibase.8.4"].get("db__send_column_value", "cdecl")
    db__send_column_value.argtypes = [POINTER(dbColumn)]
    db__send_column_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 257
if _libs["grass_dbmibase.8.4"].has("db__send_datetime", "cdecl"):
    db__send_datetime = _libs["grass_dbmibase.8.4"].get("db__send_datetime", "cdecl")
    db__send_datetime.argtypes = [POINTER(dbDateTime)]
    db__send_datetime.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 258
if _libs["grass_dbmibase.8.4"].has("db__send_double", "cdecl"):
    db__send_double = _libs["grass_dbmibase.8.4"].get("db__send_double", "cdecl")
    db__send_double.argtypes = [c_double]
    db__send_double.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 259
if _libs["grass_dbmibase.8.4"].has("db__send_double_array", "cdecl"):
    db__send_double_array = _libs["grass_dbmibase.8.4"].get("db__send_double_array", "cdecl")
    db__send_double_array.argtypes = [POINTER(c_double), c_int]
    db__send_double_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 260
if _libs["grass_dbmibase.8.4"].has("db__send_failure", "cdecl"):
    db__send_failure = _libs["grass_dbmibase.8.4"].get("db__send_failure", "cdecl")
    db__send_failure.argtypes = []
    db__send_failure.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 261
if _libs["grass_dbmibase.8.4"].has("db__send_float", "cdecl"):
    db__send_float = _libs["grass_dbmibase.8.4"].get("db__send_float", "cdecl")
    db__send_float.argtypes = [c_float]
    db__send_float.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 262
if _libs["grass_dbmibase.8.4"].has("db__send_float_array", "cdecl"):
    db__send_float_array = _libs["grass_dbmibase.8.4"].get("db__send_float_array", "cdecl")
    db__send_float_array.argtypes = [POINTER(c_float), c_int]
    db__send_float_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 263
if _libs["grass_dbmibase.8.4"].has("db__send_handle", "cdecl"):
    db__send_handle = _libs["grass_dbmibase.8.4"].get("db__send_handle", "cdecl")
    db__send_handle.argtypes = [POINTER(dbHandle)]
    db__send_handle.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 264
if _libs["grass_dbmibase.8.4"].has("db__send_index", "cdecl"):
    db__send_index = _libs["grass_dbmibase.8.4"].get("db__send_index", "cdecl")
    db__send_index.argtypes = [POINTER(dbIndex)]
    db__send_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 265
if _libs["grass_dbmibase.8.4"].has("db__send_index_array", "cdecl"):
    db__send_index_array = _libs["grass_dbmibase.8.4"].get("db__send_index_array", "cdecl")
    db__send_index_array.argtypes = [POINTER(dbIndex), c_int]
    db__send_index_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 266
if _libs["grass_dbmibase.8.4"].has("db__send_int", "cdecl"):
    db__send_int = _libs["grass_dbmibase.8.4"].get("db__send_int", "cdecl")
    db__send_int.argtypes = [c_int]
    db__send_int.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 267
if _libs["grass_dbmibase.8.4"].has("db__send_int_array", "cdecl"):
    db__send_int_array = _libs["grass_dbmibase.8.4"].get("db__send_int_array", "cdecl")
    db__send_int_array.argtypes = [POINTER(c_int), c_int]
    db__send_int_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 268
if _libs["grass_dbmibase.8.4"].has("db__send_procedure_not_implemented", "cdecl"):
    db__send_procedure_not_implemented = _libs["grass_dbmibase.8.4"].get("db__send_procedure_not_implemented", "cdecl")
    db__send_procedure_not_implemented.argtypes = [c_int]
    db__send_procedure_not_implemented.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 269
if _libs["grass_dbmibase.8.4"].has("db__send_procedure_ok", "cdecl"):
    db__send_procedure_ok = _libs["grass_dbmibase.8.4"].get("db__send_procedure_ok", "cdecl")
    db__send_procedure_ok.argtypes = [c_int]
    db__send_procedure_ok.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 270
if _libs["grass_dbmibase.8.4"].has("db__send_short", "cdecl"):
    db__send_short = _libs["grass_dbmibase.8.4"].get("db__send_short", "cdecl")
    db__send_short.argtypes = [c_int]
    db__send_short.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 271
if _libs["grass_dbmibase.8.4"].has("db__send_short_array", "cdecl"):
    db__send_short_array = _libs["grass_dbmibase.8.4"].get("db__send_short_array", "cdecl")
    db__send_short_array.argtypes = [POINTER(c_short), c_int]
    db__send_short_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 272
if _libs["grass_dbmibase.8.4"].has("db__send_string", "cdecl"):
    db__send_string = _libs["grass_dbmibase.8.4"].get("db__send_string", "cdecl")
    db__send_string.argtypes = [POINTER(dbString)]
    db__send_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 273
if _libs["grass_dbmibase.8.4"].has("db__send_string_array", "cdecl"):
    db__send_string_array = _libs["grass_dbmibase.8.4"].get("db__send_string_array", "cdecl")
    db__send_string_array.argtypes = [POINTER(dbString), c_int]
    db__send_string_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 274
if _libs["grass_dbmibase.8.4"].has("db__send_success", "cdecl"):
    db__send_success = _libs["grass_dbmibase.8.4"].get("db__send_success", "cdecl")
    db__send_success.argtypes = []
    db__send_success.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 275
if _libs["grass_dbmibase.8.4"].has("db__send_table_data", "cdecl"):
    db__send_table_data = _libs["grass_dbmibase.8.4"].get("db__send_table_data", "cdecl")
    db__send_table_data.argtypes = [POINTER(dbTable)]
    db__send_table_data.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 276
if _libs["grass_dbmibase.8.4"].has("db__send_table_definition", "cdecl"):
    db__send_table_definition = _libs["grass_dbmibase.8.4"].get("db__send_table_definition", "cdecl")
    db__send_table_definition.argtypes = [POINTER(dbTable)]
    db__send_table_definition.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 277
if _libs["grass_dbmibase.8.4"].has("db__send_token", "cdecl"):
    db__send_token = _libs["grass_dbmibase.8.4"].get("db__send_token", "cdecl")
    db__send_token.argtypes = [POINTER(dbToken)]
    db__send_token.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 278
if _libs["grass_dbmibase.8.4"].has("db__send_value", "cdecl"):
    db__send_value = _libs["grass_dbmibase.8.4"].get("db__send_value", "cdecl")
    db__send_value.argtypes = [POINTER(dbValue), c_int]
    db__send_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 279
if _libs["grass_dbmiclient.8.4"].has("db_select_CatValArray", "cdecl"):
    db_select_CatValArray = _libs["grass_dbmiclient.8.4"].get("db_select_CatValArray", "cdecl")
    db_select_CatValArray.argtypes = [POINTER(dbDriver), String, String, String, String, POINTER(dbCatValArray)]
    db_select_CatValArray.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 281
if _libs["grass_dbmiclient.8.4"].has("db_select_int", "cdecl"):
    db_select_int = _libs["grass_dbmiclient.8.4"].get("db_select_int", "cdecl")
    db_select_int.argtypes = [POINTER(dbDriver), String, String, String, POINTER(POINTER(c_int))]
    db_select_int.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 282
if _libs["grass_dbmiclient.8.4"].has("db_select_value", "cdecl"):
    db_select_value = _libs["grass_dbmiclient.8.4"].get("db_select_value", "cdecl")
    db_select_value.argtypes = [POINTER(dbDriver), String, String, c_int, String, POINTER(dbValue)]
    db_select_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 284
if _libs["grass_dbmibase.8.4"].has("db_set_column_description", "cdecl"):
    db_set_column_description = _libs["grass_dbmibase.8.4"].get("db_set_column_description", "cdecl")
    db_set_column_description.argtypes = [POINTER(dbColumn), String]
    db_set_column_description.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 285
if _libs["grass_dbmibase.8.4"].has("db_set_column_has_defined_default_value", "cdecl"):
    db_set_column_has_defined_default_value = _libs["grass_dbmibase.8.4"].get("db_set_column_has_defined_default_value", "cdecl")
    db_set_column_has_defined_default_value.argtypes = [POINTER(dbColumn)]
    db_set_column_has_defined_default_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 286
if _libs["grass_dbmibase.8.4"].has("db_set_column_has_undefined_default_value", "cdecl"):
    db_set_column_has_undefined_default_value = _libs["grass_dbmibase.8.4"].get("db_set_column_has_undefined_default_value", "cdecl")
    db_set_column_has_undefined_default_value.argtypes = [POINTER(dbColumn)]
    db_set_column_has_undefined_default_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 287
if _libs["grass_dbmibase.8.4"].has("db_set_column_host_type", "cdecl"):
    db_set_column_host_type = _libs["grass_dbmibase.8.4"].get("db_set_column_host_type", "cdecl")
    db_set_column_host_type.argtypes = [POINTER(dbColumn), c_int]
    db_set_column_host_type.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 288
if _libs["grass_dbmibase.8.4"].has("db_set_column_length", "cdecl"):
    db_set_column_length = _libs["grass_dbmibase.8.4"].get("db_set_column_length", "cdecl")
    db_set_column_length.argtypes = [POINTER(dbColumn), c_int]
    db_set_column_length.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 289
if _libs["grass_dbmibase.8.4"].has("db_set_column_name", "cdecl"):
    db_set_column_name = _libs["grass_dbmibase.8.4"].get("db_set_column_name", "cdecl")
    db_set_column_name.argtypes = [POINTER(dbColumn), String]
    db_set_column_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 290
if _libs["grass_dbmibase.8.4"].has("db_set_column_null_allowed", "cdecl"):
    db_set_column_null_allowed = _libs["grass_dbmibase.8.4"].get("db_set_column_null_allowed", "cdecl")
    db_set_column_null_allowed.argtypes = [POINTER(dbColumn)]
    db_set_column_null_allowed.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 291
if _libs["grass_dbmibase.8.4"].has("db_set_column_precision", "cdecl"):
    db_set_column_precision = _libs["grass_dbmibase.8.4"].get("db_set_column_precision", "cdecl")
    db_set_column_precision.argtypes = [POINTER(dbColumn), c_int]
    db_set_column_precision.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 292
if _libs["grass_dbmibase.8.4"].has("db_set_column_scale", "cdecl"):
    db_set_column_scale = _libs["grass_dbmibase.8.4"].get("db_set_column_scale", "cdecl")
    db_set_column_scale.argtypes = [POINTER(dbColumn), c_int]
    db_set_column_scale.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 293
if _libs["grass_dbmibase.8.4"].has("db_set_column_select_priv_granted", "cdecl"):
    db_set_column_select_priv_granted = _libs["grass_dbmibase.8.4"].get("db_set_column_select_priv_granted", "cdecl")
    db_set_column_select_priv_granted.argtypes = [POINTER(dbColumn)]
    db_set_column_select_priv_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 294
if _libs["grass_dbmibase.8.4"].has("db_set_column_select_priv_not_granted", "cdecl"):
    db_set_column_select_priv_not_granted = _libs["grass_dbmibase.8.4"].get("db_set_column_select_priv_not_granted", "cdecl")
    db_set_column_select_priv_not_granted.argtypes = [POINTER(dbColumn)]
    db_set_column_select_priv_not_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 295
if _libs["grass_dbmibase.8.4"].has("db_set_column_sqltype", "cdecl"):
    db_set_column_sqltype = _libs["grass_dbmibase.8.4"].get("db_set_column_sqltype", "cdecl")
    db_set_column_sqltype.argtypes = [POINTER(dbColumn), c_int]
    db_set_column_sqltype.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 296
if _libs["grass_dbmibase.8.4"].has("db_set_column_update_priv_granted", "cdecl"):
    db_set_column_update_priv_granted = _libs["grass_dbmibase.8.4"].get("db_set_column_update_priv_granted", "cdecl")
    db_set_column_update_priv_granted.argtypes = [POINTER(dbColumn)]
    db_set_column_update_priv_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 297
if _libs["grass_dbmibase.8.4"].has("db_set_column_update_priv_not_granted", "cdecl"):
    db_set_column_update_priv_not_granted = _libs["grass_dbmibase.8.4"].get("db_set_column_update_priv_not_granted", "cdecl")
    db_set_column_update_priv_not_granted.argtypes = [POINTER(dbColumn)]
    db_set_column_update_priv_not_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 298
if _libs["grass_dbmibase.8.4"].has("db_set_column_use_default_value", "cdecl"):
    db_set_column_use_default_value = _libs["grass_dbmibase.8.4"].get("db_set_column_use_default_value", "cdecl")
    db_set_column_use_default_value.argtypes = [POINTER(dbColumn)]
    db_set_column_use_default_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 299
if _libs["grass_dbmibase.8.4"].has("db_set_connection", "cdecl"):
    db_set_connection = _libs["grass_dbmibase.8.4"].get("db_set_connection", "cdecl")
    db_set_connection.argtypes = [POINTER(dbConnection)]
    db_set_connection.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 300
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_column_flag", "cdecl"):
    db_set_cursor_column_flag = _libs["grass_dbmibase.8.4"].get("db_set_cursor_column_flag", "cdecl")
    db_set_cursor_column_flag.argtypes = [POINTER(dbCursor), c_int]
    db_set_cursor_column_flag.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 301
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_column_for_update", "cdecl"):
    db_set_cursor_column_for_update = _libs["grass_dbmibase.8.4"].get("db_set_cursor_column_for_update", "cdecl")
    db_set_cursor_column_for_update.argtypes = [POINTER(dbCursor), c_int]
    db_set_cursor_column_for_update.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 302
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_mode", "cdecl"):
    db_set_cursor_mode = _libs["grass_dbmibase.8.4"].get("db_set_cursor_mode", "cdecl")
    db_set_cursor_mode.argtypes = [POINTER(dbCursor), c_int]
    db_set_cursor_mode.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 303
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_mode_insensitive", "cdecl"):
    db_set_cursor_mode_insensitive = _libs["grass_dbmibase.8.4"].get("db_set_cursor_mode_insensitive", "cdecl")
    db_set_cursor_mode_insensitive.argtypes = [POINTER(dbCursor)]
    db_set_cursor_mode_insensitive.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 304
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_mode_scroll", "cdecl"):
    db_set_cursor_mode_scroll = _libs["grass_dbmibase.8.4"].get("db_set_cursor_mode_scroll", "cdecl")
    db_set_cursor_mode_scroll.argtypes = [POINTER(dbCursor)]
    db_set_cursor_mode_scroll.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 305
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_table", "cdecl"):
    db_set_cursor_table = _libs["grass_dbmibase.8.4"].get("db_set_cursor_table", "cdecl")
    db_set_cursor_table.argtypes = [POINTER(dbCursor), POINTER(dbTable)]
    db_set_cursor_table.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 306
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_token", "cdecl"):
    db_set_cursor_token = _libs["grass_dbmibase.8.4"].get("db_set_cursor_token", "cdecl")
    db_set_cursor_token.argtypes = [POINTER(dbCursor), dbToken]
    db_set_cursor_token.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 307
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_type_insert", "cdecl"):
    db_set_cursor_type_insert = _libs["grass_dbmibase.8.4"].get("db_set_cursor_type_insert", "cdecl")
    db_set_cursor_type_insert.argtypes = [POINTER(dbCursor)]
    db_set_cursor_type_insert.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 308
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_type_readonly", "cdecl"):
    db_set_cursor_type_readonly = _libs["grass_dbmibase.8.4"].get("db_set_cursor_type_readonly", "cdecl")
    db_set_cursor_type_readonly.argtypes = [POINTER(dbCursor)]
    db_set_cursor_type_readonly.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 309
if _libs["grass_dbmibase.8.4"].has("db_set_cursor_type_update", "cdecl"):
    db_set_cursor_type_update = _libs["grass_dbmibase.8.4"].get("db_set_cursor_type_update", "cdecl")
    db_set_cursor_type_update.argtypes = [POINTER(dbCursor)]
    db_set_cursor_type_update.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 310
if _libs["grass_dbmibase.8.4"].has("db_set_default_connection", "cdecl"):
    db_set_default_connection = _libs["grass_dbmibase.8.4"].get("db_set_default_connection", "cdecl")
    db_set_default_connection.argtypes = []
    db_set_default_connection.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 311
if _libs["grass_dbmibase.8.4"].has("db_set_error_who", "cdecl"):
    db_set_error_who = _libs["grass_dbmibase.8.4"].get("db_set_error_who", "cdecl")
    db_set_error_who.argtypes = [String]
    db_set_error_who.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 312
if _libs["grass_dbmibase.8.4"].has("db_set_handle", "cdecl"):
    db_set_handle = _libs["grass_dbmibase.8.4"].get("db_set_handle", "cdecl")
    db_set_handle.argtypes = [POINTER(dbHandle), String, String]
    db_set_handle.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 313
if _libs["grass_dbmiclient.8.4"].has("db_set_error_handler_driver", "cdecl"):
    db_set_error_handler_driver = _libs["grass_dbmiclient.8.4"].get("db_set_error_handler_driver", "cdecl")
    db_set_error_handler_driver.argtypes = [POINTER(dbDriver)]
    db_set_error_handler_driver.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 314
if _libs["grass_dbmiclient.8.4"].has("db_unset_error_handler_driver", "cdecl"):
    db_unset_error_handler_driver = _libs["grass_dbmiclient.8.4"].get("db_unset_error_handler_driver", "cdecl")
    db_unset_error_handler_driver.argtypes = [POINTER(dbDriver)]
    db_unset_error_handler_driver.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 315
if _libs["grass_dbmibase.8.4"].has("db_set_index_column_name", "cdecl"):
    db_set_index_column_name = _libs["grass_dbmibase.8.4"].get("db_set_index_column_name", "cdecl")
    db_set_index_column_name.argtypes = [POINTER(dbIndex), c_int, String]
    db_set_index_column_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 316
if _libs["grass_dbmibase.8.4"].has("db_set_index_name", "cdecl"):
    db_set_index_name = _libs["grass_dbmibase.8.4"].get("db_set_index_name", "cdecl")
    db_set_index_name.argtypes = [POINTER(dbIndex), String]
    db_set_index_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 317
if _libs["grass_dbmibase.8.4"].has("db_set_index_table_name", "cdecl"):
    db_set_index_table_name = _libs["grass_dbmibase.8.4"].get("db_set_index_table_name", "cdecl")
    db_set_index_table_name.argtypes = [POINTER(dbIndex), String]
    db_set_index_table_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 318
if _libs["grass_dbmibase.8.4"].has("db_set_index_type_non_unique", "cdecl"):
    db_set_index_type_non_unique = _libs["grass_dbmibase.8.4"].get("db_set_index_type_non_unique", "cdecl")
    db_set_index_type_non_unique.argtypes = [POINTER(dbIndex)]
    db_set_index_type_non_unique.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 319
if _libs["grass_dbmibase.8.4"].has("db_set_index_type_unique", "cdecl"):
    db_set_index_type_unique = _libs["grass_dbmibase.8.4"].get("db_set_index_type_unique", "cdecl")
    db_set_index_type_unique.argtypes = [POINTER(dbIndex)]
    db_set_index_type_unique.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 320
if _libs["grass_dbmibase.8.4"].has("db__set_protocol_fds", "cdecl"):
    db__set_protocol_fds = _libs["grass_dbmibase.8.4"].get("db__set_protocol_fds", "cdecl")
    db__set_protocol_fds.argtypes = [POINTER(FILE), POINTER(FILE)]
    db__set_protocol_fds.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 321
if _libs["grass_dbmibase.8.4"].has("db_set_string", "cdecl"):
    db_set_string = _libs["grass_dbmibase.8.4"].get("db_set_string", "cdecl")
    db_set_string.argtypes = [POINTER(dbString), String]
    db_set_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 322
if _libs["grass_dbmibase.8.4"].has("db_set_string_no_copy", "cdecl"):
    db_set_string_no_copy = _libs["grass_dbmibase.8.4"].get("db_set_string_no_copy", "cdecl")
    db_set_string_no_copy.argtypes = [POINTER(dbString), String]
    db_set_string_no_copy.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 323
if _libs["grass_dbmibase.8.4"].has("db_set_table_column", "cdecl"):
    db_set_table_column = _libs["grass_dbmibase.8.4"].get("db_set_table_column", "cdecl")
    db_set_table_column.argtypes = [POINTER(dbTable), c_int, POINTER(dbColumn)]
    db_set_table_column.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 324
if _libs["grass_dbmibase.8.4"].has("db_set_table_delete_priv_granted", "cdecl"):
    db_set_table_delete_priv_granted = _libs["grass_dbmibase.8.4"].get("db_set_table_delete_priv_granted", "cdecl")
    db_set_table_delete_priv_granted.argtypes = [POINTER(dbTable)]
    db_set_table_delete_priv_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 325
if _libs["grass_dbmibase.8.4"].has("db_set_table_delete_priv_not_granted", "cdecl"):
    db_set_table_delete_priv_not_granted = _libs["grass_dbmibase.8.4"].get("db_set_table_delete_priv_not_granted", "cdecl")
    db_set_table_delete_priv_not_granted.argtypes = [POINTER(dbTable)]
    db_set_table_delete_priv_not_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 326
if _libs["grass_dbmibase.8.4"].has("db_set_table_description", "cdecl"):
    db_set_table_description = _libs["grass_dbmibase.8.4"].get("db_set_table_description", "cdecl")
    db_set_table_description.argtypes = [POINTER(dbTable), String]
    db_set_table_description.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 327
if _libs["grass_dbmibase.8.4"].has("db_set_table_insert_priv_granted", "cdecl"):
    db_set_table_insert_priv_granted = _libs["grass_dbmibase.8.4"].get("db_set_table_insert_priv_granted", "cdecl")
    db_set_table_insert_priv_granted.argtypes = [POINTER(dbTable)]
    db_set_table_insert_priv_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 328
if _libs["grass_dbmibase.8.4"].has("db_set_table_insert_priv_not_granted", "cdecl"):
    db_set_table_insert_priv_not_granted = _libs["grass_dbmibase.8.4"].get("db_set_table_insert_priv_not_granted", "cdecl")
    db_set_table_insert_priv_not_granted.argtypes = [POINTER(dbTable)]
    db_set_table_insert_priv_not_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 329
if _libs["grass_dbmibase.8.4"].has("db_set_table_name", "cdecl"):
    db_set_table_name = _libs["grass_dbmibase.8.4"].get("db_set_table_name", "cdecl")
    db_set_table_name.argtypes = [POINTER(dbTable), String]
    db_set_table_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 330
if _libs["grass_dbmibase.8.4"].has("db_set_table_select_priv_granted", "cdecl"):
    db_set_table_select_priv_granted = _libs["grass_dbmibase.8.4"].get("db_set_table_select_priv_granted", "cdecl")
    db_set_table_select_priv_granted.argtypes = [POINTER(dbTable)]
    db_set_table_select_priv_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 331
if _libs["grass_dbmibase.8.4"].has("db_set_table_select_priv_not_granted", "cdecl"):
    db_set_table_select_priv_not_granted = _libs["grass_dbmibase.8.4"].get("db_set_table_select_priv_not_granted", "cdecl")
    db_set_table_select_priv_not_granted.argtypes = [POINTER(dbTable)]
    db_set_table_select_priv_not_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 332
if _libs["grass_dbmibase.8.4"].has("db_set_table_update_priv_granted", "cdecl"):
    db_set_table_update_priv_granted = _libs["grass_dbmibase.8.4"].get("db_set_table_update_priv_granted", "cdecl")
    db_set_table_update_priv_granted.argtypes = [POINTER(dbTable)]
    db_set_table_update_priv_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 333
if _libs["grass_dbmibase.8.4"].has("db_set_table_update_priv_not_granted", "cdecl"):
    db_set_table_update_priv_not_granted = _libs["grass_dbmibase.8.4"].get("db_set_table_update_priv_not_granted", "cdecl")
    db_set_table_update_priv_not_granted.argtypes = [POINTER(dbTable)]
    db_set_table_update_priv_not_granted.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 334
if _libs["grass_dbmibase.8.4"].has("db_set_value_datetime_current", "cdecl"):
    db_set_value_datetime_current = _libs["grass_dbmibase.8.4"].get("db_set_value_datetime_current", "cdecl")
    db_set_value_datetime_current.argtypes = [POINTER(dbValue)]
    db_set_value_datetime_current.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 335
if _libs["grass_dbmibase.8.4"].has("db_set_value_datetime_not_current", "cdecl"):
    db_set_value_datetime_not_current = _libs["grass_dbmibase.8.4"].get("db_set_value_datetime_not_current", "cdecl")
    db_set_value_datetime_not_current.argtypes = [POINTER(dbValue)]
    db_set_value_datetime_not_current.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 336
if _libs["grass_dbmibase.8.4"].has("db_set_value_day", "cdecl"):
    db_set_value_day = _libs["grass_dbmibase.8.4"].get("db_set_value_day", "cdecl")
    db_set_value_day.argtypes = [POINTER(dbValue), c_int]
    db_set_value_day.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 337
if _libs["grass_dbmibase.8.4"].has("db_set_value_double", "cdecl"):
    db_set_value_double = _libs["grass_dbmibase.8.4"].get("db_set_value_double", "cdecl")
    db_set_value_double.argtypes = [POINTER(dbValue), c_double]
    db_set_value_double.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 338
if _libs["grass_dbmibase.8.4"].has("db_set_value_hour", "cdecl"):
    db_set_value_hour = _libs["grass_dbmibase.8.4"].get("db_set_value_hour", "cdecl")
    db_set_value_hour.argtypes = [POINTER(dbValue), c_int]
    db_set_value_hour.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 339
if _libs["grass_dbmibase.8.4"].has("db_set_value_int", "cdecl"):
    db_set_value_int = _libs["grass_dbmibase.8.4"].get("db_set_value_int", "cdecl")
    db_set_value_int.argtypes = [POINTER(dbValue), c_int]
    db_set_value_int.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 340
if _libs["grass_dbmibase.8.4"].has("db_set_value_minute", "cdecl"):
    db_set_value_minute = _libs["grass_dbmibase.8.4"].get("db_set_value_minute", "cdecl")
    db_set_value_minute.argtypes = [POINTER(dbValue), c_int]
    db_set_value_minute.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 341
if _libs["grass_dbmibase.8.4"].has("db_set_value_month", "cdecl"):
    db_set_value_month = _libs["grass_dbmibase.8.4"].get("db_set_value_month", "cdecl")
    db_set_value_month.argtypes = [POINTER(dbValue), c_int]
    db_set_value_month.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 342
if _libs["grass_dbmibase.8.4"].has("db_set_value_not_null", "cdecl"):
    db_set_value_not_null = _libs["grass_dbmibase.8.4"].get("db_set_value_not_null", "cdecl")
    db_set_value_not_null.argtypes = [POINTER(dbValue)]
    db_set_value_not_null.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 343
if _libs["grass_dbmibase.8.4"].has("db_set_value_null", "cdecl"):
    db_set_value_null = _libs["grass_dbmibase.8.4"].get("db_set_value_null", "cdecl")
    db_set_value_null.argtypes = [POINTER(dbValue)]
    db_set_value_null.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 344
if _libs["grass_dbmibase.8.4"].has("db_set_value_seconds", "cdecl"):
    db_set_value_seconds = _libs["grass_dbmibase.8.4"].get("db_set_value_seconds", "cdecl")
    db_set_value_seconds.argtypes = [POINTER(dbValue), c_double]
    db_set_value_seconds.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 345
if _libs["grass_dbmibase.8.4"].has("db_set_value_string", "cdecl"):
    db_set_value_string = _libs["grass_dbmibase.8.4"].get("db_set_value_string", "cdecl")
    db_set_value_string.argtypes = [POINTER(dbValue), String]
    db_set_value_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 346
if _libs["grass_dbmibase.8.4"].has("db_set_value_year", "cdecl"):
    db_set_value_year = _libs["grass_dbmibase.8.4"].get("db_set_value_year", "cdecl")
    db_set_value_year.argtypes = [POINTER(dbValue), c_int]
    db_set_value_year.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 347
if _libs["grass_dbmiclient.8.4"].has("db_shutdown_driver", "cdecl"):
    db_shutdown_driver = _libs["grass_dbmiclient.8.4"].get("db_shutdown_driver", "cdecl")
    db_shutdown_driver.argtypes = [POINTER(dbDriver)]
    db_shutdown_driver.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 348
if _libs["grass_dbmibase.8.4"].has("db_sqltype_name", "cdecl"):
    db_sqltype_name = _libs["grass_dbmibase.8.4"].get("db_sqltype_name", "cdecl")
    db_sqltype_name.argtypes = [c_int]
    db_sqltype_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 349
if _libs["grass_dbmibase.8.4"].has("db_sqltype_to_Ctype", "cdecl"):
    db_sqltype_to_Ctype = _libs["grass_dbmibase.8.4"].get("db_sqltype_to_Ctype", "cdecl")
    db_sqltype_to_Ctype.argtypes = [c_int]
    db_sqltype_to_Ctype.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 350
if _libs["grass_dbmiclient.8.4"].has("db_start_driver", "cdecl"):
    db_start_driver = _libs["grass_dbmiclient.8.4"].get("db_start_driver", "cdecl")
    db_start_driver.argtypes = [String]
    db_start_driver.restype = POINTER(dbDriver)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 351
if _libs["grass_dbmiclient.8.4"].has("db_start_driver_open_database", "cdecl"):
    db_start_driver_open_database = _libs["grass_dbmiclient.8.4"].get("db_start_driver_open_database", "cdecl")
    db_start_driver_open_database.argtypes = [String, String]
    db_start_driver_open_database.restype = POINTER(dbDriver)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 352
if _libs["grass_dbmibase.8.4"].has("db__start_procedure_call", "cdecl"):
    db__start_procedure_call = _libs["grass_dbmibase.8.4"].get("db__start_procedure_call", "cdecl")
    db__start_procedure_call.argtypes = [c_int]
    db__start_procedure_call.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 353
if _libs["grass_dbmibase.8.4"].has("db_store", "cdecl"):
    db_store = _libs["grass_dbmibase.8.4"].get("db_store", "cdecl")
    db_store.argtypes = [String]
    if sizeof(c_int) == sizeof(c_void_p):
        db_store.restype = ReturnString
    else:
        db_store.restype = String
        db_store.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 354
if _libs["grass_dbmibase.8.4"].has("db_strip", "cdecl"):
    db_strip = _libs["grass_dbmibase.8.4"].get("db_strip", "cdecl")
    db_strip.argtypes = [String]
    db_strip.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 355
if _libs["grass_dbmibase.8.4"].has("db_syserror", "cdecl"):
    db_syserror = _libs["grass_dbmibase.8.4"].get("db_syserror", "cdecl")
    db_syserror.argtypes = [String]
    db_syserror.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 356
if _libs["grass_dbmiclient.8.4"].has("db_table_exists", "cdecl"):
    db_table_exists = _libs["grass_dbmiclient.8.4"].get("db_table_exists", "cdecl")
    db_table_exists.argtypes = [String, String, String]
    db_table_exists.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 357
if _libs["grass_dbmibase.8.4"].has("db_test_column_has_default_value", "cdecl"):
    db_test_column_has_default_value = _libs["grass_dbmibase.8.4"].get("db_test_column_has_default_value", "cdecl")
    db_test_column_has_default_value.argtypes = [POINTER(dbColumn)]
    db_test_column_has_default_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 358
if _libs["grass_dbmibase.8.4"].has("db_test_column_has_defined_default_value", "cdecl"):
    db_test_column_has_defined_default_value = _libs["grass_dbmibase.8.4"].get("db_test_column_has_defined_default_value", "cdecl")
    db_test_column_has_defined_default_value.argtypes = [POINTER(dbColumn)]
    db_test_column_has_defined_default_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 359
if _libs["grass_dbmibase.8.4"].has("db_test_column_has_undefined_default_value", "cdecl"):
    db_test_column_has_undefined_default_value = _libs["grass_dbmibase.8.4"].get("db_test_column_has_undefined_default_value", "cdecl")
    db_test_column_has_undefined_default_value.argtypes = [POINTER(dbColumn)]
    db_test_column_has_undefined_default_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 360
if _libs["grass_dbmibase.8.4"].has("db_test_column_null_allowed", "cdecl"):
    db_test_column_null_allowed = _libs["grass_dbmibase.8.4"].get("db_test_column_null_allowed", "cdecl")
    db_test_column_null_allowed.argtypes = [POINTER(dbColumn)]
    db_test_column_null_allowed.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 361
if _libs["grass_dbmibase.8.4"].has("db_test_column_use_default_value", "cdecl"):
    db_test_column_use_default_value = _libs["grass_dbmibase.8.4"].get("db_test_column_use_default_value", "cdecl")
    db_test_column_use_default_value.argtypes = [POINTER(dbColumn)]
    db_test_column_use_default_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 362
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_any_column_flag", "cdecl"):
    db_test_cursor_any_column_flag = _libs["grass_dbmibase.8.4"].get("db_test_cursor_any_column_flag", "cdecl")
    db_test_cursor_any_column_flag.argtypes = [POINTER(dbCursor)]
    db_test_cursor_any_column_flag.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 363
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_any_column_for_update", "cdecl"):
    db_test_cursor_any_column_for_update = _libs["grass_dbmibase.8.4"].get("db_test_cursor_any_column_for_update", "cdecl")
    db_test_cursor_any_column_for_update.argtypes = [POINTER(dbCursor)]
    db_test_cursor_any_column_for_update.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 364
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_column_flag", "cdecl"):
    db_test_cursor_column_flag = _libs["grass_dbmibase.8.4"].get("db_test_cursor_column_flag", "cdecl")
    db_test_cursor_column_flag.argtypes = [POINTER(dbCursor), c_int]
    db_test_cursor_column_flag.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 365
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_column_for_update", "cdecl"):
    db_test_cursor_column_for_update = _libs["grass_dbmibase.8.4"].get("db_test_cursor_column_for_update", "cdecl")
    db_test_cursor_column_for_update.argtypes = [POINTER(dbCursor), c_int]
    db_test_cursor_column_for_update.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 366
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_mode_insensitive", "cdecl"):
    db_test_cursor_mode_insensitive = _libs["grass_dbmibase.8.4"].get("db_test_cursor_mode_insensitive", "cdecl")
    db_test_cursor_mode_insensitive.argtypes = [POINTER(dbCursor)]
    db_test_cursor_mode_insensitive.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 367
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_mode_scroll", "cdecl"):
    db_test_cursor_mode_scroll = _libs["grass_dbmibase.8.4"].get("db_test_cursor_mode_scroll", "cdecl")
    db_test_cursor_mode_scroll.argtypes = [POINTER(dbCursor)]
    db_test_cursor_mode_scroll.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 368
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_type_fetch", "cdecl"):
    db_test_cursor_type_fetch = _libs["grass_dbmibase.8.4"].get("db_test_cursor_type_fetch", "cdecl")
    db_test_cursor_type_fetch.argtypes = [POINTER(dbCursor)]
    db_test_cursor_type_fetch.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 369
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_type_insert", "cdecl"):
    db_test_cursor_type_insert = _libs["grass_dbmibase.8.4"].get("db_test_cursor_type_insert", "cdecl")
    db_test_cursor_type_insert.argtypes = [POINTER(dbCursor)]
    db_test_cursor_type_insert.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 370
if _libs["grass_dbmibase.8.4"].has("db_test_cursor_type_update", "cdecl"):
    db_test_cursor_type_update = _libs["grass_dbmibase.8.4"].get("db_test_cursor_type_update", "cdecl")
    db_test_cursor_type_update.argtypes = [POINTER(dbCursor)]
    db_test_cursor_type_update.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 371
for _lib in _libs.values():
    if not _lib.has("db__test_database_open", "cdecl"):
        continue
    db__test_database_open = _lib.get("db__test_database_open", "cdecl")
    db__test_database_open.argtypes = []
    db__test_database_open.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 372
if _libs["grass_dbmibase.8.4"].has("db_test_index_type_unique", "cdecl"):
    db_test_index_type_unique = _libs["grass_dbmibase.8.4"].get("db_test_index_type_unique", "cdecl")
    db_test_index_type_unique.argtypes = [POINTER(dbIndex)]
    db_test_index_type_unique.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 373
if _libs["grass_dbmibase.8.4"].has("db_test_value_datetime_current", "cdecl"):
    db_test_value_datetime_current = _libs["grass_dbmibase.8.4"].get("db_test_value_datetime_current", "cdecl")
    db_test_value_datetime_current.argtypes = [POINTER(dbValue)]
    db_test_value_datetime_current.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 374
if _libs["grass_dbmibase.8.4"].has("db_test_value_isnull", "cdecl"):
    db_test_value_isnull = _libs["grass_dbmibase.8.4"].get("db_test_value_isnull", "cdecl")
    db_test_value_isnull.argtypes = [POINTER(dbValue)]
    db_test_value_isnull.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 375
if _libs["grass_dbmibase.8.4"].has("db_unset_column_has_default_value", "cdecl"):
    db_unset_column_has_default_value = _libs["grass_dbmibase.8.4"].get("db_unset_column_has_default_value", "cdecl")
    db_unset_column_has_default_value.argtypes = [POINTER(dbColumn)]
    db_unset_column_has_default_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 376
if _libs["grass_dbmibase.8.4"].has("db_unset_column_null_allowed", "cdecl"):
    db_unset_column_null_allowed = _libs["grass_dbmibase.8.4"].get("db_unset_column_null_allowed", "cdecl")
    db_unset_column_null_allowed.argtypes = [POINTER(dbColumn)]
    db_unset_column_null_allowed.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 377
if _libs["grass_dbmibase.8.4"].has("db_unset_column_use_default_value", "cdecl"):
    db_unset_column_use_default_value = _libs["grass_dbmibase.8.4"].get("db_unset_column_use_default_value", "cdecl")
    db_unset_column_use_default_value.argtypes = [POINTER(dbColumn)]
    db_unset_column_use_default_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 378
if _libs["grass_dbmibase.8.4"].has("db_unset_cursor_column_flag", "cdecl"):
    db_unset_cursor_column_flag = _libs["grass_dbmibase.8.4"].get("db_unset_cursor_column_flag", "cdecl")
    db_unset_cursor_column_flag.argtypes = [POINTER(dbCursor), c_int]
    db_unset_cursor_column_flag.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 379
if _libs["grass_dbmibase.8.4"].has("db_unset_cursor_column_for_update", "cdecl"):
    db_unset_cursor_column_for_update = _libs["grass_dbmibase.8.4"].get("db_unset_cursor_column_for_update", "cdecl")
    db_unset_cursor_column_for_update.argtypes = [POINTER(dbCursor), c_int]
    db_unset_cursor_column_for_update.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 380
if _libs["grass_dbmibase.8.4"].has("db_unset_cursor_mode", "cdecl"):
    db_unset_cursor_mode = _libs["grass_dbmibase.8.4"].get("db_unset_cursor_mode", "cdecl")
    db_unset_cursor_mode.argtypes = [POINTER(dbCursor)]
    db_unset_cursor_mode.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 381
if _libs["grass_dbmibase.8.4"].has("db_unset_cursor_mode_insensitive", "cdecl"):
    db_unset_cursor_mode_insensitive = _libs["grass_dbmibase.8.4"].get("db_unset_cursor_mode_insensitive", "cdecl")
    db_unset_cursor_mode_insensitive.argtypes = [POINTER(dbCursor)]
    db_unset_cursor_mode_insensitive.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 382
if _libs["grass_dbmibase.8.4"].has("db_unset_cursor_mode_scroll", "cdecl"):
    db_unset_cursor_mode_scroll = _libs["grass_dbmibase.8.4"].get("db_unset_cursor_mode_scroll", "cdecl")
    db_unset_cursor_mode_scroll.argtypes = [POINTER(dbCursor)]
    db_unset_cursor_mode_scroll.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 383
if _libs["grass_dbmiclient.8.4"].has("db_update", "cdecl"):
    db_update = _libs["grass_dbmiclient.8.4"].get("db_update", "cdecl")
    db_update.argtypes = [POINTER(dbCursor)]
    db_update.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 384
if _libs["grass_dbmiclient.8.4"].has("db_gversion", "cdecl"):
    db_gversion = _libs["grass_dbmiclient.8.4"].get("db_gversion", "cdecl")
    db_gversion.argtypes = [POINTER(dbDriver), POINTER(dbString), POINTER(dbString)]
    db_gversion.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 385
if _libs["grass_dbmibase.8.4"].has("db_whoami", "cdecl"):
    db_whoami = _libs["grass_dbmibase.8.4"].get("db_whoami", "cdecl")
    db_whoami.argtypes = []
    db_whoami.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 386
if _libs["grass_dbmibase.8.4"].has("db_zero", "cdecl"):
    db_zero = _libs["grass_dbmibase.8.4"].get("db_zero", "cdecl")
    db_zero.argtypes = [POINTER(None), c_int]
    db_zero.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 387
if _libs["grass_dbmibase.8.4"].has("db_zero_string", "cdecl"):
    db_zero_string = _libs["grass_dbmibase.8.4"].get("db_zero_string", "cdecl")
    db_zero_string.argtypes = [POINTER(dbString)]
    db_zero_string.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 388
if _libs["grass_dbmibase.8.4"].has("db_sizeof_string", "cdecl"):
    db_sizeof_string = _libs["grass_dbmibase.8.4"].get("db_sizeof_string", "cdecl")
    db_sizeof_string.argtypes = [POINTER(dbString)]
    db_sizeof_string.restype = c_uint

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 389
if _libs["grass_dbmibase.8.4"].has("db_set_login", "cdecl"):
    db_set_login = _libs["grass_dbmibase.8.4"].get("db_set_login", "cdecl")
    db_set_login.argtypes = [String, String, String, String]
    db_set_login.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 390
if _libs["grass_dbmibase.8.4"].has("db_set_login2", "cdecl"):
    db_set_login2 = _libs["grass_dbmibase.8.4"].get("db_set_login2", "cdecl")
    db_set_login2.argtypes = [String, String, String, String, String, String, c_int]
    db_set_login2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 392
if _libs["grass_dbmibase.8.4"].has("db_get_login", "cdecl"):
    db_get_login = _libs["grass_dbmibase.8.4"].get("db_get_login", "cdecl")
    db_get_login.argtypes = [String, String, POINTER(POINTER(c_char)), POINTER(POINTER(c_char))]
    db_get_login.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 393
if _libs["grass_dbmibase.8.4"].has("db_get_login2", "cdecl"):
    db_get_login2 = _libs["grass_dbmibase.8.4"].get("db_get_login2", "cdecl")
    db_get_login2.argtypes = [String, String, POINTER(POINTER(c_char)), POINTER(POINTER(c_char)), POINTER(POINTER(c_char)), POINTER(POINTER(c_char))]
    db_get_login2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/dbmi.h: 395
if _libs["grass_dbmibase.8.4"].has("db_get_login_dump", "cdecl"):
    db_get_login_dump = _libs["grass_dbmibase.8.4"].get("db_get_login_dump", "cdecl")
    db_get_login_dump.argtypes = [POINTER(FILE)]
    db_get_login_dump.restype = c_int

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 18
try:
    DB_VERSION = '0'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 21
try:
    DB_DEFAULT_DRIVER = 'sqlite'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 27
try:
    DB_PROC_VERSION = 999
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 29
try:
    DB_PROC_CLOSE_DATABASE = 101
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 30
try:
    DB_PROC_CREATE_DATABASE = 102
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 31
try:
    DB_PROC_DELETE_DATABASE = 103
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 32
try:
    DB_PROC_FIND_DATABASE = 104
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 33
try:
    DB_PROC_LIST_DATABASES = 105
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 34
try:
    DB_PROC_OPEN_DATABASE = 106
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 35
try:
    DB_PROC_SHUTDOWN_DRIVER = 107
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 37
try:
    DB_PROC_CLOSE_CURSOR = 201
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 38
try:
    DB_PROC_DELETE = 202
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 39
try:
    DB_PROC_FETCH = 203
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 40
try:
    DB_PROC_INSERT = 204
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 41
try:
    DB_PROC_OPEN_INSERT_CURSOR = 205
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 42
try:
    DB_PROC_OPEN_SELECT_CURSOR = 206
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 43
try:
    DB_PROC_OPEN_UPDATE_CURSOR = 207
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 44
try:
    DB_PROC_UPDATE = 208
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 45
try:
    DB_PROC_ROWS = 209
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 46
try:
    DB_PROC_BIND_UPDATE = 220
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 47
try:
    DB_PROC_BIND_INSERT = 221
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 49
try:
    DB_PROC_EXECUTE_IMMEDIATE = 301
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 50
try:
    DB_PROC_BEGIN_TRANSACTION = 302
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 51
try:
    DB_PROC_COMMIT_TRANSACTION = 303
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 53
try:
    DB_PROC_CREATE_TABLE = 401
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 54
try:
    DB_PROC_DESCRIBE_TABLE = 402
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 55
try:
    DB_PROC_DROP_TABLE = 403
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 56
try:
    DB_PROC_LIST_TABLES = 404
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 57
try:
    DB_PROC_ADD_COLUMN = 405
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 58
try:
    DB_PROC_DROP_COLUMN = 406
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 59
try:
    DB_PROC_GRANT_ON_TABLE = 407
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 61
try:
    DB_PROC_CREATE_INDEX = 701
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 62
try:
    DB_PROC_LIST_INDEXES = 702
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 63
try:
    DB_PROC_DROP_INDEX = 703
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 66
try:
    DB_PERM_R = 0o1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 67
try:
    DB_PERM_W = 0o2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 68
try:
    DB_PERM_X = 0o4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 71
try:
    DB_OK = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 72
try:
    DB_FAILED = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 73
try:
    DB_NOPROC = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 74
try:
    DB_MEMORY_ERR = (-1)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 75
try:
    DB_PROTOCOL_ERR = (-2)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 76
try:
    DB_EOF = (-1)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 79
try:
    DB_SQL_TYPE_UNKNOWN = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 81
try:
    DB_SQL_TYPE_CHARACTER = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 82
try:
    DB_SQL_TYPE_SMALLINT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 83
try:
    DB_SQL_TYPE_INTEGER = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 84
try:
    DB_SQL_TYPE_REAL = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 85
try:
    DB_SQL_TYPE_DOUBLE_PRECISION = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 86
try:
    DB_SQL_TYPE_DECIMAL = 7
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 87
try:
    DB_SQL_TYPE_NUMERIC = 8
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 88
try:
    DB_SQL_TYPE_DATE = 9
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 89
try:
    DB_SQL_TYPE_TIME = 10
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 90
try:
    DB_SQL_TYPE_TIMESTAMP = 11
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 91
try:
    DB_SQL_TYPE_INTERVAL = 12
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 92
try:
    DB_SQL_TYPE_TEXT = 13
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 94
try:
    DB_SQL_TYPE_SERIAL = 21
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 97
try:
    DB_YEAR = 0x4000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 98
try:
    DB_MONTH = 0x2000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 99
try:
    DB_DAY = 0x1000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 100
try:
    DB_HOUR = 0x0800
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 101
try:
    DB_MINUTE = 0x0400
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 102
try:
    DB_SECOND = 0x0200
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 103
try:
    DB_FRACTION = 0x0100
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 104
try:
    DB_DATETIME_MASK = 0xFF00
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 107
try:
    DB_C_TYPE_STRING = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 108
try:
    DB_C_TYPE_INT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 109
try:
    DB_C_TYPE_DOUBLE = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 110
try:
    DB_C_TYPE_DATETIME = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 113
try:
    DB_CURRENT = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 114
try:
    DB_NEXT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 115
try:
    DB_PREVIOUS = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 116
try:
    DB_FIRST = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 117
try:
    DB_LAST = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 120
try:
    DB_READONLY = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 121
try:
    DB_INSERT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 122
try:
    DB_UPDATE = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 123
try:
    DB_SEQUENTIAL = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 124
try:
    DB_SCROLL = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 125
try:
    DB_INSENSITIVE = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 128
try:
    DB_GRANTED = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 129
try:
    DB_NOT_GRANTED = (-1)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 132
try:
    DB_PRIV_SELECT = 0x01
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 134
try:
    DB_GROUP = 0x01
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 135
try:
    DB_PUBLIC = 0x02
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 138
try:
    DB_DEFINED = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 139
try:
    DB_UNDEFINED = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 142
try:
    DB_SQL_MAX = 65536
except:
    pass

_db_string = struct__db_string# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 150

_dbmscap = struct__dbmscap# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 152

_db_dirent = struct__db_dirent# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 163

_db_driver = struct__db_driver# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 169

_db_handle = struct__db_handle# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 175

_db_date_time = struct__db_date_time# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 185

_db_value = struct__db_value# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 193

_db_column = struct__db_column# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 210

_db_table = struct__db_table# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 219

_db_cursor = struct__db_cursor# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 228

_db_index = struct__db_index# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 236

_db_driver_state = struct__db_driver_state# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 244

_db_connection = struct__db_connection# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\dbmi.h: 287

# No inserted files

# No prefix-stripping

