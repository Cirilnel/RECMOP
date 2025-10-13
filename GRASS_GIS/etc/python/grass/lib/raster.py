r"""Wrapper for raster.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_raster.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/raster.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h -o OBJ.x86_64-w64-mingw32/raster.py

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
_libs["grass_raster.8.4"] = load_library("grass_raster.8.4")

# 1 libraries
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

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 439
class struct_Cell_head(Structure):
    pass

struct_Cell_head.__slots__ = [
    'format',
    'compressed',
    'rows',
    'rows3',
    'cols',
    'cols3',
    'depths',
    'proj',
    'zone',
    'ew_res',
    'ew_res3',
    'ns_res',
    'ns_res3',
    'tb_res',
    'north',
    'south',
    'east',
    'west',
    'top',
    'bottom',
]
struct_Cell_head._fields_ = [
    ('format', c_int),
    ('compressed', c_int),
    ('rows', c_int),
    ('rows3', c_int),
    ('cols', c_int),
    ('cols3', c_int),
    ('depths', c_int),
    ('proj', c_int),
    ('zone', c_int),
    ('ew_res', c_double),
    ('ew_res3', c_double),
    ('ns_res', c_double),
    ('ns_res3', c_double),
    ('tb_res', c_double),
    ('north', c_double),
    ('south', c_double),
    ('east', c_double),
    ('west', c_double),
    ('top', c_double),
    ('bottom', c_double),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 556
class struct_Option(Structure):
    pass

struct_Option.__slots__ = [
    'key',
    'type',
    'required',
    'multiple',
    'options',
    'opts',
    'key_desc',
    'label',
    'description',
    'descriptions',
    'descs',
    'answer',
    'def',
    'answers',
    'next_opt',
    'gisprompt',
    'guisection',
    'guidependency',
    'checker',
    'count',
]
struct_Option._fields_ = [
    ('key', String),
    ('type', c_int),
    ('required', c_int),
    ('multiple', c_int),
    ('options', String),
    ('opts', POINTER(POINTER(c_char))),
    ('key_desc', String),
    ('label', String),
    ('description', String),
    ('descriptions', String),
    ('descs', POINTER(POINTER(c_char))),
    ('answer', String),
    ('def', String),
    ('answers', POINTER(POINTER(c_char))),
    ('next_opt', POINTER(struct_Option)),
    ('gisprompt', String),
    ('guisection', String),
    ('guidependency', String),
    ('checker', CFUNCTYPE(UNCHECKED(c_int), String)),
    ('count', c_int),
]

CELL = c_int# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 627

DCELL = c_double# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 628

FCELL = c_float# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 629

grass_int64 = c_int64# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 636

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 648
class struct__Color_Value_(Structure):
    pass

struct__Color_Value_.__slots__ = [
    'value',
    'red',
    'grn',
    'blu',
]
struct__Color_Value_._fields_ = [
    ('value', DCELL),
    ('red', c_ubyte),
    ('grn', c_ubyte),
    ('blu', c_ubyte),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 655
class struct__Color_Rule_(Structure):
    pass

struct__Color_Rule_.__slots__ = [
    'low',
    'high',
    'next',
    'prev',
]
struct__Color_Rule_._fields_ = [
    ('low', struct__Color_Value_),
    ('high', struct__Color_Value_),
    ('next', POINTER(struct__Color_Rule_)),
    ('prev', POINTER(struct__Color_Rule_)),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 665
class struct_anon_5(Structure):
    pass

struct_anon_5.__slots__ = [
    'red',
    'grn',
    'blu',
    'set',
    'nalloc',
    'active',
]
struct_anon_5._fields_ = [
    ('red', POINTER(c_ubyte)),
    ('grn', POINTER(c_ubyte)),
    ('blu', POINTER(c_ubyte)),
    ('set', POINTER(c_ubyte)),
    ('nalloc', c_int),
    ('active', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 674
class struct_anon_6(Structure):
    pass

struct_anon_6.__slots__ = [
    'vals',
    'rules',
    'nalloc',
    'active',
]
struct_anon_6._fields_ = [
    ('vals', POINTER(DCELL)),
    ('rules', POINTER(POINTER(struct__Color_Rule_))),
    ('nalloc', c_int),
    ('active', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 661
class struct__Color_Info_(Structure):
    pass

struct__Color_Info_.__slots__ = [
    'rules',
    'n_rules',
    'lookup',
    'fp_lookup',
    'min',
    'max',
]
struct__Color_Info_._fields_ = [
    ('rules', POINTER(struct__Color_Rule_)),
    ('n_rules', c_int),
    ('lookup', struct_anon_5),
    ('fp_lookup', struct_anon_6),
    ('min', DCELL),
    ('max', DCELL),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 685
class struct_Colors(Structure):
    pass

struct_Colors.__slots__ = [
    'version',
    'shift',
    'invert',
    'is_float',
    'null_set',
    'null_red',
    'null_grn',
    'null_blu',
    'undef_set',
    'undef_red',
    'undef_grn',
    'undef_blu',
    'fixed',
    'modular',
    'cmin',
    'cmax',
    'organizing',
]
struct_Colors._fields_ = [
    ('version', c_int),
    ('shift', DCELL),
    ('invert', c_int),
    ('is_float', c_int),
    ('null_set', c_int),
    ('null_red', c_ubyte),
    ('null_grn', c_ubyte),
    ('null_blu', c_ubyte),
    ('undef_set', c_int),
    ('undef_red', c_ubyte),
    ('undef_grn', c_ubyte),
    ('undef_blu', c_ubyte),
    ('fixed', struct__Color_Info_),
    ('modular', struct__Color_Info_),
    ('cmin', DCELL),
    ('cmax', DCELL),
    ('organizing', c_int),
]

RASTER_MAP_TYPE = c_int# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 25

INTERP_TYPE = c_int# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 28

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 31
class struct_Reclass(Structure):
    pass

struct_Reclass.__slots__ = [
    'name',
    'mapset',
    'type',
    'num',
    'min',
    'max',
    'table',
]
struct_Reclass._fields_ = [
    ('name', String),
    ('mapset', String),
    ('type', c_int),
    ('num', c_int),
    ('min', CELL),
    ('max', CELL),
    ('table', POINTER(CELL)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 41
class struct_FPReclass_table(Structure):
    pass

struct_FPReclass_table.__slots__ = [
    'dLow',
    'dHigh',
    'rLow',
    'rHigh',
]
struct_FPReclass_table._fields_ = [
    ('dLow', DCELL),
    ('dHigh', DCELL),
    ('rLow', DCELL),
    ('rHigh', DCELL),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 50
class struct_FPReclass(Structure):
    pass

struct_FPReclass.__slots__ = [
    'defaultDRuleSet',
    'defaultRRuleSet',
    'infiniteLeftSet',
    'infiniteRightSet',
    'rRangeSet',
    'maxNofRules',
    'nofRules',
    'defaultDMin',
    'defaultDMax',
    'defaultRMin',
    'defaultRMax',
    'infiniteDLeft',
    'infiniteDRight',
    'infiniteRLeft',
    'infiniteRRight',
    'dMin',
    'dMax',
    'rMin',
    'rMax',
    'table',
]
struct_FPReclass._fields_ = [
    ('defaultDRuleSet', c_int),
    ('defaultRRuleSet', c_int),
    ('infiniteLeftSet', c_int),
    ('infiniteRightSet', c_int),
    ('rRangeSet', c_int),
    ('maxNofRules', c_int),
    ('nofRules', c_int),
    ('defaultDMin', DCELL),
    ('defaultDMax', DCELL),
    ('defaultRMin', DCELL),
    ('defaultRMax', DCELL),
    ('infiniteDLeft', DCELL),
    ('infiniteDRight', DCELL),
    ('infiniteRLeft', DCELL),
    ('infiniteRRight', DCELL),
    ('dMin', DCELL),
    ('dMax', DCELL),
    ('rMin', DCELL),
    ('rMax', DCELL),
    ('table', POINTER(struct_FPReclass_table)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 73
class struct_Quant_table(Structure):
    pass

struct_Quant_table.__slots__ = [
    'dLow',
    'dHigh',
    'cLow',
    'cHigh',
]
struct_Quant_table._fields_ = [
    ('dLow', DCELL),
    ('dHigh', DCELL),
    ('cLow', CELL),
    ('cHigh', CELL),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 104
class struct_anon_7(Structure):
    pass

struct_anon_7.__slots__ = [
    'vals',
    'rules',
    'nalloc',
    'active',
    'inf_dmin',
    'inf_dmax',
    'inf_min',
    'inf_max',
]
struct_anon_7._fields_ = [
    ('vals', POINTER(DCELL)),
    ('rules', POINTER(POINTER(struct_Quant_table))),
    ('nalloc', c_int),
    ('active', c_int),
    ('inf_dmin', DCELL),
    ('inf_dmax', DCELL),
    ('inf_min', CELL),
    ('inf_max', CELL),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 80
class struct_Quant(Structure):
    pass

struct_Quant.__slots__ = [
    'truncate_only',
    'round_only',
    'defaultDRuleSet',
    'defaultCRuleSet',
    'infiniteLeftSet',
    'infiniteRightSet',
    'cRangeSet',
    'maxNofRules',
    'nofRules',
    'defaultDMin',
    'defaultDMax',
    'defaultCMin',
    'defaultCMax',
    'infiniteDLeft',
    'infiniteDRight',
    'infiniteCLeft',
    'infiniteCRight',
    'dMin',
    'dMax',
    'cMin',
    'cMax',
    'table',
    'fp_lookup',
]
struct_Quant._fields_ = [
    ('truncate_only', c_int),
    ('round_only', c_int),
    ('defaultDRuleSet', c_int),
    ('defaultCRuleSet', c_int),
    ('infiniteLeftSet', c_int),
    ('infiniteRightSet', c_int),
    ('cRangeSet', c_int),
    ('maxNofRules', c_int),
    ('nofRules', c_int),
    ('defaultDMin', DCELL),
    ('defaultDMax', DCELL),
    ('defaultCMin', CELL),
    ('defaultCMax', CELL),
    ('infiniteDLeft', DCELL),
    ('infiniteDRight', DCELL),
    ('infiniteCLeft', CELL),
    ('infiniteCRight', CELL),
    ('dMin', DCELL),
    ('dMax', DCELL),
    ('cMin', CELL),
    ('cMax', CELL),
    ('table', POINTER(struct_Quant_table)),
    ('fp_lookup', struct_anon_7),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 121
class struct_Categories(Structure):
    pass

struct_Categories.__slots__ = [
    'ncats',
    'num',
    'title',
    'fmt',
    'm1',
    'a1',
    'm2',
    'a2',
    'q',
    'labels',
    'marks',
    'nalloc',
    'last_marked_rule',
]
struct_Categories._fields_ = [
    ('ncats', CELL),
    ('num', CELL),
    ('title', String),
    ('fmt', String),
    ('m1', c_float),
    ('a1', c_float),
    ('m2', c_float),
    ('a2', c_float),
    ('q', struct_Quant),
    ('labels', POINTER(POINTER(c_char))),
    ('marks', POINTER(c_int)),
    ('nalloc', c_int),
    ('last_marked_rule', c_int),
]

enum_History_field = c_int# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_MAPID = 0# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_TITLE = (HIST_MAPID + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_MAPSET = (HIST_TITLE + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_CREATOR = (HIST_MAPSET + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_MAPTYPE = (HIST_CREATOR + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_DATSRC_1 = (HIST_MAPTYPE + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_DATSRC_2 = (HIST_DATSRC_1 + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_KEYWRD = (HIST_DATSRC_2 + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

HIST_NUM_FIELDS = (HIST_KEYWRD + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 150

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 172
class struct_History(Structure):
    pass

struct_History.__slots__ = [
    'fields',
    'nlines',
    'lines',
]
struct_History._fields_ = [
    ('fields', POINTER(c_char) * int(HIST_NUM_FIELDS)),
    ('nlines', c_int),
    ('lines', POINTER(POINTER(c_char))),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 182
class struct_Cell_stats_node(Structure):
    pass

struct_Cell_stats_node.__slots__ = [
    'idx',
    'count',
    'left',
    'right',
]
struct_Cell_stats_node._fields_ = [
    ('idx', c_int),
    ('count', POINTER(c_long)),
    ('left', c_int),
    ('right', c_int),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 181
class struct_Cell_stats(Structure):
    pass

struct_Cell_stats.__slots__ = [
    'node',
    'tlen',
    'N',
    'curp',
    'null_data_count',
    'curoffset',
]
struct_Cell_stats._fields_ = [
    ('node', POINTER(struct_Cell_stats_node)),
    ('tlen', c_int),
    ('N', c_int),
    ('curp', c_int),
    ('null_data_count', c_long),
    ('curoffset', c_int),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 199
class struct_Histogram_list(Structure):
    pass

struct_Histogram_list.__slots__ = [
    'cat',
    'count',
]
struct_Histogram_list._fields_ = [
    ('cat', CELL),
    ('count', c_long),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 196
class struct_Histogram(Structure):
    pass

struct_Histogram.__slots__ = [
    'num',
    'list',
]
struct_Histogram._fields_ = [
    ('num', c_int),
    ('list', POINTER(struct_Histogram_list)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 205
class struct_R_stats(Structure):
    pass

struct_R_stats.__slots__ = [
    'sum',
    'sumsq',
    'count',
]
struct_R_stats._fields_ = [
    ('sum', DCELL),
    ('sumsq', DCELL),
    ('count', grass_int64),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 211
class struct_Range(Structure):
    pass

struct_Range.__slots__ = [
    'min',
    'max',
    'first_time',
    'rstats',
]
struct_Range._fields_ = [
    ('min', CELL),
    ('max', CELL),
    ('first_time', c_int),
    ('rstats', struct_R_stats),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 218
class struct_FPRange(Structure):
    pass

struct_FPRange.__slots__ = [
    'min',
    'max',
    'first_time',
    'rstats',
]
struct_FPRange._fields_ = [
    ('min', DCELL),
    ('max', DCELL),
    ('first_time', c_int),
    ('rstats', struct_R_stats),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 225
class struct_FP_stats(Structure):
    pass

struct_FP_stats.__slots__ = [
    'geometric',
    'geom_abs',
    'flip',
    'count',
    'min',
    'max',
    'stats',
    'total',
]
struct_FP_stats._fields_ = [
    ('geometric', c_int),
    ('geom_abs', c_int),
    ('flip', c_int),
    ('count', c_int),
    ('min', DCELL),
    ('max', DCELL),
    ('stats', POINTER(c_ulong)),
    ('total', c_ulong),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 235
class struct_GDAL_link(Structure):
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 236
class struct_R_vrt(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 9
if _libs["grass_raster.8.4"].has("Rast_align_window", "cdecl"):
    Rast_align_window = _libs["grass_raster.8.4"].get("Rast_align_window", "cdecl")
    Rast_align_window.argtypes = [POINTER(struct_Cell_head), POINTER(struct_Cell_head)]
    Rast_align_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 12
if _libs["grass_raster.8.4"].has("Rast_cell_size", "cdecl"):
    Rast_cell_size = _libs["grass_raster.8.4"].get("Rast_cell_size", "cdecl")
    Rast_cell_size.argtypes = [RASTER_MAP_TYPE]
    Rast_cell_size.restype = c_size_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 13
if _libs["grass_raster.8.4"].has("Rast_allocate_buf", "cdecl"):
    Rast_allocate_buf = _libs["grass_raster.8.4"].get("Rast_allocate_buf", "cdecl")
    Rast_allocate_buf.argtypes = [RASTER_MAP_TYPE]
    Rast_allocate_buf.restype = POINTER(c_ubyte)
    Rast_allocate_buf.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 14
if _libs["grass_raster.8.4"].has("Rast_allocate_c_buf", "cdecl"):
    Rast_allocate_c_buf = _libs["grass_raster.8.4"].get("Rast_allocate_c_buf", "cdecl")
    Rast_allocate_c_buf.argtypes = []
    Rast_allocate_c_buf.restype = POINTER(CELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 15
if _libs["grass_raster.8.4"].has("Rast_allocate_f_buf", "cdecl"):
    Rast_allocate_f_buf = _libs["grass_raster.8.4"].get("Rast_allocate_f_buf", "cdecl")
    Rast_allocate_f_buf.argtypes = []
    Rast_allocate_f_buf.restype = POINTER(FCELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 16
if _libs["grass_raster.8.4"].has("Rast_allocate_d_buf", "cdecl"):
    Rast_allocate_d_buf = _libs["grass_raster.8.4"].get("Rast_allocate_d_buf", "cdecl")
    Rast_allocate_d_buf.argtypes = []
    Rast_allocate_d_buf.restype = POINTER(DCELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 17
if _libs["grass_raster.8.4"].has("Rast_allocate_null_buf", "cdecl"):
    Rast_allocate_null_buf = _libs["grass_raster.8.4"].get("Rast_allocate_null_buf", "cdecl")
    Rast_allocate_null_buf.argtypes = []
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_allocate_null_buf.restype = ReturnString
    else:
        Rast_allocate_null_buf.restype = String
        Rast_allocate_null_buf.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 18
if _libs["grass_raster.8.4"].has("Rast__allocate_null_bits", "cdecl"):
    Rast__allocate_null_bits = _libs["grass_raster.8.4"].get("Rast__allocate_null_bits", "cdecl")
    Rast__allocate_null_bits.argtypes = [c_int]
    Rast__allocate_null_bits.restype = POINTER(c_ubyte)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 19
if _libs["grass_raster.8.4"].has("Rast__null_bitstream_size", "cdecl"):
    Rast__null_bitstream_size = _libs["grass_raster.8.4"].get("Rast__null_bitstream_size", "cdecl")
    Rast__null_bitstream_size.argtypes = [c_int]
    Rast__null_bitstream_size.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 21
if _libs["grass_raster.8.4"].has("Rast_allocate_input_buf", "cdecl"):
    Rast_allocate_input_buf = _libs["grass_raster.8.4"].get("Rast_allocate_input_buf", "cdecl")
    Rast_allocate_input_buf.argtypes = [RASTER_MAP_TYPE]
    Rast_allocate_input_buf.restype = POINTER(c_ubyte)
    Rast_allocate_input_buf.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 22
if _libs["grass_raster.8.4"].has("Rast_allocate_c_input_buf", "cdecl"):
    Rast_allocate_c_input_buf = _libs["grass_raster.8.4"].get("Rast_allocate_c_input_buf", "cdecl")
    Rast_allocate_c_input_buf.argtypes = []
    Rast_allocate_c_input_buf.restype = POINTER(CELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 23
if _libs["grass_raster.8.4"].has("Rast_allocate_f_input_buf", "cdecl"):
    Rast_allocate_f_input_buf = _libs["grass_raster.8.4"].get("Rast_allocate_f_input_buf", "cdecl")
    Rast_allocate_f_input_buf.argtypes = []
    Rast_allocate_f_input_buf.restype = POINTER(FCELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 24
if _libs["grass_raster.8.4"].has("Rast_allocate_d_input_buf", "cdecl"):
    Rast_allocate_d_input_buf = _libs["grass_raster.8.4"].get("Rast_allocate_d_input_buf", "cdecl")
    Rast_allocate_d_input_buf.argtypes = []
    Rast_allocate_d_input_buf.restype = POINTER(DCELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 25
if _libs["grass_raster.8.4"].has("Rast_allocate_null_input_buf", "cdecl"):
    Rast_allocate_null_input_buf = _libs["grass_raster.8.4"].get("Rast_allocate_null_input_buf", "cdecl")
    Rast_allocate_null_input_buf.argtypes = []
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_allocate_null_input_buf.restype = ReturnString
    else:
        Rast_allocate_null_input_buf.restype = String
        Rast_allocate_null_input_buf.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 27
if _libs["grass_raster.8.4"].has("Rast_allocate_output_buf", "cdecl"):
    Rast_allocate_output_buf = _libs["grass_raster.8.4"].get("Rast_allocate_output_buf", "cdecl")
    Rast_allocate_output_buf.argtypes = [RASTER_MAP_TYPE]
    Rast_allocate_output_buf.restype = POINTER(c_ubyte)
    Rast_allocate_output_buf.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 28
if _libs["grass_raster.8.4"].has("Rast_allocate_c_output_buf", "cdecl"):
    Rast_allocate_c_output_buf = _libs["grass_raster.8.4"].get("Rast_allocate_c_output_buf", "cdecl")
    Rast_allocate_c_output_buf.argtypes = []
    Rast_allocate_c_output_buf.restype = POINTER(CELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 29
if _libs["grass_raster.8.4"].has("Rast_allocate_f_output_buf", "cdecl"):
    Rast_allocate_f_output_buf = _libs["grass_raster.8.4"].get("Rast_allocate_f_output_buf", "cdecl")
    Rast_allocate_f_output_buf.argtypes = []
    Rast_allocate_f_output_buf.restype = POINTER(FCELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 30
if _libs["grass_raster.8.4"].has("Rast_allocate_d_output_buf", "cdecl"):
    Rast_allocate_d_output_buf = _libs["grass_raster.8.4"].get("Rast_allocate_d_output_buf", "cdecl")
    Rast_allocate_d_output_buf.argtypes = []
    Rast_allocate_d_output_buf.restype = POINTER(DCELL)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 31
if _libs["grass_raster.8.4"].has("Rast_allocate_null_output_buf", "cdecl"):
    Rast_allocate_null_output_buf = _libs["grass_raster.8.4"].get("Rast_allocate_null_output_buf", "cdecl")
    Rast_allocate_null_output_buf.argtypes = []
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_allocate_null_output_buf.restype = ReturnString
    else:
        Rast_allocate_null_output_buf.restype = String
        Rast_allocate_null_output_buf.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 34
if _libs["grass_raster.8.4"].has("Rast__check_for_auto_masking", "cdecl"):
    Rast__check_for_auto_masking = _libs["grass_raster.8.4"].get("Rast__check_for_auto_masking", "cdecl")
    Rast__check_for_auto_masking.argtypes = []
    Rast__check_for_auto_masking.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 35
if _libs["grass_raster.8.4"].has("Rast_suppress_masking", "cdecl"):
    Rast_suppress_masking = _libs["grass_raster.8.4"].get("Rast_suppress_masking", "cdecl")
    Rast_suppress_masking.argtypes = []
    Rast_suppress_masking.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 36
if _libs["grass_raster.8.4"].has("Rast_unsuppress_masking", "cdecl"):
    Rast_unsuppress_masking = _libs["grass_raster.8.4"].get("Rast_unsuppress_masking", "cdecl")
    Rast_unsuppress_masking.argtypes = []
    Rast_unsuppress_masking.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 39
if _libs["grass_raster.8.4"].has("Rast_read_cats", "cdecl"):
    Rast_read_cats = _libs["grass_raster.8.4"].get("Rast_read_cats", "cdecl")
    Rast_read_cats.argtypes = [String, String, POINTER(struct_Categories)]
    Rast_read_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 40
if _libs["grass_raster.8.4"].has("Rast_read_vector_cats", "cdecl"):
    Rast_read_vector_cats = _libs["grass_raster.8.4"].get("Rast_read_vector_cats", "cdecl")
    Rast_read_vector_cats.argtypes = [String, String, POINTER(struct_Categories)]
    Rast_read_vector_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 41
if _libs["grass_raster.8.4"].has("Rast_get_max_c_cat", "cdecl"):
    Rast_get_max_c_cat = _libs["grass_raster.8.4"].get("Rast_get_max_c_cat", "cdecl")
    Rast_get_max_c_cat.argtypes = [String, String]
    Rast_get_max_c_cat.restype = CELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 42
if _libs["grass_raster.8.4"].has("Rast_get_cats_title", "cdecl"):
    Rast_get_cats_title = _libs["grass_raster.8.4"].get("Rast_get_cats_title", "cdecl")
    Rast_get_cats_title.argtypes = [POINTER(struct_Categories)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_cats_title.restype = ReturnString
    else:
        Rast_get_cats_title.restype = String
        Rast_get_cats_title.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 43
if _libs["grass_raster.8.4"].has("Rast_get_c_cat", "cdecl"):
    Rast_get_c_cat = _libs["grass_raster.8.4"].get("Rast_get_c_cat", "cdecl")
    Rast_get_c_cat.argtypes = [POINTER(CELL), POINTER(struct_Categories)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_c_cat.restype = ReturnString
    else:
        Rast_get_c_cat.restype = String
        Rast_get_c_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 44
if _libs["grass_raster.8.4"].has("Rast_get_f_cat", "cdecl"):
    Rast_get_f_cat = _libs["grass_raster.8.4"].get("Rast_get_f_cat", "cdecl")
    Rast_get_f_cat.argtypes = [POINTER(FCELL), POINTER(struct_Categories)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_f_cat.restype = ReturnString
    else:
        Rast_get_f_cat.restype = String
        Rast_get_f_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 45
if _libs["grass_raster.8.4"].has("Rast_get_d_cat", "cdecl"):
    Rast_get_d_cat = _libs["grass_raster.8.4"].get("Rast_get_d_cat", "cdecl")
    Rast_get_d_cat.argtypes = [POINTER(DCELL), POINTER(struct_Categories)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_d_cat.restype = ReturnString
    else:
        Rast_get_d_cat.restype = String
        Rast_get_d_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 46
if _libs["grass_raster.8.4"].has("Rast_get_cat", "cdecl"):
    Rast_get_cat = _libs["grass_raster.8.4"].get("Rast_get_cat", "cdecl")
    Rast_get_cat.argtypes = [POINTER(None), POINTER(struct_Categories), RASTER_MAP_TYPE]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_cat.restype = ReturnString
    else:
        Rast_get_cat.restype = String
        Rast_get_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 47
if _libs["grass_raster.8.4"].has("Rast_unmark_cats", "cdecl"):
    Rast_unmark_cats = _libs["grass_raster.8.4"].get("Rast_unmark_cats", "cdecl")
    Rast_unmark_cats.argtypes = [POINTER(struct_Categories)]
    Rast_unmark_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 48
if _libs["grass_raster.8.4"].has("Rast_mark_c_cats", "cdecl"):
    Rast_mark_c_cats = _libs["grass_raster.8.4"].get("Rast_mark_c_cats", "cdecl")
    Rast_mark_c_cats.argtypes = [POINTER(CELL), c_int, POINTER(struct_Categories)]
    Rast_mark_c_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 49
if _libs["grass_raster.8.4"].has("Rast_mark_f_cats", "cdecl"):
    Rast_mark_f_cats = _libs["grass_raster.8.4"].get("Rast_mark_f_cats", "cdecl")
    Rast_mark_f_cats.argtypes = [POINTER(FCELL), c_int, POINTER(struct_Categories)]
    Rast_mark_f_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 50
if _libs["grass_raster.8.4"].has("Rast_mark_d_cats", "cdecl"):
    Rast_mark_d_cats = _libs["grass_raster.8.4"].get("Rast_mark_d_cats", "cdecl")
    Rast_mark_d_cats.argtypes = [POINTER(DCELL), c_int, POINTER(struct_Categories)]
    Rast_mark_d_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 51
if _libs["grass_raster.8.4"].has("Rast_mark_cats", "cdecl"):
    Rast_mark_cats = _libs["grass_raster.8.4"].get("Rast_mark_cats", "cdecl")
    Rast_mark_cats.argtypes = [POINTER(None), c_int, POINTER(struct_Categories), RASTER_MAP_TYPE]
    Rast_mark_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 52
if _libs["grass_raster.8.4"].has("Rast_rewind_cats", "cdecl"):
    Rast_rewind_cats = _libs["grass_raster.8.4"].get("Rast_rewind_cats", "cdecl")
    Rast_rewind_cats.argtypes = [POINTER(struct_Categories)]
    Rast_rewind_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 53
if _libs["grass_raster.8.4"].has("Rast_get_next_marked_d_cat", "cdecl"):
    Rast_get_next_marked_d_cat = _libs["grass_raster.8.4"].get("Rast_get_next_marked_d_cat", "cdecl")
    Rast_get_next_marked_d_cat.argtypes = [POINTER(struct_Categories), POINTER(DCELL), POINTER(DCELL), POINTER(c_long)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_next_marked_d_cat.restype = ReturnString
    else:
        Rast_get_next_marked_d_cat.restype = String
        Rast_get_next_marked_d_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 54
if _libs["grass_raster.8.4"].has("Rast_get_next_marked_c_cat", "cdecl"):
    Rast_get_next_marked_c_cat = _libs["grass_raster.8.4"].get("Rast_get_next_marked_c_cat", "cdecl")
    Rast_get_next_marked_c_cat.argtypes = [POINTER(struct_Categories), POINTER(CELL), POINTER(CELL), POINTER(c_long)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_next_marked_c_cat.restype = ReturnString
    else:
        Rast_get_next_marked_c_cat.restype = String
        Rast_get_next_marked_c_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 55
if _libs["grass_raster.8.4"].has("Rast_get_next_marked_f_cat", "cdecl"):
    Rast_get_next_marked_f_cat = _libs["grass_raster.8.4"].get("Rast_get_next_marked_f_cat", "cdecl")
    Rast_get_next_marked_f_cat.argtypes = [POINTER(struct_Categories), POINTER(FCELL), POINTER(FCELL), POINTER(c_long)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_next_marked_f_cat.restype = ReturnString
    else:
        Rast_get_next_marked_f_cat.restype = String
        Rast_get_next_marked_f_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 56
if _libs["grass_raster.8.4"].has("Rast_get_next_marked_cat", "cdecl"):
    Rast_get_next_marked_cat = _libs["grass_raster.8.4"].get("Rast_get_next_marked_cat", "cdecl")
    Rast_get_next_marked_cat.argtypes = [POINTER(struct_Categories), POINTER(None), POINTER(None), POINTER(c_long), RASTER_MAP_TYPE]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_next_marked_cat.restype = ReturnString
    else:
        Rast_get_next_marked_cat.restype = String
        Rast_get_next_marked_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 58
if _libs["grass_raster.8.4"].has("Rast_set_c_cat", "cdecl"):
    Rast_set_c_cat = _libs["grass_raster.8.4"].get("Rast_set_c_cat", "cdecl")
    Rast_set_c_cat.argtypes = [POINTER(CELL), POINTER(CELL), String, POINTER(struct_Categories)]
    Rast_set_c_cat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 60
if _libs["grass_raster.8.4"].has("Rast_set_f_cat", "cdecl"):
    Rast_set_f_cat = _libs["grass_raster.8.4"].get("Rast_set_f_cat", "cdecl")
    Rast_set_f_cat.argtypes = [POINTER(FCELL), POINTER(FCELL), String, POINTER(struct_Categories)]
    Rast_set_f_cat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 62
if _libs["grass_raster.8.4"].has("Rast_set_d_cat", "cdecl"):
    Rast_set_d_cat = _libs["grass_raster.8.4"].get("Rast_set_d_cat", "cdecl")
    Rast_set_d_cat.argtypes = [POINTER(DCELL), POINTER(DCELL), String, POINTER(struct_Categories)]
    Rast_set_d_cat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 64
if _libs["grass_raster.8.4"].has("Rast_set_cat", "cdecl"):
    Rast_set_cat = _libs["grass_raster.8.4"].get("Rast_set_cat", "cdecl")
    Rast_set_cat.argtypes = [POINTER(None), POINTER(None), String, POINTER(struct_Categories), RASTER_MAP_TYPE]
    Rast_set_cat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 66
if _libs["grass_raster.8.4"].has("Rast_write_cats", "cdecl"):
    Rast_write_cats = _libs["grass_raster.8.4"].get("Rast_write_cats", "cdecl")
    Rast_write_cats.argtypes = [String, POINTER(struct_Categories)]
    Rast_write_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 67
if _libs["grass_raster.8.4"].has("Rast_write_vector_cats", "cdecl"):
    Rast_write_vector_cats = _libs["grass_raster.8.4"].get("Rast_write_vector_cats", "cdecl")
    Rast_write_vector_cats.argtypes = [String, POINTER(struct_Categories)]
    Rast_write_vector_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 68
if _libs["grass_raster.8.4"].has("Rast_get_ith_d_cat", "cdecl"):
    Rast_get_ith_d_cat = _libs["grass_raster.8.4"].get("Rast_get_ith_d_cat", "cdecl")
    Rast_get_ith_d_cat.argtypes = [POINTER(struct_Categories), c_int, POINTER(DCELL), POINTER(DCELL)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_ith_d_cat.restype = ReturnString
    else:
        Rast_get_ith_d_cat.restype = String
        Rast_get_ith_d_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 69
if _libs["grass_raster.8.4"].has("Rast_get_ith_f_cat", "cdecl"):
    Rast_get_ith_f_cat = _libs["grass_raster.8.4"].get("Rast_get_ith_f_cat", "cdecl")
    Rast_get_ith_f_cat.argtypes = [POINTER(struct_Categories), c_int, POINTER(None), POINTER(None)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_ith_f_cat.restype = ReturnString
    else:
        Rast_get_ith_f_cat.restype = String
        Rast_get_ith_f_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 70
if _libs["grass_raster.8.4"].has("Rast_get_ith_c_cat", "cdecl"):
    Rast_get_ith_c_cat = _libs["grass_raster.8.4"].get("Rast_get_ith_c_cat", "cdecl")
    Rast_get_ith_c_cat.argtypes = [POINTER(struct_Categories), c_int, POINTER(None), POINTER(None)]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_ith_c_cat.restype = ReturnString
    else:
        Rast_get_ith_c_cat.restype = String
        Rast_get_ith_c_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 71
if _libs["grass_raster.8.4"].has("Rast_get_ith_cat", "cdecl"):
    Rast_get_ith_cat = _libs["grass_raster.8.4"].get("Rast_get_ith_cat", "cdecl")
    Rast_get_ith_cat.argtypes = [POINTER(struct_Categories), c_int, POINTER(None), POINTER(None), RASTER_MAP_TYPE]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_ith_cat.restype = ReturnString
    else:
        Rast_get_ith_cat.restype = String
        Rast_get_ith_cat.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 73
if _libs["grass_raster.8.4"].has("Rast_init_cats", "cdecl"):
    Rast_init_cats = _libs["grass_raster.8.4"].get("Rast_init_cats", "cdecl")
    Rast_init_cats.argtypes = [String, POINTER(struct_Categories)]
    Rast_init_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 74
if _libs["grass_raster.8.4"].has("Rast_set_cats_title", "cdecl"):
    Rast_set_cats_title = _libs["grass_raster.8.4"].get("Rast_set_cats_title", "cdecl")
    Rast_set_cats_title.argtypes = [String, POINTER(struct_Categories)]
    Rast_set_cats_title.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 75
if _libs["grass_raster.8.4"].has("Rast_set_cats_fmt", "cdecl"):
    Rast_set_cats_fmt = _libs["grass_raster.8.4"].get("Rast_set_cats_fmt", "cdecl")
    Rast_set_cats_fmt.argtypes = [String, c_double, c_double, c_double, c_double, POINTER(struct_Categories)]
    Rast_set_cats_fmt.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 77
if _libs["grass_raster.8.4"].has("Rast_free_cats", "cdecl"):
    Rast_free_cats = _libs["grass_raster.8.4"].get("Rast_free_cats", "cdecl")
    Rast_free_cats.argtypes = [POINTER(struct_Categories)]
    Rast_free_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 78
if _libs["grass_raster.8.4"].has("Rast_copy_cats", "cdecl"):
    Rast_copy_cats = _libs["grass_raster.8.4"].get("Rast_copy_cats", "cdecl")
    Rast_copy_cats.argtypes = [POINTER(struct_Categories), POINTER(struct_Categories)]
    Rast_copy_cats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 79
if _libs["grass_raster.8.4"].has("Rast_number_of_cats", "cdecl"):
    Rast_number_of_cats = _libs["grass_raster.8.4"].get("Rast_number_of_cats", "cdecl")
    Rast_number_of_cats.argtypes = [POINTER(struct_Categories)]
    Rast_number_of_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 80
if _libs["grass_raster.8.4"].has("Rast_sort_cats", "cdecl"):
    Rast_sort_cats = _libs["grass_raster.8.4"].get("Rast_sort_cats", "cdecl")
    Rast_sort_cats.argtypes = [POINTER(struct_Categories)]
    Rast_sort_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 83
if _libs["grass_raster.8.4"].has("Rast_init_cell_stats", "cdecl"):
    Rast_init_cell_stats = _libs["grass_raster.8.4"].get("Rast_init_cell_stats", "cdecl")
    Rast_init_cell_stats.argtypes = [POINTER(struct_Cell_stats)]
    Rast_init_cell_stats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 84
if _libs["grass_raster.8.4"].has("Rast_update_cell_stats", "cdecl"):
    Rast_update_cell_stats = _libs["grass_raster.8.4"].get("Rast_update_cell_stats", "cdecl")
    Rast_update_cell_stats.argtypes = [POINTER(CELL), c_int, POINTER(struct_Cell_stats)]
    Rast_update_cell_stats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 85
if _libs["grass_raster.8.4"].has("Rast_find_cell_stat", "cdecl"):
    Rast_find_cell_stat = _libs["grass_raster.8.4"].get("Rast_find_cell_stat", "cdecl")
    Rast_find_cell_stat.argtypes = [CELL, POINTER(c_long), POINTER(struct_Cell_stats)]
    Rast_find_cell_stat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 86
if _libs["grass_raster.8.4"].has("Rast_rewind_cell_stats", "cdecl"):
    Rast_rewind_cell_stats = _libs["grass_raster.8.4"].get("Rast_rewind_cell_stats", "cdecl")
    Rast_rewind_cell_stats.argtypes = [POINTER(struct_Cell_stats)]
    Rast_rewind_cell_stats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 87
if _libs["grass_raster.8.4"].has("Rast_next_cell_stat", "cdecl"):
    Rast_next_cell_stat = _libs["grass_raster.8.4"].get("Rast_next_cell_stat", "cdecl")
    Rast_next_cell_stat.argtypes = [POINTER(CELL), POINTER(c_long), POINTER(struct_Cell_stats)]
    Rast_next_cell_stat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 88
if _libs["grass_raster.8.4"].has("Rast_get_stats_for_null_value", "cdecl"):
    Rast_get_stats_for_null_value = _libs["grass_raster.8.4"].get("Rast_get_stats_for_null_value", "cdecl")
    Rast_get_stats_for_null_value.argtypes = [POINTER(c_long), POINTER(struct_Cell_stats)]
    Rast_get_stats_for_null_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 89
if _libs["grass_raster.8.4"].has("Rast_free_cell_stats", "cdecl"):
    Rast_free_cell_stats = _libs["grass_raster.8.4"].get("Rast_free_cell_stats", "cdecl")
    Rast_free_cell_stats.argtypes = [POINTER(struct_Cell_stats)]
    Rast_free_cell_stats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 92
if _libs["grass_raster.8.4"].has("Rast_get_cell_title", "cdecl"):
    Rast_get_cell_title = _libs["grass_raster.8.4"].get("Rast_get_cell_title", "cdecl")
    Rast_get_cell_title.argtypes = [String, String]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_cell_title.restype = ReturnString
    else:
        Rast_get_cell_title.restype = String
        Rast_get_cell_title.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 95
if _libs["grass_raster.8.4"].has("Rast_cell_stats_histo_eq", "cdecl"):
    Rast_cell_stats_histo_eq = _libs["grass_raster.8.4"].get("Rast_cell_stats_histo_eq", "cdecl")
    Rast_cell_stats_histo_eq.argtypes = [POINTER(struct_Cell_stats), CELL, CELL, CELL, CELL, c_int, CFUNCTYPE(UNCHECKED(None), CELL, CELL, CELL)]
    Rast_cell_stats_histo_eq.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 99
if _libs["grass_raster.8.4"].has("Rast_close", "cdecl"):
    Rast_close = _libs["grass_raster.8.4"].get("Rast_close", "cdecl")
    Rast_close.argtypes = [c_int]
    Rast_close.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 100
if _libs["grass_raster.8.4"].has("Rast_unopen", "cdecl"):
    Rast_unopen = _libs["grass_raster.8.4"].get("Rast_unopen", "cdecl")
    Rast_unopen.argtypes = [c_int]
    Rast_unopen.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 101
if _libs["grass_raster.8.4"].has("Rast__unopen_all", "cdecl"):
    Rast__unopen_all = _libs["grass_raster.8.4"].get("Rast__unopen_all", "cdecl")
    Rast__unopen_all.argtypes = []
    Rast__unopen_all.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 102
if _libs["grass_raster.8.4"].has("Rast__close_null", "cdecl"):
    Rast__close_null = _libs["grass_raster.8.4"].get("Rast__close_null", "cdecl")
    Rast__close_null.argtypes = [c_int]
    Rast__close_null.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 105
if _libs["grass_raster.8.4"].has("Rast_make_ryg_colors", "cdecl"):
    Rast_make_ryg_colors = _libs["grass_raster.8.4"].get("Rast_make_ryg_colors", "cdecl")
    Rast_make_ryg_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_ryg_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 106
if _libs["grass_raster.8.4"].has("Rast_make_ryg_fp_colors", "cdecl"):
    Rast_make_ryg_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_ryg_fp_colors", "cdecl")
    Rast_make_ryg_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_ryg_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 107
if _libs["grass_raster.8.4"].has("Rast_make_aspect_colors", "cdecl"):
    Rast_make_aspect_colors = _libs["grass_raster.8.4"].get("Rast_make_aspect_colors", "cdecl")
    Rast_make_aspect_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_aspect_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 108
if _libs["grass_raster.8.4"].has("Rast_make_aspect_fp_colors", "cdecl"):
    Rast_make_aspect_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_aspect_fp_colors", "cdecl")
    Rast_make_aspect_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_aspect_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 109
if _libs["grass_raster.8.4"].has("Rast_make_byr_colors", "cdecl"):
    Rast_make_byr_colors = _libs["grass_raster.8.4"].get("Rast_make_byr_colors", "cdecl")
    Rast_make_byr_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_byr_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 110
if _libs["grass_raster.8.4"].has("Rast_make_byr_fp_colors", "cdecl"):
    Rast_make_byr_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_byr_fp_colors", "cdecl")
    Rast_make_byr_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_byr_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 111
if _libs["grass_raster.8.4"].has("Rast_make_bgyr_colors", "cdecl"):
    Rast_make_bgyr_colors = _libs["grass_raster.8.4"].get("Rast_make_bgyr_colors", "cdecl")
    Rast_make_bgyr_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_bgyr_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 112
if _libs["grass_raster.8.4"].has("Rast_make_bgyr_fp_colors", "cdecl"):
    Rast_make_bgyr_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_bgyr_fp_colors", "cdecl")
    Rast_make_bgyr_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_bgyr_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 113
if _libs["grass_raster.8.4"].has("Rast_make_byg_colors", "cdecl"):
    Rast_make_byg_colors = _libs["grass_raster.8.4"].get("Rast_make_byg_colors", "cdecl")
    Rast_make_byg_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_byg_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 114
if _libs["grass_raster.8.4"].has("Rast_make_byg_fp_colors", "cdecl"):
    Rast_make_byg_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_byg_fp_colors", "cdecl")
    Rast_make_byg_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_byg_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 115
if _libs["grass_raster.8.4"].has("Rast_make_grey_scale_colors", "cdecl"):
    Rast_make_grey_scale_colors = _libs["grass_raster.8.4"].get("Rast_make_grey_scale_colors", "cdecl")
    Rast_make_grey_scale_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_grey_scale_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 116
if _libs["grass_raster.8.4"].has("Rast_make_grey_scale_fp_colors", "cdecl"):
    Rast_make_grey_scale_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_grey_scale_fp_colors", "cdecl")
    Rast_make_grey_scale_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_grey_scale_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 117
if _libs["grass_raster.8.4"].has("Rast_make_gyr_colors", "cdecl"):
    Rast_make_gyr_colors = _libs["grass_raster.8.4"].get("Rast_make_gyr_colors", "cdecl")
    Rast_make_gyr_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_gyr_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 118
if _libs["grass_raster.8.4"].has("Rast_make_gyr_fp_colors", "cdecl"):
    Rast_make_gyr_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_gyr_fp_colors", "cdecl")
    Rast_make_gyr_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_gyr_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 119
if _libs["grass_raster.8.4"].has("Rast_make_rainbow_colors", "cdecl"):
    Rast_make_rainbow_colors = _libs["grass_raster.8.4"].get("Rast_make_rainbow_colors", "cdecl")
    Rast_make_rainbow_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_rainbow_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 120
if _libs["grass_raster.8.4"].has("Rast_make_rainbow_fp_colors", "cdecl"):
    Rast_make_rainbow_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_rainbow_fp_colors", "cdecl")
    Rast_make_rainbow_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_rainbow_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 121
if _libs["grass_raster.8.4"].has("Rast_make_ramp_colors", "cdecl"):
    Rast_make_ramp_colors = _libs["grass_raster.8.4"].get("Rast_make_ramp_colors", "cdecl")
    Rast_make_ramp_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_ramp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 122
if _libs["grass_raster.8.4"].has("Rast_make_ramp_fp_colors", "cdecl"):
    Rast_make_ramp_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_ramp_fp_colors", "cdecl")
    Rast_make_ramp_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_ramp_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 123
if _libs["grass_raster.8.4"].has("Rast_make_wave_colors", "cdecl"):
    Rast_make_wave_colors = _libs["grass_raster.8.4"].get("Rast_make_wave_colors", "cdecl")
    Rast_make_wave_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_wave_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 124
if _libs["grass_raster.8.4"].has("Rast_make_wave_fp_colors", "cdecl"):
    Rast_make_wave_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_wave_fp_colors", "cdecl")
    Rast_make_wave_fp_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL]
    Rast_make_wave_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 127
if _libs["grass_raster.8.4"].has("Rast_free_colors", "cdecl"):
    Rast_free_colors = _libs["grass_raster.8.4"].get("Rast_free_colors", "cdecl")
    Rast_free_colors.argtypes = [POINTER(struct_Colors)]
    Rast_free_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 128
if _libs["grass_raster.8.4"].has("Rast__color_free_rules", "cdecl"):
    Rast__color_free_rules = _libs["grass_raster.8.4"].get("Rast__color_free_rules", "cdecl")
    Rast__color_free_rules.argtypes = [POINTER(struct__Color_Info_)]
    Rast__color_free_rules.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 129
if _libs["grass_raster.8.4"].has("Rast__color_free_lookup", "cdecl"):
    Rast__color_free_lookup = _libs["grass_raster.8.4"].get("Rast__color_free_lookup", "cdecl")
    Rast__color_free_lookup.argtypes = [POINTER(struct__Color_Info_)]
    Rast__color_free_lookup.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 130
if _libs["grass_raster.8.4"].has("Rast__color_free_fp_lookup", "cdecl"):
    Rast__color_free_fp_lookup = _libs["grass_raster.8.4"].get("Rast__color_free_fp_lookup", "cdecl")
    Rast__color_free_fp_lookup.argtypes = [POINTER(struct__Color_Info_)]
    Rast__color_free_fp_lookup.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 131
if _libs["grass_raster.8.4"].has("Rast__color_reset", "cdecl"):
    Rast__color_reset = _libs["grass_raster.8.4"].get("Rast__color_reset", "cdecl")
    Rast__color_reset.argtypes = [POINTER(struct_Colors)]
    Rast__color_reset.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 134
if _libs["grass_raster.8.4"].has("Rast_get_color", "cdecl"):
    Rast_get_color = _libs["grass_raster.8.4"].get("Rast_get_color", "cdecl")
    Rast_get_color.argtypes = [POINTER(None), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(struct_Colors), RASTER_MAP_TYPE]
    Rast_get_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 136
if _libs["grass_raster.8.4"].has("Rast_get_c_color", "cdecl"):
    Rast_get_c_color = _libs["grass_raster.8.4"].get("Rast_get_c_color", "cdecl")
    Rast_get_c_color.argtypes = [POINTER(CELL), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(struct_Colors)]
    Rast_get_c_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 137
if _libs["grass_raster.8.4"].has("Rast_get_f_color", "cdecl"):
    Rast_get_f_color = _libs["grass_raster.8.4"].get("Rast_get_f_color", "cdecl")
    Rast_get_f_color.argtypes = [POINTER(FCELL), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(struct_Colors)]
    Rast_get_f_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 138
if _libs["grass_raster.8.4"].has("Rast_get_d_color", "cdecl"):
    Rast_get_d_color = _libs["grass_raster.8.4"].get("Rast_get_d_color", "cdecl")
    Rast_get_d_color.argtypes = [POINTER(DCELL), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(struct_Colors)]
    Rast_get_d_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 139
if _libs["grass_raster.8.4"].has("Rast_get_null_value_color", "cdecl"):
    Rast_get_null_value_color = _libs["grass_raster.8.4"].get("Rast_get_null_value_color", "cdecl")
    Rast_get_null_value_color.argtypes = [POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(struct_Colors)]
    Rast_get_null_value_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 140
if _libs["grass_raster.8.4"].has("Rast_get_default_color", "cdecl"):
    Rast_get_default_color = _libs["grass_raster.8.4"].get("Rast_get_default_color", "cdecl")
    Rast_get_default_color.argtypes = [POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(struct_Colors)]
    Rast_get_default_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 143
if _libs["grass_raster.8.4"].has("Rast_make_histogram_eq_colors", "cdecl"):
    Rast_make_histogram_eq_colors = _libs["grass_raster.8.4"].get("Rast_make_histogram_eq_colors", "cdecl")
    Rast_make_histogram_eq_colors.argtypes = [POINTER(struct_Colors), POINTER(struct_Cell_stats)]
    Rast_make_histogram_eq_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 144
if _libs["grass_raster.8.4"].has("Rast_make_histogram_log_colors", "cdecl"):
    Rast_make_histogram_log_colors = _libs["grass_raster.8.4"].get("Rast_make_histogram_log_colors", "cdecl")
    Rast_make_histogram_log_colors.argtypes = [POINTER(struct_Colors), POINTER(struct_Cell_stats), c_int, c_int]
    Rast_make_histogram_log_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 148
if _libs["grass_raster.8.4"].has("Rast_init_colors", "cdecl"):
    Rast_init_colors = _libs["grass_raster.8.4"].get("Rast_init_colors", "cdecl")
    Rast_init_colors.argtypes = [POINTER(struct_Colors)]
    Rast_init_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 151
if _libs["grass_raster.8.4"].has("Rast__insert_color_into_lookup", "cdecl"):
    Rast__insert_color_into_lookup = _libs["grass_raster.8.4"].get("Rast__insert_color_into_lookup", "cdecl")
    Rast__insert_color_into_lookup.argtypes = [CELL, c_int, c_int, c_int, POINTER(struct__Color_Info_)]
    Rast__insert_color_into_lookup.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 154
if _libs["grass_raster.8.4"].has("Rast_invert_colors", "cdecl"):
    Rast_invert_colors = _libs["grass_raster.8.4"].get("Rast_invert_colors", "cdecl")
    Rast_invert_colors.argtypes = [POINTER(struct_Colors)]
    Rast_invert_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 157
if _libs["grass_raster.8.4"].has("Rast_lookup_c_colors", "cdecl"):
    Rast_lookup_c_colors = _libs["grass_raster.8.4"].get("Rast_lookup_c_colors", "cdecl")
    Rast_lookup_c_colors.argtypes = [POINTER(CELL), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), c_int, POINTER(struct_Colors)]
    Rast_lookup_c_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 160
if _libs["grass_raster.8.4"].has("Rast_lookup_colors", "cdecl"):
    Rast_lookup_colors = _libs["grass_raster.8.4"].get("Rast_lookup_colors", "cdecl")
    Rast_lookup_colors.argtypes = [POINTER(None), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), c_int, POINTER(struct_Colors), RASTER_MAP_TYPE]
    Rast_lookup_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 163
if _libs["grass_raster.8.4"].has("Rast_lookup_f_colors", "cdecl"):
    Rast_lookup_f_colors = _libs["grass_raster.8.4"].get("Rast_lookup_f_colors", "cdecl")
    Rast_lookup_f_colors.argtypes = [POINTER(FCELL), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), c_int, POINTER(struct_Colors)]
    Rast_lookup_f_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 166
if _libs["grass_raster.8.4"].has("Rast_lookup_d_colors", "cdecl"):
    Rast_lookup_d_colors = _libs["grass_raster.8.4"].get("Rast_lookup_d_colors", "cdecl")
    Rast_lookup_d_colors.argtypes = [POINTER(DCELL), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), c_int, POINTER(struct_Colors)]
    Rast_lookup_d_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 169
if _libs["grass_raster.8.4"].has("Rast__lookup_colors", "cdecl"):
    Rast__lookup_colors = _libs["grass_raster.8.4"].get("Rast__lookup_colors", "cdecl")
    Rast__lookup_colors.argtypes = [POINTER(None), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), c_int, POINTER(struct_Colors), c_int, c_int, RASTER_MAP_TYPE]
    Rast__lookup_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 172
if _libs["grass_raster.8.4"].has("Rast__interpolate_color_rule", "cdecl"):
    Rast__interpolate_color_rule = _libs["grass_raster.8.4"].get("Rast__interpolate_color_rule", "cdecl")
    Rast__interpolate_color_rule.argtypes = [DCELL, POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(struct__Color_Rule_)]
    Rast__interpolate_color_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 176
if _libs["grass_raster.8.4"].has("Rast__organize_colors", "cdecl"):
    Rast__organize_colors = _libs["grass_raster.8.4"].get("Rast__organize_colors", "cdecl")
    Rast__organize_colors.argtypes = [POINTER(struct_Colors)]
    Rast__organize_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 179
if _libs["grass_raster.8.4"].has("Rast_print_colors", "cdecl"):
    Rast_print_colors = _libs["grass_raster.8.4"].get("Rast_print_colors", "cdecl")
    Rast_print_colors.argtypes = [POINTER(struct_Colors), DCELL, DCELL, POINTER(FILE), c_int]
    Rast_print_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 182
if _libs["grass_raster.8.4"].has("Rast_make_random_colors", "cdecl"):
    Rast_make_random_colors = _libs["grass_raster.8.4"].get("Rast_make_random_colors", "cdecl")
    Rast_make_random_colors.argtypes = [POINTER(struct_Colors), CELL, CELL]
    Rast_make_random_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 185
if _libs["grass_raster.8.4"].has("Rast_set_c_color_range", "cdecl"):
    Rast_set_c_color_range = _libs["grass_raster.8.4"].get("Rast_set_c_color_range", "cdecl")
    Rast_set_c_color_range.argtypes = [CELL, CELL, POINTER(struct_Colors)]
    Rast_set_c_color_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 186
if _libs["grass_raster.8.4"].has("Rast_set_d_color_range", "cdecl"):
    Rast_set_d_color_range = _libs["grass_raster.8.4"].get("Rast_set_d_color_range", "cdecl")
    Rast_set_d_color_range.argtypes = [DCELL, DCELL, POINTER(struct_Colors)]
    Rast_set_d_color_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 187
if _libs["grass_raster.8.4"].has("Rast_get_c_color_range", "cdecl"):
    Rast_get_c_color_range = _libs["grass_raster.8.4"].get("Rast_get_c_color_range", "cdecl")
    Rast_get_c_color_range.argtypes = [POINTER(CELL), POINTER(CELL), POINTER(struct_Colors)]
    Rast_get_c_color_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 188
if _libs["grass_raster.8.4"].has("Rast_get_d_color_range", "cdecl"):
    Rast_get_d_color_range = _libs["grass_raster.8.4"].get("Rast_get_d_color_range", "cdecl")
    Rast_get_d_color_range.argtypes = [POINTER(DCELL), POINTER(DCELL), POINTER(struct_Colors)]
    Rast_get_d_color_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 191
if _libs["grass_raster.8.4"].has("Rast_read_colors", "cdecl"):
    Rast_read_colors = _libs["grass_raster.8.4"].get("Rast_read_colors", "cdecl")
    Rast_read_colors.argtypes = [String, String, POINTER(struct_Colors)]
    Rast_read_colors.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 192
if _libs["grass_raster.8.4"].has("Rast__read_colors", "cdecl"):
    Rast__read_colors = _libs["grass_raster.8.4"].get("Rast__read_colors", "cdecl")
    Rast__read_colors.argtypes = [String, String, String, POINTER(struct_Colors)]
    Rast__read_colors.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 194
if _libs["grass_raster.8.4"].has("Rast_mark_colors_as_fp", "cdecl"):
    Rast_mark_colors_as_fp = _libs["grass_raster.8.4"].get("Rast_mark_colors_as_fp", "cdecl")
    Rast_mark_colors_as_fp.argtypes = [POINTER(struct_Colors)]
    Rast_mark_colors_as_fp.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 197
if _libs["grass_raster.8.4"].has("Rast_remove_colors", "cdecl"):
    Rast_remove_colors = _libs["grass_raster.8.4"].get("Rast_remove_colors", "cdecl")
    Rast_remove_colors.argtypes = [String, String]
    Rast_remove_colors.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 200
if _libs["grass_raster.8.4"].has("Rast_add_d_color_rule", "cdecl"):
    Rast_add_d_color_rule = _libs["grass_raster.8.4"].get("Rast_add_d_color_rule", "cdecl")
    Rast_add_d_color_rule.argtypes = [POINTER(DCELL), c_int, c_int, c_int, POINTER(DCELL), c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_add_d_color_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 202
if _libs["grass_raster.8.4"].has("Rast_add_f_color_rule", "cdecl"):
    Rast_add_f_color_rule = _libs["grass_raster.8.4"].get("Rast_add_f_color_rule", "cdecl")
    Rast_add_f_color_rule.argtypes = [POINTER(FCELL), c_int, c_int, c_int, POINTER(FCELL), c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_add_f_color_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 204
if _libs["grass_raster.8.4"].has("Rast_add_c_color_rule", "cdecl"):
    Rast_add_c_color_rule = _libs["grass_raster.8.4"].get("Rast_add_c_color_rule", "cdecl")
    Rast_add_c_color_rule.argtypes = [POINTER(CELL), c_int, c_int, c_int, POINTER(CELL), c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_add_c_color_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 206
if _libs["grass_raster.8.4"].has("Rast_add_color_rule", "cdecl"):
    Rast_add_color_rule = _libs["grass_raster.8.4"].get("Rast_add_color_rule", "cdecl")
    Rast_add_color_rule.argtypes = [POINTER(None), c_int, c_int, c_int, POINTER(None), c_int, c_int, c_int, POINTER(struct_Colors), RASTER_MAP_TYPE]
    Rast_add_color_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 208
if _libs["grass_raster.8.4"].has("Rast_add_modular_d_color_rule", "cdecl"):
    Rast_add_modular_d_color_rule = _libs["grass_raster.8.4"].get("Rast_add_modular_d_color_rule", "cdecl")
    Rast_add_modular_d_color_rule.argtypes = [POINTER(DCELL), c_int, c_int, c_int, POINTER(DCELL), c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_add_modular_d_color_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 210
if _libs["grass_raster.8.4"].has("Rast_add_modular_f_color_rule", "cdecl"):
    Rast_add_modular_f_color_rule = _libs["grass_raster.8.4"].get("Rast_add_modular_f_color_rule", "cdecl")
    Rast_add_modular_f_color_rule.argtypes = [POINTER(FCELL), c_int, c_int, c_int, POINTER(FCELL), c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_add_modular_f_color_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 212
if _libs["grass_raster.8.4"].has("Rast_add_modular_c_color_rule", "cdecl"):
    Rast_add_modular_c_color_rule = _libs["grass_raster.8.4"].get("Rast_add_modular_c_color_rule", "cdecl")
    Rast_add_modular_c_color_rule.argtypes = [POINTER(CELL), c_int, c_int, c_int, POINTER(CELL), c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_add_modular_c_color_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 214
if _libs["grass_raster.8.4"].has("Rast_add_modular_color_rule", "cdecl"):
    Rast_add_modular_color_rule = _libs["grass_raster.8.4"].get("Rast_add_modular_color_rule", "cdecl")
    Rast_add_modular_color_rule.argtypes = [POINTER(None), c_int, c_int, c_int, POINTER(None), c_int, c_int, c_int, POINTER(struct_Colors), RASTER_MAP_TYPE]
    Rast_add_modular_color_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 218
if _libs["grass_raster.8.4"].has("Rast_colors_count", "cdecl"):
    Rast_colors_count = _libs["grass_raster.8.4"].get("Rast_colors_count", "cdecl")
    Rast_colors_count.argtypes = [POINTER(struct_Colors)]
    Rast_colors_count.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 219
if _libs["grass_raster.8.4"].has("Rast_get_fp_color_rule", "cdecl"):
    Rast_get_fp_color_rule = _libs["grass_raster.8.4"].get("Rast_get_fp_color_rule", "cdecl")
    Rast_get_fp_color_rule.argtypes = [POINTER(DCELL), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(DCELL), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(struct_Colors), c_int]
    Rast_get_fp_color_rule.restype = c_int

read_rule_fn = CFUNCTYPE(UNCHECKED(c_int), POINTER(None), DCELL, DCELL, POINTER(DCELL), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int))# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 225

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 227
if _libs["grass_raster.8.4"].has("Rast_parse_color_rule", "cdecl"):
    Rast_parse_color_rule = _libs["grass_raster.8.4"].get("Rast_parse_color_rule", "cdecl")
    Rast_parse_color_rule.argtypes = [DCELL, DCELL, String, POINTER(DCELL), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    Rast_parse_color_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 229
if _libs["grass_raster.8.4"].has("Rast_parse_color_rule_error", "cdecl"):
    Rast_parse_color_rule_error = _libs["grass_raster.8.4"].get("Rast_parse_color_rule_error", "cdecl")
    Rast_parse_color_rule_error.argtypes = [c_int]
    Rast_parse_color_rule_error.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 230
if _libs["grass_raster.8.4"].has("Rast_read_color_rule", "cdecl"):
    Rast_read_color_rule = _libs["grass_raster.8.4"].get("Rast_read_color_rule", "cdecl")
    Rast_read_color_rule.argtypes = [POINTER(None), DCELL, DCELL, POINTER(DCELL), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    Rast_read_color_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 232
if _libs["grass_raster.8.4"].has("Rast_read_color_rules", "cdecl"):
    Rast_read_color_rules = _libs["grass_raster.8.4"].get("Rast_read_color_rules", "cdecl")
    Rast_read_color_rules.argtypes = [POINTER(struct_Colors), DCELL, DCELL, POINTER(read_rule_fn), POINTER(None)]
    Rast_read_color_rules.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 234
if _libs["grass_raster.8.4"].has("Rast_load_colors", "cdecl"):
    Rast_load_colors = _libs["grass_raster.8.4"].get("Rast_load_colors", "cdecl")
    Rast_load_colors.argtypes = [POINTER(struct_Colors), String, CELL, CELL]
    Rast_load_colors.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 235
if _libs["grass_raster.8.4"].has("Rast_load_fp_colors", "cdecl"):
    Rast_load_fp_colors = _libs["grass_raster.8.4"].get("Rast_load_fp_colors", "cdecl")
    Rast_load_fp_colors.argtypes = [POINTER(struct_Colors), String, DCELL, DCELL]
    Rast_load_fp_colors.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 236
if _libs["grass_raster.8.4"].has("Rast_make_colors", "cdecl"):
    Rast_make_colors = _libs["grass_raster.8.4"].get("Rast_make_colors", "cdecl")
    Rast_make_colors.argtypes = [POINTER(struct_Colors), String, CELL, CELL]
    Rast_make_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 237
if _libs["grass_raster.8.4"].has("Rast_make_fp_colors", "cdecl"):
    Rast_make_fp_colors = _libs["grass_raster.8.4"].get("Rast_make_fp_colors", "cdecl")
    Rast_make_fp_colors.argtypes = [POINTER(struct_Colors), String, DCELL, DCELL]
    Rast_make_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 240
if _libs["grass_raster.8.4"].has("Rast_set_c_color", "cdecl"):
    Rast_set_c_color = _libs["grass_raster.8.4"].get("Rast_set_c_color", "cdecl")
    Rast_set_c_color.argtypes = [CELL, c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_set_c_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 241
if _libs["grass_raster.8.4"].has("Rast_set_d_color", "cdecl"):
    Rast_set_d_color = _libs["grass_raster.8.4"].get("Rast_set_d_color", "cdecl")
    Rast_set_d_color.argtypes = [DCELL, c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_set_d_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 242
if _libs["grass_raster.8.4"].has("Rast_set_null_value_color", "cdecl"):
    Rast_set_null_value_color = _libs["grass_raster.8.4"].get("Rast_set_null_value_color", "cdecl")
    Rast_set_null_value_color.argtypes = [c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_set_null_value_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 243
if _libs["grass_raster.8.4"].has("Rast_set_default_color", "cdecl"):
    Rast_set_default_color = _libs["grass_raster.8.4"].get("Rast_set_default_color", "cdecl")
    Rast_set_default_color.argtypes = [c_int, c_int, c_int, POINTER(struct_Colors)]
    Rast_set_default_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 246
if _libs["grass_raster.8.4"].has("Rast_shift_c_colors", "cdecl"):
    Rast_shift_c_colors = _libs["grass_raster.8.4"].get("Rast_shift_c_colors", "cdecl")
    Rast_shift_c_colors.argtypes = [CELL, POINTER(struct_Colors)]
    Rast_shift_c_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 247
if _libs["grass_raster.8.4"].has("Rast_shift_d_colors", "cdecl"):
    Rast_shift_d_colors = _libs["grass_raster.8.4"].get("Rast_shift_d_colors", "cdecl")
    Rast_shift_d_colors.argtypes = [DCELL, POINTER(struct_Colors)]
    Rast_shift_d_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 250
if _libs["grass_raster.8.4"].has("Rast_write_colors", "cdecl"):
    Rast_write_colors = _libs["grass_raster.8.4"].get("Rast_write_colors", "cdecl")
    Rast_write_colors.argtypes = [String, String, POINTER(struct_Colors)]
    Rast_write_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 251
if _libs["grass_raster.8.4"].has("Rast__write_colors", "cdecl"):
    Rast__write_colors = _libs["grass_raster.8.4"].get("Rast__write_colors", "cdecl")
    Rast__write_colors.argtypes = [POINTER(FILE), POINTER(struct_Colors)]
    Rast__write_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 254
if _libs["grass_raster.8.4"].has("Rast_histogram_eq_colors", "cdecl"):
    Rast_histogram_eq_colors = _libs["grass_raster.8.4"].get("Rast_histogram_eq_colors", "cdecl")
    Rast_histogram_eq_colors.argtypes = [POINTER(struct_Colors), POINTER(struct_Colors), POINTER(struct_Cell_stats)]
    Rast_histogram_eq_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 256
if _libs["grass_raster.8.4"].has("Rast_histogram_eq_fp_colors", "cdecl"):
    Rast_histogram_eq_fp_colors = _libs["grass_raster.8.4"].get("Rast_histogram_eq_fp_colors", "cdecl")
    Rast_histogram_eq_fp_colors.argtypes = [POINTER(struct_Colors), POINTER(struct_Colors), POINTER(struct_FP_stats)]
    Rast_histogram_eq_fp_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 258
if _libs["grass_raster.8.4"].has("Rast_log_colors", "cdecl"):
    Rast_log_colors = _libs["grass_raster.8.4"].get("Rast_log_colors", "cdecl")
    Rast_log_colors.argtypes = [POINTER(struct_Colors), POINTER(struct_Colors), c_int]
    Rast_log_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 259
if _libs["grass_raster.8.4"].has("Rast_abs_log_colors", "cdecl"):
    Rast_abs_log_colors = _libs["grass_raster.8.4"].get("Rast_abs_log_colors", "cdecl")
    Rast_abs_log_colors.argtypes = [POINTER(struct_Colors), POINTER(struct_Colors), c_int]
    Rast_abs_log_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 262
if _libs["grass_raster.8.4"].has("Rast__check_format", "cdecl"):
    Rast__check_format = _libs["grass_raster.8.4"].get("Rast__check_format", "cdecl")
    Rast__check_format.argtypes = [c_int]
    Rast__check_format.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 263
if _libs["grass_raster.8.4"].has("Rast__read_row_ptrs", "cdecl"):
    Rast__read_row_ptrs = _libs["grass_raster.8.4"].get("Rast__read_row_ptrs", "cdecl")
    Rast__read_row_ptrs.argtypes = [c_int]
    Rast__read_row_ptrs.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 264
if _libs["grass_raster.8.4"].has("Rast__read_null_row_ptrs", "cdecl"):
    Rast__read_null_row_ptrs = _libs["grass_raster.8.4"].get("Rast__read_null_row_ptrs", "cdecl")
    Rast__read_null_row_ptrs.argtypes = [c_int, c_int]
    Rast__read_null_row_ptrs.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 265
if _libs["grass_raster.8.4"].has("Rast__write_row_ptrs", "cdecl"):
    Rast__write_row_ptrs = _libs["grass_raster.8.4"].get("Rast__write_row_ptrs", "cdecl")
    Rast__write_row_ptrs.argtypes = [c_int]
    Rast__write_row_ptrs.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 266
if _libs["grass_raster.8.4"].has("Rast__write_null_row_ptrs", "cdecl"):
    Rast__write_null_row_ptrs = _libs["grass_raster.8.4"].get("Rast__write_null_row_ptrs", "cdecl")
    Rast__write_null_row_ptrs.argtypes = [c_int, c_int]
    Rast__write_null_row_ptrs.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 269
if _libs["grass_raster.8.4"].has("Rast_fpreclass_clear", "cdecl"):
    Rast_fpreclass_clear = _libs["grass_raster.8.4"].get("Rast_fpreclass_clear", "cdecl")
    Rast_fpreclass_clear.argtypes = [POINTER(struct_FPReclass)]
    Rast_fpreclass_clear.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 270
if _libs["grass_raster.8.4"].has("Rast_fpreclass_reset", "cdecl"):
    Rast_fpreclass_reset = _libs["grass_raster.8.4"].get("Rast_fpreclass_reset", "cdecl")
    Rast_fpreclass_reset.argtypes = [POINTER(struct_FPReclass)]
    Rast_fpreclass_reset.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 271
if _libs["grass_raster.8.4"].has("Rast_fpreclass_init", "cdecl"):
    Rast_fpreclass_init = _libs["grass_raster.8.4"].get("Rast_fpreclass_init", "cdecl")
    Rast_fpreclass_init.argtypes = [POINTER(struct_FPReclass)]
    Rast_fpreclass_init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 272
if _libs["grass_raster.8.4"].has("Rast_fpreclass_set_domain", "cdecl"):
    Rast_fpreclass_set_domain = _libs["grass_raster.8.4"].get("Rast_fpreclass_set_domain", "cdecl")
    Rast_fpreclass_set_domain.argtypes = [POINTER(struct_FPReclass), DCELL, DCELL]
    Rast_fpreclass_set_domain.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 273
if _libs["grass_raster.8.4"].has("Rast_fpreclass_set_range", "cdecl"):
    Rast_fpreclass_set_range = _libs["grass_raster.8.4"].get("Rast_fpreclass_set_range", "cdecl")
    Rast_fpreclass_set_range.argtypes = [POINTER(struct_FPReclass), DCELL, DCELL]
    Rast_fpreclass_set_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 274
if _libs["grass_raster.8.4"].has("Rast_fpreclass_get_limits", "cdecl"):
    Rast_fpreclass_get_limits = _libs["grass_raster.8.4"].get("Rast_fpreclass_get_limits", "cdecl")
    Rast_fpreclass_get_limits.argtypes = [POINTER(struct_FPReclass), POINTER(DCELL), POINTER(DCELL), POINTER(DCELL), POINTER(DCELL)]
    Rast_fpreclass_get_limits.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 276
if _libs["grass_raster.8.4"].has("Rast_fpreclass_nof_rules", "cdecl"):
    Rast_fpreclass_nof_rules = _libs["grass_raster.8.4"].get("Rast_fpreclass_nof_rules", "cdecl")
    Rast_fpreclass_nof_rules.argtypes = [POINTER(struct_FPReclass)]
    Rast_fpreclass_nof_rules.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 277
if _libs["grass_raster.8.4"].has("Rast_fpreclass_get_ith_rule", "cdecl"):
    Rast_fpreclass_get_ith_rule = _libs["grass_raster.8.4"].get("Rast_fpreclass_get_ith_rule", "cdecl")
    Rast_fpreclass_get_ith_rule.argtypes = [POINTER(struct_FPReclass), c_int, POINTER(DCELL), POINTER(DCELL), POINTER(DCELL), POINTER(DCELL)]
    Rast_fpreclass_get_ith_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 279
if _libs["grass_raster.8.4"].has("Rast_fpreclass_set_neg_infinite_rule", "cdecl"):
    Rast_fpreclass_set_neg_infinite_rule = _libs["grass_raster.8.4"].get("Rast_fpreclass_set_neg_infinite_rule", "cdecl")
    Rast_fpreclass_set_neg_infinite_rule.argtypes = [POINTER(struct_FPReclass), DCELL, DCELL]
    Rast_fpreclass_set_neg_infinite_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 280
if _libs["grass_raster.8.4"].has("Rast_fpreclass_get_neg_infinite_rule", "cdecl"):
    Rast_fpreclass_get_neg_infinite_rule = _libs["grass_raster.8.4"].get("Rast_fpreclass_get_neg_infinite_rule", "cdecl")
    Rast_fpreclass_get_neg_infinite_rule.argtypes = [POINTER(struct_FPReclass), POINTER(DCELL), POINTER(DCELL)]
    Rast_fpreclass_get_neg_infinite_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 282
if _libs["grass_raster.8.4"].has("Rast_fpreclass_set_pos_infinite_rule", "cdecl"):
    Rast_fpreclass_set_pos_infinite_rule = _libs["grass_raster.8.4"].get("Rast_fpreclass_set_pos_infinite_rule", "cdecl")
    Rast_fpreclass_set_pos_infinite_rule.argtypes = [POINTER(struct_FPReclass), DCELL, DCELL]
    Rast_fpreclass_set_pos_infinite_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 283
if _libs["grass_raster.8.4"].has("Rast_fpreclass_get_pos_infinite_rule", "cdecl"):
    Rast_fpreclass_get_pos_infinite_rule = _libs["grass_raster.8.4"].get("Rast_fpreclass_get_pos_infinite_rule", "cdecl")
    Rast_fpreclass_get_pos_infinite_rule.argtypes = [POINTER(struct_FPReclass), POINTER(DCELL), POINTER(DCELL)]
    Rast_fpreclass_get_pos_infinite_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 285
if _libs["grass_raster.8.4"].has("Rast_fpreclass_add_rule", "cdecl"):
    Rast_fpreclass_add_rule = _libs["grass_raster.8.4"].get("Rast_fpreclass_add_rule", "cdecl")
    Rast_fpreclass_add_rule.argtypes = [POINTER(struct_FPReclass), DCELL, DCELL, DCELL, DCELL]
    Rast_fpreclass_add_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 286
if _libs["grass_raster.8.4"].has("Rast_fpreclass_reverse_rule_order", "cdecl"):
    Rast_fpreclass_reverse_rule_order = _libs["grass_raster.8.4"].get("Rast_fpreclass_reverse_rule_order", "cdecl")
    Rast_fpreclass_reverse_rule_order.argtypes = [POINTER(struct_FPReclass)]
    Rast_fpreclass_reverse_rule_order.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 287
if _libs["grass_raster.8.4"].has("Rast_fpreclass_get_cell_value", "cdecl"):
    Rast_fpreclass_get_cell_value = _libs["grass_raster.8.4"].get("Rast_fpreclass_get_cell_value", "cdecl")
    Rast_fpreclass_get_cell_value.argtypes = [POINTER(struct_FPReclass), DCELL]
    Rast_fpreclass_get_cell_value.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 288
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_di", "cdecl"):
    Rast_fpreclass_perform_di = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_di", "cdecl")
    Rast_fpreclass_perform_di.argtypes = [POINTER(struct_FPReclass), POINTER(DCELL), POINTER(CELL), c_int]
    Rast_fpreclass_perform_di.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 290
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_df", "cdecl"):
    Rast_fpreclass_perform_df = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_df", "cdecl")
    Rast_fpreclass_perform_df.argtypes = [POINTER(struct_FPReclass), POINTER(DCELL), POINTER(FCELL), c_int]
    Rast_fpreclass_perform_df.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 292
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_dd", "cdecl"):
    Rast_fpreclass_perform_dd = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_dd", "cdecl")
    Rast_fpreclass_perform_dd.argtypes = [POINTER(struct_FPReclass), POINTER(DCELL), POINTER(DCELL), c_int]
    Rast_fpreclass_perform_dd.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 294
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_fi", "cdecl"):
    Rast_fpreclass_perform_fi = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_fi", "cdecl")
    Rast_fpreclass_perform_fi.argtypes = [POINTER(struct_FPReclass), POINTER(FCELL), POINTER(CELL), c_int]
    Rast_fpreclass_perform_fi.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 296
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_ff", "cdecl"):
    Rast_fpreclass_perform_ff = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_ff", "cdecl")
    Rast_fpreclass_perform_ff.argtypes = [POINTER(struct_FPReclass), POINTER(FCELL), POINTER(FCELL), c_int]
    Rast_fpreclass_perform_ff.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 298
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_fd", "cdecl"):
    Rast_fpreclass_perform_fd = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_fd", "cdecl")
    Rast_fpreclass_perform_fd.argtypes = [POINTER(struct_FPReclass), POINTER(FCELL), POINTER(DCELL), c_int]
    Rast_fpreclass_perform_fd.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 300
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_ii", "cdecl"):
    Rast_fpreclass_perform_ii = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_ii", "cdecl")
    Rast_fpreclass_perform_ii.argtypes = [POINTER(struct_FPReclass), POINTER(CELL), POINTER(CELL), c_int]
    Rast_fpreclass_perform_ii.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 302
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_if", "cdecl"):
    Rast_fpreclass_perform_if = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_if", "cdecl")
    Rast_fpreclass_perform_if.argtypes = [POINTER(struct_FPReclass), POINTER(CELL), POINTER(FCELL), c_int]
    Rast_fpreclass_perform_if.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 304
if _libs["grass_raster.8.4"].has("Rast_fpreclass_perform_id", "cdecl"):
    Rast_fpreclass_perform_id = _libs["grass_raster.8.4"].get("Rast_fpreclass_perform_id", "cdecl")
    Rast_fpreclass_perform_id.argtypes = [POINTER(struct_FPReclass), POINTER(CELL), POINTER(DCELL), c_int]
    Rast_fpreclass_perform_id.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 307
if _libs["grass_raster.8.4"].has("Rast_init_gdal", "cdecl"):
    Rast_init_gdal = _libs["grass_raster.8.4"].get("Rast_init_gdal", "cdecl")
    Rast_init_gdal.argtypes = []
    Rast_init_gdal.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 308
if _libs["grass_raster.8.4"].has("Rast_get_gdal_link", "cdecl"):
    Rast_get_gdal_link = _libs["grass_raster.8.4"].get("Rast_get_gdal_link", "cdecl")
    Rast_get_gdal_link.argtypes = [String, String]
    Rast_get_gdal_link.restype = POINTER(struct_GDAL_link)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 309
if _libs["grass_raster.8.4"].has("Rast_create_gdal_link", "cdecl"):
    Rast_create_gdal_link = _libs["grass_raster.8.4"].get("Rast_create_gdal_link", "cdecl")
    Rast_create_gdal_link.argtypes = [String, RASTER_MAP_TYPE]
    Rast_create_gdal_link.restype = POINTER(struct_GDAL_link)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 310
if _libs["grass_raster.8.4"].has("Rast_close_gdal_link", "cdecl"):
    Rast_close_gdal_link = _libs["grass_raster.8.4"].get("Rast_close_gdal_link", "cdecl")
    Rast_close_gdal_link.argtypes = [POINTER(struct_GDAL_link)]
    Rast_close_gdal_link.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 311
if _libs["grass_raster.8.4"].has("Rast_close_gdal_write_link", "cdecl"):
    Rast_close_gdal_write_link = _libs["grass_raster.8.4"].get("Rast_close_gdal_write_link", "cdecl")
    Rast_close_gdal_write_link.argtypes = [POINTER(struct_GDAL_link)]
    Rast_close_gdal_write_link.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 314
if _libs["grass_raster.8.4"].has("Rast_get_cellhd", "cdecl"):
    Rast_get_cellhd = _libs["grass_raster.8.4"].get("Rast_get_cellhd", "cdecl")
    Rast_get_cellhd.argtypes = [String, String, POINTER(struct_Cell_head)]
    Rast_get_cellhd.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 317
if _libs["grass_raster.8.4"].has("Rast_get_row_nomask", "cdecl"):
    Rast_get_row_nomask = _libs["grass_raster.8.4"].get("Rast_get_row_nomask", "cdecl")
    Rast_get_row_nomask.argtypes = [c_int, POINTER(None), c_int, RASTER_MAP_TYPE]
    Rast_get_row_nomask.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 318
if _libs["grass_raster.8.4"].has("Rast_get_c_row_nomask", "cdecl"):
    Rast_get_c_row_nomask = _libs["grass_raster.8.4"].get("Rast_get_c_row_nomask", "cdecl")
    Rast_get_c_row_nomask.argtypes = [c_int, POINTER(CELL), c_int]
    Rast_get_c_row_nomask.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 319
if _libs["grass_raster.8.4"].has("Rast_get_f_row_nomask", "cdecl"):
    Rast_get_f_row_nomask = _libs["grass_raster.8.4"].get("Rast_get_f_row_nomask", "cdecl")
    Rast_get_f_row_nomask.argtypes = [c_int, POINTER(FCELL), c_int]
    Rast_get_f_row_nomask.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 320
if _libs["grass_raster.8.4"].has("Rast_get_d_row_nomask", "cdecl"):
    Rast_get_d_row_nomask = _libs["grass_raster.8.4"].get("Rast_get_d_row_nomask", "cdecl")
    Rast_get_d_row_nomask.argtypes = [c_int, POINTER(DCELL), c_int]
    Rast_get_d_row_nomask.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 321
if _libs["grass_raster.8.4"].has("Rast_get_row", "cdecl"):
    Rast_get_row = _libs["grass_raster.8.4"].get("Rast_get_row", "cdecl")
    Rast_get_row.argtypes = [c_int, POINTER(None), c_int, RASTER_MAP_TYPE]
    Rast_get_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 322
if _libs["grass_raster.8.4"].has("Rast_get_c_row", "cdecl"):
    Rast_get_c_row = _libs["grass_raster.8.4"].get("Rast_get_c_row", "cdecl")
    Rast_get_c_row.argtypes = [c_int, POINTER(CELL), c_int]
    Rast_get_c_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 323
if _libs["grass_raster.8.4"].has("Rast_get_f_row", "cdecl"):
    Rast_get_f_row = _libs["grass_raster.8.4"].get("Rast_get_f_row", "cdecl")
    Rast_get_f_row.argtypes = [c_int, POINTER(FCELL), c_int]
    Rast_get_f_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 324
if _libs["grass_raster.8.4"].has("Rast_get_d_row", "cdecl"):
    Rast_get_d_row = _libs["grass_raster.8.4"].get("Rast_get_d_row", "cdecl")
    Rast_get_d_row.argtypes = [c_int, POINTER(DCELL), c_int]
    Rast_get_d_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 325
if _libs["grass_raster.8.4"].has("Rast_get_null_value_row", "cdecl"):
    Rast_get_null_value_row = _libs["grass_raster.8.4"].get("Rast_get_null_value_row", "cdecl")
    Rast_get_null_value_row.argtypes = [c_int, String, c_int]
    Rast_get_null_value_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 326
if _libs["grass_raster.8.4"].has("Rast__read_null_bits", "cdecl"):
    Rast__read_null_bits = _libs["grass_raster.8.4"].get("Rast__read_null_bits", "cdecl")
    Rast__read_null_bits.argtypes = [c_int, c_int, POINTER(c_ubyte)]
    Rast__read_null_bits.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 329
if _libs["grass_raster.8.4"].has("Rast_get_row_colors", "cdecl"):
    Rast_get_row_colors = _libs["grass_raster.8.4"].get("Rast_get_row_colors", "cdecl")
    Rast_get_row_colors.argtypes = [c_int, c_int, POINTER(struct_Colors), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte), POINTER(c_ubyte)]
    Rast_get_row_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 332
if _libs["grass_raster.8.4"].has("Rast_histogram_eq", "cdecl"):
    Rast_histogram_eq = _libs["grass_raster.8.4"].get("Rast_histogram_eq", "cdecl")
    Rast_histogram_eq.argtypes = [POINTER(struct_Histogram), POINTER(POINTER(c_ubyte)), POINTER(CELL), POINTER(CELL)]
    Rast_histogram_eq.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 336
if _libs["grass_raster.8.4"].has("Rast_init_histogram", "cdecl"):
    Rast_init_histogram = _libs["grass_raster.8.4"].get("Rast_init_histogram", "cdecl")
    Rast_init_histogram.argtypes = [POINTER(struct_Histogram)]
    Rast_init_histogram.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 337
if _libs["grass_raster.8.4"].has("Rast_read_histogram", "cdecl"):
    Rast_read_histogram = _libs["grass_raster.8.4"].get("Rast_read_histogram", "cdecl")
    Rast_read_histogram.argtypes = [String, String, POINTER(struct_Histogram)]
    Rast_read_histogram.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 338
if _libs["grass_raster.8.4"].has("Rast_write_histogram", "cdecl"):
    Rast_write_histogram = _libs["grass_raster.8.4"].get("Rast_write_histogram", "cdecl")
    Rast_write_histogram.argtypes = [String, POINTER(struct_Histogram)]
    Rast_write_histogram.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 339
if _libs["grass_raster.8.4"].has("Rast_write_histogram_cs", "cdecl"):
    Rast_write_histogram_cs = _libs["grass_raster.8.4"].get("Rast_write_histogram_cs", "cdecl")
    Rast_write_histogram_cs.argtypes = [String, POINTER(struct_Cell_stats)]
    Rast_write_histogram_cs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 340
if _libs["grass_raster.8.4"].has("Rast_make_histogram_cs", "cdecl"):
    Rast_make_histogram_cs = _libs["grass_raster.8.4"].get("Rast_make_histogram_cs", "cdecl")
    Rast_make_histogram_cs.argtypes = [POINTER(struct_Cell_stats), POINTER(struct_Histogram)]
    Rast_make_histogram_cs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 341
if _libs["grass_raster.8.4"].has("Rast_get_histogram_num", "cdecl"):
    Rast_get_histogram_num = _libs["grass_raster.8.4"].get("Rast_get_histogram_num", "cdecl")
    Rast_get_histogram_num.argtypes = [POINTER(struct_Histogram)]
    Rast_get_histogram_num.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 342
if _libs["grass_raster.8.4"].has("Rast_get_histogram_cat", "cdecl"):
    Rast_get_histogram_cat = _libs["grass_raster.8.4"].get("Rast_get_histogram_cat", "cdecl")
    Rast_get_histogram_cat.argtypes = [c_int, POINTER(struct_Histogram)]
    Rast_get_histogram_cat.restype = CELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 343
if _libs["grass_raster.8.4"].has("Rast_get_histogram_count", "cdecl"):
    Rast_get_histogram_count = _libs["grass_raster.8.4"].get("Rast_get_histogram_count", "cdecl")
    Rast_get_histogram_count.argtypes = [c_int, POINTER(struct_Histogram)]
    Rast_get_histogram_count.restype = c_long

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 344
if _libs["grass_raster.8.4"].has("Rast_free_histogram", "cdecl"):
    Rast_free_histogram = _libs["grass_raster.8.4"].get("Rast_free_histogram", "cdecl")
    Rast_free_histogram.argtypes = [POINTER(struct_Histogram)]
    Rast_free_histogram.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 345
if _libs["grass_raster.8.4"].has("Rast_sort_histogram", "cdecl"):
    Rast_sort_histogram = _libs["grass_raster.8.4"].get("Rast_sort_histogram", "cdecl")
    Rast_sort_histogram.argtypes = [POINTER(struct_Histogram)]
    Rast_sort_histogram.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 346
if _libs["grass_raster.8.4"].has("Rast_sort_histogram_by_count", "cdecl"):
    Rast_sort_histogram_by_count = _libs["grass_raster.8.4"].get("Rast_sort_histogram_by_count", "cdecl")
    Rast_sort_histogram_by_count.argtypes = [POINTER(struct_Histogram)]
    Rast_sort_histogram_by_count.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 347
if _libs["grass_raster.8.4"].has("Rast_remove_histogram", "cdecl"):
    Rast_remove_histogram = _libs["grass_raster.8.4"].get("Rast_remove_histogram", "cdecl")
    Rast_remove_histogram.argtypes = [String]
    Rast_remove_histogram.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 348
if _libs["grass_raster.8.4"].has("Rast_add_histogram", "cdecl"):
    Rast_add_histogram = _libs["grass_raster.8.4"].get("Rast_add_histogram", "cdecl")
    Rast_add_histogram.argtypes = [CELL, c_long, POINTER(struct_Histogram)]
    Rast_add_histogram.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 349
if _libs["grass_raster.8.4"].has("Rast_set_histogram", "cdecl"):
    Rast_set_histogram = _libs["grass_raster.8.4"].get("Rast_set_histogram", "cdecl")
    Rast_set_histogram.argtypes = [CELL, c_long, POINTER(struct_Histogram)]
    Rast_set_histogram.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 350
if _libs["grass_raster.8.4"].has("Rast_extend_histogram", "cdecl"):
    Rast_extend_histogram = _libs["grass_raster.8.4"].get("Rast_extend_histogram", "cdecl")
    Rast_extend_histogram.argtypes = [CELL, c_long, POINTER(struct_Histogram)]
    Rast_extend_histogram.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 351
if _libs["grass_raster.8.4"].has("Rast_zero_histogram", "cdecl"):
    Rast_zero_histogram = _libs["grass_raster.8.4"].get("Rast_zero_histogram", "cdecl")
    Rast_zero_histogram.argtypes = [POINTER(struct_Histogram)]
    Rast_zero_histogram.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 354
if _libs["grass_raster.8.4"].has("Rast__read_history", "cdecl"):
    Rast__read_history = _libs["grass_raster.8.4"].get("Rast__read_history", "cdecl")
    Rast__read_history.argtypes = [POINTER(struct_History), POINTER(FILE)]
    Rast__read_history.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 355
if _libs["grass_raster.8.4"].has("Rast_read_history", "cdecl"):
    Rast_read_history = _libs["grass_raster.8.4"].get("Rast_read_history", "cdecl")
    Rast_read_history.argtypes = [String, String, POINTER(struct_History)]
    Rast_read_history.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 356
if _libs["grass_raster.8.4"].has("Rast__write_history", "cdecl"):
    Rast__write_history = _libs["grass_raster.8.4"].get("Rast__write_history", "cdecl")
    Rast__write_history.argtypes = [POINTER(struct_History), POINTER(FILE)]
    Rast__write_history.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 357
if _libs["grass_raster.8.4"].has("Rast_write_history", "cdecl"):
    Rast_write_history = _libs["grass_raster.8.4"].get("Rast_write_history", "cdecl")
    Rast_write_history.argtypes = [String, POINTER(struct_History)]
    Rast_write_history.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 358
if _libs["grass_raster.8.4"].has("Rast_short_history", "cdecl"):
    Rast_short_history = _libs["grass_raster.8.4"].get("Rast_short_history", "cdecl")
    Rast_short_history.argtypes = [String, String, POINTER(struct_History)]
    Rast_short_history.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 359
if _libs["grass_raster.8.4"].has("Rast_command_history", "cdecl"):
    Rast_command_history = _libs["grass_raster.8.4"].get("Rast_command_history", "cdecl")
    Rast_command_history.argtypes = [POINTER(struct_History)]
    Rast_command_history.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 360
if _libs["grass_raster.8.4"].has("Rast_append_history", "cdecl"):
    Rast_append_history = _libs["grass_raster.8.4"].get("Rast_append_history", "cdecl")
    Rast_append_history.argtypes = [POINTER(struct_History), String]
    Rast_append_history.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 361
if _libs["grass_raster.8.4"].has("Rast_append_format_history", "cdecl"):
    _func = _libs["grass_raster.8.4"].get("Rast_append_format_history", "cdecl")
    _restype = None
    _errcheck = None
    _argtypes = [POINTER(struct_History), String]
    Rast_append_format_history = _variadic_function(_func,_restype,_argtypes,_errcheck)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 363
if _libs["grass_raster.8.4"].has("Rast_get_history", "cdecl"):
    Rast_get_history = _libs["grass_raster.8.4"].get("Rast_get_history", "cdecl")
    Rast_get_history.argtypes = [POINTER(struct_History), c_int]
    Rast_get_history.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 364
if _libs["grass_raster.8.4"].has("Rast_set_history", "cdecl"):
    Rast_set_history = _libs["grass_raster.8.4"].get("Rast_set_history", "cdecl")
    Rast_set_history.argtypes = [POINTER(struct_History), c_int, String]
    Rast_set_history.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 365
if _libs["grass_raster.8.4"].has("Rast_format_history", "cdecl"):
    _func = _libs["grass_raster.8.4"].get("Rast_format_history", "cdecl")
    _restype = None
    _errcheck = None
    _argtypes = [POINTER(struct_History), c_int, String]
    Rast_format_history = _variadic_function(_func,_restype,_argtypes,_errcheck)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 367
if _libs["grass_raster.8.4"].has("Rast_clear_history", "cdecl"):
    Rast_clear_history = _libs["grass_raster.8.4"].get("Rast_clear_history", "cdecl")
    Rast_clear_history.argtypes = [POINTER(struct_History)]
    Rast_clear_history.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 368
if _libs["grass_raster.8.4"].has("Rast_free_history", "cdecl"):
    Rast_free_history = _libs["grass_raster.8.4"].get("Rast_free_history", "cdecl")
    Rast_free_history.argtypes = [POINTER(struct_History)]
    Rast_free_history.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 369
if _libs["grass_raster.8.4"].has("Rast_history_length", "cdecl"):
    Rast_history_length = _libs["grass_raster.8.4"].get("Rast_history_length", "cdecl")
    Rast_history_length.argtypes = [POINTER(struct_History)]
    Rast_history_length.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 370
if _libs["grass_raster.8.4"].has("Rast_history_line", "cdecl"):
    Rast_history_line = _libs["grass_raster.8.4"].get("Rast_history_line", "cdecl")
    Rast_history_line.argtypes = [POINTER(struct_History), c_int]
    Rast_history_line.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 373
if _libs["grass_raster.8.4"].has("Rast_init", "cdecl"):
    Rast_init = _libs["grass_raster.8.4"].get("Rast_init", "cdecl")
    Rast_init.argtypes = []
    Rast_init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 374
if _libs["grass_raster.8.4"].has("Rast__check_init", "cdecl"):
    Rast__check_init = _libs["grass_raster.8.4"].get("Rast__check_init", "cdecl")
    Rast__check_init.argtypes = []
    Rast__check_init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 375
if _libs["grass_raster.8.4"].has("Rast_init_all", "cdecl"):
    Rast_init_all = _libs["grass_raster.8.4"].get("Rast_init_all", "cdecl")
    Rast_init_all.argtypes = []
    Rast_init_all.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 376
if _libs["grass_raster.8.4"].has("Rast__init", "cdecl"):
    Rast__init = _libs["grass_raster.8.4"].get("Rast__init", "cdecl")
    Rast__init.argtypes = []
    Rast__init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 377
if _libs["grass_raster.8.4"].has("Rast__error_handler", "cdecl"):
    Rast__error_handler = _libs["grass_raster.8.4"].get("Rast__error_handler", "cdecl")
    Rast__error_handler.argtypes = [POINTER(None)]
    Rast__error_handler.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 380
if _libs["grass_raster.8.4"].has("Rast_interp_linear", "cdecl"):
    Rast_interp_linear = _libs["grass_raster.8.4"].get("Rast_interp_linear", "cdecl")
    Rast_interp_linear.argtypes = [c_double, DCELL, DCELL]
    Rast_interp_linear.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 381
if _libs["grass_raster.8.4"].has("Rast_interp_bilinear", "cdecl"):
    Rast_interp_bilinear = _libs["grass_raster.8.4"].get("Rast_interp_bilinear", "cdecl")
    Rast_interp_bilinear.argtypes = [c_double, c_double, DCELL, DCELL, DCELL, DCELL]
    Rast_interp_bilinear.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 382
if _libs["grass_raster.8.4"].has("Rast_interp_cubic", "cdecl"):
    Rast_interp_cubic = _libs["grass_raster.8.4"].get("Rast_interp_cubic", "cdecl")
    Rast_interp_cubic.argtypes = [c_double, DCELL, DCELL, DCELL, DCELL]
    Rast_interp_cubic.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 383
if _libs["grass_raster.8.4"].has("Rast_interp_bicubic", "cdecl"):
    Rast_interp_bicubic = _libs["grass_raster.8.4"].get("Rast_interp_bicubic", "cdecl")
    Rast_interp_bicubic.argtypes = [c_double, c_double, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL]
    Rast_interp_bicubic.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 386
if _libs["grass_raster.8.4"].has("Rast_interp_lanczos", "cdecl"):
    Rast_interp_lanczos = _libs["grass_raster.8.4"].get("Rast_interp_lanczos", "cdecl")
    Rast_interp_lanczos.argtypes = [c_double, c_double, POINTER(DCELL)]
    Rast_interp_lanczos.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 387
if _libs["grass_raster.8.4"].has("Rast_interp_cubic_bspline", "cdecl"):
    Rast_interp_cubic_bspline = _libs["grass_raster.8.4"].get("Rast_interp_cubic_bspline", "cdecl")
    Rast_interp_cubic_bspline.argtypes = [c_double, DCELL, DCELL, DCELL, DCELL]
    Rast_interp_cubic_bspline.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 388
if _libs["grass_raster.8.4"].has("Rast_interp_bicubic_bspline", "cdecl"):
    Rast_interp_bicubic_bspline = _libs["grass_raster.8.4"].get("Rast_interp_bicubic_bspline", "cdecl")
    Rast_interp_bicubic_bspline.argtypes = [c_double, c_double, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL, DCELL]
    Rast_interp_bicubic_bspline.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 391
if _libs["grass_raster.8.4"].has("Rast_option_to_interp_type", "cdecl"):
    Rast_option_to_interp_type = _libs["grass_raster.8.4"].get("Rast_option_to_interp_type", "cdecl")
    Rast_option_to_interp_type.argtypes = [POINTER(struct_Option)]
    Rast_option_to_interp_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 394
if _libs["grass_raster.8.4"].has("Rast_mask_info", "cdecl"):
    Rast_mask_info = _libs["grass_raster.8.4"].get("Rast_mask_info", "cdecl")
    Rast_mask_info.argtypes = []
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_mask_info.restype = ReturnString
    else:
        Rast_mask_info.restype = String
        Rast_mask_info.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 395
if _libs["grass_raster.8.4"].has("Rast__mask_info", "cdecl"):
    Rast__mask_info = _libs["grass_raster.8.4"].get("Rast__mask_info", "cdecl")
    Rast__mask_info.argtypes = [String, String]
    Rast__mask_info.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 398
if _libs["grass_raster.8.4"].has("Rast_maskfd", "cdecl"):
    Rast_maskfd = _libs["grass_raster.8.4"].get("Rast_maskfd", "cdecl")
    Rast_maskfd.argtypes = []
    Rast_maskfd.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 408
if _libs["grass_raster.8.4"].has("Rast__set_null_value", "cdecl"):
    Rast__set_null_value = _libs["grass_raster.8.4"].get("Rast__set_null_value", "cdecl")
    Rast__set_null_value.argtypes = [POINTER(None), c_int, c_int, RASTER_MAP_TYPE]
    Rast__set_null_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 409
if _libs["grass_raster.8.4"].has("Rast_set_null_value", "cdecl"):
    Rast_set_null_value = _libs["grass_raster.8.4"].get("Rast_set_null_value", "cdecl")
    Rast_set_null_value.argtypes = [POINTER(None), c_int, RASTER_MAP_TYPE]
    Rast_set_null_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 410
if _libs["grass_raster.8.4"].has("Rast_set_c_null_value", "cdecl"):
    Rast_set_c_null_value = _libs["grass_raster.8.4"].get("Rast_set_c_null_value", "cdecl")
    Rast_set_c_null_value.argtypes = [POINTER(CELL), c_int]
    Rast_set_c_null_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 411
if _libs["grass_raster.8.4"].has("Rast_set_f_null_value", "cdecl"):
    Rast_set_f_null_value = _libs["grass_raster.8.4"].get("Rast_set_f_null_value", "cdecl")
    Rast_set_f_null_value.argtypes = [POINTER(FCELL), c_int]
    Rast_set_f_null_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 412
if _libs["grass_raster.8.4"].has("Rast_set_d_null_value", "cdecl"):
    Rast_set_d_null_value = _libs["grass_raster.8.4"].get("Rast_set_d_null_value", "cdecl")
    Rast_set_d_null_value.argtypes = [POINTER(DCELL), c_int]
    Rast_set_d_null_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 413
if _libs["grass_raster.8.4"].has("Rast_is_null_value", "cdecl"):
    Rast_is_null_value = _libs["grass_raster.8.4"].get("Rast_is_null_value", "cdecl")
    Rast_is_null_value.argtypes = [POINTER(None), RASTER_MAP_TYPE]
    Rast_is_null_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 424
if _libs["grass_raster.8.4"].has("Rast_insert_null_values", "cdecl"):
    Rast_insert_null_values = _libs["grass_raster.8.4"].get("Rast_insert_null_values", "cdecl")
    Rast_insert_null_values.argtypes = [POINTER(None), String, c_int, RASTER_MAP_TYPE]
    Rast_insert_null_values.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 425
if _libs["grass_raster.8.4"].has("Rast_insert_c_null_values", "cdecl"):
    Rast_insert_c_null_values = _libs["grass_raster.8.4"].get("Rast_insert_c_null_values", "cdecl")
    Rast_insert_c_null_values.argtypes = [POINTER(CELL), String, c_int]
    Rast_insert_c_null_values.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 426
if _libs["grass_raster.8.4"].has("Rast_insert_f_null_values", "cdecl"):
    Rast_insert_f_null_values = _libs["grass_raster.8.4"].get("Rast_insert_f_null_values", "cdecl")
    Rast_insert_f_null_values.argtypes = [POINTER(FCELL), String, c_int]
    Rast_insert_f_null_values.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 427
if _libs["grass_raster.8.4"].has("Rast_insert_d_null_values", "cdecl"):
    Rast_insert_d_null_values = _libs["grass_raster.8.4"].get("Rast_insert_d_null_values", "cdecl")
    Rast_insert_d_null_values.argtypes = [POINTER(DCELL), String, c_int]
    Rast_insert_d_null_values.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 428
if _libs["grass_raster.8.4"].has("Rast__check_null_bit", "cdecl"):
    Rast__check_null_bit = _libs["grass_raster.8.4"].get("Rast__check_null_bit", "cdecl")
    Rast__check_null_bit.argtypes = [POINTER(c_ubyte), c_int, c_int]
    Rast__check_null_bit.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 429
if _libs["grass_raster.8.4"].has("Rast__convert_01_flags", "cdecl"):
    Rast__convert_01_flags = _libs["grass_raster.8.4"].get("Rast__convert_01_flags", "cdecl")
    Rast__convert_01_flags.argtypes = [String, POINTER(c_ubyte), c_int]
    Rast__convert_01_flags.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 430
if _libs["grass_raster.8.4"].has("Rast__convert_flags_01", "cdecl"):
    Rast__convert_flags_01 = _libs["grass_raster.8.4"].get("Rast__convert_flags_01", "cdecl")
    Rast__convert_flags_01.argtypes = [String, POINTER(c_ubyte), c_int]
    Rast__convert_flags_01.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 431
if _libs["grass_raster.8.4"].has("Rast__init_null_bits", "cdecl"):
    Rast__init_null_bits = _libs["grass_raster.8.4"].get("Rast__init_null_bits", "cdecl")
    Rast__init_null_bits.argtypes = [POINTER(c_ubyte), c_int]
    Rast__init_null_bits.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 434
if _libs["grass_raster.8.4"].has("Rast_open_old", "cdecl"):
    Rast_open_old = _libs["grass_raster.8.4"].get("Rast_open_old", "cdecl")
    Rast_open_old.argtypes = [String, String]
    Rast_open_old.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 435
if _libs["grass_raster.8.4"].has("Rast__open_old", "cdecl"):
    Rast__open_old = _libs["grass_raster.8.4"].get("Rast__open_old", "cdecl")
    Rast__open_old.argtypes = [String, String]
    Rast__open_old.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 436
if _libs["grass_raster.8.4"].has("Rast_open_c_new", "cdecl"):
    Rast_open_c_new = _libs["grass_raster.8.4"].get("Rast_open_c_new", "cdecl")
    Rast_open_c_new.argtypes = [String]
    Rast_open_c_new.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 437
if _libs["grass_raster.8.4"].has("Rast_open_c_new_uncompressed", "cdecl"):
    Rast_open_c_new_uncompressed = _libs["grass_raster.8.4"].get("Rast_open_c_new_uncompressed", "cdecl")
    Rast_open_c_new_uncompressed.argtypes = [String]
    Rast_open_c_new_uncompressed.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 438
if _libs["grass_raster.8.4"].has("Rast_want_histogram", "cdecl"):
    Rast_want_histogram = _libs["grass_raster.8.4"].get("Rast_want_histogram", "cdecl")
    Rast_want_histogram.argtypes = [c_int]
    Rast_want_histogram.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 439
if _libs["grass_raster.8.4"].has("Rast_set_cell_format", "cdecl"):
    Rast_set_cell_format = _libs["grass_raster.8.4"].get("Rast_set_cell_format", "cdecl")
    Rast_set_cell_format.argtypes = [c_int]
    Rast_set_cell_format.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 440
if _libs["grass_raster.8.4"].has("Rast_get_cell_format", "cdecl"):
    Rast_get_cell_format = _libs["grass_raster.8.4"].get("Rast_get_cell_format", "cdecl")
    Rast_get_cell_format.argtypes = [CELL]
    Rast_get_cell_format.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 441
if _libs["grass_raster.8.4"].has("Rast_open_fp_new", "cdecl"):
    Rast_open_fp_new = _libs["grass_raster.8.4"].get("Rast_open_fp_new", "cdecl")
    Rast_open_fp_new.argtypes = [String]
    Rast_open_fp_new.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 442
if _libs["grass_raster.8.4"].has("Rast_open_fp_new_uncompressed", "cdecl"):
    Rast_open_fp_new_uncompressed = _libs["grass_raster.8.4"].get("Rast_open_fp_new_uncompressed", "cdecl")
    Rast_open_fp_new_uncompressed.argtypes = [String]
    Rast_open_fp_new_uncompressed.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 443
if _libs["grass_raster.8.4"].has("Rast_set_fp_type", "cdecl"):
    Rast_set_fp_type = _libs["grass_raster.8.4"].get("Rast_set_fp_type", "cdecl")
    Rast_set_fp_type.argtypes = [RASTER_MAP_TYPE]
    Rast_set_fp_type.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 444
if _libs["grass_raster.8.4"].has("Rast_map_is_fp", "cdecl"):
    Rast_map_is_fp = _libs["grass_raster.8.4"].get("Rast_map_is_fp", "cdecl")
    Rast_map_is_fp.argtypes = [String, String]
    Rast_map_is_fp.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 445
if _libs["grass_raster.8.4"].has("Rast_map_type", "cdecl"):
    Rast_map_type = _libs["grass_raster.8.4"].get("Rast_map_type", "cdecl")
    Rast_map_type.argtypes = [String, String]
    Rast_map_type.restype = RASTER_MAP_TYPE

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 446
if _libs["grass_raster.8.4"].has("Rast__check_fp_type", "cdecl"):
    Rast__check_fp_type = _libs["grass_raster.8.4"].get("Rast__check_fp_type", "cdecl")
    Rast__check_fp_type.argtypes = [String, String]
    Rast__check_fp_type.restype = RASTER_MAP_TYPE

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 447
if _libs["grass_raster.8.4"].has("Rast_get_map_type", "cdecl"):
    Rast_get_map_type = _libs["grass_raster.8.4"].get("Rast_get_map_type", "cdecl")
    Rast_get_map_type.argtypes = [c_int]
    Rast_get_map_type.restype = RASTER_MAP_TYPE

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 448
if _libs["grass_raster.8.4"].has("Rast_open_new", "cdecl"):
    Rast_open_new = _libs["grass_raster.8.4"].get("Rast_open_new", "cdecl")
    Rast_open_new.argtypes = [String, RASTER_MAP_TYPE]
    Rast_open_new.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 449
if _libs["grass_raster.8.4"].has("Rast_open_new_uncompressed", "cdecl"):
    Rast_open_new_uncompressed = _libs["grass_raster.8.4"].get("Rast_open_new_uncompressed", "cdecl")
    Rast_open_new_uncompressed.argtypes = [String, RASTER_MAP_TYPE]
    Rast_open_new_uncompressed.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 450
if _libs["grass_raster.8.4"].has("Rast_set_quant_rules", "cdecl"):
    Rast_set_quant_rules = _libs["grass_raster.8.4"].get("Rast_set_quant_rules", "cdecl")
    Rast_set_quant_rules.argtypes = [c_int, POINTER(struct_Quant)]
    Rast_set_quant_rules.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 451
if _libs["grass_raster.8.4"].has("Rast__open_null_write", "cdecl"):
    Rast__open_null_write = _libs["grass_raster.8.4"].get("Rast__open_null_write", "cdecl")
    Rast__open_null_write.argtypes = [String]
    Rast__open_null_write.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 454
if _libs["grass_raster.8.4"].has("Rast_put_cellhd", "cdecl"):
    Rast_put_cellhd = _libs["grass_raster.8.4"].get("Rast_put_cellhd", "cdecl")
    Rast_put_cellhd.argtypes = [String, POINTER(struct_Cell_head)]
    Rast_put_cellhd.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 457
if _libs["grass_raster.8.4"].has("Rast_put_row", "cdecl"):
    Rast_put_row = _libs["grass_raster.8.4"].get("Rast_put_row", "cdecl")
    Rast_put_row.argtypes = [c_int, POINTER(None), RASTER_MAP_TYPE]
    Rast_put_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 458
if _libs["grass_raster.8.4"].has("Rast_put_c_row", "cdecl"):
    Rast_put_c_row = _libs["grass_raster.8.4"].get("Rast_put_c_row", "cdecl")
    Rast_put_c_row.argtypes = [c_int, POINTER(CELL)]
    Rast_put_c_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 459
if _libs["grass_raster.8.4"].has("Rast_put_f_row", "cdecl"):
    Rast_put_f_row = _libs["grass_raster.8.4"].get("Rast_put_f_row", "cdecl")
    Rast_put_f_row.argtypes = [c_int, POINTER(FCELL)]
    Rast_put_f_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 460
if _libs["grass_raster.8.4"].has("Rast_put_d_row", "cdecl"):
    Rast_put_d_row = _libs["grass_raster.8.4"].get("Rast_put_d_row", "cdecl")
    Rast_put_d_row.argtypes = [c_int, POINTER(DCELL)]
    Rast_put_d_row.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 461
if _libs["grass_raster.8.4"].has("Rast__write_null_bits", "cdecl"):
    Rast__write_null_bits = _libs["grass_raster.8.4"].get("Rast__write_null_bits", "cdecl")
    Rast__write_null_bits.argtypes = [c_int, POINTER(c_ubyte)]
    Rast__write_null_bits.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 464
if _libs["grass_raster.8.4"].has("Rast_put_cell_title", "cdecl"):
    Rast_put_cell_title = _libs["grass_raster.8.4"].get("Rast_put_cell_title", "cdecl")
    Rast_put_cell_title.argtypes = [String, String]
    Rast_put_cell_title.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 467
if _libs["grass_raster.8.4"].has("Rast_quant_clear", "cdecl"):
    Rast_quant_clear = _libs["grass_raster.8.4"].get("Rast_quant_clear", "cdecl")
    Rast_quant_clear.argtypes = [POINTER(struct_Quant)]
    Rast_quant_clear.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 468
if _libs["grass_raster.8.4"].has("Rast_quant_free", "cdecl"):
    Rast_quant_free = _libs["grass_raster.8.4"].get("Rast_quant_free", "cdecl")
    Rast_quant_free.argtypes = [POINTER(struct_Quant)]
    Rast_quant_free.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 469
if _libs["grass_raster.8.4"].has("Rast__quant_organize_fp_lookup", "cdecl"):
    Rast__quant_organize_fp_lookup = _libs["grass_raster.8.4"].get("Rast__quant_organize_fp_lookup", "cdecl")
    Rast__quant_organize_fp_lookup.argtypes = [POINTER(struct_Quant)]
    Rast__quant_organize_fp_lookup.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 470
if _libs["grass_raster.8.4"].has("Rast_quant_init", "cdecl"):
    Rast_quant_init = _libs["grass_raster.8.4"].get("Rast_quant_init", "cdecl")
    Rast_quant_init.argtypes = [POINTER(struct_Quant)]
    Rast_quant_init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 471
if _libs["grass_raster.8.4"].has("Rast_quant_is_truncate", "cdecl"):
    Rast_quant_is_truncate = _libs["grass_raster.8.4"].get("Rast_quant_is_truncate", "cdecl")
    Rast_quant_is_truncate.argtypes = [POINTER(struct_Quant)]
    Rast_quant_is_truncate.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 472
if _libs["grass_raster.8.4"].has("Rast_quant_is_round", "cdecl"):
    Rast_quant_is_round = _libs["grass_raster.8.4"].get("Rast_quant_is_round", "cdecl")
    Rast_quant_is_round.argtypes = [POINTER(struct_Quant)]
    Rast_quant_is_round.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 473
if _libs["grass_raster.8.4"].has("Rast_quant_truncate", "cdecl"):
    Rast_quant_truncate = _libs["grass_raster.8.4"].get("Rast_quant_truncate", "cdecl")
    Rast_quant_truncate.argtypes = [POINTER(struct_Quant)]
    Rast_quant_truncate.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 474
if _libs["grass_raster.8.4"].has("Rast_quant_round", "cdecl"):
    Rast_quant_round = _libs["grass_raster.8.4"].get("Rast_quant_round", "cdecl")
    Rast_quant_round.argtypes = [POINTER(struct_Quant)]
    Rast_quant_round.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 475
if _libs["grass_raster.8.4"].has("Rast_quant_get_limits", "cdecl"):
    Rast_quant_get_limits = _libs["grass_raster.8.4"].get("Rast_quant_get_limits", "cdecl")
    Rast_quant_get_limits.argtypes = [POINTER(struct_Quant), POINTER(DCELL), POINTER(DCELL), POINTER(CELL), POINTER(CELL)]
    Rast_quant_get_limits.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 477
if _libs["grass_raster.8.4"].has("Rast_quant_nof_rules", "cdecl"):
    Rast_quant_nof_rules = _libs["grass_raster.8.4"].get("Rast_quant_nof_rules", "cdecl")
    Rast_quant_nof_rules.argtypes = [POINTER(struct_Quant)]
    Rast_quant_nof_rules.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 478
if _libs["grass_raster.8.4"].has("Rast_quant_get_ith_rule", "cdecl"):
    Rast_quant_get_ith_rule = _libs["grass_raster.8.4"].get("Rast_quant_get_ith_rule", "cdecl")
    Rast_quant_get_ith_rule.argtypes = [POINTER(struct_Quant), c_int, POINTER(DCELL), POINTER(DCELL), POINTER(CELL), POINTER(CELL)]
    Rast_quant_get_ith_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 480
if _libs["grass_raster.8.4"].has("Rast_quant_set_neg_infinite_rule", "cdecl"):
    Rast_quant_set_neg_infinite_rule = _libs["grass_raster.8.4"].get("Rast_quant_set_neg_infinite_rule", "cdecl")
    Rast_quant_set_neg_infinite_rule.argtypes = [POINTER(struct_Quant), DCELL, CELL]
    Rast_quant_set_neg_infinite_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 481
if _libs["grass_raster.8.4"].has("Rast_quant_get_neg_infinite_rule", "cdecl"):
    Rast_quant_get_neg_infinite_rule = _libs["grass_raster.8.4"].get("Rast_quant_get_neg_infinite_rule", "cdecl")
    Rast_quant_get_neg_infinite_rule.argtypes = [POINTER(struct_Quant), POINTER(DCELL), POINTER(CELL)]
    Rast_quant_get_neg_infinite_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 482
if _libs["grass_raster.8.4"].has("Rast_quant_set_pos_infinite_rule", "cdecl"):
    Rast_quant_set_pos_infinite_rule = _libs["grass_raster.8.4"].get("Rast_quant_set_pos_infinite_rule", "cdecl")
    Rast_quant_set_pos_infinite_rule.argtypes = [POINTER(struct_Quant), DCELL, CELL]
    Rast_quant_set_pos_infinite_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 483
if _libs["grass_raster.8.4"].has("Rast_quant_get_pos_infinite_rule", "cdecl"):
    Rast_quant_get_pos_infinite_rule = _libs["grass_raster.8.4"].get("Rast_quant_get_pos_infinite_rule", "cdecl")
    Rast_quant_get_pos_infinite_rule.argtypes = [POINTER(struct_Quant), POINTER(DCELL), POINTER(CELL)]
    Rast_quant_get_pos_infinite_rule.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 484
if _libs["grass_raster.8.4"].has("Rast_quant_add_rule", "cdecl"):
    Rast_quant_add_rule = _libs["grass_raster.8.4"].get("Rast_quant_add_rule", "cdecl")
    Rast_quant_add_rule.argtypes = [POINTER(struct_Quant), DCELL, DCELL, CELL, CELL]
    Rast_quant_add_rule.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 485
if _libs["grass_raster.8.4"].has("Rast_quant_reverse_rule_order", "cdecl"):
    Rast_quant_reverse_rule_order = _libs["grass_raster.8.4"].get("Rast_quant_reverse_rule_order", "cdecl")
    Rast_quant_reverse_rule_order.argtypes = [POINTER(struct_Quant)]
    Rast_quant_reverse_rule_order.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 486
if _libs["grass_raster.8.4"].has("Rast_quant_get_cell_value", "cdecl"):
    Rast_quant_get_cell_value = _libs["grass_raster.8.4"].get("Rast_quant_get_cell_value", "cdecl")
    Rast_quant_get_cell_value.argtypes = [POINTER(struct_Quant), DCELL]
    Rast_quant_get_cell_value.restype = CELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 487
if _libs["grass_raster.8.4"].has("Rast_quant_perform_d", "cdecl"):
    Rast_quant_perform_d = _libs["grass_raster.8.4"].get("Rast_quant_perform_d", "cdecl")
    Rast_quant_perform_d.argtypes = [POINTER(struct_Quant), POINTER(DCELL), POINTER(CELL), c_int]
    Rast_quant_perform_d.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 488
if _libs["grass_raster.8.4"].has("Rast_quant_perform_f", "cdecl"):
    Rast_quant_perform_f = _libs["grass_raster.8.4"].get("Rast_quant_perform_f", "cdecl")
    Rast_quant_perform_f.argtypes = [POINTER(struct_Quant), POINTER(FCELL), POINTER(CELL), c_int]
    Rast_quant_perform_f.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 489
if _libs["grass_raster.8.4"].has("Rast__quant_get_rule_for_d_raster_val", "cdecl"):
    Rast__quant_get_rule_for_d_raster_val = _libs["grass_raster.8.4"].get("Rast__quant_get_rule_for_d_raster_val", "cdecl")
    Rast__quant_get_rule_for_d_raster_val.argtypes = [POINTER(struct_Quant), DCELL]
    Rast__quant_get_rule_for_d_raster_val.restype = POINTER(struct_Quant_table)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 493
if _libs["grass_raster.8.4"].has("Rast__quant_import", "cdecl"):
    Rast__quant_import = _libs["grass_raster.8.4"].get("Rast__quant_import", "cdecl")
    Rast__quant_import.argtypes = [String, String, POINTER(struct_Quant)]
    Rast__quant_import.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 494
if _libs["grass_raster.8.4"].has("Rast__quant_export", "cdecl"):
    Rast__quant_export = _libs["grass_raster.8.4"].get("Rast__quant_export", "cdecl")
    Rast__quant_export.argtypes = [String, String, POINTER(struct_Quant)]
    Rast__quant_export.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 497
if _libs["grass_raster.8.4"].has("Rast_truncate_fp_map", "cdecl"):
    Rast_truncate_fp_map = _libs["grass_raster.8.4"].get("Rast_truncate_fp_map", "cdecl")
    Rast_truncate_fp_map.argtypes = [String, String]
    Rast_truncate_fp_map.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 498
if _libs["grass_raster.8.4"].has("Rast_round_fp_map", "cdecl"):
    Rast_round_fp_map = _libs["grass_raster.8.4"].get("Rast_round_fp_map", "cdecl")
    Rast_round_fp_map.argtypes = [String, String]
    Rast_round_fp_map.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 499
if _libs["grass_raster.8.4"].has("Rast_quantize_fp_map", "cdecl"):
    Rast_quantize_fp_map = _libs["grass_raster.8.4"].get("Rast_quantize_fp_map", "cdecl")
    Rast_quantize_fp_map.argtypes = [String, String, CELL, CELL]
    Rast_quantize_fp_map.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 500
if _libs["grass_raster.8.4"].has("Rast_quantize_fp_map_range", "cdecl"):
    Rast_quantize_fp_map_range = _libs["grass_raster.8.4"].get("Rast_quantize_fp_map_range", "cdecl")
    Rast_quantize_fp_map_range.argtypes = [String, String, DCELL, DCELL, CELL, CELL]
    Rast_quantize_fp_map_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 502
if _libs["grass_raster.8.4"].has("Rast_write_quant", "cdecl"):
    Rast_write_quant = _libs["grass_raster.8.4"].get("Rast_write_quant", "cdecl")
    Rast_write_quant.argtypes = [String, String, POINTER(struct_Quant)]
    Rast_write_quant.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 503
if _libs["grass_raster.8.4"].has("Rast_read_quant", "cdecl"):
    Rast_read_quant = _libs["grass_raster.8.4"].get("Rast_read_quant", "cdecl")
    Rast_read_quant.argtypes = [String, String, POINTER(struct_Quant)]
    Rast_read_quant.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 506
if _libs["grass_raster.8.4"].has("Rast__remove_fp_range", "cdecl"):
    Rast__remove_fp_range = _libs["grass_raster.8.4"].get("Rast__remove_fp_range", "cdecl")
    Rast__remove_fp_range.argtypes = [String]
    Rast__remove_fp_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 507
if _libs["grass_raster.8.4"].has("Rast_construct_default_range", "cdecl"):
    Rast_construct_default_range = _libs["grass_raster.8.4"].get("Rast_construct_default_range", "cdecl")
    Rast_construct_default_range.argtypes = [POINTER(struct_Range)]
    Rast_construct_default_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 508
if _libs["grass_raster.8.4"].has("Rast_read_fp_range", "cdecl"):
    Rast_read_fp_range = _libs["grass_raster.8.4"].get("Rast_read_fp_range", "cdecl")
    Rast_read_fp_range.argtypes = [String, String, POINTER(struct_FPRange)]
    Rast_read_fp_range.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 509
if _libs["grass_raster.8.4"].has("Rast_read_range", "cdecl"):
    Rast_read_range = _libs["grass_raster.8.4"].get("Rast_read_range", "cdecl")
    Rast_read_range.argtypes = [String, String, POINTER(struct_Range)]
    Rast_read_range.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 510
if _libs["grass_raster.8.4"].has("Rast_write_range", "cdecl"):
    Rast_write_range = _libs["grass_raster.8.4"].get("Rast_write_range", "cdecl")
    Rast_write_range.argtypes = [String, POINTER(struct_Range)]
    Rast_write_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 511
if _libs["grass_raster.8.4"].has("Rast_write_fp_range", "cdecl"):
    Rast_write_fp_range = _libs["grass_raster.8.4"].get("Rast_write_fp_range", "cdecl")
    Rast_write_fp_range.argtypes = [String, POINTER(struct_FPRange)]
    Rast_write_fp_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 512
if _libs["grass_raster.8.4"].has("Rast_update_range", "cdecl"):
    Rast_update_range = _libs["grass_raster.8.4"].get("Rast_update_range", "cdecl")
    Rast_update_range.argtypes = [CELL, POINTER(struct_Range)]
    Rast_update_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 513
if _libs["grass_raster.8.4"].has("Rast_update_fp_range", "cdecl"):
    Rast_update_fp_range = _libs["grass_raster.8.4"].get("Rast_update_fp_range", "cdecl")
    Rast_update_fp_range.argtypes = [DCELL, POINTER(struct_FPRange)]
    Rast_update_fp_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 514
if _libs["grass_raster.8.4"].has("Rast_row_update_range", "cdecl"):
    Rast_row_update_range = _libs["grass_raster.8.4"].get("Rast_row_update_range", "cdecl")
    Rast_row_update_range.argtypes = [POINTER(CELL), c_int, POINTER(struct_Range)]
    Rast_row_update_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 515
if _libs["grass_raster.8.4"].has("Rast__row_update_range", "cdecl"):
    Rast__row_update_range = _libs["grass_raster.8.4"].get("Rast__row_update_range", "cdecl")
    Rast__row_update_range.argtypes = [POINTER(CELL), c_int, POINTER(struct_Range), c_int]
    Rast__row_update_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 516
if _libs["grass_raster.8.4"].has("Rast_row_update_fp_range", "cdecl"):
    Rast_row_update_fp_range = _libs["grass_raster.8.4"].get("Rast_row_update_fp_range", "cdecl")
    Rast_row_update_fp_range.argtypes = [POINTER(None), c_int, POINTER(struct_FPRange), RASTER_MAP_TYPE]
    Rast_row_update_fp_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 518
if _libs["grass_raster.8.4"].has("Rast_init_range", "cdecl"):
    Rast_init_range = _libs["grass_raster.8.4"].get("Rast_init_range", "cdecl")
    Rast_init_range.argtypes = [POINTER(struct_Range)]
    Rast_init_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 519
if _libs["grass_raster.8.4"].has("Rast_get_range_min_max", "cdecl"):
    Rast_get_range_min_max = _libs["grass_raster.8.4"].get("Rast_get_range_min_max", "cdecl")
    Rast_get_range_min_max.argtypes = [POINTER(struct_Range), POINTER(CELL), POINTER(CELL)]
    Rast_get_range_min_max.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 520
if _libs["grass_raster.8.4"].has("Rast_init_fp_range", "cdecl"):
    Rast_init_fp_range = _libs["grass_raster.8.4"].get("Rast_init_fp_range", "cdecl")
    Rast_init_fp_range.argtypes = [POINTER(struct_FPRange)]
    Rast_init_fp_range.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 521
if _libs["grass_raster.8.4"].has("Rast_get_fp_range_min_max", "cdecl"):
    Rast_get_fp_range_min_max = _libs["grass_raster.8.4"].get("Rast_get_fp_range_min_max", "cdecl")
    Rast_get_fp_range_min_max.argtypes = [POINTER(struct_FPRange), POINTER(DCELL), POINTER(DCELL)]
    Rast_get_fp_range_min_max.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 523
if _libs["grass_raster.8.4"].has("Rast_read_rstats", "cdecl"):
    Rast_read_rstats = _libs["grass_raster.8.4"].get("Rast_read_rstats", "cdecl")
    Rast_read_rstats.argtypes = [String, String, POINTER(struct_R_stats)]
    Rast_read_rstats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 524
if _libs["grass_raster.8.4"].has("Rast_write_rstats", "cdecl"):
    Rast_write_rstats = _libs["grass_raster.8.4"].get("Rast_write_rstats", "cdecl")
    Rast_write_rstats.argtypes = [String, POINTER(struct_R_stats)]
    Rast_write_rstats.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 527
if _libs["grass_raster.8.4"].has("Rast_raster_cmp", "cdecl"):
    Rast_raster_cmp = _libs["grass_raster.8.4"].get("Rast_raster_cmp", "cdecl")
    Rast_raster_cmp.argtypes = [POINTER(None), POINTER(None), RASTER_MAP_TYPE]
    Rast_raster_cmp.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 528
if _libs["grass_raster.8.4"].has("Rast_raster_cpy", "cdecl"):
    Rast_raster_cpy = _libs["grass_raster.8.4"].get("Rast_raster_cpy", "cdecl")
    Rast_raster_cpy.argtypes = [POINTER(None), POINTER(None), c_int, RASTER_MAP_TYPE]
    Rast_raster_cpy.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 529
if _libs["grass_raster.8.4"].has("Rast_set_c_value", "cdecl"):
    Rast_set_c_value = _libs["grass_raster.8.4"].get("Rast_set_c_value", "cdecl")
    Rast_set_c_value.argtypes = [POINTER(None), CELL, RASTER_MAP_TYPE]
    Rast_set_c_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 530
if _libs["grass_raster.8.4"].has("Rast_set_f_value", "cdecl"):
    Rast_set_f_value = _libs["grass_raster.8.4"].get("Rast_set_f_value", "cdecl")
    Rast_set_f_value.argtypes = [POINTER(None), FCELL, RASTER_MAP_TYPE]
    Rast_set_f_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 531
if _libs["grass_raster.8.4"].has("Rast_set_d_value", "cdecl"):
    Rast_set_d_value = _libs["grass_raster.8.4"].get("Rast_set_d_value", "cdecl")
    Rast_set_d_value.argtypes = [POINTER(None), DCELL, RASTER_MAP_TYPE]
    Rast_set_d_value.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 532
if _libs["grass_raster.8.4"].has("Rast_get_c_value", "cdecl"):
    Rast_get_c_value = _libs["grass_raster.8.4"].get("Rast_get_c_value", "cdecl")
    Rast_get_c_value.argtypes = [POINTER(None), RASTER_MAP_TYPE]
    Rast_get_c_value.restype = CELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 533
if _libs["grass_raster.8.4"].has("Rast_get_f_value", "cdecl"):
    Rast_get_f_value = _libs["grass_raster.8.4"].get("Rast_get_f_value", "cdecl")
    Rast_get_f_value.argtypes = [POINTER(None), RASTER_MAP_TYPE]
    Rast_get_f_value.restype = FCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 534
if _libs["grass_raster.8.4"].has("Rast_get_d_value", "cdecl"):
    Rast_get_d_value = _libs["grass_raster.8.4"].get("Rast_get_d_value", "cdecl")
    Rast_get_d_value.argtypes = [POINTER(None), RASTER_MAP_TYPE]
    Rast_get_d_value.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 537
if _libs["grass_raster.8.4"].has("Rast_read_units", "cdecl"):
    Rast_read_units = _libs["grass_raster.8.4"].get("Rast_read_units", "cdecl")
    Rast_read_units.argtypes = [String, String]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_read_units.restype = ReturnString
    else:
        Rast_read_units.restype = String
        Rast_read_units.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 538
if _libs["grass_raster.8.4"].has("Rast_read_vdatum", "cdecl"):
    Rast_read_vdatum = _libs["grass_raster.8.4"].get("Rast_read_vdatum", "cdecl")
    Rast_read_vdatum.argtypes = [String, String]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_read_vdatum.restype = ReturnString
    else:
        Rast_read_vdatum.restype = String
        Rast_read_vdatum.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 539
if _libs["grass_raster.8.4"].has("Rast_read_semantic_label", "cdecl"):
    Rast_read_semantic_label = _libs["grass_raster.8.4"].get("Rast_read_semantic_label", "cdecl")
    Rast_read_semantic_label.argtypes = [String, String]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_read_semantic_label.restype = ReturnString
    else:
        Rast_read_semantic_label.restype = String
        Rast_read_semantic_label.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 540
if _libs["grass_raster.8.4"].has("Rast_get_semantic_label_or_name", "cdecl"):
    Rast_get_semantic_label_or_name = _libs["grass_raster.8.4"].get("Rast_get_semantic_label_or_name", "cdecl")
    Rast_get_semantic_label_or_name.argtypes = [String, String]
    if sizeof(c_int) == sizeof(c_void_p):
        Rast_get_semantic_label_or_name.restype = ReturnString
    else:
        Rast_get_semantic_label_or_name.restype = String
        Rast_get_semantic_label_or_name.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 541
if _libs["grass_raster.8.4"].has("Rast_write_units", "cdecl"):
    Rast_write_units = _libs["grass_raster.8.4"].get("Rast_write_units", "cdecl")
    Rast_write_units.argtypes = [String, String]
    Rast_write_units.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 542
if _libs["grass_raster.8.4"].has("Rast_write_vdatum", "cdecl"):
    Rast_write_vdatum = _libs["grass_raster.8.4"].get("Rast_write_vdatum", "cdecl")
    Rast_write_vdatum.argtypes = [String, String]
    Rast_write_vdatum.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 543
if _libs["grass_raster.8.4"].has("Rast_write_semantic_label", "cdecl"):
    Rast_write_semantic_label = _libs["grass_raster.8.4"].get("Rast_write_semantic_label", "cdecl")
    Rast_write_semantic_label.argtypes = [String, String]
    Rast_write_semantic_label.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 544
if _libs["grass_raster.8.4"].has("Rast_legal_semantic_label", "cdecl"):
    Rast_legal_semantic_label = _libs["grass_raster.8.4"].get("Rast_legal_semantic_label", "cdecl")
    Rast_legal_semantic_label.argtypes = [String]
    Rast_legal_semantic_label.restype = c_bool

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 547
if _libs["grass_raster.8.4"].has("Rast_map_to_img_str", "cdecl"):
    Rast_map_to_img_str = _libs["grass_raster.8.4"].get("Rast_map_to_img_str", "cdecl")
    Rast_map_to_img_str.argtypes = [String, c_int, POINTER(c_ubyte)]
    Rast_map_to_img_str.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 550
if _libs["grass_raster.8.4"].has("Rast_is_reclass", "cdecl"):
    Rast_is_reclass = _libs["grass_raster.8.4"].get("Rast_is_reclass", "cdecl")
    Rast_is_reclass.argtypes = [String, String, String, String]
    Rast_is_reclass.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 551
if _libs["grass_raster.8.4"].has("Rast_is_reclassed_to", "cdecl"):
    Rast_is_reclassed_to = _libs["grass_raster.8.4"].get("Rast_is_reclassed_to", "cdecl")
    Rast_is_reclassed_to.argtypes = [String, String, POINTER(c_int), POINTER(POINTER(POINTER(c_char)))]
    Rast_is_reclassed_to.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 552
if _libs["grass_raster.8.4"].has("Rast_get_reclass", "cdecl"):
    Rast_get_reclass = _libs["grass_raster.8.4"].get("Rast_get_reclass", "cdecl")
    Rast_get_reclass.argtypes = [String, String, POINTER(struct_Reclass)]
    Rast_get_reclass.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 553
if _libs["grass_raster.8.4"].has("Rast_free_reclass", "cdecl"):
    Rast_free_reclass = _libs["grass_raster.8.4"].get("Rast_free_reclass", "cdecl")
    Rast_free_reclass.argtypes = [POINTER(struct_Reclass)]
    Rast_free_reclass.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 554
if _libs["grass_raster.8.4"].has("Rast_put_reclass", "cdecl"):
    Rast_put_reclass = _libs["grass_raster.8.4"].get("Rast_put_reclass", "cdecl")
    Rast_put_reclass.argtypes = [String, POINTER(struct_Reclass)]
    Rast_put_reclass.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 557
if _libs["grass_raster.8.4"].has("Rast_get_sample_nearest", "cdecl"):
    Rast_get_sample_nearest = _libs["grass_raster.8.4"].get("Rast_get_sample_nearest", "cdecl")
    Rast_get_sample_nearest.argtypes = [c_int, POINTER(struct_Cell_head), POINTER(struct_Categories), c_double, c_double, c_int]
    Rast_get_sample_nearest.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 559
if _libs["grass_raster.8.4"].has("Rast_get_sample_bilinear", "cdecl"):
    Rast_get_sample_bilinear = _libs["grass_raster.8.4"].get("Rast_get_sample_bilinear", "cdecl")
    Rast_get_sample_bilinear.argtypes = [c_int, POINTER(struct_Cell_head), POINTER(struct_Categories), c_double, c_double, c_int]
    Rast_get_sample_bilinear.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 561
if _libs["grass_raster.8.4"].has("Rast_get_sample_cubic", "cdecl"):
    Rast_get_sample_cubic = _libs["grass_raster.8.4"].get("Rast_get_sample_cubic", "cdecl")
    Rast_get_sample_cubic.argtypes = [c_int, POINTER(struct_Cell_head), POINTER(struct_Categories), c_double, c_double, c_int]
    Rast_get_sample_cubic.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 563
if _libs["grass_raster.8.4"].has("Rast_get_sample", "cdecl"):
    Rast_get_sample = _libs["grass_raster.8.4"].get("Rast_get_sample", "cdecl")
    Rast_get_sample.argtypes = [c_int, POINTER(struct_Cell_head), POINTER(struct_Categories), c_double, c_double, c_int, INTERP_TYPE]
    Rast_get_sample.restype = DCELL

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 567
if _libs["grass_raster.8.4"].has("Rast__init_window", "cdecl"):
    Rast__init_window = _libs["grass_raster.8.4"].get("Rast__init_window", "cdecl")
    Rast__init_window.argtypes = []
    Rast__init_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 568
if _libs["grass_raster.8.4"].has("Rast_set_window", "cdecl"):
    Rast_set_window = _libs["grass_raster.8.4"].get("Rast_set_window", "cdecl")
    Rast_set_window.argtypes = [POINTER(struct_Cell_head)]
    Rast_set_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 569
if _libs["grass_raster.8.4"].has("Rast_unset_window", "cdecl"):
    Rast_unset_window = _libs["grass_raster.8.4"].get("Rast_unset_window", "cdecl")
    Rast_unset_window.argtypes = []
    Rast_unset_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 570
if _libs["grass_raster.8.4"].has("Rast_set_output_window", "cdecl"):
    Rast_set_output_window = _libs["grass_raster.8.4"].get("Rast_set_output_window", "cdecl")
    Rast_set_output_window.argtypes = [POINTER(struct_Cell_head)]
    Rast_set_output_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 571
if _libs["grass_raster.8.4"].has("Rast_set_input_window", "cdecl"):
    Rast_set_input_window = _libs["grass_raster.8.4"].get("Rast_set_input_window", "cdecl")
    Rast_set_input_window.argtypes = [POINTER(struct_Cell_head)]
    Rast_set_input_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 574
if _libs["grass_raster.8.4"].has("Rast_get_vrt", "cdecl"):
    Rast_get_vrt = _libs["grass_raster.8.4"].get("Rast_get_vrt", "cdecl")
    Rast_get_vrt.argtypes = [String, String]
    Rast_get_vrt.restype = POINTER(struct_R_vrt)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 575
if _libs["grass_raster.8.4"].has("Rast_close_vrt", "cdecl"):
    Rast_close_vrt = _libs["grass_raster.8.4"].get("Rast_close_vrt", "cdecl")
    Rast_close_vrt.argtypes = [POINTER(struct_R_vrt)]
    Rast_close_vrt.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 576
if _libs["grass_raster.8.4"].has("Rast_get_vrt_row", "cdecl"):
    Rast_get_vrt_row = _libs["grass_raster.8.4"].get("Rast_get_vrt_row", "cdecl")
    Rast_get_vrt_row.argtypes = [c_int, POINTER(None), c_int, RASTER_MAP_TYPE]
    Rast_get_vrt_row.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 579
if _libs["grass_raster.8.4"].has("Rast_get_window", "cdecl"):
    Rast_get_window = _libs["grass_raster.8.4"].get("Rast_get_window", "cdecl")
    Rast_get_window.argtypes = [POINTER(struct_Cell_head)]
    Rast_get_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 580
if _libs["grass_raster.8.4"].has("Rast_get_input_window", "cdecl"):
    Rast_get_input_window = _libs["grass_raster.8.4"].get("Rast_get_input_window", "cdecl")
    Rast_get_input_window.argtypes = [POINTER(struct_Cell_head)]
    Rast_get_input_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 581
if _libs["grass_raster.8.4"].has("Rast_get_output_window", "cdecl"):
    Rast_get_output_window = _libs["grass_raster.8.4"].get("Rast_get_output_window", "cdecl")
    Rast_get_output_window.argtypes = [POINTER(struct_Cell_head)]
    Rast_get_output_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 582
if _libs["grass_raster.8.4"].has("Rast_window_rows", "cdecl"):
    Rast_window_rows = _libs["grass_raster.8.4"].get("Rast_window_rows", "cdecl")
    Rast_window_rows.argtypes = []
    Rast_window_rows.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 583
if _libs["grass_raster.8.4"].has("Rast_window_cols", "cdecl"):
    Rast_window_cols = _libs["grass_raster.8.4"].get("Rast_window_cols", "cdecl")
    Rast_window_cols.argtypes = []
    Rast_window_cols.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 584
if _libs["grass_raster.8.4"].has("Rast_input_window_rows", "cdecl"):
    Rast_input_window_rows = _libs["grass_raster.8.4"].get("Rast_input_window_rows", "cdecl")
    Rast_input_window_rows.argtypes = []
    Rast_input_window_rows.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 585
if _libs["grass_raster.8.4"].has("Rast_input_window_cols", "cdecl"):
    Rast_input_window_cols = _libs["grass_raster.8.4"].get("Rast_input_window_cols", "cdecl")
    Rast_input_window_cols.argtypes = []
    Rast_input_window_cols.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 586
if _libs["grass_raster.8.4"].has("Rast_output_window_rows", "cdecl"):
    Rast_output_window_rows = _libs["grass_raster.8.4"].get("Rast_output_window_rows", "cdecl")
    Rast_output_window_rows.argtypes = []
    Rast_output_window_rows.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 587
if _libs["grass_raster.8.4"].has("Rast_output_window_cols", "cdecl"):
    Rast_output_window_cols = _libs["grass_raster.8.4"].get("Rast_output_window_cols", "cdecl")
    Rast_output_window_cols.argtypes = []
    Rast_output_window_cols.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 588
if _libs["grass_raster.8.4"].has("Rast_northing_to_row", "cdecl"):
    Rast_northing_to_row = _libs["grass_raster.8.4"].get("Rast_northing_to_row", "cdecl")
    Rast_northing_to_row.argtypes = [c_double, POINTER(struct_Cell_head)]
    Rast_northing_to_row.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 589
if _libs["grass_raster.8.4"].has("Rast_easting_to_col", "cdecl"):
    Rast_easting_to_col = _libs["grass_raster.8.4"].get("Rast_easting_to_col", "cdecl")
    Rast_easting_to_col.argtypes = [c_double, POINTER(struct_Cell_head)]
    Rast_easting_to_col.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 590
if _libs["grass_raster.8.4"].has("Rast_row_to_northing", "cdecl"):
    Rast_row_to_northing = _libs["grass_raster.8.4"].get("Rast_row_to_northing", "cdecl")
    Rast_row_to_northing.argtypes = [c_double, POINTER(struct_Cell_head)]
    Rast_row_to_northing.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 591
if _libs["grass_raster.8.4"].has("Rast_col_to_easting", "cdecl"):
    Rast_col_to_easting = _libs["grass_raster.8.4"].get("Rast_col_to_easting", "cdecl")
    Rast_col_to_easting.argtypes = [c_double, POINTER(struct_Cell_head)]
    Rast_col_to_easting.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 594
if _libs["grass_raster.8.4"].has("Rast__create_window_mapping", "cdecl"):
    Rast__create_window_mapping = _libs["grass_raster.8.4"].get("Rast__create_window_mapping", "cdecl")
    Rast__create_window_mapping.argtypes = [c_int]
    Rast__create_window_mapping.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 595
if _libs["grass_raster.8.4"].has("Rast_row_repeat_nomask", "cdecl"):
    Rast_row_repeat_nomask = _libs["grass_raster.8.4"].get("Rast_row_repeat_nomask", "cdecl")
    Rast_row_repeat_nomask.argtypes = [c_int, c_int]
    Rast_row_repeat_nomask.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 598
if _libs["grass_raster.8.4"].has("Rast_zero_buf", "cdecl"):
    Rast_zero_buf = _libs["grass_raster.8.4"].get("Rast_zero_buf", "cdecl")
    Rast_zero_buf.argtypes = [POINTER(None), RASTER_MAP_TYPE]
    Rast_zero_buf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 599
if _libs["grass_raster.8.4"].has("Rast_zero_input_buf", "cdecl"):
    Rast_zero_input_buf = _libs["grass_raster.8.4"].get("Rast_zero_input_buf", "cdecl")
    Rast_zero_input_buf.argtypes = [POINTER(None), RASTER_MAP_TYPE]
    Rast_zero_input_buf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 600
if _libs["grass_raster.8.4"].has("Rast_zero_output_buf", "cdecl"):
    Rast_zero_output_buf = _libs["grass_raster.8.4"].get("Rast_zero_output_buf", "cdecl")
    Rast_zero_output_buf.argtypes = [POINTER(None), RASTER_MAP_TYPE]
    Rast_zero_output_buf.restype = None

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 7
try:
    RECLASS_TABLE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 8
try:
    RECLASS_RULES = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 9
try:
    RECLASS_SCALE = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 11
try:
    CELL_TYPE = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 12
try:
    FCELL_TYPE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 13
try:
    DCELL_TYPE = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 19
try:
    INTERP_UNKNOWN = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 20
try:
    INTERP_NEAREST = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 21
try:
    INTERP_BILINEAR = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 22
try:
    INTERP_BICUBIC = 3
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 401
def Rast_is_c_null_value(cellVal):
    return ((cast(cellVal, POINTER(CELL))[0]) == (CELL (ord_if_char(0x80000000))).value)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 403
def Rast_is_f_null_value(fcellVal):
    return ((cast(fcellVal, POINTER(FCELL))[0]) != (cast(fcellVal, POINTER(FCELL))[0]))

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/raster.h: 405
def Rast_is_d_null_value(dcellVal):
    return ((cast(dcellVal, POINTER(DCELL))[0]) != (cast(dcellVal, POINTER(DCELL))[0]))

Reclass = struct_Reclass# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 31

FPReclass_table = struct_FPReclass_table# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 41

FPReclass = struct_FPReclass# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 50

Quant_table = struct_Quant_table# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 73

Quant = struct_Quant# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 80

Categories = struct_Categories# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 121

History = struct_History# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 172

Cell_stats_node = struct_Cell_stats_node# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 182

Cell_stats = struct_Cell_stats# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 181

Histogram_list = struct_Histogram_list# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 199

Histogram = struct_Histogram# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 196

R_stats = struct_R_stats# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 205

Range = struct_Range# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 211

FPRange = struct_FPRange# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 218

FP_stats = struct_FP_stats# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 225

GDAL_link = struct_GDAL_link# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 235

R_vrt = struct_R_vrt# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\raster.h: 236

# No inserted files

# No prefix-stripping

