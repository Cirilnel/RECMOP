r"""Wrapper for display.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_display.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/display.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h -o OBJ.x86_64-w64-mingw32/display.py

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
_libs["grass_display.8.4"] = load_library("grass_display.8.4")

# 1 libraries
# End libraries

# No modules

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

CELL = c_int# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 627

DCELL = c_double# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 628

FCELL = c_float# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 629

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

RASTER_MAP_TYPE = c_int# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/raster.h: 25

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 26
class struct_anon_8(Structure):
    pass

struct_anon_8.__slots__ = [
    'color',
    'r',
    'g',
    'b',
    'fr',
    'fg',
    'fb',
]
struct_anon_8._fields_ = [
    ('color', c_int),
    ('r', c_int),
    ('g', c_int),
    ('b', c_int),
    ('fr', c_double),
    ('fg', c_double),
    ('fb', c_double),
]

SYMBCOLOR = struct_anon_8# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 26

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 32
class struct_anon_9(Structure):
    pass

struct_anon_9.__slots__ = [
    'count',
    'alloc',
    'x',
    'y',
]
struct_anon_9._fields_ = [
    ('count', c_int),
    ('alloc', c_int),
    ('x', POINTER(c_double)),
    ('y', POINTER(c_double)),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 36
class struct_anon_10(Structure):
    pass

struct_anon_10.__slots__ = [
    'clock',
    'x',
    'y',
    'r',
    'a1',
    'a2',
]
struct_anon_10._fields_ = [
    ('clock', c_int),
    ('x', c_double),
    ('y', c_double),
    ('r', c_double),
    ('a1', c_double),
    ('a2', c_double),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 31
class union_anon_11(Union):
    pass

union_anon_11.__slots__ = [
    'line',
    'arc',
]
union_anon_11._fields_ = [
    ('line', struct_anon_9),
    ('arc', struct_anon_10),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 41
class struct_anon_12(Structure):
    pass

struct_anon_12.__slots__ = [
    'type',
    'coor',
]
struct_anon_12._fields_ = [
    ('type', c_int),
    ('coor', union_anon_11),
]

SYMBEL = struct_anon_12# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 41

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 49
class struct_anon_13(Structure):
    pass

struct_anon_13.__slots__ = [
    'count',
    'alloc',
    'elem',
    'scount',
    'salloc',
    'sx',
    'sy',
]
struct_anon_13._fields_ = [
    ('count', c_int),
    ('alloc', c_int),
    ('elem', POINTER(POINTER(SYMBEL))),
    ('scount', c_int),
    ('salloc', c_int),
    ('sx', POINTER(c_double)),
    ('sy', POINTER(c_double)),
]

SYMBCHAIN = struct_anon_13# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 49

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 58
class struct_anon_14(Structure):
    pass

struct_anon_14.__slots__ = [
    'type',
    'color',
    'fcolor',
    'count',
    'alloc',
    'chain',
]
struct_anon_14._fields_ = [
    ('type', c_int),
    ('color', SYMBCOLOR),
    ('fcolor', SYMBCOLOR),
    ('count', c_int),
    ('alloc', c_int),
    ('chain', POINTER(POINTER(SYMBCHAIN))),
]

SYMBPART = struct_anon_14# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 58

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 67
class struct_anon_15(Structure):
    pass

struct_anon_15.__slots__ = [
    'scale',
    'yscale',
    'xscale',
    'count',
    'alloc',
    'part',
]
struct_anon_15._fields_ = [
    ('scale', c_double),
    ('yscale', c_double),
    ('xscale', c_double),
    ('count', c_int),
    ('alloc', c_int),
    ('part', POINTER(POINTER(SYMBPART))),
]

SYMBOL = struct_anon_15# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/symbol.h: 67

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 8
class struct_color_rgba(Structure):
    pass

struct_color_rgba.__slots__ = [
    'r',
    'g',
    'b',
    'a',
]
struct_color_rgba._fields_ = [
    ('r', c_ubyte),
    ('g', c_ubyte),
    ('b', c_ubyte),
    ('a', c_ubyte),
]

RGBA_Color = struct_color_rgba# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 12

enum_clip_mode = c_int# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 19

M_NONE = 0# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 19

M_CULL = (M_NONE + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 19

M_CLIP = (M_CULL + 1)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 19

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 5
if _libs["grass_display.8.4"].has("D_update_conversions", "cdecl"):
    D_update_conversions = _libs["grass_display.8.4"].get("D_update_conversions", "cdecl")
    D_update_conversions.argtypes = []
    D_update_conversions.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 6
if _libs["grass_display.8.4"].has("D_fit_d_to_u", "cdecl"):
    D_fit_d_to_u = _libs["grass_display.8.4"].get("D_fit_d_to_u", "cdecl")
    D_fit_d_to_u.argtypes = []
    D_fit_d_to_u.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 7
if _libs["grass_display.8.4"].has("D_fit_u_to_d", "cdecl"):
    D_fit_u_to_d = _libs["grass_display.8.4"].get("D_fit_u_to_d", "cdecl")
    D_fit_u_to_d.argtypes = []
    D_fit_u_to_d.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 8
if _libs["grass_display.8.4"].has("D_show_conversions", "cdecl"):
    D_show_conversions = _libs["grass_display.8.4"].get("D_show_conversions", "cdecl")
    D_show_conversions.argtypes = []
    D_show_conversions.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 10
if _libs["grass_display.8.4"].has("D_do_conversions", "cdecl"):
    D_do_conversions = _libs["grass_display.8.4"].get("D_do_conversions", "cdecl")
    D_do_conversions.argtypes = [POINTER(struct_Cell_head), c_double, c_double, c_double, c_double]
    D_do_conversions.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 12
if _libs["grass_display.8.4"].has("D_is_lat_lon", "cdecl"):
    D_is_lat_lon = _libs["grass_display.8.4"].get("D_is_lat_lon", "cdecl")
    D_is_lat_lon.argtypes = []
    D_is_lat_lon.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 14
if _libs["grass_display.8.4"].has("D_get_d_to_a_xconv", "cdecl"):
    D_get_d_to_a_xconv = _libs["grass_display.8.4"].get("D_get_d_to_a_xconv", "cdecl")
    D_get_d_to_a_xconv.argtypes = []
    D_get_d_to_a_xconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 15
if _libs["grass_display.8.4"].has("D_get_d_to_a_yconv", "cdecl"):
    D_get_d_to_a_yconv = _libs["grass_display.8.4"].get("D_get_d_to_a_yconv", "cdecl")
    D_get_d_to_a_yconv.argtypes = []
    D_get_d_to_a_yconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 16
if _libs["grass_display.8.4"].has("D_get_d_to_u_xconv", "cdecl"):
    D_get_d_to_u_xconv = _libs["grass_display.8.4"].get("D_get_d_to_u_xconv", "cdecl")
    D_get_d_to_u_xconv.argtypes = []
    D_get_d_to_u_xconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 17
if _libs["grass_display.8.4"].has("D_get_d_to_u_yconv", "cdecl"):
    D_get_d_to_u_yconv = _libs["grass_display.8.4"].get("D_get_d_to_u_yconv", "cdecl")
    D_get_d_to_u_yconv.argtypes = []
    D_get_d_to_u_yconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 18
if _libs["grass_display.8.4"].has("D_get_a_to_u_xconv", "cdecl"):
    D_get_a_to_u_xconv = _libs["grass_display.8.4"].get("D_get_a_to_u_xconv", "cdecl")
    D_get_a_to_u_xconv.argtypes = []
    D_get_a_to_u_xconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 19
if _libs["grass_display.8.4"].has("D_get_a_to_u_yconv", "cdecl"):
    D_get_a_to_u_yconv = _libs["grass_display.8.4"].get("D_get_a_to_u_yconv", "cdecl")
    D_get_a_to_u_yconv.argtypes = []
    D_get_a_to_u_yconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 20
if _libs["grass_display.8.4"].has("D_get_a_to_d_xconv", "cdecl"):
    D_get_a_to_d_xconv = _libs["grass_display.8.4"].get("D_get_a_to_d_xconv", "cdecl")
    D_get_a_to_d_xconv.argtypes = []
    D_get_a_to_d_xconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 21
if _libs["grass_display.8.4"].has("D_get_a_to_d_yconv", "cdecl"):
    D_get_a_to_d_yconv = _libs["grass_display.8.4"].get("D_get_a_to_d_yconv", "cdecl")
    D_get_a_to_d_yconv.argtypes = []
    D_get_a_to_d_yconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 22
if _libs["grass_display.8.4"].has("D_get_u_to_d_xconv", "cdecl"):
    D_get_u_to_d_xconv = _libs["grass_display.8.4"].get("D_get_u_to_d_xconv", "cdecl")
    D_get_u_to_d_xconv.argtypes = []
    D_get_u_to_d_xconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 23
if _libs["grass_display.8.4"].has("D_get_u_to_d_yconv", "cdecl"):
    D_get_u_to_d_yconv = _libs["grass_display.8.4"].get("D_get_u_to_d_yconv", "cdecl")
    D_get_u_to_d_yconv.argtypes = []
    D_get_u_to_d_yconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 24
if _libs["grass_display.8.4"].has("D_get_u_to_a_xconv", "cdecl"):
    D_get_u_to_a_xconv = _libs["grass_display.8.4"].get("D_get_u_to_a_xconv", "cdecl")
    D_get_u_to_a_xconv.argtypes = []
    D_get_u_to_a_xconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 25
if _libs["grass_display.8.4"].has("D_get_u_to_a_yconv", "cdecl"):
    D_get_u_to_a_yconv = _libs["grass_display.8.4"].get("D_get_u_to_a_yconv", "cdecl")
    D_get_u_to_a_yconv.argtypes = []
    D_get_u_to_a_yconv.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 27
if _libs["grass_display.8.4"].has("D_get_ns_resolution", "cdecl"):
    D_get_ns_resolution = _libs["grass_display.8.4"].get("D_get_ns_resolution", "cdecl")
    D_get_ns_resolution.argtypes = []
    D_get_ns_resolution.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 28
if _libs["grass_display.8.4"].has("D_get_ew_resolution", "cdecl"):
    D_get_ew_resolution = _libs["grass_display.8.4"].get("D_get_ew_resolution", "cdecl")
    D_get_ew_resolution.argtypes = []
    D_get_ew_resolution.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 30
if _libs["grass_display.8.4"].has("D_get_u_west", "cdecl"):
    D_get_u_west = _libs["grass_display.8.4"].get("D_get_u_west", "cdecl")
    D_get_u_west.argtypes = []
    D_get_u_west.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 31
if _libs["grass_display.8.4"].has("D_get_u_east", "cdecl"):
    D_get_u_east = _libs["grass_display.8.4"].get("D_get_u_east", "cdecl")
    D_get_u_east.argtypes = []
    D_get_u_east.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 32
if _libs["grass_display.8.4"].has("D_get_u_north", "cdecl"):
    D_get_u_north = _libs["grass_display.8.4"].get("D_get_u_north", "cdecl")
    D_get_u_north.argtypes = []
    D_get_u_north.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 33
if _libs["grass_display.8.4"].has("D_get_u_south", "cdecl"):
    D_get_u_south = _libs["grass_display.8.4"].get("D_get_u_south", "cdecl")
    D_get_u_south.argtypes = []
    D_get_u_south.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 34
if _libs["grass_display.8.4"].has("D_get_a_west", "cdecl"):
    D_get_a_west = _libs["grass_display.8.4"].get("D_get_a_west", "cdecl")
    D_get_a_west.argtypes = []
    D_get_a_west.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 35
if _libs["grass_display.8.4"].has("D_get_a_east", "cdecl"):
    D_get_a_east = _libs["grass_display.8.4"].get("D_get_a_east", "cdecl")
    D_get_a_east.argtypes = []
    D_get_a_east.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 36
if _libs["grass_display.8.4"].has("D_get_a_north", "cdecl"):
    D_get_a_north = _libs["grass_display.8.4"].get("D_get_a_north", "cdecl")
    D_get_a_north.argtypes = []
    D_get_a_north.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 37
if _libs["grass_display.8.4"].has("D_get_a_south", "cdecl"):
    D_get_a_south = _libs["grass_display.8.4"].get("D_get_a_south", "cdecl")
    D_get_a_south.argtypes = []
    D_get_a_south.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 38
if _libs["grass_display.8.4"].has("D_get_d_west", "cdecl"):
    D_get_d_west = _libs["grass_display.8.4"].get("D_get_d_west", "cdecl")
    D_get_d_west.argtypes = []
    D_get_d_west.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 39
if _libs["grass_display.8.4"].has("D_get_d_east", "cdecl"):
    D_get_d_east = _libs["grass_display.8.4"].get("D_get_d_east", "cdecl")
    D_get_d_east.argtypes = []
    D_get_d_east.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 40
if _libs["grass_display.8.4"].has("D_get_d_north", "cdecl"):
    D_get_d_north = _libs["grass_display.8.4"].get("D_get_d_north", "cdecl")
    D_get_d_north.argtypes = []
    D_get_d_north.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 41
if _libs["grass_display.8.4"].has("D_get_d_south", "cdecl"):
    D_get_d_south = _libs["grass_display.8.4"].get("D_get_d_south", "cdecl")
    D_get_d_south.argtypes = []
    D_get_d_south.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 43
if _libs["grass_display.8.4"].has("D_set_region", "cdecl"):
    D_set_region = _libs["grass_display.8.4"].get("D_set_region", "cdecl")
    D_set_region.argtypes = [POINTER(struct_Cell_head)]
    D_set_region.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 44
if _libs["grass_display.8.4"].has("D_set_src", "cdecl"):
    D_set_src = _libs["grass_display.8.4"].get("D_set_src", "cdecl")
    D_set_src.argtypes = [c_double, c_double, c_double, c_double]
    D_set_src.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 45
if _libs["grass_display.8.4"].has("D_get_src", "cdecl"):
    D_get_src = _libs["grass_display.8.4"].get("D_get_src", "cdecl")
    D_get_src.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    D_get_src.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 46
if _libs["grass_display.8.4"].has("D_set_grid", "cdecl"):
    D_set_grid = _libs["grass_display.8.4"].get("D_set_grid", "cdecl")
    D_set_grid.argtypes = [c_int, c_int, c_int, c_int]
    D_set_grid.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 47
if _libs["grass_display.8.4"].has("D_get_grid", "cdecl"):
    D_get_grid = _libs["grass_display.8.4"].get("D_get_grid", "cdecl")
    D_get_grid.argtypes = [POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    D_get_grid.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 48
if _libs["grass_display.8.4"].has("D_set_dst", "cdecl"):
    D_set_dst = _libs["grass_display.8.4"].get("D_set_dst", "cdecl")
    D_set_dst.argtypes = [c_double, c_double, c_double, c_double]
    D_set_dst.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 49
if _libs["grass_display.8.4"].has("D_get_dst", "cdecl"):
    D_get_dst = _libs["grass_display.8.4"].get("D_get_dst", "cdecl")
    D_get_dst.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    D_get_dst.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 51
if _libs["grass_display.8.4"].has("D_get_u", "cdecl"):
    D_get_u = _libs["grass_display.8.4"].get("D_get_u", "cdecl")
    D_get_u.argtypes = [(c_double * int(2)) * int(2)]
    D_get_u.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 52
if _libs["grass_display.8.4"].has("D_get_a", "cdecl"):
    D_get_a = _libs["grass_display.8.4"].get("D_get_a", "cdecl")
    D_get_a.argtypes = [(c_int * int(2)) * int(2)]
    D_get_a.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 53
if _libs["grass_display.8.4"].has("D_get_d", "cdecl"):
    D_get_d = _libs["grass_display.8.4"].get("D_get_d", "cdecl")
    D_get_d.argtypes = [(c_double * int(2)) * int(2)]
    D_get_d.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 55
if _libs["grass_display.8.4"].has("D_d_to_a_row", "cdecl"):
    D_d_to_a_row = _libs["grass_display.8.4"].get("D_d_to_a_row", "cdecl")
    D_d_to_a_row.argtypes = [c_double]
    D_d_to_a_row.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 56
if _libs["grass_display.8.4"].has("D_d_to_a_col", "cdecl"):
    D_d_to_a_col = _libs["grass_display.8.4"].get("D_d_to_a_col", "cdecl")
    D_d_to_a_col.argtypes = [c_double]
    D_d_to_a_col.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 57
if _libs["grass_display.8.4"].has("D_d_to_u_row", "cdecl"):
    D_d_to_u_row = _libs["grass_display.8.4"].get("D_d_to_u_row", "cdecl")
    D_d_to_u_row.argtypes = [c_double]
    D_d_to_u_row.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 58
if _libs["grass_display.8.4"].has("D_d_to_u_col", "cdecl"):
    D_d_to_u_col = _libs["grass_display.8.4"].get("D_d_to_u_col", "cdecl")
    D_d_to_u_col.argtypes = [c_double]
    D_d_to_u_col.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 59
if _libs["grass_display.8.4"].has("D_a_to_u_row", "cdecl"):
    D_a_to_u_row = _libs["grass_display.8.4"].get("D_a_to_u_row", "cdecl")
    D_a_to_u_row.argtypes = [c_double]
    D_a_to_u_row.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 60
if _libs["grass_display.8.4"].has("D_a_to_u_col", "cdecl"):
    D_a_to_u_col = _libs["grass_display.8.4"].get("D_a_to_u_col", "cdecl")
    D_a_to_u_col.argtypes = [c_double]
    D_a_to_u_col.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 61
if _libs["grass_display.8.4"].has("D_a_to_d_row", "cdecl"):
    D_a_to_d_row = _libs["grass_display.8.4"].get("D_a_to_d_row", "cdecl")
    D_a_to_d_row.argtypes = [c_double]
    D_a_to_d_row.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 62
if _libs["grass_display.8.4"].has("D_a_to_d_col", "cdecl"):
    D_a_to_d_col = _libs["grass_display.8.4"].get("D_a_to_d_col", "cdecl")
    D_a_to_d_col.argtypes = [c_double]
    D_a_to_d_col.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 63
if _libs["grass_display.8.4"].has("D_u_to_d_row", "cdecl"):
    D_u_to_d_row = _libs["grass_display.8.4"].get("D_u_to_d_row", "cdecl")
    D_u_to_d_row.argtypes = [c_double]
    D_u_to_d_row.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 64
if _libs["grass_display.8.4"].has("D_u_to_d_col", "cdecl"):
    D_u_to_d_col = _libs["grass_display.8.4"].get("D_u_to_d_col", "cdecl")
    D_u_to_d_col.argtypes = [c_double]
    D_u_to_d_col.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 65
if _libs["grass_display.8.4"].has("D_u_to_a_row", "cdecl"):
    D_u_to_a_row = _libs["grass_display.8.4"].get("D_u_to_a_row", "cdecl")
    D_u_to_a_row.argtypes = [c_double]
    D_u_to_a_row.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 66
if _libs["grass_display.8.4"].has("D_u_to_a_col", "cdecl"):
    D_u_to_a_col = _libs["grass_display.8.4"].get("D_u_to_a_col", "cdecl")
    D_u_to_a_col.argtypes = [c_double]
    D_u_to_a_col.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 70
if _libs["grass_display.8.4"].has("D_set_clip", "cdecl"):
    D_set_clip = _libs["grass_display.8.4"].get("D_set_clip", "cdecl")
    D_set_clip.argtypes = [c_double, c_double, c_double, c_double]
    D_set_clip.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 71
if _libs["grass_display.8.4"].has("D_clip_to_map", "cdecl"):
    D_clip_to_map = _libs["grass_display.8.4"].get("D_clip_to_map", "cdecl")
    D_clip_to_map.argtypes = []
    D_clip_to_map.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 72
if _libs["grass_display.8.4"].has("D_set_clip_mode", "cdecl"):
    D_set_clip_mode = _libs["grass_display.8.4"].get("D_set_clip_mode", "cdecl")
    D_set_clip_mode.argtypes = [c_int]
    D_set_clip_mode.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 73
if _libs["grass_display.8.4"].has("D_set_reduction", "cdecl"):
    D_set_reduction = _libs["grass_display.8.4"].get("D_set_reduction", "cdecl")
    D_set_reduction.argtypes = [c_double]
    D_set_reduction.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 75
if _libs["grass_display.8.4"].has("D_line_width", "cdecl"):
    D_line_width = _libs["grass_display.8.4"].get("D_line_width", "cdecl")
    D_line_width.argtypes = [c_double]
    D_line_width.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 76
if _libs["grass_display.8.4"].has("D_get_text_box", "cdecl"):
    D_get_text_box = _libs["grass_display.8.4"].get("D_get_text_box", "cdecl")
    D_get_text_box.argtypes = [String, POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    D_get_text_box.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 78
if _libs["grass_display.8.4"].has("D_pos_abs", "cdecl"):
    D_pos_abs = _libs["grass_display.8.4"].get("D_pos_abs", "cdecl")
    D_pos_abs.argtypes = [c_double, c_double]
    D_pos_abs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 79
if _libs["grass_display.8.4"].has("D_pos_rel", "cdecl"):
    D_pos_rel = _libs["grass_display.8.4"].get("D_pos_rel", "cdecl")
    D_pos_rel.argtypes = [c_double, c_double]
    D_pos_rel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 80
if _libs["grass_display.8.4"].has("D_move_abs", "cdecl"):
    D_move_abs = _libs["grass_display.8.4"].get("D_move_abs", "cdecl")
    D_move_abs.argtypes = [c_double, c_double]
    D_move_abs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 81
if _libs["grass_display.8.4"].has("D_move_rel", "cdecl"):
    D_move_rel = _libs["grass_display.8.4"].get("D_move_rel", "cdecl")
    D_move_rel.argtypes = [c_double, c_double]
    D_move_rel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 82
if _libs["grass_display.8.4"].has("D_cont_abs", "cdecl"):
    D_cont_abs = _libs["grass_display.8.4"].get("D_cont_abs", "cdecl")
    D_cont_abs.argtypes = [c_double, c_double]
    D_cont_abs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 83
if _libs["grass_display.8.4"].has("D_cont_rel", "cdecl"):
    D_cont_rel = _libs["grass_display.8.4"].get("D_cont_rel", "cdecl")
    D_cont_rel.argtypes = [c_double, c_double]
    D_cont_rel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 84
if _libs["grass_display.8.4"].has("D_line_abs", "cdecl"):
    D_line_abs = _libs["grass_display.8.4"].get("D_line_abs", "cdecl")
    D_line_abs.argtypes = [c_double, c_double, c_double, c_double]
    D_line_abs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 85
if _libs["grass_display.8.4"].has("D_line_rel", "cdecl"):
    D_line_rel = _libs["grass_display.8.4"].get("D_line_rel", "cdecl")
    D_line_rel.argtypes = [c_double, c_double, c_double, c_double]
    D_line_rel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 86
if _libs["grass_display.8.4"].has("D_polydots_abs", "cdecl"):
    D_polydots_abs = _libs["grass_display.8.4"].get("D_polydots_abs", "cdecl")
    D_polydots_abs.argtypes = [POINTER(c_double), POINTER(c_double), c_int]
    D_polydots_abs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 87
if _libs["grass_display.8.4"].has("D_polydots_rel", "cdecl"):
    D_polydots_rel = _libs["grass_display.8.4"].get("D_polydots_rel", "cdecl")
    D_polydots_rel.argtypes = [POINTER(c_double), POINTER(c_double), c_int]
    D_polydots_rel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 88
if _libs["grass_display.8.4"].has("D_polyline_abs", "cdecl"):
    D_polyline_abs = _libs["grass_display.8.4"].get("D_polyline_abs", "cdecl")
    D_polyline_abs.argtypes = [POINTER(c_double), POINTER(c_double), c_int]
    D_polyline_abs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 89
if _libs["grass_display.8.4"].has("D_polyline_rel", "cdecl"):
    D_polyline_rel = _libs["grass_display.8.4"].get("D_polyline_rel", "cdecl")
    D_polyline_rel.argtypes = [POINTER(c_double), POINTER(c_double), c_int]
    D_polyline_rel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 90
if _libs["grass_display.8.4"].has("D_polygon_abs", "cdecl"):
    D_polygon_abs = _libs["grass_display.8.4"].get("D_polygon_abs", "cdecl")
    D_polygon_abs.argtypes = [POINTER(c_double), POINTER(c_double), c_int]
    D_polygon_abs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 91
if _libs["grass_display.8.4"].has("D_polygon_rel", "cdecl"):
    D_polygon_rel = _libs["grass_display.8.4"].get("D_polygon_rel", "cdecl")
    D_polygon_rel.argtypes = [POINTER(c_double), POINTER(c_double), c_int]
    D_polygon_rel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 92
if _libs["grass_display.8.4"].has("D_box_abs", "cdecl"):
    D_box_abs = _libs["grass_display.8.4"].get("D_box_abs", "cdecl")
    D_box_abs.argtypes = [c_double, c_double, c_double, c_double]
    D_box_abs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 93
if _libs["grass_display.8.4"].has("D_box_rel", "cdecl"):
    D_box_rel = _libs["grass_display.8.4"].get("D_box_rel", "cdecl")
    D_box_rel.argtypes = [c_double, c_double]
    D_box_rel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 95
if _libs["grass_display.8.4"].has("D_begin", "cdecl"):
    D_begin = _libs["grass_display.8.4"].get("D_begin", "cdecl")
    D_begin.argtypes = []
    D_begin.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 96
if _libs["grass_display.8.4"].has("D_end", "cdecl"):
    D_end = _libs["grass_display.8.4"].get("D_end", "cdecl")
    D_end.argtypes = []
    D_end.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 97
if _libs["grass_display.8.4"].has("D_close", "cdecl"):
    D_close = _libs["grass_display.8.4"].get("D_close", "cdecl")
    D_close.argtypes = []
    D_close.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 98
if _libs["grass_display.8.4"].has("D_stroke", "cdecl"):
    D_stroke = _libs["grass_display.8.4"].get("D_stroke", "cdecl")
    D_stroke.argtypes = []
    D_stroke.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 99
if _libs["grass_display.8.4"].has("D_fill", "cdecl"):
    D_fill = _libs["grass_display.8.4"].get("D_fill", "cdecl")
    D_fill.argtypes = []
    D_fill.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 100
if _libs["grass_display.8.4"].has("D_dots", "cdecl"):
    D_dots = _libs["grass_display.8.4"].get("D_dots", "cdecl")
    D_dots.argtypes = []
    D_dots.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 103
if _libs["grass_display.8.4"].has("D_plot_icon", "cdecl"):
    D_plot_icon = _libs["grass_display.8.4"].get("D_plot_icon", "cdecl")
    D_plot_icon.argtypes = [c_double, c_double, c_int, c_double, c_double]
    D_plot_icon.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 106
if _libs["grass_display.8.4"].has("D_draw_raster", "cdecl"):
    D_draw_raster = _libs["grass_display.8.4"].get("D_draw_raster", "cdecl")
    D_draw_raster.argtypes = [c_int, POINTER(None), POINTER(struct_Colors), RASTER_MAP_TYPE]
    D_draw_raster.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 107
if _libs["grass_display.8.4"].has("D_draw_d_raster", "cdecl"):
    D_draw_d_raster = _libs["grass_display.8.4"].get("D_draw_d_raster", "cdecl")
    D_draw_d_raster.argtypes = [c_int, POINTER(DCELL), POINTER(struct_Colors)]
    D_draw_d_raster.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 108
if _libs["grass_display.8.4"].has("D_draw_f_raster", "cdecl"):
    D_draw_f_raster = _libs["grass_display.8.4"].get("D_draw_f_raster", "cdecl")
    D_draw_f_raster.argtypes = [c_int, POINTER(FCELL), POINTER(struct_Colors)]
    D_draw_f_raster.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 109
if _libs["grass_display.8.4"].has("D_draw_c_raster", "cdecl"):
    D_draw_c_raster = _libs["grass_display.8.4"].get("D_draw_c_raster", "cdecl")
    D_draw_c_raster.argtypes = [c_int, POINTER(CELL), POINTER(struct_Colors)]
    D_draw_c_raster.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 110
if _libs["grass_display.8.4"].has("D_raster_draw_begin", "cdecl"):
    D_raster_draw_begin = _libs["grass_display.8.4"].get("D_raster_draw_begin", "cdecl")
    D_raster_draw_begin.argtypes = []
    D_raster_draw_begin.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 111
if _libs["grass_display.8.4"].has("D_draw_raster_RGB", "cdecl"):
    D_draw_raster_RGB = _libs["grass_display.8.4"].get("D_draw_raster_RGB", "cdecl")
    D_draw_raster_RGB.argtypes = [c_int, POINTER(None), POINTER(None), POINTER(None), POINTER(struct_Colors), POINTER(struct_Colors), POINTER(struct_Colors), RASTER_MAP_TYPE, RASTER_MAP_TYPE, RASTER_MAP_TYPE]
    D_draw_raster_RGB.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 114
if _libs["grass_display.8.4"].has("D_raster_draw_end", "cdecl"):
    D_raster_draw_end = _libs["grass_display.8.4"].get("D_raster_draw_end", "cdecl")
    D_raster_draw_end.argtypes = []
    D_raster_draw_end.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 117
if _libs["grass_display.8.4"].has("D_set_overlay_mode", "cdecl"):
    D_set_overlay_mode = _libs["grass_display.8.4"].get("D_set_overlay_mode", "cdecl")
    D_set_overlay_mode.argtypes = [c_int]
    D_set_overlay_mode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 118
if _libs["grass_display.8.4"].has("D_color", "cdecl"):
    D_color = _libs["grass_display.8.4"].get("D_color", "cdecl")
    D_color.argtypes = [CELL, POINTER(struct_Colors)]
    D_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 119
if _libs["grass_display.8.4"].has("D_c_color", "cdecl"):
    D_c_color = _libs["grass_display.8.4"].get("D_c_color", "cdecl")
    D_c_color.argtypes = [CELL, POINTER(struct_Colors)]
    D_c_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 120
if _libs["grass_display.8.4"].has("D_d_color", "cdecl"):
    D_d_color = _libs["grass_display.8.4"].get("D_d_color", "cdecl")
    D_d_color.argtypes = [DCELL, POINTER(struct_Colors)]
    D_d_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 121
if _libs["grass_display.8.4"].has("D_f_color", "cdecl"):
    D_f_color = _libs["grass_display.8.4"].get("D_f_color", "cdecl")
    D_f_color.argtypes = [FCELL, POINTER(struct_Colors)]
    D_f_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 122
if _libs["grass_display.8.4"].has("D_color_of_type", "cdecl"):
    D_color_of_type = _libs["grass_display.8.4"].get("D_color_of_type", "cdecl")
    D_color_of_type.argtypes = [POINTER(None), POINTER(struct_Colors), RASTER_MAP_TYPE]
    D_color_of_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 125
if _libs["grass_display.8.4"].has("D_setup", "cdecl"):
    D_setup = _libs["grass_display.8.4"].get("D_setup", "cdecl")
    D_setup.argtypes = [c_int]
    D_setup.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 126
if _libs["grass_display.8.4"].has("D_setup_unity", "cdecl"):
    D_setup_unity = _libs["grass_display.8.4"].get("D_setup_unity", "cdecl")
    D_setup_unity.argtypes = [c_int]
    D_setup_unity.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 127
if _libs["grass_display.8.4"].has("D_setup2", "cdecl"):
    D_setup2 = _libs["grass_display.8.4"].get("D_setup2", "cdecl")
    D_setup2.argtypes = [c_int, c_int, c_double, c_double, c_double, c_double]
    D_setup2.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 130
if _libs["grass_display.8.4"].has("D_symbol", "cdecl"):
    D_symbol = _libs["grass_display.8.4"].get("D_symbol", "cdecl")
    D_symbol.argtypes = [POINTER(SYMBOL), c_double, c_double, POINTER(RGBA_Color), POINTER(RGBA_Color)]
    D_symbol.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 132
if _libs["grass_display.8.4"].has("D_symbol2", "cdecl"):
    D_symbol2 = _libs["grass_display.8.4"].get("D_symbol2", "cdecl")
    D_symbol2.argtypes = [POINTER(SYMBOL), c_double, c_double, POINTER(RGBA_Color), POINTER(RGBA_Color)]
    D_symbol2.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 136
if _libs["grass_display.8.4"].has("D_translate_color", "cdecl"):
    D_translate_color = _libs["grass_display.8.4"].get("D_translate_color", "cdecl")
    D_translate_color.argtypes = [String]
    D_translate_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 137
if _libs["grass_display.8.4"].has("D_parse_color", "cdecl"):
    D_parse_color = _libs["grass_display.8.4"].get("D_parse_color", "cdecl")
    D_parse_color.argtypes = [String, c_int]
    D_parse_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 138
if _libs["grass_display.8.4"].has("D_use_color", "cdecl"):
    D_use_color = _libs["grass_display.8.4"].get("D_use_color", "cdecl")
    D_use_color.argtypes = [c_int]
    D_use_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 139
if _libs["grass_display.8.4"].has("D_color_number_to_RGB", "cdecl"):
    D_color_number_to_RGB = _libs["grass_display.8.4"].get("D_color_number_to_RGB", "cdecl")
    D_color_number_to_RGB.argtypes = [c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    D_color_number_to_RGB.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 140
if _libs["grass_display.8.4"].has("D_RGB_color", "cdecl"):
    D_RGB_color = _libs["grass_display.8.4"].get("D_RGB_color", "cdecl")
    D_RGB_color.argtypes = [c_int, c_int, c_int]
    D_RGB_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 143
if _libs["grass_display.8.4"].has("D_erase", "cdecl"):
    D_erase = _libs["grass_display.8.4"].get("D_erase", "cdecl")
    D_erase.argtypes = [String]
    D_erase.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 147
if _libs["grass_display.8.4"].has("D_open_driver", "cdecl"):
    D_open_driver = _libs["grass_display.8.4"].get("D_open_driver", "cdecl")
    D_open_driver.argtypes = []
    D_open_driver.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 148
if _libs["grass_display.8.4"].has("D_close_driver", "cdecl"):
    D_close_driver = _libs["grass_display.8.4"].get("D_close_driver", "cdecl")
    D_close_driver.argtypes = []
    D_close_driver.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 149
if _libs["grass_display.8.4"].has("D_save_command", "cdecl"):
    D_save_command = _libs["grass_display.8.4"].get("D_save_command", "cdecl")
    D_save_command.argtypes = [String]
    D_save_command.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 151
if _libs["grass_display.8.4"].has("D__erase", "cdecl"):
    D__erase = _libs["grass_display.8.4"].get("D__erase", "cdecl")
    D__erase.argtypes = []
    D__erase.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 153
if _libs["grass_display.8.4"].has("D_text_size", "cdecl"):
    D_text_size = _libs["grass_display.8.4"].get("D_text_size", "cdecl")
    D_text_size.argtypes = [c_double, c_double]
    D_text_size.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 154
if _libs["grass_display.8.4"].has("D_text_rotation", "cdecl"):
    D_text_rotation = _libs["grass_display.8.4"].get("D_text_rotation", "cdecl")
    D_text_rotation.argtypes = [c_double]
    D_text_rotation.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 155
if _libs["grass_display.8.4"].has("D_text", "cdecl"):
    D_text = _libs["grass_display.8.4"].get("D_text", "cdecl")
    D_text.argtypes = [String]
    D_text.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 157
if _libs["grass_display.8.4"].has("D_font", "cdecl"):
    D_font = _libs["grass_display.8.4"].get("D_font", "cdecl")
    D_font.argtypes = [String]
    D_font.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 158
if _libs["grass_display.8.4"].has("D_encoding", "cdecl"):
    D_encoding = _libs["grass_display.8.4"].get("D_encoding", "cdecl")
    D_encoding.argtypes = [String]
    D_encoding.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 159
if _libs["grass_display.8.4"].has("D_font_list", "cdecl"):
    D_font_list = _libs["grass_display.8.4"].get("D_font_list", "cdecl")
    D_font_list.argtypes = [POINTER(POINTER(POINTER(c_char))), POINTER(c_int)]
    D_font_list.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 160
if _libs["grass_display.8.4"].has("D_font_info", "cdecl"):
    D_font_info = _libs["grass_display.8.4"].get("D_font_info", "cdecl")
    D_font_info.argtypes = [POINTER(POINTER(POINTER(c_char))), POINTER(c_int)]
    D_font_info.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 162
if _libs["grass_display.8.4"].has("D_get_clip_window", "cdecl"):
    D_get_clip_window = _libs["grass_display.8.4"].get("D_get_clip_window", "cdecl")
    D_get_clip_window.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    D_get_clip_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 163
if _libs["grass_display.8.4"].has("D_set_clip_window", "cdecl"):
    D_set_clip_window = _libs["grass_display.8.4"].get("D_set_clip_window", "cdecl")
    D_set_clip_window.argtypes = [c_double, c_double, c_double, c_double]
    D_set_clip_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 164
if _libs["grass_display.8.4"].has("D_get_frame", "cdecl"):
    D_get_frame = _libs["grass_display.8.4"].get("D_get_frame", "cdecl")
    D_get_frame.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    D_get_frame.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 165
if _libs["grass_display.8.4"].has("D_get_screen", "cdecl"):
    D_get_screen = _libs["grass_display.8.4"].get("D_get_screen", "cdecl")
    D_get_screen.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    D_get_screen.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 166
if _libs["grass_display.8.4"].has("D_set_clip_window_to_map_window", "cdecl"):
    D_set_clip_window_to_map_window = _libs["grass_display.8.4"].get("D_set_clip_window_to_map_window", "cdecl")
    D_set_clip_window_to_map_window.argtypes = []
    D_set_clip_window_to_map_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 167
if _libs["grass_display.8.4"].has("D_set_clip_window_to_screen_window", "cdecl"):
    D_set_clip_window_to_screen_window = _libs["grass_display.8.4"].get("D_set_clip_window_to_screen_window", "cdecl")
    D_set_clip_window_to_screen_window.argtypes = []
    D_set_clip_window_to_screen_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/display.h: 169
if _libs["grass_display.8.4"].has("D_get_file", "cdecl"):
    D_get_file = _libs["grass_display.8.4"].get("D_get_file", "cdecl")
    D_get_file.argtypes = []
    D_get_file.restype = c_char_p

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 15
try:
    RGBA_COLOR_OPAQUE = 255
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 16
try:
    RGBA_COLOR_TRANSPARENT = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 17
try:
    RGBA_COLOR_NONE = 0
except:
    pass

color_rgba = struct_color_rgba# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\display.h: 8

# No inserted files

# No prefix-stripping

