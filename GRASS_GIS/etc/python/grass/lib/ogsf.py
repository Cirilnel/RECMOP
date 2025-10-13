r"""Wrapper for ogsf.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_ogsf.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/ogsf.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h -o OBJ.x86_64-w64-mingw32/ogsf.py

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
_libs["grass_ogsf.8.4"] = load_library("grass_ogsf.8.4")

# 1 libraries
# End libraries

# No modules

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/linkm.h: 12
class struct_link_head(Structure):
    pass

struct_link_head.__slots__ = [
    'ptr_array',
    'max_ptr',
    'alloced',
    'chunk_size',
    'unit_size',
    'Unused',
    'exit_flag',
]
struct_link_head._fields_ = [
    ('ptr_array', POINTER(POINTER(c_char))),
    ('max_ptr', c_int),
    ('alloced', c_int),
    ('chunk_size', c_int),
    ('unit_size', c_int),
    ('Unused', String),
    ('exit_flag', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/bitmap.h: 17
class struct_BM(Structure):
    pass

struct_BM.__slots__ = [
    'rows',
    'cols',
    'bytes',
    'data',
    'sparse',
    'token',
]
struct_BM._fields_ = [
    ('rows', c_int),
    ('cols', c_int),
    ('bytes', c_size_t),
    ('data', POINTER(c_ubyte)),
    ('sparse', c_int),
    ('token', POINTER(struct_link_head)),
]

GLint = c_int# C:/msys64/mingw64/include/GL/gl.h: 27

GLuint = c_uint# C:/msys64/mingw64/include/GL/gl.h: 31

GLdouble = c_double# C:/msys64/mingw64/include/GL/gl.h: 34

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

DCELL = c_double# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 628

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
class struct_anon_405(Structure):
    pass

struct_anon_405.__slots__ = [
    'red',
    'grn',
    'blu',
    'set',
    'nalloc',
    'active',
]
struct_anon_405._fields_ = [
    ('red', POINTER(c_ubyte)),
    ('grn', POINTER(c_ubyte)),
    ('blu', POINTER(c_ubyte)),
    ('set', POINTER(c_ubyte)),
    ('nalloc', c_int),
    ('active', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 674
class struct_anon_406(Structure):
    pass

struct_anon_406.__slots__ = [
    'vals',
    'rules',
    'nalloc',
    'active',
]
struct_anon_406._fields_ = [
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
    ('lookup', struct_anon_405),
    ('fp_lookup', struct_anon_406),
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

Point4 = c_float * int(4)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 204

Point3 = c_float * int(3)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 205

Point2 = c_float * int(2)# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 206

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 217
class struct_anon_407(Structure):
    pass

struct_anon_407.__slots__ = [
    'fb',
    'ib',
    'sb',
    'cb',
    'bm',
    'nm',
    'tfunc',
    'k',
]
struct_anon_407._fields_ = [
    ('fb', POINTER(c_float)),
    ('ib', POINTER(c_int)),
    ('sb', POINTER(c_short)),
    ('cb', POINTER(c_ubyte)),
    ('bm', POINTER(struct_BM)),
    ('nm', POINTER(struct_BM)),
    ('tfunc', CFUNCTYPE(UNCHECKED(c_float), c_float, c_int)),
    ('k', c_float),
]

typbuff = struct_anon_407# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 217

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 223
class struct_anon_408(Structure):
    pass

struct_anon_408.__slots__ = [
    'n_elem',
    'index',
    'value',
]
struct_anon_408._fields_ = [
    ('n_elem', c_int),
    ('index', String),
    ('value', POINTER(c_int)),
]

table256 = struct_anon_408# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 223

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 230
class struct_anon_409(Structure):
    pass

struct_anon_409.__slots__ = [
    'offset',
    'mult',
    'use_lookup',
    'lookup',
]
struct_anon_409._fields_ = [
    ('offset', c_float),
    ('mult', c_float),
    ('use_lookup', c_int),
    ('lookup', table256),
]

transform = struct_anon_409# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 230

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 242
class struct_anon_410(Structure):
    pass

struct_anon_410.__slots__ = [
    'data_id',
    'dims',
    'ndims',
    'numbytes',
    'unique_name',
    'databuff',
    'changed',
    'need_reload',
]
struct_anon_410._fields_ = [
    ('data_id', c_int),
    ('dims', c_int * int(4)),
    ('ndims', c_int),
    ('numbytes', c_size_t),
    ('unique_name', String),
    ('databuff', typbuff),
    ('changed', c_uint),
    ('need_reload', c_int),
]

dataset = struct_anon_410# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 242

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 254
class struct_anon_411(Structure):
    pass

struct_anon_411.__slots__ = [
    'att_src',
    'att_type',
    'hdata',
    'user_func',
    'constant',
    'lookup',
    'min_nz',
    'max_nz',
    'range_nz',
    'default_null',
]
struct_anon_411._fields_ = [
    ('att_src', c_uint),
    ('att_type', c_uint),
    ('hdata', c_int),
    ('user_func', CFUNCTYPE(UNCHECKED(c_int), )),
    ('constant', c_float),
    ('lookup', POINTER(c_int)),
    ('min_nz', c_float),
    ('max_nz', c_float),
    ('range_nz', c_float),
    ('default_null', c_float),
]

gsurf_att = struct_anon_411# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 254

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 256
class struct_g_surf(Structure):
    pass

struct_g_surf.__slots__ = [
    'gsurf_id',
    'cols',
    'rows',
    'att',
    'draw_mode',
    'wire_color',
    'ox',
    'oy',
    'xres',
    'yres',
    'z_exag',
    'x_trans',
    'y_trans',
    'z_trans',
    'xmin',
    'xmax',
    'ymin',
    'ymax',
    'zmin',
    'zmax',
    'zminmasked',
    'xrange',
    'yrange',
    'zrange',
    'zmin_nz',
    'zmax_nz',
    'zrange_nz',
    'x_mod',
    'y_mod',
    'x_modw',
    'y_modw',
    'nz_topo',
    'nz_color',
    'mask_needupdate',
    'norm_needupdate',
    'norms',
    'curmask',
    'next',
    'clientdata',
]
struct_g_surf._fields_ = [
    ('gsurf_id', c_int),
    ('cols', c_int),
    ('rows', c_int),
    ('att', gsurf_att * int(7)),
    ('draw_mode', c_uint),
    ('wire_color', c_long),
    ('ox', c_double),
    ('oy', c_double),
    ('xres', c_double),
    ('yres', c_double),
    ('z_exag', c_float),
    ('x_trans', c_float),
    ('y_trans', c_float),
    ('z_trans', c_float),
    ('xmin', c_float),
    ('xmax', c_float),
    ('ymin', c_float),
    ('ymax', c_float),
    ('zmin', c_float),
    ('zmax', c_float),
    ('zminmasked', c_float),
    ('xrange', c_float),
    ('yrange', c_float),
    ('zrange', c_float),
    ('zmin_nz', c_float),
    ('zmax_nz', c_float),
    ('zrange_nz', c_float),
    ('x_mod', c_int),
    ('y_mod', c_int),
    ('x_modw', c_int),
    ('y_modw', c_int),
    ('nz_topo', c_int),
    ('nz_color', c_int),
    ('mask_needupdate', c_int),
    ('norm_needupdate', c_int),
    ('norms', POINTER(c_ulong)),
    ('curmask', POINTER(struct_BM)),
    ('next', POINTER(struct_g_surf)),
    ('clientdata', POINTER(None)),
]

geosurf = struct_g_surf# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 277

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 285
class struct_g_vect_style(Structure):
    pass

struct_g_vect_style.__slots__ = [
    'color',
    'symbol',
    'size',
    'width',
    'next',
]
struct_g_vect_style._fields_ = [
    ('color', c_int),
    ('symbol', c_int),
    ('size', c_float),
    ('width', c_int),
    ('next', POINTER(struct_g_vect_style)),
]

gvstyle = struct_g_vect_style# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 298

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 309
class struct_g_vect_style_thematic(Structure):
    pass

struct_g_vect_style_thematic.__slots__ = [
    'active',
    'layer',
    'color_column',
    'symbol_column',
    'size_column',
    'width_column',
]
struct_g_vect_style_thematic._fields_ = [
    ('active', c_int),
    ('layer', c_int),
    ('color_column', String),
    ('symbol_column', String),
    ('size_column', String),
    ('width_column', String),
]

gvstyle_thematic = struct_g_vect_style_thematic# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 309

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 319
class struct_line_cats(Structure):
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 312
class struct_g_line(Structure):
    pass

struct_g_line.__slots__ = [
    'type',
    'norm',
    'dims',
    'npts',
    'p3',
    'p2',
    'cats',
    'style',
    'highlighted',
    'next',
]
struct_g_line._fields_ = [
    ('type', c_int),
    ('norm', c_float * int(3)),
    ('dims', c_int),
    ('npts', c_int),
    ('p3', POINTER(Point3)),
    ('p2', POINTER(Point2)),
    ('cats', POINTER(struct_line_cats)),
    ('style', POINTER(gvstyle)),
    ('highlighted', c_char),
    ('next', POINTER(struct_g_line)),
]

geoline = struct_g_line# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 325

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 328
class struct_g_vect(Structure):
    pass

struct_g_vect.__slots__ = [
    'gvect_id',
    'use_mem',
    'n_lines',
    'drape_surf_id',
    'use_z',
    'n_surfs',
    'filename',
    'x_trans',
    'y_trans',
    'z_trans',
    'lines',
    'fastlines',
    'bgn_read',
    'end_read',
    'nxt_line',
    'next',
    'clientdata',
    'tstyle',
    'style',
    'hstyle',
]
struct_g_vect._fields_ = [
    ('gvect_id', c_int),
    ('use_mem', c_int),
    ('n_lines', c_int),
    ('drape_surf_id', c_int * int(12)),
    ('use_z', c_int),
    ('n_surfs', c_int),
    ('filename', String),
    ('x_trans', c_float),
    ('y_trans', c_float),
    ('z_trans', c_float),
    ('lines', POINTER(geoline)),
    ('fastlines', POINTER(geoline)),
    ('bgn_read', CFUNCTYPE(UNCHECKED(c_int), )),
    ('end_read', CFUNCTYPE(UNCHECKED(c_int), )),
    ('nxt_line', CFUNCTYPE(UNCHECKED(c_int), )),
    ('next', POINTER(struct_g_vect)),
    ('clientdata', POINTER(None)),
    ('tstyle', POINTER(gvstyle_thematic)),
    ('style', POINTER(gvstyle)),
    ('hstyle', POINTER(gvstyle)),
]

geovect = struct_g_vect# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 346

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 349
class struct_g_point(Structure):
    pass

struct_g_point.__slots__ = [
    'dims',
    'p3',
    'cats',
    'style',
    'highlighted',
    'next',
]
struct_g_point._fields_ = [
    ('dims', c_int),
    ('p3', Point3),
    ('cats', POINTER(struct_line_cats)),
    ('style', POINTER(gvstyle)),
    ('highlighted', c_char),
    ('next', POINTER(struct_g_point)),
]

geopoint = struct_g_point# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 359

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 362
class struct_g_site(Structure):
    pass

struct_g_site.__slots__ = [
    'gsite_id',
    'drape_surf_id',
    'n_surfs',
    'n_sites',
    'use_z',
    'use_mem',
    'has_z',
    'filename',
    'attr_trans',
    'x_trans',
    'y_trans',
    'z_trans',
    'points',
    'bgn_read',
    'end_read',
    'nxt_site',
    'next',
    'clientdata',
    'tstyle',
    'style',
    'hstyle',
]
struct_g_site._fields_ = [
    ('gsite_id', c_int),
    ('drape_surf_id', c_int * int(12)),
    ('n_surfs', c_int),
    ('n_sites', c_int),
    ('use_z', c_int),
    ('use_mem', c_int),
    ('has_z', c_int),
    ('filename', String),
    ('attr_trans', transform),
    ('x_trans', c_float),
    ('y_trans', c_float),
    ('z_trans', c_float),
    ('points', POINTER(geopoint)),
    ('bgn_read', CFUNCTYPE(UNCHECKED(c_int), )),
    ('end_read', CFUNCTYPE(UNCHECKED(c_int), )),
    ('nxt_site', CFUNCTYPE(UNCHECKED(c_int), )),
    ('next', POINTER(struct_g_site)),
    ('clientdata', POINTER(None)),
    ('tstyle', POINTER(gvstyle_thematic)),
    ('style', POINTER(gvstyle)),
    ('hstyle', POINTER(gvstyle)),
]

geosite = struct_g_site# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 380

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 396
class struct_anon_412(Structure):
    pass

struct_anon_412.__slots__ = [
    'data_id',
    'file_type',
    'count',
    'file_name',
    'data_type',
    'map',
    'min',
    'max',
    'status',
    'mode',
    'buff',
]
struct_anon_412._fields_ = [
    ('data_id', c_int),
    ('file_type', c_uint),
    ('count', c_uint),
    ('file_name', String),
    ('data_type', c_uint),
    ('map', POINTER(None)),
    ('min', c_double),
    ('max', c_double),
    ('status', c_uint),
    ('mode', c_uint),
    ('buff', POINTER(None)),
]

geovol_file = struct_anon_412# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 396

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 407
class struct_anon_413(Structure):
    pass

struct_anon_413.__slots__ = [
    'att_src',
    'hfile',
    'user_func',
    'constant',
    'att_data',
    'changed',
]
struct_anon_413._fields_ = [
    ('att_src', c_uint),
    ('hfile', c_int),
    ('user_func', CFUNCTYPE(UNCHECKED(c_int), )),
    ('constant', c_float),
    ('att_data', POINTER(None)),
    ('changed', c_int),
]

geovol_isosurf_att = struct_anon_413# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 407

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 415
class struct_anon_414(Structure):
    pass

struct_anon_414.__slots__ = [
    'inout_mode',
    'att',
    'data_desc',
    'data',
]
struct_anon_414._fields_ = [
    ('inout_mode', c_int),
    ('att', geovol_isosurf_att * int(7)),
    ('data_desc', c_int),
    ('data', POINTER(c_ubyte)),
]

geovol_isosurf = struct_anon_414# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 415

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 424
class struct_anon_415(Structure):
    pass

struct_anon_415.__slots__ = [
    'dir',
    'x1',
    'x2',
    'y1',
    'y2',
    'z1',
    'z2',
    'data',
    'changed',
    'mode',
    'transp',
]
struct_anon_415._fields_ = [
    ('dir', c_int),
    ('x1', c_float),
    ('x2', c_float),
    ('y1', c_float),
    ('y2', c_float),
    ('z1', c_float),
    ('z2', c_float),
    ('data', POINTER(c_ubyte)),
    ('changed', c_int),
    ('mode', c_int),
    ('transp', c_int),
]

geovol_slice = struct_anon_415# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 424

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 426
class struct_g_vol(Structure):
    pass

struct_g_vol.__slots__ = [
    'gvol_id',
    'next',
    'hfile',
    'cols',
    'rows',
    'depths',
    'ox',
    'oy',
    'oz',
    'xres',
    'yres',
    'zres',
    'xmin',
    'xmax',
    'ymin',
    'ymax',
    'zmin',
    'zmax',
    'xrange',
    'yrange',
    'zrange',
    'x_trans',
    'y_trans',
    'z_trans',
    'draw_wire',
    'n_isosurfs',
    'isosurf',
    'isosurf_x_mod',
    'isosurf_y_mod',
    'isosurf_z_mod',
    'isosurf_draw_mode',
    'n_slices',
    'slice',
    'slice_x_mod',
    'slice_y_mod',
    'slice_z_mod',
    'slice_draw_mode',
    'clientdata',
]
struct_g_vol._fields_ = [
    ('gvol_id', c_int),
    ('next', POINTER(struct_g_vol)),
    ('hfile', c_int),
    ('cols', c_int),
    ('rows', c_int),
    ('depths', c_int),
    ('ox', c_double),
    ('oy', c_double),
    ('oz', c_double),
    ('xres', c_double),
    ('yres', c_double),
    ('zres', c_double),
    ('xmin', c_double),
    ('xmax', c_double),
    ('ymin', c_double),
    ('ymax', c_double),
    ('zmin', c_double),
    ('zmax', c_double),
    ('xrange', c_double),
    ('yrange', c_double),
    ('zrange', c_double),
    ('x_trans', c_float),
    ('y_trans', c_float),
    ('z_trans', c_float),
    ('draw_wire', c_int),
    ('n_isosurfs', c_int),
    ('isosurf', POINTER(geovol_isosurf) * int(12)),
    ('isosurf_x_mod', c_int),
    ('isosurf_y_mod', c_int),
    ('isosurf_z_mod', c_int),
    ('isosurf_draw_mode', c_uint),
    ('n_slices', c_int),
    ('slice', POINTER(geovol_slice) * int(12)),
    ('slice_x_mod', c_int),
    ('slice_y_mod', c_int),
    ('slice_z_mod', c_int),
    ('slice_draw_mode', c_uint),
    ('clientdata', POINTER(None)),
]

geovol = struct_g_vol# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 450

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 452
class struct_lightdefs(Structure):
    pass

struct_lightdefs.__slots__ = [
    'position',
    'color',
    'ambient',
    'emission',
    'shine',
]
struct_lightdefs._fields_ = [
    ('position', c_float * int(4)),
    ('color', c_float * int(3)),
    ('ambient', c_float * int(3)),
    ('emission', c_float * int(3)),
    ('shine', c_float),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 460
class struct_georot(Structure):
    pass

struct_georot.__slots__ = [
    'do_rot',
    'rot_angle',
    'rot_axes',
    'rotMatrix',
]
struct_georot._fields_ = [
    ('do_rot', c_int),
    ('rot_angle', c_double),
    ('rot_axes', c_double * int(3)),
    ('rotMatrix', GLdouble * int(16)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 477
class struct_anon_416(Structure):
    pass

struct_anon_416.__slots__ = [
    'coord_sys',
    'view_proj',
    'infocus',
    'from_to',
    'rotate',
    'twist',
    'fov',
    'incl',
    'look',
    'real_to',
    'vert_exag',
    'scale',
    'lights',
]
struct_anon_416._fields_ = [
    ('coord_sys', c_int),
    ('view_proj', c_int),
    ('infocus', c_int),
    ('from_to', (c_float * int(4)) * int(2)),
    ('rotate', struct_georot),
    ('twist', c_int),
    ('fov', c_int),
    ('incl', c_int),
    ('look', c_int),
    ('real_to', c_float * int(4)),
    ('vert_exag', c_float),
    ('scale', c_float),
    ('lights', struct_lightdefs * int(3)),
]

geoview = struct_anon_416# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 477

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 483
class struct_anon_417(Structure):
    pass

struct_anon_417.__slots__ = [
    'nearclip',
    'farclip',
    'aspect',
    'left',
    'right',
    'bottom',
    'top',
    'bgcol',
]
struct_anon_417._fields_ = [
    ('nearclip', c_float),
    ('farclip', c_float),
    ('aspect', c_float),
    ('left', c_short),
    ('right', c_short),
    ('bottom', c_short),
    ('top', c_short),
    ('bgcol', c_int),
]

geodisplay = struct_anon_417# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 483

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 485
try:
    Cxl_func = (POINTER(CFUNCTYPE(UNCHECKED(None), ))).in_dll(_libs["grass_ogsf.8.4"], "Cxl_func")
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 486
for _lib in _libs.values():
    try:
        Swap_func = (POINTER(CFUNCTYPE(UNCHECKED(None), ))).in_dll(_lib, "Swap_func")
        break
    except:
        pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 529
class struct_view_node(Structure):
    pass

struct_view_node.__slots__ = [
    'fields',
]
struct_view_node._fields_ = [
    ('fields', c_float * int(8)),
]

Viewnode = struct_view_node# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 529

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 531
class struct_key_node(Structure):
    pass

struct_key_node.__slots__ = [
    'pos',
    'fields',
    'look_ahead',
    'fieldmask',
    'next',
    'prior',
]
struct_key_node._fields_ = [
    ('pos', c_float),
    ('fields', c_float * int(8)),
    ('look_ahead', c_int),
    ('fieldmask', c_ulong),
    ('next', POINTER(struct_key_node)),
    ('prior', POINTER(struct_key_node)),
]

Keylist = struct_key_node# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 536

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 5
if _libs["grass_ogsf.8.4"].has("GK_set_interpmode", "cdecl"):
    GK_set_interpmode = _libs["grass_ogsf.8.4"].get("GK_set_interpmode", "cdecl")
    GK_set_interpmode.argtypes = [c_int]
    GK_set_interpmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 6
if _libs["grass_ogsf.8.4"].has("GK_set_tension", "cdecl"):
    GK_set_tension = _libs["grass_ogsf.8.4"].get("GK_set_tension", "cdecl")
    GK_set_tension.argtypes = [c_float]
    GK_set_tension.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 7
if _libs["grass_ogsf.8.4"].has("GK_showtension_start", "cdecl"):
    GK_showtension_start = _libs["grass_ogsf.8.4"].get("GK_showtension_start", "cdecl")
    GK_showtension_start.argtypes = []
    GK_showtension_start.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 8
if _libs["grass_ogsf.8.4"].has("GK_showtension_stop", "cdecl"):
    GK_showtension_stop = _libs["grass_ogsf.8.4"].get("GK_showtension_stop", "cdecl")
    GK_showtension_stop.argtypes = []
    GK_showtension_stop.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 9
if _libs["grass_ogsf.8.4"].has("GK_update_tension", "cdecl"):
    GK_update_tension = _libs["grass_ogsf.8.4"].get("GK_update_tension", "cdecl")
    GK_update_tension.argtypes = []
    GK_update_tension.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 10
if _libs["grass_ogsf.8.4"].has("GK_update_frames", "cdecl"):
    GK_update_frames = _libs["grass_ogsf.8.4"].get("GK_update_frames", "cdecl")
    GK_update_frames.argtypes = []
    GK_update_frames.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 11
if _libs["grass_ogsf.8.4"].has("GK_set_numsteps", "cdecl"):
    GK_set_numsteps = _libs["grass_ogsf.8.4"].get("GK_set_numsteps", "cdecl")
    GK_set_numsteps.argtypes = [c_int]
    GK_set_numsteps.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 12
if _libs["grass_ogsf.8.4"].has("GK_clear_keys", "cdecl"):
    GK_clear_keys = _libs["grass_ogsf.8.4"].get("GK_clear_keys", "cdecl")
    GK_clear_keys.argtypes = []
    GK_clear_keys.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 13
if _libs["grass_ogsf.8.4"].has("GK_print_keys", "cdecl"):
    GK_print_keys = _libs["grass_ogsf.8.4"].get("GK_print_keys", "cdecl")
    GK_print_keys.argtypes = [String]
    GK_print_keys.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 14
if _libs["grass_ogsf.8.4"].has("GK_move_key", "cdecl"):
    GK_move_key = _libs["grass_ogsf.8.4"].get("GK_move_key", "cdecl")
    GK_move_key.argtypes = [c_float, c_float, c_float]
    GK_move_key.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 15
if _libs["grass_ogsf.8.4"].has("GK_delete_key", "cdecl"):
    GK_delete_key = _libs["grass_ogsf.8.4"].get("GK_delete_key", "cdecl")
    GK_delete_key.argtypes = [c_float, c_float, c_int]
    GK_delete_key.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 16
if _libs["grass_ogsf.8.4"].has("GK_add_key", "cdecl"):
    GK_add_key = _libs["grass_ogsf.8.4"].get("GK_add_key", "cdecl")
    GK_add_key.argtypes = [c_float, c_ulong, c_int, c_float]
    GK_add_key.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 17
if _libs["grass_ogsf.8.4"].has("GK_do_framestep", "cdecl"):
    GK_do_framestep = _libs["grass_ogsf.8.4"].get("GK_do_framestep", "cdecl")
    GK_do_framestep.argtypes = [c_int, c_int]
    GK_do_framestep.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 18
if _libs["grass_ogsf.8.4"].has("GK_show_path", "cdecl"):
    GK_show_path = _libs["grass_ogsf.8.4"].get("GK_show_path", "cdecl")
    GK_show_path.argtypes = [c_int]
    GK_show_path.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 19
if _libs["grass_ogsf.8.4"].has("GK_show_vect", "cdecl"):
    GK_show_vect = _libs["grass_ogsf.8.4"].get("GK_show_vect", "cdecl")
    GK_show_vect.argtypes = [c_int]
    GK_show_vect.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 20
if _libs["grass_ogsf.8.4"].has("GK_show_site", "cdecl"):
    GK_show_site = _libs["grass_ogsf.8.4"].get("GK_show_site", "cdecl")
    GK_show_site.argtypes = [c_int]
    GK_show_site.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 21
if _libs["grass_ogsf.8.4"].has("GK_show_vol", "cdecl"):
    GK_show_vol = _libs["grass_ogsf.8.4"].get("GK_show_vol", "cdecl")
    GK_show_vol.argtypes = [c_int]
    GK_show_vol.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 22
if _libs["grass_ogsf.8.4"].has("GK_show_list", "cdecl"):
    GK_show_list = _libs["grass_ogsf.8.4"].get("GK_show_list", "cdecl")
    GK_show_list.argtypes = [c_int]
    GK_show_list.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 25
if _libs["grass_ogsf.8.4"].has("GP_site_exists", "cdecl"):
    GP_site_exists = _libs["grass_ogsf.8.4"].get("GP_site_exists", "cdecl")
    GP_site_exists.argtypes = [c_int]
    GP_site_exists.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 26
if _libs["grass_ogsf.8.4"].has("GP_new_site", "cdecl"):
    GP_new_site = _libs["grass_ogsf.8.4"].get("GP_new_site", "cdecl")
    GP_new_site.argtypes = []
    GP_new_site.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 27
if _libs["grass_ogsf.8.4"].has("GP_num_sites", "cdecl"):
    GP_num_sites = _libs["grass_ogsf.8.4"].get("GP_num_sites", "cdecl")
    GP_num_sites.argtypes = []
    GP_num_sites.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 28
if _libs["grass_ogsf.8.4"].has("GP_get_site_list", "cdecl"):
    GP_get_site_list = _libs["grass_ogsf.8.4"].get("GP_get_site_list", "cdecl")
    GP_get_site_list.argtypes = [POINTER(c_int)]
    GP_get_site_list.restype = POINTER(c_int)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 29
if _libs["grass_ogsf.8.4"].has("GP_delete_site", "cdecl"):
    GP_delete_site = _libs["grass_ogsf.8.4"].get("GP_delete_site", "cdecl")
    GP_delete_site.argtypes = [c_int]
    GP_delete_site.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 30
if _libs["grass_ogsf.8.4"].has("GP_load_site", "cdecl"):
    GP_load_site = _libs["grass_ogsf.8.4"].get("GP_load_site", "cdecl")
    GP_load_site.argtypes = [c_int, String]
    GP_load_site.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 31
if _libs["grass_ogsf.8.4"].has("GP_get_sitename", "cdecl"):
    GP_get_sitename = _libs["grass_ogsf.8.4"].get("GP_get_sitename", "cdecl")
    GP_get_sitename.argtypes = [c_int, POINTER(POINTER(c_char))]
    GP_get_sitename.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 32
if _libs["grass_ogsf.8.4"].has("GP_get_style", "cdecl"):
    GP_get_style = _libs["grass_ogsf.8.4"].get("GP_get_style", "cdecl")
    GP_get_style.argtypes = [c_int, POINTER(c_int), POINTER(c_int), POINTER(c_float), POINTER(c_int)]
    GP_get_style.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 33
if _libs["grass_ogsf.8.4"].has("GP_set_style", "cdecl"):
    GP_set_style = _libs["grass_ogsf.8.4"].get("GP_set_style", "cdecl")
    GP_set_style.argtypes = [c_int, c_int, c_int, c_float, c_int]
    GP_set_style.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 34
if _libs["grass_ogsf.8.4"].has("GP_set_style_thematic", "cdecl"):
    GP_set_style_thematic = _libs["grass_ogsf.8.4"].get("GP_set_style_thematic", "cdecl")
    GP_set_style_thematic.argtypes = [c_int, c_int, String, String, String, String, POINTER(struct_Colors)]
    GP_set_style_thematic.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 36
if _libs["grass_ogsf.8.4"].has("GP_unset_style_thematic", "cdecl"):
    GP_unset_style_thematic = _libs["grass_ogsf.8.4"].get("GP_unset_style_thematic", "cdecl")
    GP_unset_style_thematic.argtypes = [c_int]
    GP_unset_style_thematic.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 37
for _lib in _libs.values():
    if not _lib.has("GP_attmode_color", "cdecl"):
        continue
    GP_attmode_color = _lib.get("GP_attmode_color", "cdecl")
    GP_attmode_color.argtypes = [c_int, String]
    GP_attmode_color.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 38
for _lib in _libs.values():
    if not _lib.has("GP_attmode_none", "cdecl"):
        continue
    GP_attmode_none = _lib.get("GP_attmode_none", "cdecl")
    GP_attmode_none.argtypes = [c_int]
    GP_attmode_none.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 39
if _libs["grass_ogsf.8.4"].has("GP_set_zmode", "cdecl"):
    GP_set_zmode = _libs["grass_ogsf.8.4"].get("GP_set_zmode", "cdecl")
    GP_set_zmode.argtypes = [c_int, c_int]
    GP_set_zmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 40
if _libs["grass_ogsf.8.4"].has("GP_get_zmode", "cdecl"):
    GP_get_zmode = _libs["grass_ogsf.8.4"].get("GP_get_zmode", "cdecl")
    GP_get_zmode.argtypes = [c_int, POINTER(c_int)]
    GP_get_zmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 41
if _libs["grass_ogsf.8.4"].has("GP_set_trans", "cdecl"):
    GP_set_trans = _libs["grass_ogsf.8.4"].get("GP_set_trans", "cdecl")
    GP_set_trans.argtypes = [c_int, c_float, c_float, c_float]
    GP_set_trans.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 42
if _libs["grass_ogsf.8.4"].has("GP_get_trans", "cdecl"):
    GP_get_trans = _libs["grass_ogsf.8.4"].get("GP_get_trans", "cdecl")
    GP_get_trans.argtypes = [c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GP_get_trans.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 43
if _libs["grass_ogsf.8.4"].has("GP_select_surf", "cdecl"):
    GP_select_surf = _libs["grass_ogsf.8.4"].get("GP_select_surf", "cdecl")
    GP_select_surf.argtypes = [c_int, c_int]
    GP_select_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 44
if _libs["grass_ogsf.8.4"].has("GP_unselect_surf", "cdecl"):
    GP_unselect_surf = _libs["grass_ogsf.8.4"].get("GP_unselect_surf", "cdecl")
    GP_unselect_surf.argtypes = [c_int, c_int]
    GP_unselect_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 45
if _libs["grass_ogsf.8.4"].has("GP_surf_is_selected", "cdecl"):
    GP_surf_is_selected = _libs["grass_ogsf.8.4"].get("GP_surf_is_selected", "cdecl")
    GP_surf_is_selected.argtypes = [c_int, c_int]
    GP_surf_is_selected.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 46
if _libs["grass_ogsf.8.4"].has("GP_draw_site", "cdecl"):
    GP_draw_site = _libs["grass_ogsf.8.4"].get("GP_draw_site", "cdecl")
    GP_draw_site.argtypes = [c_int]
    GP_draw_site.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 47
if _libs["grass_ogsf.8.4"].has("GP_alldraw_site", "cdecl"):
    GP_alldraw_site = _libs["grass_ogsf.8.4"].get("GP_alldraw_site", "cdecl")
    GP_alldraw_site.argtypes = []
    GP_alldraw_site.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 48
if _libs["grass_ogsf.8.4"].has("GP_Set_ClientData", "cdecl"):
    GP_Set_ClientData = _libs["grass_ogsf.8.4"].get("GP_Set_ClientData", "cdecl")
    GP_Set_ClientData.argtypes = [c_int, POINTER(None)]
    GP_Set_ClientData.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 49
if _libs["grass_ogsf.8.4"].has("GP_Get_ClientData", "cdecl"):
    GP_Get_ClientData = _libs["grass_ogsf.8.4"].get("GP_Get_ClientData", "cdecl")
    GP_Get_ClientData.argtypes = [c_int]
    GP_Get_ClientData.restype = POINTER(c_ubyte)
    GP_Get_ClientData.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 50
if _libs["grass_ogsf.8.4"].has("GP_str_to_marker", "cdecl"):
    GP_str_to_marker = _libs["grass_ogsf.8.4"].get("GP_str_to_marker", "cdecl")
    GP_str_to_marker.argtypes = [String]
    GP_str_to_marker.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 53
if _libs["grass_ogsf.8.4"].has("void_func", "cdecl"):
    void_func = _libs["grass_ogsf.8.4"].get("void_func", "cdecl")
    void_func.argtypes = []
    void_func.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 54
if _libs["grass_ogsf.8.4"].has("GS_libinit", "cdecl"):
    GS_libinit = _libs["grass_ogsf.8.4"].get("GS_libinit", "cdecl")
    GS_libinit.argtypes = []
    GS_libinit.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 55
if _libs["grass_ogsf.8.4"].has("GS_get_longdim", "cdecl"):
    GS_get_longdim = _libs["grass_ogsf.8.4"].get("GS_get_longdim", "cdecl")
    GS_get_longdim.argtypes = [POINTER(c_float)]
    GS_get_longdim.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 56
if _libs["grass_ogsf.8.4"].has("GS_get_region", "cdecl"):
    GS_get_region = _libs["grass_ogsf.8.4"].get("GS_get_region", "cdecl")
    GS_get_region.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_get_region.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 57
if _libs["grass_ogsf.8.4"].has("GS_set_att_defaults", "cdecl"):
    GS_set_att_defaults = _libs["grass_ogsf.8.4"].get("GS_set_att_defaults", "cdecl")
    GS_set_att_defaults.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_set_att_defaults.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 58
if _libs["grass_ogsf.8.4"].has("GS_surf_exists", "cdecl"):
    GS_surf_exists = _libs["grass_ogsf.8.4"].get("GS_surf_exists", "cdecl")
    GS_surf_exists.argtypes = [c_int]
    GS_surf_exists.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 59
if _libs["grass_ogsf.8.4"].has("GS_new_surface", "cdecl"):
    GS_new_surface = _libs["grass_ogsf.8.4"].get("GS_new_surface", "cdecl")
    GS_new_surface.argtypes = []
    GS_new_surface.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 60
if _libs["grass_ogsf.8.4"].has("GS_new_light", "cdecl"):
    GS_new_light = _libs["grass_ogsf.8.4"].get("GS_new_light", "cdecl")
    GS_new_light.argtypes = []
    GS_new_light.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 61
if _libs["grass_ogsf.8.4"].has("GS_set_light_reset", "cdecl"):
    GS_set_light_reset = _libs["grass_ogsf.8.4"].get("GS_set_light_reset", "cdecl")
    GS_set_light_reset.argtypes = [c_int]
    GS_set_light_reset.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 62
if _libs["grass_ogsf.8.4"].has("GS_get_light_reset", "cdecl"):
    GS_get_light_reset = _libs["grass_ogsf.8.4"].get("GS_get_light_reset", "cdecl")
    GS_get_light_reset.argtypes = []
    GS_get_light_reset.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 63
if _libs["grass_ogsf.8.4"].has("GS_setlight_position", "cdecl"):
    GS_setlight_position = _libs["grass_ogsf.8.4"].get("GS_setlight_position", "cdecl")
    GS_setlight_position.argtypes = [c_int, c_float, c_float, c_float, c_int]
    GS_setlight_position.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 64
if _libs["grass_ogsf.8.4"].has("GS_setlight_color", "cdecl"):
    GS_setlight_color = _libs["grass_ogsf.8.4"].get("GS_setlight_color", "cdecl")
    GS_setlight_color.argtypes = [c_int, c_float, c_float, c_float]
    GS_setlight_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 65
if _libs["grass_ogsf.8.4"].has("GS_setlight_ambient", "cdecl"):
    GS_setlight_ambient = _libs["grass_ogsf.8.4"].get("GS_setlight_ambient", "cdecl")
    GS_setlight_ambient.argtypes = [c_int, c_float, c_float, c_float]
    GS_setlight_ambient.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 66
if _libs["grass_ogsf.8.4"].has("GS_lights_off", "cdecl"):
    GS_lights_off = _libs["grass_ogsf.8.4"].get("GS_lights_off", "cdecl")
    GS_lights_off.argtypes = []
    GS_lights_off.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 67
if _libs["grass_ogsf.8.4"].has("GS_lights_on", "cdecl"):
    GS_lights_on = _libs["grass_ogsf.8.4"].get("GS_lights_on", "cdecl")
    GS_lights_on.argtypes = []
    GS_lights_on.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 68
if _libs["grass_ogsf.8.4"].has("GS_switchlight", "cdecl"):
    GS_switchlight = _libs["grass_ogsf.8.4"].get("GS_switchlight", "cdecl")
    GS_switchlight.argtypes = [c_int, c_int]
    GS_switchlight.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 69
if _libs["grass_ogsf.8.4"].has("GS_transp_is_set", "cdecl"):
    GS_transp_is_set = _libs["grass_ogsf.8.4"].get("GS_transp_is_set", "cdecl")
    GS_transp_is_set.argtypes = []
    GS_transp_is_set.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 70
if _libs["grass_ogsf.8.4"].has("GS_get_modelposition1", "cdecl"):
    GS_get_modelposition1 = _libs["grass_ogsf.8.4"].get("GS_get_modelposition1", "cdecl")
    GS_get_modelposition1.argtypes = [POINTER(c_float)]
    GS_get_modelposition1.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 71
if _libs["grass_ogsf.8.4"].has("GS_get_modelposition", "cdecl"):
    GS_get_modelposition = _libs["grass_ogsf.8.4"].get("GS_get_modelposition", "cdecl")
    GS_get_modelposition.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_get_modelposition.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 72
if _libs["grass_ogsf.8.4"].has("GS_draw_X", "cdecl"):
    GS_draw_X = _libs["grass_ogsf.8.4"].get("GS_draw_X", "cdecl")
    GS_draw_X.argtypes = [c_int, POINTER(c_float)]
    GS_draw_X.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 73
if _libs["grass_ogsf.8.4"].has("GS_set_Narrow", "cdecl"):
    GS_set_Narrow = _libs["grass_ogsf.8.4"].get("GS_set_Narrow", "cdecl")
    GS_set_Narrow.argtypes = [POINTER(c_int), c_int, POINTER(c_float)]
    GS_set_Narrow.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 74
if _libs["grass_ogsf.8.4"].has("GS_draw_line_onsurf", "cdecl"):
    GS_draw_line_onsurf = _libs["grass_ogsf.8.4"].get("GS_draw_line_onsurf", "cdecl")
    GS_draw_line_onsurf.argtypes = [c_int, c_float, c_float, c_float, c_float]
    GS_draw_line_onsurf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 75
if _libs["grass_ogsf.8.4"].has("GS_draw_nline_onsurf", "cdecl"):
    GS_draw_nline_onsurf = _libs["grass_ogsf.8.4"].get("GS_draw_nline_onsurf", "cdecl")
    GS_draw_nline_onsurf.argtypes = [c_int, c_float, c_float, c_float, c_float, POINTER(c_float), c_int]
    GS_draw_nline_onsurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 76
if _libs["grass_ogsf.8.4"].has("GS_draw_flowline_at_xy", "cdecl"):
    GS_draw_flowline_at_xy = _libs["grass_ogsf.8.4"].get("GS_draw_flowline_at_xy", "cdecl")
    GS_draw_flowline_at_xy.argtypes = [c_int, c_float, c_float]
    GS_draw_flowline_at_xy.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 77
if _libs["grass_ogsf.8.4"].has("GS_draw_lighting_model1", "cdecl"):
    GS_draw_lighting_model1 = _libs["grass_ogsf.8.4"].get("GS_draw_lighting_model1", "cdecl")
    GS_draw_lighting_model1.argtypes = []
    GS_draw_lighting_model1.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 78
if _libs["grass_ogsf.8.4"].has("GS_draw_lighting_model", "cdecl"):
    GS_draw_lighting_model = _libs["grass_ogsf.8.4"].get("GS_draw_lighting_model", "cdecl")
    GS_draw_lighting_model.argtypes = []
    GS_draw_lighting_model.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 79
if _libs["grass_ogsf.8.4"].has("GS_update_curmask", "cdecl"):
    GS_update_curmask = _libs["grass_ogsf.8.4"].get("GS_update_curmask", "cdecl")
    GS_update_curmask.argtypes = [c_int]
    GS_update_curmask.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 80
if _libs["grass_ogsf.8.4"].has("GS_is_masked", "cdecl"):
    GS_is_masked = _libs["grass_ogsf.8.4"].get("GS_is_masked", "cdecl")
    GS_is_masked.argtypes = [c_int, POINTER(c_float)]
    GS_is_masked.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 81
if _libs["grass_ogsf.8.4"].has("GS_unset_SDsurf", "cdecl"):
    GS_unset_SDsurf = _libs["grass_ogsf.8.4"].get("GS_unset_SDsurf", "cdecl")
    GS_unset_SDsurf.argtypes = []
    GS_unset_SDsurf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 82
if _libs["grass_ogsf.8.4"].has("GS_set_SDsurf", "cdecl"):
    GS_set_SDsurf = _libs["grass_ogsf.8.4"].get("GS_set_SDsurf", "cdecl")
    GS_set_SDsurf.argtypes = [c_int]
    GS_set_SDsurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 83
if _libs["grass_ogsf.8.4"].has("GS_set_SDscale", "cdecl"):
    GS_set_SDscale = _libs["grass_ogsf.8.4"].get("GS_set_SDscale", "cdecl")
    GS_set_SDscale.argtypes = [c_float]
    GS_set_SDscale.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 84
if _libs["grass_ogsf.8.4"].has("GS_get_SDsurf", "cdecl"):
    GS_get_SDsurf = _libs["grass_ogsf.8.4"].get("GS_get_SDsurf", "cdecl")
    GS_get_SDsurf.argtypes = [POINTER(c_int)]
    GS_get_SDsurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 85
if _libs["grass_ogsf.8.4"].has("GS_get_SDscale", "cdecl"):
    GS_get_SDscale = _libs["grass_ogsf.8.4"].get("GS_get_SDscale", "cdecl")
    GS_get_SDscale.argtypes = [POINTER(c_float)]
    GS_get_SDscale.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 86
if _libs["grass_ogsf.8.4"].has("GS_update_normals", "cdecl"):
    GS_update_normals = _libs["grass_ogsf.8.4"].get("GS_update_normals", "cdecl")
    GS_update_normals.argtypes = [c_int]
    GS_update_normals.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 87
if _libs["grass_ogsf.8.4"].has("GS_get_att", "cdecl"):
    GS_get_att = _libs["grass_ogsf.8.4"].get("GS_get_att", "cdecl")
    GS_get_att.argtypes = [c_int, c_int, POINTER(c_int), POINTER(c_float), String]
    GS_get_att.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 88
if _libs["grass_ogsf.8.4"].has("GS_get_cat_at_xy", "cdecl"):
    GS_get_cat_at_xy = _libs["grass_ogsf.8.4"].get("GS_get_cat_at_xy", "cdecl")
    GS_get_cat_at_xy.argtypes = [c_int, c_int, String, c_float, c_float]
    GS_get_cat_at_xy.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 89
if _libs["grass_ogsf.8.4"].has("GS_get_norm_at_xy", "cdecl"):
    GS_get_norm_at_xy = _libs["grass_ogsf.8.4"].get("GS_get_norm_at_xy", "cdecl")
    GS_get_norm_at_xy.argtypes = [c_int, c_float, c_float, POINTER(c_float)]
    GS_get_norm_at_xy.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 90
if _libs["grass_ogsf.8.4"].has("GS_get_val_at_xy", "cdecl"):
    GS_get_val_at_xy = _libs["grass_ogsf.8.4"].get("GS_get_val_at_xy", "cdecl")
    GS_get_val_at_xy.argtypes = [c_int, c_int, String, c_float, c_float]
    GS_get_val_at_xy.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 91
if _libs["grass_ogsf.8.4"].has("GS_unset_att", "cdecl"):
    GS_unset_att = _libs["grass_ogsf.8.4"].get("GS_unset_att", "cdecl")
    GS_unset_att.argtypes = [c_int, c_int]
    GS_unset_att.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 92
if _libs["grass_ogsf.8.4"].has("GS_set_att_const", "cdecl"):
    GS_set_att_const = _libs["grass_ogsf.8.4"].get("GS_set_att_const", "cdecl")
    GS_set_att_const.argtypes = [c_int, c_int, c_float]
    GS_set_att_const.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 93
if _libs["grass_ogsf.8.4"].has("GS_set_maskmode", "cdecl"):
    GS_set_maskmode = _libs["grass_ogsf.8.4"].get("GS_set_maskmode", "cdecl")
    GS_set_maskmode.argtypes = [c_int, c_int]
    GS_set_maskmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 94
if _libs["grass_ogsf.8.4"].has("GS_get_maskmode", "cdecl"):
    GS_get_maskmode = _libs["grass_ogsf.8.4"].get("GS_get_maskmode", "cdecl")
    GS_get_maskmode.argtypes = [c_int, POINTER(c_int)]
    GS_get_maskmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 95
if _libs["grass_ogsf.8.4"].has("GS_Set_ClientData", "cdecl"):
    GS_Set_ClientData = _libs["grass_ogsf.8.4"].get("GS_Set_ClientData", "cdecl")
    GS_Set_ClientData.argtypes = [c_int, POINTER(None)]
    GS_Set_ClientData.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 96
if _libs["grass_ogsf.8.4"].has("GS_Get_ClientData", "cdecl"):
    GS_Get_ClientData = _libs["grass_ogsf.8.4"].get("GS_Get_ClientData", "cdecl")
    GS_Get_ClientData.argtypes = [c_int]
    GS_Get_ClientData.restype = POINTER(c_ubyte)
    GS_Get_ClientData.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 97
if _libs["grass_ogsf.8.4"].has("GS_num_surfs", "cdecl"):
    GS_num_surfs = _libs["grass_ogsf.8.4"].get("GS_num_surfs", "cdecl")
    GS_num_surfs.argtypes = []
    GS_num_surfs.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 98
if _libs["grass_ogsf.8.4"].has("GS_get_surf_list", "cdecl"):
    GS_get_surf_list = _libs["grass_ogsf.8.4"].get("GS_get_surf_list", "cdecl")
    GS_get_surf_list.argtypes = [POINTER(c_int)]
    GS_get_surf_list.restype = POINTER(c_int)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 99
if _libs["grass_ogsf.8.4"].has("GS_delete_surface", "cdecl"):
    GS_delete_surface = _libs["grass_ogsf.8.4"].get("GS_delete_surface", "cdecl")
    GS_delete_surface.argtypes = [c_int]
    GS_delete_surface.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 100
if _libs["grass_ogsf.8.4"].has("GS_load_att_map", "cdecl"):
    GS_load_att_map = _libs["grass_ogsf.8.4"].get("GS_load_att_map", "cdecl")
    GS_load_att_map.argtypes = [c_int, String, c_int]
    GS_load_att_map.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 101
if _libs["grass_ogsf.8.4"].has("GS_draw_surf", "cdecl"):
    GS_draw_surf = _libs["grass_ogsf.8.4"].get("GS_draw_surf", "cdecl")
    GS_draw_surf.argtypes = [c_int]
    GS_draw_surf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 102
if _libs["grass_ogsf.8.4"].has("GS_draw_wire", "cdecl"):
    GS_draw_wire = _libs["grass_ogsf.8.4"].get("GS_draw_wire", "cdecl")
    GS_draw_wire.argtypes = [c_int]
    GS_draw_wire.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 103
if _libs["grass_ogsf.8.4"].has("GS_alldraw_wire", "cdecl"):
    GS_alldraw_wire = _libs["grass_ogsf.8.4"].get("GS_alldraw_wire", "cdecl")
    GS_alldraw_wire.argtypes = []
    GS_alldraw_wire.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 104
if _libs["grass_ogsf.8.4"].has("GS_alldraw_surf", "cdecl"):
    GS_alldraw_surf = _libs["grass_ogsf.8.4"].get("GS_alldraw_surf", "cdecl")
    GS_alldraw_surf.argtypes = []
    GS_alldraw_surf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 105
if _libs["grass_ogsf.8.4"].has("GS_set_exag", "cdecl"):
    GS_set_exag = _libs["grass_ogsf.8.4"].get("GS_set_exag", "cdecl")
    GS_set_exag.argtypes = [c_int, c_float]
    GS_set_exag.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 106
if _libs["grass_ogsf.8.4"].has("GS_set_global_exag", "cdecl"):
    GS_set_global_exag = _libs["grass_ogsf.8.4"].get("GS_set_global_exag", "cdecl")
    GS_set_global_exag.argtypes = [c_float]
    GS_set_global_exag.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 107
if _libs["grass_ogsf.8.4"].has("GS_global_exag", "cdecl"):
    GS_global_exag = _libs["grass_ogsf.8.4"].get("GS_global_exag", "cdecl")
    GS_global_exag.argtypes = []
    GS_global_exag.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 108
if _libs["grass_ogsf.8.4"].has("GS_set_wire_color", "cdecl"):
    GS_set_wire_color = _libs["grass_ogsf.8.4"].get("GS_set_wire_color", "cdecl")
    GS_set_wire_color.argtypes = [c_int, c_int]
    GS_set_wire_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 109
if _libs["grass_ogsf.8.4"].has("GS_get_wire_color", "cdecl"):
    GS_get_wire_color = _libs["grass_ogsf.8.4"].get("GS_get_wire_color", "cdecl")
    GS_get_wire_color.argtypes = [c_int, POINTER(c_int)]
    GS_get_wire_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 110
if _libs["grass_ogsf.8.4"].has("GS_setall_drawmode", "cdecl"):
    GS_setall_drawmode = _libs["grass_ogsf.8.4"].get("GS_setall_drawmode", "cdecl")
    GS_setall_drawmode.argtypes = [c_int]
    GS_setall_drawmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 111
if _libs["grass_ogsf.8.4"].has("GS_set_drawmode", "cdecl"):
    GS_set_drawmode = _libs["grass_ogsf.8.4"].get("GS_set_drawmode", "cdecl")
    GS_set_drawmode.argtypes = [c_int, c_int]
    GS_set_drawmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 112
if _libs["grass_ogsf.8.4"].has("GS_get_drawmode", "cdecl"):
    GS_get_drawmode = _libs["grass_ogsf.8.4"].get("GS_get_drawmode", "cdecl")
    GS_get_drawmode.argtypes = [c_int, POINTER(c_int)]
    GS_get_drawmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 113
if _libs["grass_ogsf.8.4"].has("GS_set_nozero", "cdecl"):
    GS_set_nozero = _libs["grass_ogsf.8.4"].get("GS_set_nozero", "cdecl")
    GS_set_nozero.argtypes = [c_int, c_int, c_int]
    GS_set_nozero.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 114
if _libs["grass_ogsf.8.4"].has("GS_get_nozero", "cdecl"):
    GS_get_nozero = _libs["grass_ogsf.8.4"].get("GS_get_nozero", "cdecl")
    GS_get_nozero.argtypes = [c_int, c_int, POINTER(c_int)]
    GS_get_nozero.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 115
if _libs["grass_ogsf.8.4"].has("GS_setall_drawres", "cdecl"):
    GS_setall_drawres = _libs["grass_ogsf.8.4"].get("GS_setall_drawres", "cdecl")
    GS_setall_drawres.argtypes = [c_int, c_int, c_int, c_int]
    GS_setall_drawres.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 116
if _libs["grass_ogsf.8.4"].has("GS_set_drawres", "cdecl"):
    GS_set_drawres = _libs["grass_ogsf.8.4"].get("GS_set_drawres", "cdecl")
    GS_set_drawres.argtypes = [c_int, c_int, c_int, c_int, c_int]
    GS_set_drawres.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 117
if _libs["grass_ogsf.8.4"].has("GS_get_drawres", "cdecl"):
    GS_get_drawres = _libs["grass_ogsf.8.4"].get("GS_get_drawres", "cdecl")
    GS_get_drawres.argtypes = [c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    GS_get_drawres.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 118
if _libs["grass_ogsf.8.4"].has("GS_get_dims", "cdecl"):
    GS_get_dims = _libs["grass_ogsf.8.4"].get("GS_get_dims", "cdecl")
    GS_get_dims.argtypes = [c_int, POINTER(c_int), POINTER(c_int)]
    GS_get_dims.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 119
if _libs["grass_ogsf.8.4"].has("GS_get_exag_guess", "cdecl"):
    GS_get_exag_guess = _libs["grass_ogsf.8.4"].get("GS_get_exag_guess", "cdecl")
    GS_get_exag_guess.argtypes = [c_int, POINTER(c_float)]
    GS_get_exag_guess.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 120
if _libs["grass_ogsf.8.4"].has("GS_get_zrange_nz", "cdecl"):
    GS_get_zrange_nz = _libs["grass_ogsf.8.4"].get("GS_get_zrange_nz", "cdecl")
    GS_get_zrange_nz.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_get_zrange_nz.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 121
if _libs["grass_ogsf.8.4"].has("GS_set_trans", "cdecl"):
    GS_set_trans = _libs["grass_ogsf.8.4"].get("GS_set_trans", "cdecl")
    GS_set_trans.argtypes = [c_int, c_float, c_float, c_float]
    GS_set_trans.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 122
if _libs["grass_ogsf.8.4"].has("GS_get_trans", "cdecl"):
    GS_get_trans = _libs["grass_ogsf.8.4"].get("GS_get_trans", "cdecl")
    GS_get_trans.argtypes = [c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_get_trans.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 123
if _libs["grass_ogsf.8.4"].has("GS_default_draw_color", "cdecl"):
    GS_default_draw_color = _libs["grass_ogsf.8.4"].get("GS_default_draw_color", "cdecl")
    GS_default_draw_color.argtypes = []
    GS_default_draw_color.restype = c_uint

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 124
if _libs["grass_ogsf.8.4"].has("GS_background_color", "cdecl"):
    GS_background_color = _libs["grass_ogsf.8.4"].get("GS_background_color", "cdecl")
    GS_background_color.argtypes = []
    GS_background_color.restype = c_uint

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 125
if _libs["grass_ogsf.8.4"].has("GS_set_draw", "cdecl"):
    GS_set_draw = _libs["grass_ogsf.8.4"].get("GS_set_draw", "cdecl")
    GS_set_draw.argtypes = [c_int]
    GS_set_draw.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 126
if _libs["grass_ogsf.8.4"].has("GS_ready_draw", "cdecl"):
    GS_ready_draw = _libs["grass_ogsf.8.4"].get("GS_ready_draw", "cdecl")
    GS_ready_draw.argtypes = []
    GS_ready_draw.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 127
if _libs["grass_ogsf.8.4"].has("GS_done_draw", "cdecl"):
    GS_done_draw = _libs["grass_ogsf.8.4"].get("GS_done_draw", "cdecl")
    GS_done_draw.argtypes = []
    GS_done_draw.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 128
if _libs["grass_ogsf.8.4"].has("GS_set_focus", "cdecl"):
    GS_set_focus = _libs["grass_ogsf.8.4"].get("GS_set_focus", "cdecl")
    GS_set_focus.argtypes = [POINTER(c_float)]
    GS_set_focus.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 129
if _libs["grass_ogsf.8.4"].has("GS_get_focus", "cdecl"):
    GS_get_focus = _libs["grass_ogsf.8.4"].get("GS_get_focus", "cdecl")
    GS_get_focus.argtypes = [POINTER(c_float)]
    GS_get_focus.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 130
if _libs["grass_ogsf.8.4"].has("GS_set_focus_center_map", "cdecl"):
    GS_set_focus_center_map = _libs["grass_ogsf.8.4"].get("GS_set_focus_center_map", "cdecl")
    GS_set_focus_center_map.argtypes = [c_int]
    GS_set_focus_center_map.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 131
if _libs["grass_ogsf.8.4"].has("GS_moveto", "cdecl"):
    GS_moveto = _libs["grass_ogsf.8.4"].get("GS_moveto", "cdecl")
    GS_moveto.argtypes = [POINTER(c_float)]
    GS_moveto.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 132
if _libs["grass_ogsf.8.4"].has("GS_moveto_real", "cdecl"):
    GS_moveto_real = _libs["grass_ogsf.8.4"].get("GS_moveto_real", "cdecl")
    GS_moveto_real.argtypes = [POINTER(c_float)]
    GS_moveto_real.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 133
if _libs["grass_ogsf.8.4"].has("GS_set_focus_real", "cdecl"):
    GS_set_focus_real = _libs["grass_ogsf.8.4"].get("GS_set_focus_real", "cdecl")
    GS_set_focus_real.argtypes = [POINTER(c_float)]
    GS_set_focus_real.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 134
if _libs["grass_ogsf.8.4"].has("GS_get_to_real", "cdecl"):
    GS_get_to_real = _libs["grass_ogsf.8.4"].get("GS_get_to_real", "cdecl")
    GS_get_to_real.argtypes = [POINTER(c_float)]
    GS_get_to_real.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 135
if _libs["grass_ogsf.8.4"].has("GS_get_zextents", "cdecl"):
    GS_get_zextents = _libs["grass_ogsf.8.4"].get("GS_get_zextents", "cdecl")
    GS_get_zextents.argtypes = [c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_get_zextents.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 136
if _libs["grass_ogsf.8.4"].has("GS_get_zrange", "cdecl"):
    GS_get_zrange = _libs["grass_ogsf.8.4"].get("GS_get_zrange", "cdecl")
    GS_get_zrange.argtypes = [POINTER(c_float), POINTER(c_float), c_int]
    GS_get_zrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 137
if _libs["grass_ogsf.8.4"].has("GS_get_from", "cdecl"):
    GS_get_from = _libs["grass_ogsf.8.4"].get("GS_get_from", "cdecl")
    GS_get_from.argtypes = [POINTER(c_float)]
    GS_get_from.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 138
if _libs["grass_ogsf.8.4"].has("GS_get_from_real", "cdecl"):
    GS_get_from_real = _libs["grass_ogsf.8.4"].get("GS_get_from_real", "cdecl")
    GS_get_from_real.argtypes = [POINTER(c_float)]
    GS_get_from_real.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 139
if _libs["grass_ogsf.8.4"].has("GS_get_to", "cdecl"):
    GS_get_to = _libs["grass_ogsf.8.4"].get("GS_get_to", "cdecl")
    GS_get_to.argtypes = [POINTER(c_float)]
    GS_get_to.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 140
if _libs["grass_ogsf.8.4"].has("GS_get_viewdir", "cdecl"):
    GS_get_viewdir = _libs["grass_ogsf.8.4"].get("GS_get_viewdir", "cdecl")
    GS_get_viewdir.argtypes = [POINTER(c_float)]
    GS_get_viewdir.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 141
if _libs["grass_ogsf.8.4"].has("GS_set_viewdir", "cdecl"):
    GS_set_viewdir = _libs["grass_ogsf.8.4"].get("GS_set_viewdir", "cdecl")
    GS_set_viewdir.argtypes = [POINTER(c_float)]
    GS_set_viewdir.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 142
if _libs["grass_ogsf.8.4"].has("GS_set_fov", "cdecl"):
    GS_set_fov = _libs["grass_ogsf.8.4"].get("GS_set_fov", "cdecl")
    GS_set_fov.argtypes = [c_int]
    GS_set_fov.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 143
if _libs["grass_ogsf.8.4"].has("GS_set_rotation", "cdecl"):
    GS_set_rotation = _libs["grass_ogsf.8.4"].get("GS_set_rotation", "cdecl")
    GS_set_rotation.argtypes = [c_double, c_double, c_double, c_double]
    GS_set_rotation.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 144
if _libs["grass_ogsf.8.4"].has("GS_get_rotation_matrix", "cdecl"):
    GS_get_rotation_matrix = _libs["grass_ogsf.8.4"].get("GS_get_rotation_matrix", "cdecl")
    GS_get_rotation_matrix.argtypes = [POINTER(c_double)]
    GS_get_rotation_matrix.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 145
if _libs["grass_ogsf.8.4"].has("GS_set_rotation_matrix", "cdecl"):
    GS_set_rotation_matrix = _libs["grass_ogsf.8.4"].get("GS_set_rotation_matrix", "cdecl")
    GS_set_rotation_matrix.argtypes = [POINTER(c_double)]
    GS_set_rotation_matrix.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 146
if _libs["grass_ogsf.8.4"].has("GS_init_rotation", "cdecl"):
    GS_init_rotation = _libs["grass_ogsf.8.4"].get("GS_init_rotation", "cdecl")
    GS_init_rotation.argtypes = []
    GS_init_rotation.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 147
if _libs["grass_ogsf.8.4"].has("GS_unset_rotation", "cdecl"):
    GS_unset_rotation = _libs["grass_ogsf.8.4"].get("GS_unset_rotation", "cdecl")
    GS_unset_rotation.argtypes = []
    GS_unset_rotation.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 148
if _libs["grass_ogsf.8.4"].has("GS_get_fov", "cdecl"):
    GS_get_fov = _libs["grass_ogsf.8.4"].get("GS_get_fov", "cdecl")
    GS_get_fov.argtypes = []
    GS_get_fov.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 149
if _libs["grass_ogsf.8.4"].has("GS_get_twist", "cdecl"):
    GS_get_twist = _libs["grass_ogsf.8.4"].get("GS_get_twist", "cdecl")
    GS_get_twist.argtypes = []
    GS_get_twist.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 150
if _libs["grass_ogsf.8.4"].has("GS_set_twist", "cdecl"):
    GS_set_twist = _libs["grass_ogsf.8.4"].get("GS_set_twist", "cdecl")
    GS_set_twist.argtypes = [c_int]
    GS_set_twist.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 151
if _libs["grass_ogsf.8.4"].has("GS_set_nofocus", "cdecl"):
    GS_set_nofocus = _libs["grass_ogsf.8.4"].get("GS_set_nofocus", "cdecl")
    GS_set_nofocus.argtypes = []
    GS_set_nofocus.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 152
if _libs["grass_ogsf.8.4"].has("GS_set_infocus", "cdecl"):
    GS_set_infocus = _libs["grass_ogsf.8.4"].get("GS_set_infocus", "cdecl")
    GS_set_infocus.argtypes = []
    GS_set_infocus.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 153
if _libs["grass_ogsf.8.4"].has("GS_set_viewport", "cdecl"):
    GS_set_viewport = _libs["grass_ogsf.8.4"].get("GS_set_viewport", "cdecl")
    GS_set_viewport.argtypes = [c_int, c_int, c_int, c_int]
    GS_set_viewport.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 154
if _libs["grass_ogsf.8.4"].has("GS_look_here", "cdecl"):
    GS_look_here = _libs["grass_ogsf.8.4"].get("GS_look_here", "cdecl")
    GS_look_here.argtypes = [c_int, c_int]
    GS_look_here.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 155
if _libs["grass_ogsf.8.4"].has("GS_get_selected_point_on_surface", "cdecl"):
    GS_get_selected_point_on_surface = _libs["grass_ogsf.8.4"].get("GS_get_selected_point_on_surface", "cdecl")
    GS_get_selected_point_on_surface.argtypes = [c_int, c_int, POINTER(c_int), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_get_selected_point_on_surface.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 157
if _libs["grass_ogsf.8.4"].has("GS_set_cplane_rot", "cdecl"):
    GS_set_cplane_rot = _libs["grass_ogsf.8.4"].get("GS_set_cplane_rot", "cdecl")
    GS_set_cplane_rot.argtypes = [c_int, c_float, c_float, c_float]
    GS_set_cplane_rot.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 158
if _libs["grass_ogsf.8.4"].has("GS_set_cplane_trans", "cdecl"):
    GS_set_cplane_trans = _libs["grass_ogsf.8.4"].get("GS_set_cplane_trans", "cdecl")
    GS_set_cplane_trans.argtypes = [c_int, c_float, c_float, c_float]
    GS_set_cplane_trans.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 159
if _libs["grass_ogsf.8.4"].has("GS_draw_cplane", "cdecl"):
    GS_draw_cplane = _libs["grass_ogsf.8.4"].get("GS_draw_cplane", "cdecl")
    GS_draw_cplane.argtypes = [c_int]
    GS_draw_cplane.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 160
if _libs["grass_ogsf.8.4"].has("GS_draw_cplane_fence", "cdecl"):
    GS_draw_cplane_fence = _libs["grass_ogsf.8.4"].get("GS_draw_cplane_fence", "cdecl")
    GS_draw_cplane_fence.argtypes = [c_int, c_int, c_int]
    GS_draw_cplane_fence.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 161
if _libs["grass_ogsf.8.4"].has("GS_alldraw_cplane_fences", "cdecl"):
    GS_alldraw_cplane_fences = _libs["grass_ogsf.8.4"].get("GS_alldraw_cplane_fences", "cdecl")
    GS_alldraw_cplane_fences.argtypes = []
    GS_alldraw_cplane_fences.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 162
if _libs["grass_ogsf.8.4"].has("GS_set_cplane", "cdecl"):
    GS_set_cplane = _libs["grass_ogsf.8.4"].get("GS_set_cplane", "cdecl")
    GS_set_cplane.argtypes = [c_int]
    GS_set_cplane.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 163
if _libs["grass_ogsf.8.4"].has("GS_unset_cplane", "cdecl"):
    GS_unset_cplane = _libs["grass_ogsf.8.4"].get("GS_unset_cplane", "cdecl")
    GS_unset_cplane.argtypes = [c_int]
    GS_unset_cplane.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 164
if _libs["grass_ogsf.8.4"].has("GS_get_scale", "cdecl"):
    GS_get_scale = _libs["grass_ogsf.8.4"].get("GS_get_scale", "cdecl")
    GS_get_scale.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float), c_int]
    GS_get_scale.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 165
if _libs["grass_ogsf.8.4"].has("GS_set_fencecolor", "cdecl"):
    GS_set_fencecolor = _libs["grass_ogsf.8.4"].get("GS_set_fencecolor", "cdecl")
    GS_set_fencecolor.argtypes = [c_int]
    GS_set_fencecolor.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 166
if _libs["grass_ogsf.8.4"].has("GS_get_fencecolor", "cdecl"):
    GS_get_fencecolor = _libs["grass_ogsf.8.4"].get("GS_get_fencecolor", "cdecl")
    GS_get_fencecolor.argtypes = []
    GS_get_fencecolor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 167
if _libs["grass_ogsf.8.4"].has("GS_get_distance_alongsurf", "cdecl"):
    GS_get_distance_alongsurf = _libs["grass_ogsf.8.4"].get("GS_get_distance_alongsurf", "cdecl")
    GS_get_distance_alongsurf.argtypes = [c_int, c_float, c_float, c_float, c_float, POINTER(c_float), c_int]
    GS_get_distance_alongsurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 168
if _libs["grass_ogsf.8.4"].has("GS_save_3dview", "cdecl"):
    GS_save_3dview = _libs["grass_ogsf.8.4"].get("GS_save_3dview", "cdecl")
    GS_save_3dview.argtypes = [String, c_int]
    GS_save_3dview.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 169
if _libs["grass_ogsf.8.4"].has("GS_load_3dview", "cdecl"):
    GS_load_3dview = _libs["grass_ogsf.8.4"].get("GS_load_3dview", "cdecl")
    GS_load_3dview.argtypes = [String, c_int]
    GS_load_3dview.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 170
if _libs["grass_ogsf.8.4"].has("GS_init_view", "cdecl"):
    GS_init_view = _libs["grass_ogsf.8.4"].get("GS_init_view", "cdecl")
    GS_init_view.argtypes = []
    GS_init_view.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 171
if _libs["grass_ogsf.8.4"].has("GS_clear", "cdecl"):
    GS_clear = _libs["grass_ogsf.8.4"].get("GS_clear", "cdecl")
    GS_clear.argtypes = [c_int]
    GS_clear.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 172
if _libs["grass_ogsf.8.4"].has("GS_get_aspect", "cdecl"):
    GS_get_aspect = _libs["grass_ogsf.8.4"].get("GS_get_aspect", "cdecl")
    GS_get_aspect.argtypes = []
    GS_get_aspect.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 173
if _libs["grass_ogsf.8.4"].has("GS_has_transparency", "cdecl"):
    GS_has_transparency = _libs["grass_ogsf.8.4"].get("GS_has_transparency", "cdecl")
    GS_has_transparency.argtypes = []
    GS_has_transparency.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 174
if _libs["grass_ogsf.8.4"].has("GS_zoom_setup", "cdecl"):
    GS_zoom_setup = _libs["grass_ogsf.8.4"].get("GS_zoom_setup", "cdecl")
    GS_zoom_setup.argtypes = [POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    GS_zoom_setup.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 175
if _libs["grass_ogsf.8.4"].has("GS_write_zoom", "cdecl"):
    GS_write_zoom = _libs["grass_ogsf.8.4"].get("GS_write_zoom", "cdecl")
    GS_write_zoom.argtypes = [String, c_uint, c_uint]
    GS_write_zoom.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 176
if _libs["grass_ogsf.8.4"].has("GS_draw_all_list", "cdecl"):
    GS_draw_all_list = _libs["grass_ogsf.8.4"].get("GS_draw_all_list", "cdecl")
    GS_draw_all_list.argtypes = []
    GS_draw_all_list.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 177
if _libs["grass_ogsf.8.4"].has("GS_delete_list", "cdecl"):
    GS_delete_list = _libs["grass_ogsf.8.4"].get("GS_delete_list", "cdecl")
    GS_delete_list.argtypes = [GLuint]
    GS_delete_list.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 178
if _libs["grass_ogsf.8.4"].has("GS_draw_legend", "cdecl"):
    GS_draw_legend = _libs["grass_ogsf.8.4"].get("GS_draw_legend", "cdecl")
    GS_draw_legend.argtypes = [String, GLuint, c_int, POINTER(c_int), POINTER(c_float), POINTER(c_int)]
    GS_draw_legend.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 179
if _libs["grass_ogsf.8.4"].has("GS_draw_fringe", "cdecl"):
    GS_draw_fringe = _libs["grass_ogsf.8.4"].get("GS_draw_fringe", "cdecl")
    GS_draw_fringe.argtypes = [c_int, c_ulong, c_float, POINTER(c_int)]
    GS_draw_fringe.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 180
if _libs["grass_ogsf.8.4"].has("GS_getlight_position", "cdecl"):
    GS_getlight_position = _libs["grass_ogsf.8.4"].get("GS_getlight_position", "cdecl")
    GS_getlight_position.argtypes = [c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_int)]
    GS_getlight_position.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 181
if _libs["grass_ogsf.8.4"].has("GS_getlight_color", "cdecl"):
    GS_getlight_color = _libs["grass_ogsf.8.4"].get("GS_getlight_color", "cdecl")
    GS_getlight_color.argtypes = [c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_getlight_color.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 182
if _libs["grass_ogsf.8.4"].has("GS_getlight_ambient", "cdecl"):
    GS_getlight_ambient = _libs["grass_ogsf.8.4"].get("GS_getlight_ambient", "cdecl")
    GS_getlight_ambient.argtypes = [c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_getlight_ambient.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 185
if _libs["grass_ogsf.8.4"].has("GS_check_cancel", "cdecl"):
    GS_check_cancel = _libs["grass_ogsf.8.4"].get("GS_check_cancel", "cdecl")
    GS_check_cancel.argtypes = []
    GS_check_cancel.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 186
if _libs["grass_ogsf.8.4"].has("GS_set_cancel", "cdecl"):
    GS_set_cancel = _libs["grass_ogsf.8.4"].get("GS_set_cancel", "cdecl")
    GS_set_cancel.argtypes = [c_int]
    GS_set_cancel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 187
if _libs["grass_ogsf.8.4"].has("GS_set_cxl_func", "cdecl"):
    GS_set_cxl_func = _libs["grass_ogsf.8.4"].get("GS_set_cxl_func", "cdecl")
    GS_set_cxl_func.argtypes = [CFUNCTYPE(UNCHECKED(None), )]
    GS_set_cxl_func.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 190
if _libs["grass_ogsf.8.4"].has("GS_geodistance", "cdecl"):
    GS_geodistance = _libs["grass_ogsf.8.4"].get("GS_geodistance", "cdecl")
    GS_geodistance.argtypes = [POINTER(c_double), POINTER(c_double), String]
    GS_geodistance.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 191
if _libs["grass_ogsf.8.4"].has("GS_distance", "cdecl"):
    GS_distance = _libs["grass_ogsf.8.4"].get("GS_distance", "cdecl")
    GS_distance.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_distance.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 192
if _libs["grass_ogsf.8.4"].has("GS_P2distance", "cdecl"):
    GS_P2distance = _libs["grass_ogsf.8.4"].get("GS_P2distance", "cdecl")
    GS_P2distance.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_P2distance.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 193
if _libs["grass_ogsf.8.4"].has("GS_v3eq", "cdecl"):
    GS_v3eq = _libs["grass_ogsf.8.4"].get("GS_v3eq", "cdecl")
    GS_v3eq.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_v3eq.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 194
if _libs["grass_ogsf.8.4"].has("GS_v3add", "cdecl"):
    GS_v3add = _libs["grass_ogsf.8.4"].get("GS_v3add", "cdecl")
    GS_v3add.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_v3add.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 195
if _libs["grass_ogsf.8.4"].has("GS_v3sub", "cdecl"):
    GS_v3sub = _libs["grass_ogsf.8.4"].get("GS_v3sub", "cdecl")
    GS_v3sub.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_v3sub.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 196
if _libs["grass_ogsf.8.4"].has("GS_v3mult", "cdecl"):
    GS_v3mult = _libs["grass_ogsf.8.4"].get("GS_v3mult", "cdecl")
    GS_v3mult.argtypes = [POINTER(c_float), c_float]
    GS_v3mult.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 197
if _libs["grass_ogsf.8.4"].has("GS_v3norm", "cdecl"):
    GS_v3norm = _libs["grass_ogsf.8.4"].get("GS_v3norm", "cdecl")
    GS_v3norm.argtypes = [POINTER(c_float)]
    GS_v3norm.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 198
if _libs["grass_ogsf.8.4"].has("GS_v2norm", "cdecl"):
    GS_v2norm = _libs["grass_ogsf.8.4"].get("GS_v2norm", "cdecl")
    GS_v2norm.argtypes = [POINTER(c_float)]
    GS_v2norm.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 199
if _libs["grass_ogsf.8.4"].has("GS_dv3norm", "cdecl"):
    GS_dv3norm = _libs["grass_ogsf.8.4"].get("GS_dv3norm", "cdecl")
    GS_dv3norm.argtypes = [POINTER(c_double)]
    GS_dv3norm.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 200
if _libs["grass_ogsf.8.4"].has("GS_v3normalize", "cdecl"):
    GS_v3normalize = _libs["grass_ogsf.8.4"].get("GS_v3normalize", "cdecl")
    GS_v3normalize.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_v3normalize.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 201
if _libs["grass_ogsf.8.4"].has("GS_v3dir", "cdecl"):
    GS_v3dir = _libs["grass_ogsf.8.4"].get("GS_v3dir", "cdecl")
    GS_v3dir.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_v3dir.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 202
if _libs["grass_ogsf.8.4"].has("GS_v2dir", "cdecl"):
    GS_v2dir = _libs["grass_ogsf.8.4"].get("GS_v2dir", "cdecl")
    GS_v2dir.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_v2dir.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 203
if _libs["grass_ogsf.8.4"].has("GS_v3cross", "cdecl"):
    GS_v3cross = _libs["grass_ogsf.8.4"].get("GS_v3cross", "cdecl")
    GS_v3cross.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GS_v3cross.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 204
if _libs["grass_ogsf.8.4"].has("GS_v3mag", "cdecl"):
    GS_v3mag = _libs["grass_ogsf.8.4"].get("GS_v3mag", "cdecl")
    GS_v3mag.argtypes = [POINTER(c_float), POINTER(c_float)]
    GS_v3mag.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 205
if _libs["grass_ogsf.8.4"].has("GS_coordpair_repeats", "cdecl"):
    GS_coordpair_repeats = _libs["grass_ogsf.8.4"].get("GS_coordpair_repeats", "cdecl")
    GS_coordpair_repeats.argtypes = [POINTER(c_float), POINTER(c_float), c_int]
    GS_coordpair_repeats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 208
if _libs["grass_ogsf.8.4"].has("GV_vect_exists", "cdecl"):
    GV_vect_exists = _libs["grass_ogsf.8.4"].get("GV_vect_exists", "cdecl")
    GV_vect_exists.argtypes = [c_int]
    GV_vect_exists.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 209
if _libs["grass_ogsf.8.4"].has("GV_new_vector", "cdecl"):
    GV_new_vector = _libs["grass_ogsf.8.4"].get("GV_new_vector", "cdecl")
    GV_new_vector.argtypes = []
    GV_new_vector.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 210
if _libs["grass_ogsf.8.4"].has("GV_num_vects", "cdecl"):
    GV_num_vects = _libs["grass_ogsf.8.4"].get("GV_num_vects", "cdecl")
    GV_num_vects.argtypes = []
    GV_num_vects.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 211
if _libs["grass_ogsf.8.4"].has("GV_get_vect_list", "cdecl"):
    GV_get_vect_list = _libs["grass_ogsf.8.4"].get("GV_get_vect_list", "cdecl")
    GV_get_vect_list.argtypes = [POINTER(c_int)]
    GV_get_vect_list.restype = POINTER(c_int)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 212
if _libs["grass_ogsf.8.4"].has("GV_delete_vector", "cdecl"):
    GV_delete_vector = _libs["grass_ogsf.8.4"].get("GV_delete_vector", "cdecl")
    GV_delete_vector.argtypes = [c_int]
    GV_delete_vector.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 213
if _libs["grass_ogsf.8.4"].has("GV_load_vector", "cdecl"):
    GV_load_vector = _libs["grass_ogsf.8.4"].get("GV_load_vector", "cdecl")
    GV_load_vector.argtypes = [c_int, String]
    GV_load_vector.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 214
if _libs["grass_ogsf.8.4"].has("GV_get_vectname", "cdecl"):
    GV_get_vectname = _libs["grass_ogsf.8.4"].get("GV_get_vectname", "cdecl")
    GV_get_vectname.argtypes = [c_int, POINTER(POINTER(c_char))]
    GV_get_vectname.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 215
if _libs["grass_ogsf.8.4"].has("GV_set_style", "cdecl"):
    GV_set_style = _libs["grass_ogsf.8.4"].get("GV_set_style", "cdecl")
    GV_set_style.argtypes = [c_int, c_int, c_int, c_int, c_int]
    GV_set_style.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 216
if _libs["grass_ogsf.8.4"].has("GV_get_style", "cdecl"):
    GV_get_style = _libs["grass_ogsf.8.4"].get("GV_get_style", "cdecl")
    GV_get_style.argtypes = [c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    GV_get_style.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 217
if _libs["grass_ogsf.8.4"].has("GV_set_style_thematic", "cdecl"):
    GV_set_style_thematic = _libs["grass_ogsf.8.4"].get("GV_set_style_thematic", "cdecl")
    GV_set_style_thematic.argtypes = [c_int, c_int, String, String, POINTER(struct_Colors)]
    GV_set_style_thematic.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 219
if _libs["grass_ogsf.8.4"].has("GV_unset_style_thematic", "cdecl"):
    GV_unset_style_thematic = _libs["grass_ogsf.8.4"].get("GV_unset_style_thematic", "cdecl")
    GV_unset_style_thematic.argtypes = [c_int]
    GV_unset_style_thematic.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 220
if _libs["grass_ogsf.8.4"].has("GV_set_trans", "cdecl"):
    GV_set_trans = _libs["grass_ogsf.8.4"].get("GV_set_trans", "cdecl")
    GV_set_trans.argtypes = [c_int, c_float, c_float, c_float]
    GV_set_trans.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 221
if _libs["grass_ogsf.8.4"].has("GV_get_trans", "cdecl"):
    GV_get_trans = _libs["grass_ogsf.8.4"].get("GV_get_trans", "cdecl")
    GV_get_trans.argtypes = [c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GV_get_trans.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 222
if _libs["grass_ogsf.8.4"].has("GV_select_surf", "cdecl"):
    GV_select_surf = _libs["grass_ogsf.8.4"].get("GV_select_surf", "cdecl")
    GV_select_surf.argtypes = [c_int, c_int]
    GV_select_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 223
if _libs["grass_ogsf.8.4"].has("GV_unselect_surf", "cdecl"):
    GV_unselect_surf = _libs["grass_ogsf.8.4"].get("GV_unselect_surf", "cdecl")
    GV_unselect_surf.argtypes = [c_int, c_int]
    GV_unselect_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 224
if _libs["grass_ogsf.8.4"].has("GV_surf_is_selected", "cdecl"):
    GV_surf_is_selected = _libs["grass_ogsf.8.4"].get("GV_surf_is_selected", "cdecl")
    GV_surf_is_selected.argtypes = [c_int, c_int]
    GV_surf_is_selected.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 225
if _libs["grass_ogsf.8.4"].has("GV_draw_vect", "cdecl"):
    GV_draw_vect = _libs["grass_ogsf.8.4"].get("GV_draw_vect", "cdecl")
    GV_draw_vect.argtypes = [c_int]
    GV_draw_vect.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 226
if _libs["grass_ogsf.8.4"].has("GV_alldraw_vect", "cdecl"):
    GV_alldraw_vect = _libs["grass_ogsf.8.4"].get("GV_alldraw_vect", "cdecl")
    GV_alldraw_vect.argtypes = []
    GV_alldraw_vect.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 227
if _libs["grass_ogsf.8.4"].has("GV_alldraw_fastvect", "cdecl"):
    GV_alldraw_fastvect = _libs["grass_ogsf.8.4"].get("GV_alldraw_fastvect", "cdecl")
    GV_alldraw_fastvect.argtypes = []
    GV_alldraw_fastvect.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 228
if _libs["grass_ogsf.8.4"].has("GV_draw_fastvect", "cdecl"):
    GV_draw_fastvect = _libs["grass_ogsf.8.4"].get("GV_draw_fastvect", "cdecl")
    GV_draw_fastvect.argtypes = [c_int]
    GV_draw_fastvect.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 229
if _libs["grass_ogsf.8.4"].has("GV_Set_ClientData", "cdecl"):
    GV_Set_ClientData = _libs["grass_ogsf.8.4"].get("GV_Set_ClientData", "cdecl")
    GV_Set_ClientData.argtypes = [c_int, POINTER(None)]
    GV_Set_ClientData.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 230
if _libs["grass_ogsf.8.4"].has("GV_Get_ClientData", "cdecl"):
    GV_Get_ClientData = _libs["grass_ogsf.8.4"].get("GV_Get_ClientData", "cdecl")
    GV_Get_ClientData.argtypes = [c_int]
    GV_Get_ClientData.restype = POINTER(c_ubyte)
    GV_Get_ClientData.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 233
if _libs["grass_ogsf.8.4"].has("GVL_libinit", "cdecl"):
    GVL_libinit = _libs["grass_ogsf.8.4"].get("GVL_libinit", "cdecl")
    GVL_libinit.argtypes = []
    GVL_libinit.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 234
if _libs["grass_ogsf.8.4"].has("GVL_init_region", "cdecl"):
    GVL_init_region = _libs["grass_ogsf.8.4"].get("GVL_init_region", "cdecl")
    GVL_init_region.argtypes = []
    GVL_init_region.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 235
if _libs["grass_ogsf.8.4"].has("GVL_get_region", "cdecl"):
    GVL_get_region = _libs["grass_ogsf.8.4"].get("GVL_get_region", "cdecl")
    GVL_get_region.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GVL_get_region.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 236
if _libs["grass_ogsf.8.4"].has("GVL_get_window", "cdecl"):
    GVL_get_window = _libs["grass_ogsf.8.4"].get("GVL_get_window", "cdecl")
    GVL_get_window.argtypes = []
    GVL_get_window.restype = POINTER(c_ubyte)
    GVL_get_window.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 237
if _libs["grass_ogsf.8.4"].has("GVL_vol_exists", "cdecl"):
    GVL_vol_exists = _libs["grass_ogsf.8.4"].get("GVL_vol_exists", "cdecl")
    GVL_vol_exists.argtypes = [c_int]
    GVL_vol_exists.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 238
if _libs["grass_ogsf.8.4"].has("GVL_new_vol", "cdecl"):
    GVL_new_vol = _libs["grass_ogsf.8.4"].get("GVL_new_vol", "cdecl")
    GVL_new_vol.argtypes = []
    GVL_new_vol.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 239
if _libs["grass_ogsf.8.4"].has("GVL_num_vols", "cdecl"):
    GVL_num_vols = _libs["grass_ogsf.8.4"].get("GVL_num_vols", "cdecl")
    GVL_num_vols.argtypes = []
    GVL_num_vols.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 240
if _libs["grass_ogsf.8.4"].has("GVL_get_vol_list", "cdecl"):
    GVL_get_vol_list = _libs["grass_ogsf.8.4"].get("GVL_get_vol_list", "cdecl")
    GVL_get_vol_list.argtypes = [POINTER(c_int)]
    GVL_get_vol_list.restype = POINTER(c_int)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 241
if _libs["grass_ogsf.8.4"].has("GVL_delete_vol", "cdecl"):
    GVL_delete_vol = _libs["grass_ogsf.8.4"].get("GVL_delete_vol", "cdecl")
    GVL_delete_vol.argtypes = [c_int]
    GVL_delete_vol.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 242
if _libs["grass_ogsf.8.4"].has("GVL_load_vol", "cdecl"):
    GVL_load_vol = _libs["grass_ogsf.8.4"].get("GVL_load_vol", "cdecl")
    GVL_load_vol.argtypes = [c_int, String]
    GVL_load_vol.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 243
if _libs["grass_ogsf.8.4"].has("GVL_get_volname", "cdecl"):
    GVL_get_volname = _libs["grass_ogsf.8.4"].get("GVL_get_volname", "cdecl")
    GVL_get_volname.argtypes = [c_int, String]
    GVL_get_volname.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 244
if _libs["grass_ogsf.8.4"].has("GVL_set_trans", "cdecl"):
    GVL_set_trans = _libs["grass_ogsf.8.4"].get("GVL_set_trans", "cdecl")
    GVL_set_trans.argtypes = [c_int, c_float, c_float, c_float]
    GVL_set_trans.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 245
if _libs["grass_ogsf.8.4"].has("GVL_get_trans", "cdecl"):
    GVL_get_trans = _libs["grass_ogsf.8.4"].get("GVL_get_trans", "cdecl")
    GVL_get_trans.argtypes = [c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    GVL_get_trans.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 246
if _libs["grass_ogsf.8.4"].has("GVL_set_draw_wire", "cdecl"):
    GVL_set_draw_wire = _libs["grass_ogsf.8.4"].get("GVL_set_draw_wire", "cdecl")
    GVL_set_draw_wire.argtypes = [c_int, c_int]
    GVL_set_draw_wire.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 247
if _libs["grass_ogsf.8.4"].has("GVL_draw_vol", "cdecl"):
    GVL_draw_vol = _libs["grass_ogsf.8.4"].get("GVL_draw_vol", "cdecl")
    GVL_draw_vol.argtypes = [c_int]
    GVL_draw_vol.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 248
if _libs["grass_ogsf.8.4"].has("GVL_draw_wire", "cdecl"):
    GVL_draw_wire = _libs["grass_ogsf.8.4"].get("GVL_draw_wire", "cdecl")
    GVL_draw_wire.argtypes = [c_int]
    GVL_draw_wire.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 249
if _libs["grass_ogsf.8.4"].has("GVL_alldraw_vol", "cdecl"):
    GVL_alldraw_vol = _libs["grass_ogsf.8.4"].get("GVL_alldraw_vol", "cdecl")
    GVL_alldraw_vol.argtypes = []
    GVL_alldraw_vol.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 250
if _libs["grass_ogsf.8.4"].has("GVL_alldraw_wire", "cdecl"):
    GVL_alldraw_wire = _libs["grass_ogsf.8.4"].get("GVL_alldraw_wire", "cdecl")
    GVL_alldraw_wire.argtypes = []
    GVL_alldraw_wire.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 251
if _libs["grass_ogsf.8.4"].has("GVL_Set_ClientData", "cdecl"):
    GVL_Set_ClientData = _libs["grass_ogsf.8.4"].get("GVL_Set_ClientData", "cdecl")
    GVL_Set_ClientData.argtypes = [c_int, POINTER(None)]
    GVL_Set_ClientData.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 252
if _libs["grass_ogsf.8.4"].has("GVL_Get_ClientData", "cdecl"):
    GVL_Get_ClientData = _libs["grass_ogsf.8.4"].get("GVL_Get_ClientData", "cdecl")
    GVL_Get_ClientData.argtypes = [c_int]
    GVL_Get_ClientData.restype = POINTER(c_ubyte)
    GVL_Get_ClientData.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 253
if _libs["grass_ogsf.8.4"].has("GVL_get_dims", "cdecl"):
    GVL_get_dims = _libs["grass_ogsf.8.4"].get("GVL_get_dims", "cdecl")
    GVL_get_dims.argtypes = [c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    GVL_get_dims.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 254
if _libs["grass_ogsf.8.4"].has("GVL_set_focus_center_map", "cdecl"):
    GVL_set_focus_center_map = _libs["grass_ogsf.8.4"].get("GVL_set_focus_center_map", "cdecl")
    GVL_set_focus_center_map.argtypes = [c_int]
    GVL_set_focus_center_map.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 256
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_move_up", "cdecl"):
    GVL_isosurf_move_up = _libs["grass_ogsf.8.4"].get("GVL_isosurf_move_up", "cdecl")
    GVL_isosurf_move_up.argtypes = [c_int, c_int]
    GVL_isosurf_move_up.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 257
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_move_down", "cdecl"):
    GVL_isosurf_move_down = _libs["grass_ogsf.8.4"].get("GVL_isosurf_move_down", "cdecl")
    GVL_isosurf_move_down.argtypes = [c_int, c_int]
    GVL_isosurf_move_down.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 258
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_get_drawres", "cdecl"):
    GVL_isosurf_get_drawres = _libs["grass_ogsf.8.4"].get("GVL_isosurf_get_drawres", "cdecl")
    GVL_isosurf_get_drawres.argtypes = [c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    GVL_isosurf_get_drawres.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 259
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_set_drawres", "cdecl"):
    GVL_isosurf_set_drawres = _libs["grass_ogsf.8.4"].get("GVL_isosurf_set_drawres", "cdecl")
    GVL_isosurf_set_drawres.argtypes = [c_int, c_int, c_int, c_int]
    GVL_isosurf_set_drawres.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 260
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_get_drawmode", "cdecl"):
    GVL_isosurf_get_drawmode = _libs["grass_ogsf.8.4"].get("GVL_isosurf_get_drawmode", "cdecl")
    GVL_isosurf_get_drawmode.argtypes = [c_int, POINTER(c_int)]
    GVL_isosurf_get_drawmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 261
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_set_drawmode", "cdecl"):
    GVL_isosurf_set_drawmode = _libs["grass_ogsf.8.4"].get("GVL_isosurf_set_drawmode", "cdecl")
    GVL_isosurf_set_drawmode.argtypes = [c_int, c_int]
    GVL_isosurf_set_drawmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 262
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_add", "cdecl"):
    GVL_isosurf_add = _libs["grass_ogsf.8.4"].get("GVL_isosurf_add", "cdecl")
    GVL_isosurf_add.argtypes = [c_int]
    GVL_isosurf_add.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 263
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_del", "cdecl"):
    GVL_isosurf_del = _libs["grass_ogsf.8.4"].get("GVL_isosurf_del", "cdecl")
    GVL_isosurf_del.argtypes = [c_int, c_int]
    GVL_isosurf_del.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 264
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_get_att", "cdecl"):
    GVL_isosurf_get_att = _libs["grass_ogsf.8.4"].get("GVL_isosurf_get_att", "cdecl")
    GVL_isosurf_get_att.argtypes = [c_int, c_int, c_int, POINTER(c_int), POINTER(c_float), String]
    GVL_isosurf_get_att.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 265
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_unset_att", "cdecl"):
    GVL_isosurf_unset_att = _libs["grass_ogsf.8.4"].get("GVL_isosurf_unset_att", "cdecl")
    GVL_isosurf_unset_att.argtypes = [c_int, c_int, c_int]
    GVL_isosurf_unset_att.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 266
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_set_att_const", "cdecl"):
    GVL_isosurf_set_att_const = _libs["grass_ogsf.8.4"].get("GVL_isosurf_set_att_const", "cdecl")
    GVL_isosurf_set_att_const.argtypes = [c_int, c_int, c_int, c_float]
    GVL_isosurf_set_att_const.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 267
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_set_att_map", "cdecl"):
    GVL_isosurf_set_att_map = _libs["grass_ogsf.8.4"].get("GVL_isosurf_set_att_map", "cdecl")
    GVL_isosurf_set_att_map.argtypes = [c_int, c_int, c_int, String]
    GVL_isosurf_set_att_map.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 268
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_get_flags", "cdecl"):
    GVL_isosurf_get_flags = _libs["grass_ogsf.8.4"].get("GVL_isosurf_get_flags", "cdecl")
    GVL_isosurf_get_flags.argtypes = [c_int, c_int, POINTER(c_int)]
    GVL_isosurf_get_flags.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 269
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_set_flags", "cdecl"):
    GVL_isosurf_set_flags = _libs["grass_ogsf.8.4"].get("GVL_isosurf_set_flags", "cdecl")
    GVL_isosurf_set_flags.argtypes = [c_int, c_int, c_int]
    GVL_isosurf_set_flags.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 270
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_num_isosurfs", "cdecl"):
    GVL_isosurf_num_isosurfs = _libs["grass_ogsf.8.4"].get("GVL_isosurf_num_isosurfs", "cdecl")
    GVL_isosurf_num_isosurfs.argtypes = [c_int]
    GVL_isosurf_num_isosurfs.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 271
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_set_maskmode", "cdecl"):
    GVL_isosurf_set_maskmode = _libs["grass_ogsf.8.4"].get("GVL_isosurf_set_maskmode", "cdecl")
    GVL_isosurf_set_maskmode.argtypes = [c_int, c_int, c_int]
    GVL_isosurf_set_maskmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 272
if _libs["grass_ogsf.8.4"].has("GVL_isosurf_get_maskmode", "cdecl"):
    GVL_isosurf_get_maskmode = _libs["grass_ogsf.8.4"].get("GVL_isosurf_get_maskmode", "cdecl")
    GVL_isosurf_get_maskmode.argtypes = [c_int, c_int, POINTER(c_int)]
    GVL_isosurf_get_maskmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 274
if _libs["grass_ogsf.8.4"].has("GVL_slice_move_up", "cdecl"):
    GVL_slice_move_up = _libs["grass_ogsf.8.4"].get("GVL_slice_move_up", "cdecl")
    GVL_slice_move_up.argtypes = [c_int, c_int]
    GVL_slice_move_up.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 275
if _libs["grass_ogsf.8.4"].has("GVL_slice_move_down", "cdecl"):
    GVL_slice_move_down = _libs["grass_ogsf.8.4"].get("GVL_slice_move_down", "cdecl")
    GVL_slice_move_down.argtypes = [c_int, c_int]
    GVL_slice_move_down.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 276
if _libs["grass_ogsf.8.4"].has("GVL_slice_get_drawres", "cdecl"):
    GVL_slice_get_drawres = _libs["grass_ogsf.8.4"].get("GVL_slice_get_drawres", "cdecl")
    GVL_slice_get_drawres.argtypes = [c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    GVL_slice_get_drawres.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 277
if _libs["grass_ogsf.8.4"].has("GVL_slice_get_transp", "cdecl"):
    GVL_slice_get_transp = _libs["grass_ogsf.8.4"].get("GVL_slice_get_transp", "cdecl")
    GVL_slice_get_transp.argtypes = [c_int, c_int, POINTER(c_int)]
    GVL_slice_get_transp.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 278
if _libs["grass_ogsf.8.4"].has("GVL_slice_set_transp", "cdecl"):
    GVL_slice_set_transp = _libs["grass_ogsf.8.4"].get("GVL_slice_set_transp", "cdecl")
    GVL_slice_set_transp.argtypes = [c_int, c_int, c_int]
    GVL_slice_set_transp.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 279
if _libs["grass_ogsf.8.4"].has("GVL_slice_set_drawres", "cdecl"):
    GVL_slice_set_drawres = _libs["grass_ogsf.8.4"].get("GVL_slice_set_drawres", "cdecl")
    GVL_slice_set_drawres.argtypes = [c_int, c_int, c_int, c_int]
    GVL_slice_set_drawres.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 280
if _libs["grass_ogsf.8.4"].has("GVL_slice_get_drawmode", "cdecl"):
    GVL_slice_get_drawmode = _libs["grass_ogsf.8.4"].get("GVL_slice_get_drawmode", "cdecl")
    GVL_slice_get_drawmode.argtypes = [c_int, POINTER(c_int)]
    GVL_slice_get_drawmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 281
if _libs["grass_ogsf.8.4"].has("GVL_slice_set_drawmode", "cdecl"):
    GVL_slice_set_drawmode = _libs["grass_ogsf.8.4"].get("GVL_slice_set_drawmode", "cdecl")
    GVL_slice_set_drawmode.argtypes = [c_int, c_int]
    GVL_slice_set_drawmode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 282
if _libs["grass_ogsf.8.4"].has("GVL_slice_add", "cdecl"):
    GVL_slice_add = _libs["grass_ogsf.8.4"].get("GVL_slice_add", "cdecl")
    GVL_slice_add.argtypes = [c_int]
    GVL_slice_add.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 283
if _libs["grass_ogsf.8.4"].has("GVL_slice_del", "cdecl"):
    GVL_slice_del = _libs["grass_ogsf.8.4"].get("GVL_slice_del", "cdecl")
    GVL_slice_del.argtypes = [c_int, c_int]
    GVL_slice_del.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 284
if _libs["grass_ogsf.8.4"].has("GVL_slice_num_slices", "cdecl"):
    GVL_slice_num_slices = _libs["grass_ogsf.8.4"].get("GVL_slice_num_slices", "cdecl")
    GVL_slice_num_slices.argtypes = [c_int]
    GVL_slice_num_slices.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 285
if _libs["grass_ogsf.8.4"].has("GVL_slice_get_pos", "cdecl"):
    GVL_slice_get_pos = _libs["grass_ogsf.8.4"].get("GVL_slice_get_pos", "cdecl")
    GVL_slice_get_pos.argtypes = [c_int, c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_int)]
    GVL_slice_get_pos.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 287
if _libs["grass_ogsf.8.4"].has("GVL_slice_set_pos", "cdecl"):
    GVL_slice_set_pos = _libs["grass_ogsf.8.4"].get("GVL_slice_set_pos", "cdecl")
    GVL_slice_set_pos.argtypes = [c_int, c_int, c_float, c_float, c_float, c_float, c_float, c_float, c_int]
    GVL_slice_set_pos.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 290
for _lib in _libs.values():
    if not _lib.has("Gp_set_color", "cdecl"):
        continue
    Gp_set_color = _lib.get("Gp_set_color", "cdecl")
    Gp_set_color.argtypes = [String, POINTER(geopoint)]
    Gp_set_color.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 291
if _libs["grass_ogsf.8.4"].has("Gp_load_sites", "cdecl"):
    Gp_load_sites = _libs["grass_ogsf.8.4"].get("Gp_load_sites", "cdecl")
    Gp_load_sites.argtypes = [String, POINTER(c_int), POINTER(c_int)]
    Gp_load_sites.restype = POINTER(geopoint)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 292
if _libs["grass_ogsf.8.4"].has("Gp_load_sites_thematic", "cdecl"):
    Gp_load_sites_thematic = _libs["grass_ogsf.8.4"].get("Gp_load_sites_thematic", "cdecl")
    Gp_load_sites_thematic.argtypes = [POINTER(geosite), POINTER(struct_Colors)]
    Gp_load_sites_thematic.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 295
if _libs["grass_ogsf.8.4"].has("Gs_distance", "cdecl"):
    Gs_distance = _libs["grass_ogsf.8.4"].get("Gs_distance", "cdecl")
    Gs_distance.argtypes = [POINTER(c_double), POINTER(c_double)]
    Gs_distance.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 296
if _libs["grass_ogsf.8.4"].has("Gs_loadmap_as_float", "cdecl"):
    Gs_loadmap_as_float = _libs["grass_ogsf.8.4"].get("Gs_loadmap_as_float", "cdecl")
    Gs_loadmap_as_float.argtypes = [POINTER(struct_Cell_head), String, POINTER(c_float), POINTER(struct_BM), POINTER(c_int)]
    Gs_loadmap_as_float.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 298
if _libs["grass_ogsf.8.4"].has("Gs_loadmap_as_int", "cdecl"):
    Gs_loadmap_as_int = _libs["grass_ogsf.8.4"].get("Gs_loadmap_as_int", "cdecl")
    Gs_loadmap_as_int.argtypes = [POINTER(struct_Cell_head), String, POINTER(c_int), POINTER(struct_BM), POINTER(c_int)]
    Gs_loadmap_as_int.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 300
if _libs["grass_ogsf.8.4"].has("Gs_numtype", "cdecl"):
    Gs_numtype = _libs["grass_ogsf.8.4"].get("Gs_numtype", "cdecl")
    Gs_numtype.argtypes = [String, POINTER(c_int)]
    Gs_numtype.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 301
if _libs["grass_ogsf.8.4"].has("Gs_loadmap_as_short", "cdecl"):
    Gs_loadmap_as_short = _libs["grass_ogsf.8.4"].get("Gs_loadmap_as_short", "cdecl")
    Gs_loadmap_as_short.argtypes = [POINTER(struct_Cell_head), String, POINTER(c_short), POINTER(struct_BM), POINTER(c_int)]
    Gs_loadmap_as_short.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 303
if _libs["grass_ogsf.8.4"].has("Gs_loadmap_as_char", "cdecl"):
    Gs_loadmap_as_char = _libs["grass_ogsf.8.4"].get("Gs_loadmap_as_char", "cdecl")
    Gs_loadmap_as_char.argtypes = [POINTER(struct_Cell_head), String, POINTER(c_ubyte), POINTER(struct_BM), POINTER(c_int)]
    Gs_loadmap_as_char.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 305
if _libs["grass_ogsf.8.4"].has("Gs_loadmap_as_bitmap", "cdecl"):
    Gs_loadmap_as_bitmap = _libs["grass_ogsf.8.4"].get("Gs_loadmap_as_bitmap", "cdecl")
    Gs_loadmap_as_bitmap.argtypes = [POINTER(struct_Cell_head), String, POINTER(struct_BM)]
    Gs_loadmap_as_bitmap.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 306
if _libs["grass_ogsf.8.4"].has("Gs_build_256lookup", "cdecl"):
    Gs_build_256lookup = _libs["grass_ogsf.8.4"].get("Gs_build_256lookup", "cdecl")
    Gs_build_256lookup.argtypes = [String, POINTER(c_int)]
    Gs_build_256lookup.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 307
if _libs["grass_ogsf.8.4"].has("Gs_pack_colors", "cdecl"):
    Gs_pack_colors = _libs["grass_ogsf.8.4"].get("Gs_pack_colors", "cdecl")
    Gs_pack_colors.argtypes = [String, POINTER(c_int), c_int, c_int]
    Gs_pack_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 308
if _libs["grass_ogsf.8.4"].has("Gs_pack_colors_float", "cdecl"):
    Gs_pack_colors_float = _libs["grass_ogsf.8.4"].get("Gs_pack_colors_float", "cdecl")
    Gs_pack_colors_float.argtypes = [String, POINTER(c_float), POINTER(c_int), c_int, c_int]
    Gs_pack_colors_float.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 309
if _libs["grass_ogsf.8.4"].has("Gs_get_cat_label", "cdecl"):
    Gs_get_cat_label = _libs["grass_ogsf.8.4"].get("Gs_get_cat_label", "cdecl")
    Gs_get_cat_label.argtypes = [String, c_int, c_int, String]
    Gs_get_cat_label.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 310
if _libs["grass_ogsf.8.4"].has("Gs_save_3dview", "cdecl"):
    Gs_save_3dview = _libs["grass_ogsf.8.4"].get("Gs_save_3dview", "cdecl")
    Gs_save_3dview.argtypes = [String, POINTER(geoview), POINTER(geodisplay), POINTER(struct_Cell_head), POINTER(geosurf)]
    Gs_save_3dview.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 312
if _libs["grass_ogsf.8.4"].has("Gs_load_3dview", "cdecl"):
    Gs_load_3dview = _libs["grass_ogsf.8.4"].get("Gs_load_3dview", "cdecl")
    Gs_load_3dview.argtypes = [String, POINTER(geoview), POINTER(geodisplay), POINTER(struct_Cell_head), POINTER(geosurf)]
    Gs_load_3dview.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 314
if _libs["grass_ogsf.8.4"].has("Gs_update_attrange", "cdecl"):
    Gs_update_attrange = _libs["grass_ogsf.8.4"].get("Gs_update_attrange", "cdecl")
    Gs_update_attrange.argtypes = [POINTER(geosurf), c_int]
    Gs_update_attrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 317
if _libs["grass_ogsf.8.4"].has("Gv_load_vect", "cdecl"):
    Gv_load_vect = _libs["grass_ogsf.8.4"].get("Gv_load_vect", "cdecl")
    Gv_load_vect.argtypes = [String, POINTER(c_int)]
    Gv_load_vect.restype = POINTER(geoline)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 318
if _libs["grass_ogsf.8.4"].has("Gv_load_vect_thematic", "cdecl"):
    Gv_load_vect_thematic = _libs["grass_ogsf.8.4"].get("Gv_load_vect_thematic", "cdecl")
    Gv_load_vect_thematic.argtypes = [POINTER(geovect), POINTER(struct_Colors)]
    Gv_load_vect_thematic.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 319
if _libs["grass_ogsf.8.4"].has("sub_Vectmem", "cdecl"):
    sub_Vectmem = _libs["grass_ogsf.8.4"].get("sub_Vectmem", "cdecl")
    sub_Vectmem.argtypes = [c_int]
    sub_Vectmem.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 322
if _libs["grass_ogsf.8.4"].has("gk_copy_key", "cdecl"):
    gk_copy_key = _libs["grass_ogsf.8.4"].get("gk_copy_key", "cdecl")
    gk_copy_key.argtypes = [POINTER(Keylist)]
    gk_copy_key.restype = POINTER(Keylist)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 323
if _libs["grass_ogsf.8.4"].has("gk_get_mask_sofar", "cdecl"):
    gk_get_mask_sofar = _libs["grass_ogsf.8.4"].get("gk_get_mask_sofar", "cdecl")
    gk_get_mask_sofar.argtypes = [c_float, POINTER(Keylist)]
    gk_get_mask_sofar.restype = c_ulong

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 324
if _libs["grass_ogsf.8.4"].has("gk_viable_keys_for_mask", "cdecl"):
    gk_viable_keys_for_mask = _libs["grass_ogsf.8.4"].get("gk_viable_keys_for_mask", "cdecl")
    gk_viable_keys_for_mask.argtypes = [c_ulong, POINTER(Keylist), POINTER(POINTER(Keylist))]
    gk_viable_keys_for_mask.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 325
if _libs["grass_ogsf.8.4"].has("gk_follow_frames", "cdecl"):
    gk_follow_frames = _libs["grass_ogsf.8.4"].get("gk_follow_frames", "cdecl")
    gk_follow_frames.argtypes = [POINTER(Viewnode), c_int, POINTER(Keylist), c_int, c_int, c_int, c_ulong]
    gk_follow_frames.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 326
if _libs["grass_ogsf.8.4"].has("gk_free_key", "cdecl"):
    gk_free_key = _libs["grass_ogsf.8.4"].get("gk_free_key", "cdecl")
    gk_free_key.argtypes = [POINTER(Keylist)]
    gk_free_key.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 327
if _libs["grass_ogsf.8.4"].has("gk_make_framesfromkeys", "cdecl"):
    gk_make_framesfromkeys = _libs["grass_ogsf.8.4"].get("gk_make_framesfromkeys", "cdecl")
    gk_make_framesfromkeys.argtypes = [POINTER(Keylist), c_int, c_int, c_int, c_float]
    gk_make_framesfromkeys.restype = POINTER(Viewnode)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 328
if _libs["grass_ogsf.8.4"].has("get_key_neighbors", "cdecl"):
    get_key_neighbors = _libs["grass_ogsf.8.4"].get("get_key_neighbors", "cdecl")
    get_key_neighbors.argtypes = [c_int, c_double, c_double, c_int, POINTER(POINTER(Keylist)), POINTER(POINTER(Keylist)), POINTER(POINTER(Keylist)), POINTER(POINTER(Keylist)), POINTER(POINTER(Keylist)), POINTER(c_double), POINTER(c_double)]
    get_key_neighbors.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 331
if _libs["grass_ogsf.8.4"].has("lin_interp", "cdecl"):
    lin_interp = _libs["grass_ogsf.8.4"].get("lin_interp", "cdecl")
    lin_interp.argtypes = [c_float, c_float, c_float]
    lin_interp.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 332
if _libs["grass_ogsf.8.4"].has("get_2key_neighbors", "cdecl"):
    get_2key_neighbors = _libs["grass_ogsf.8.4"].get("get_2key_neighbors", "cdecl")
    get_2key_neighbors.argtypes = [c_int, c_float, c_float, c_int, POINTER(POINTER(Keylist)), POINTER(POINTER(Keylist)), POINTER(POINTER(Keylist))]
    get_2key_neighbors.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 334
if _libs["grass_ogsf.8.4"].has("gk_make_linear_framesfromkeys", "cdecl"):
    gk_make_linear_framesfromkeys = _libs["grass_ogsf.8.4"].get("gk_make_linear_framesfromkeys", "cdecl")
    gk_make_linear_framesfromkeys.argtypes = [POINTER(Keylist), c_int, c_int, c_int]
    gk_make_linear_framesfromkeys.restype = POINTER(Viewnode)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 335
if _libs["grass_ogsf.8.4"].has("correct_twist", "cdecl"):
    correct_twist = _libs["grass_ogsf.8.4"].get("correct_twist", "cdecl")
    correct_twist.argtypes = [POINTER(Keylist)]
    correct_twist.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 336
if _libs["grass_ogsf.8.4"].has("gk_draw_path", "cdecl"):
    gk_draw_path = _libs["grass_ogsf.8.4"].get("gk_draw_path", "cdecl")
    gk_draw_path.argtypes = [POINTER(Viewnode), c_int, POINTER(Keylist)]
    gk_draw_path.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 339
if _libs["grass_ogsf.8.4"].has("gp_get_site", "cdecl"):
    gp_get_site = _libs["grass_ogsf.8.4"].get("gp_get_site", "cdecl")
    gp_get_site.argtypes = [c_int]
    gp_get_site.restype = POINTER(geosite)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 340
if _libs["grass_ogsf.8.4"].has("gp_get_prev_site", "cdecl"):
    gp_get_prev_site = _libs["grass_ogsf.8.4"].get("gp_get_prev_site", "cdecl")
    gp_get_prev_site.argtypes = [c_int]
    gp_get_prev_site.restype = POINTER(geosite)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 341
if _libs["grass_ogsf.8.4"].has("gp_num_sites", "cdecl"):
    gp_num_sites = _libs["grass_ogsf.8.4"].get("gp_num_sites", "cdecl")
    gp_num_sites.argtypes = []
    gp_num_sites.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 342
if _libs["grass_ogsf.8.4"].has("gp_get_last_site", "cdecl"):
    gp_get_last_site = _libs["grass_ogsf.8.4"].get("gp_get_last_site", "cdecl")
    gp_get_last_site.argtypes = []
    gp_get_last_site.restype = POINTER(geosite)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 343
if _libs["grass_ogsf.8.4"].has("gp_get_new_site", "cdecl"):
    gp_get_new_site = _libs["grass_ogsf.8.4"].get("gp_get_new_site", "cdecl")
    gp_get_new_site.argtypes = []
    gp_get_new_site.restype = POINTER(geosite)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 344
if _libs["grass_ogsf.8.4"].has("gp_update_drapesurfs", "cdecl"):
    gp_update_drapesurfs = _libs["grass_ogsf.8.4"].get("gp_update_drapesurfs", "cdecl")
    gp_update_drapesurfs.argtypes = []
    gp_update_drapesurfs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 345
if _libs["grass_ogsf.8.4"].has("gp_set_defaults", "cdecl"):
    gp_set_defaults = _libs["grass_ogsf.8.4"].get("gp_set_defaults", "cdecl")
    gp_set_defaults.argtypes = [POINTER(geosite)]
    gp_set_defaults.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 346
for _lib in _libs.values():
    if not _lib.has("print_site_fields", "cdecl"):
        continue
    print_site_fields = _lib.get("print_site_fields", "cdecl")
    print_site_fields.argtypes = [POINTER(geosite)]
    print_site_fields.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 347
if _libs["grass_ogsf.8.4"].has("gp_init_site", "cdecl"):
    gp_init_site = _libs["grass_ogsf.8.4"].get("gp_init_site", "cdecl")
    gp_init_site.argtypes = [POINTER(geosite)]
    gp_init_site.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 348
if _libs["grass_ogsf.8.4"].has("gp_delete_site", "cdecl"):
    gp_delete_site = _libs["grass_ogsf.8.4"].get("gp_delete_site", "cdecl")
    gp_delete_site.argtypes = [c_int]
    gp_delete_site.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 349
if _libs["grass_ogsf.8.4"].has("gp_free_site", "cdecl"):
    gp_free_site = _libs["grass_ogsf.8.4"].get("gp_free_site", "cdecl")
    gp_free_site.argtypes = [POINTER(geosite)]
    gp_free_site.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 350
if _libs["grass_ogsf.8.4"].has("gp_free_sitemem", "cdecl"):
    gp_free_sitemem = _libs["grass_ogsf.8.4"].get("gp_free_sitemem", "cdecl")
    gp_free_sitemem.argtypes = [POINTER(geosite)]
    gp_free_sitemem.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 351
if _libs["grass_ogsf.8.4"].has("gp_set_drapesurfs", "cdecl"):
    gp_set_drapesurfs = _libs["grass_ogsf.8.4"].get("gp_set_drapesurfs", "cdecl")
    gp_set_drapesurfs.argtypes = [POINTER(geosite), POINTER(c_int), c_int]
    gp_set_drapesurfs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 354
if _libs["grass_ogsf.8.4"].has("gs_point_in_region", "cdecl"):
    gs_point_in_region = _libs["grass_ogsf.8.4"].get("gs_point_in_region", "cdecl")
    gs_point_in_region.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float)]
    gs_point_in_region.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 355
if _libs["grass_ogsf.8.4"].has("gpd_obj", "cdecl"):
    gpd_obj = _libs["grass_ogsf.8.4"].get("gpd_obj", "cdecl")
    gpd_obj.argtypes = [POINTER(geosurf), POINTER(gvstyle), Point3]
    gpd_obj.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 356
if _libs["grass_ogsf.8.4"].has("gpd_2dsite", "cdecl"):
    gpd_2dsite = _libs["grass_ogsf.8.4"].get("gpd_2dsite", "cdecl")
    gpd_2dsite.argtypes = [POINTER(geosite), POINTER(geosurf), c_int]
    gpd_2dsite.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 357
if _libs["grass_ogsf.8.4"].has("gpd_3dsite", "cdecl"):
    gpd_3dsite = _libs["grass_ogsf.8.4"].get("gpd_3dsite", "cdecl")
    gpd_3dsite.argtypes = [POINTER(geosite), c_float, c_float, c_int]
    gpd_3dsite.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 360
if _libs["grass_ogsf.8.4"].has("gs_err", "cdecl"):
    gs_err = _libs["grass_ogsf.8.4"].get("gs_err", "cdecl")
    gs_err.argtypes = [String]
    gs_err.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 361
if _libs["grass_ogsf.8.4"].has("gs_init", "cdecl"):
    gs_init = _libs["grass_ogsf.8.4"].get("gs_init", "cdecl")
    gs_init.argtypes = []
    gs_init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 362
if _libs["grass_ogsf.8.4"].has("gs_get_surf", "cdecl"):
    gs_get_surf = _libs["grass_ogsf.8.4"].get("gs_get_surf", "cdecl")
    gs_get_surf.argtypes = [c_int]
    gs_get_surf.restype = POINTER(geosurf)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 363
if _libs["grass_ogsf.8.4"].has("gs_get_prev_surface", "cdecl"):
    gs_get_prev_surface = _libs["grass_ogsf.8.4"].get("gs_get_prev_surface", "cdecl")
    gs_get_prev_surface.argtypes = [c_int]
    gs_get_prev_surface.restype = POINTER(geosurf)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 364
if _libs["grass_ogsf.8.4"].has("gs_getall_surfaces", "cdecl"):
    gs_getall_surfaces = _libs["grass_ogsf.8.4"].get("gs_getall_surfaces", "cdecl")
    gs_getall_surfaces.argtypes = [POINTER(POINTER(geosurf))]
    gs_getall_surfaces.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 365
if _libs["grass_ogsf.8.4"].has("gs_num_surfaces", "cdecl"):
    gs_num_surfaces = _libs["grass_ogsf.8.4"].get("gs_num_surfaces", "cdecl")
    gs_num_surfaces.argtypes = []
    gs_num_surfaces.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 366
if _libs["grass_ogsf.8.4"].has("gs_att_is_set", "cdecl"):
    gs_att_is_set = _libs["grass_ogsf.8.4"].get("gs_att_is_set", "cdecl")
    gs_att_is_set.argtypes = [POINTER(geosurf), c_uint]
    gs_att_is_set.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 367
if _libs["grass_ogsf.8.4"].has("gs_get_last_surface", "cdecl"):
    gs_get_last_surface = _libs["grass_ogsf.8.4"].get("gs_get_last_surface", "cdecl")
    gs_get_last_surface.argtypes = []
    gs_get_last_surface.restype = POINTER(geosurf)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 368
if _libs["grass_ogsf.8.4"].has("gs_get_new_surface", "cdecl"):
    gs_get_new_surface = _libs["grass_ogsf.8.4"].get("gs_get_new_surface", "cdecl")
    gs_get_new_surface.argtypes = []
    gs_get_new_surface.restype = POINTER(geosurf)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 369
if _libs["grass_ogsf.8.4"].has("gs_init_surf", "cdecl"):
    gs_init_surf = _libs["grass_ogsf.8.4"].get("gs_init_surf", "cdecl")
    gs_init_surf.argtypes = [POINTER(geosurf), c_double, c_double, c_int, c_int, c_double, c_double]
    gs_init_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 370
if _libs["grass_ogsf.8.4"].has("gs_init_normbuff", "cdecl"):
    gs_init_normbuff = _libs["grass_ogsf.8.4"].get("gs_init_normbuff", "cdecl")
    gs_init_normbuff.argtypes = [POINTER(geosurf)]
    gs_init_normbuff.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 371
if _libs["grass_ogsf.8.4"].has("print_frto", "cdecl"):
    print_frto = _libs["grass_ogsf.8.4"].get("print_frto", "cdecl")
    print_frto.argtypes = [POINTER(c_float * int(4))]
    print_frto.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 372
if _libs["grass_ogsf.8.4"].has("print_realto", "cdecl"):
    print_realto = _libs["grass_ogsf.8.4"].get("print_realto", "cdecl")
    print_realto.argtypes = [POINTER(c_float)]
    print_realto.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 373
if _libs["grass_ogsf.8.4"].has("print_256lookup", "cdecl"):
    print_256lookup = _libs["grass_ogsf.8.4"].get("print_256lookup", "cdecl")
    print_256lookup.argtypes = [POINTER(c_int)]
    print_256lookup.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 374
if _libs["grass_ogsf.8.4"].has("print_surf_fields", "cdecl"):
    print_surf_fields = _libs["grass_ogsf.8.4"].get("print_surf_fields", "cdecl")
    print_surf_fields.argtypes = [POINTER(geosurf)]
    print_surf_fields.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 375
if _libs["grass_ogsf.8.4"].has("print_view_fields", "cdecl"):
    print_view_fields = _libs["grass_ogsf.8.4"].get("print_view_fields", "cdecl")
    print_view_fields.argtypes = [POINTER(geoview)]
    print_view_fields.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 376
if _libs["grass_ogsf.8.4"].has("gs_set_defaults", "cdecl"):
    gs_set_defaults = _libs["grass_ogsf.8.4"].get("gs_set_defaults", "cdecl")
    gs_set_defaults.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float)]
    gs_set_defaults.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 377
if _libs["grass_ogsf.8.4"].has("gs_delete_surf", "cdecl"):
    gs_delete_surf = _libs["grass_ogsf.8.4"].get("gs_delete_surf", "cdecl")
    gs_delete_surf.argtypes = [c_int]
    gs_delete_surf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 378
if _libs["grass_ogsf.8.4"].has("gs_free_surf", "cdecl"):
    gs_free_surf = _libs["grass_ogsf.8.4"].get("gs_free_surf", "cdecl")
    gs_free_surf.argtypes = [POINTER(geosurf)]
    gs_free_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 379
if _libs["grass_ogsf.8.4"].has("gs_free_unshared_buffs", "cdecl"):
    gs_free_unshared_buffs = _libs["grass_ogsf.8.4"].get("gs_free_unshared_buffs", "cdecl")
    gs_free_unshared_buffs.argtypes = [POINTER(geosurf)]
    gs_free_unshared_buffs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 380
if _libs["grass_ogsf.8.4"].has("gs_num_datah_reused", "cdecl"):
    gs_num_datah_reused = _libs["grass_ogsf.8.4"].get("gs_num_datah_reused", "cdecl")
    gs_num_datah_reused.argtypes = [c_int]
    gs_num_datah_reused.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 381
if _libs["grass_ogsf.8.4"].has("gs_get_att_type", "cdecl"):
    gs_get_att_type = _libs["grass_ogsf.8.4"].get("gs_get_att_type", "cdecl")
    gs_get_att_type.argtypes = [POINTER(geosurf), c_int]
    gs_get_att_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 382
if _libs["grass_ogsf.8.4"].has("gs_get_att_src", "cdecl"):
    gs_get_att_src = _libs["grass_ogsf.8.4"].get("gs_get_att_src", "cdecl")
    gs_get_att_src.argtypes = [POINTER(geosurf), c_int]
    gs_get_att_src.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 383
if _libs["grass_ogsf.8.4"].has("gs_get_att_typbuff", "cdecl"):
    gs_get_att_typbuff = _libs["grass_ogsf.8.4"].get("gs_get_att_typbuff", "cdecl")
    gs_get_att_typbuff.argtypes = [POINTER(geosurf), c_int, c_int]
    gs_get_att_typbuff.restype = POINTER(typbuff)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 384
if _libs["grass_ogsf.8.4"].has("gs_malloc_att_buff", "cdecl"):
    gs_malloc_att_buff = _libs["grass_ogsf.8.4"].get("gs_malloc_att_buff", "cdecl")
    gs_malloc_att_buff.argtypes = [POINTER(geosurf), c_int, c_int]
    gs_malloc_att_buff.restype = c_size_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 385
if _libs["grass_ogsf.8.4"].has("gs_malloc_lookup", "cdecl"):
    gs_malloc_lookup = _libs["grass_ogsf.8.4"].get("gs_malloc_lookup", "cdecl")
    gs_malloc_lookup.argtypes = [POINTER(geosurf), c_int]
    gs_malloc_lookup.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 386
if _libs["grass_ogsf.8.4"].has("gs_set_att_type", "cdecl"):
    gs_set_att_type = _libs["grass_ogsf.8.4"].get("gs_set_att_type", "cdecl")
    gs_set_att_type.argtypes = [POINTER(geosurf), c_int, c_int]
    gs_set_att_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 387
if _libs["grass_ogsf.8.4"].has("gs_set_att_src", "cdecl"):
    gs_set_att_src = _libs["grass_ogsf.8.4"].get("gs_set_att_src", "cdecl")
    gs_set_att_src.argtypes = [POINTER(geosurf), c_int, c_int]
    gs_set_att_src.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 388
if _libs["grass_ogsf.8.4"].has("gs_set_att_const", "cdecl"):
    gs_set_att_const = _libs["grass_ogsf.8.4"].get("gs_set_att_const", "cdecl")
    gs_set_att_const.argtypes = [POINTER(geosurf), c_int, c_float]
    gs_set_att_const.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 389
if _libs["grass_ogsf.8.4"].has("gs_set_maskmode", "cdecl"):
    gs_set_maskmode = _libs["grass_ogsf.8.4"].get("gs_set_maskmode", "cdecl")
    gs_set_maskmode.argtypes = [c_int]
    gs_set_maskmode.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 390
if _libs["grass_ogsf.8.4"].has("gs_mask_defined", "cdecl"):
    gs_mask_defined = _libs["grass_ogsf.8.4"].get("gs_mask_defined", "cdecl")
    gs_mask_defined.argtypes = [POINTER(geosurf)]
    gs_mask_defined.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 391
if _libs["grass_ogsf.8.4"].has("gs_masked", "cdecl"):
    gs_masked = _libs["grass_ogsf.8.4"].get("gs_masked", "cdecl")
    gs_masked.argtypes = [POINTER(typbuff), c_int, c_int, c_int]
    gs_masked.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 392
if _libs["grass_ogsf.8.4"].has("gs_mapcolor", "cdecl"):
    gs_mapcolor = _libs["grass_ogsf.8.4"].get("gs_mapcolor", "cdecl")
    gs_mapcolor.argtypes = [POINTER(typbuff), POINTER(gsurf_att), c_int]
    gs_mapcolor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 393
if _libs["grass_ogsf.8.4"].has("gs_get_zextents", "cdecl"):
    gs_get_zextents = _libs["grass_ogsf.8.4"].get("gs_get_zextents", "cdecl")
    gs_get_zextents.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    gs_get_zextents.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 394
if _libs["grass_ogsf.8.4"].has("gs_get_xextents", "cdecl"):
    gs_get_xextents = _libs["grass_ogsf.8.4"].get("gs_get_xextents", "cdecl")
    gs_get_xextents.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float)]
    gs_get_xextents.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 395
if _libs["grass_ogsf.8.4"].has("gs_get_yextents", "cdecl"):
    gs_get_yextents = _libs["grass_ogsf.8.4"].get("gs_get_yextents", "cdecl")
    gs_get_yextents.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float)]
    gs_get_yextents.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 396
if _libs["grass_ogsf.8.4"].has("gs_get_zrange0", "cdecl"):
    gs_get_zrange0 = _libs["grass_ogsf.8.4"].get("gs_get_zrange0", "cdecl")
    gs_get_zrange0.argtypes = [POINTER(c_float), POINTER(c_float)]
    gs_get_zrange0.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 397
if _libs["grass_ogsf.8.4"].has("gs_get_zrange", "cdecl"):
    gs_get_zrange = _libs["grass_ogsf.8.4"].get("gs_get_zrange", "cdecl")
    gs_get_zrange.argtypes = [POINTER(c_float), POINTER(c_float)]
    gs_get_zrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 398
if _libs["grass_ogsf.8.4"].has("gs_get_xrange", "cdecl"):
    gs_get_xrange = _libs["grass_ogsf.8.4"].get("gs_get_xrange", "cdecl")
    gs_get_xrange.argtypes = [POINTER(c_float), POINTER(c_float)]
    gs_get_xrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 399
if _libs["grass_ogsf.8.4"].has("gs_get_yrange", "cdecl"):
    gs_get_yrange = _libs["grass_ogsf.8.4"].get("gs_get_yrange", "cdecl")
    gs_get_yrange.argtypes = [POINTER(c_float), POINTER(c_float)]
    gs_get_yrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 400
if _libs["grass_ogsf.8.4"].has("gs_get_data_avg_zmax", "cdecl"):
    gs_get_data_avg_zmax = _libs["grass_ogsf.8.4"].get("gs_get_data_avg_zmax", "cdecl")
    gs_get_data_avg_zmax.argtypes = [POINTER(c_float)]
    gs_get_data_avg_zmax.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 401
if _libs["grass_ogsf.8.4"].has("gs_get_datacenter", "cdecl"):
    gs_get_datacenter = _libs["grass_ogsf.8.4"].get("gs_get_datacenter", "cdecl")
    gs_get_datacenter.argtypes = [POINTER(c_float)]
    gs_get_datacenter.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 402
if _libs["grass_ogsf.8.4"].has("gs_setall_norm_needupdate", "cdecl"):
    gs_setall_norm_needupdate = _libs["grass_ogsf.8.4"].get("gs_setall_norm_needupdate", "cdecl")
    gs_setall_norm_needupdate.argtypes = []
    gs_setall_norm_needupdate.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 403
if _libs["grass_ogsf.8.4"].has("gs_point_is_masked", "cdecl"):
    gs_point_is_masked = _libs["grass_ogsf.8.4"].get("gs_point_is_masked", "cdecl")
    gs_point_is_masked.argtypes = [POINTER(geosurf), POINTER(c_float)]
    gs_point_is_masked.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 404
if _libs["grass_ogsf.8.4"].has("gs_distance_onsurf", "cdecl"):
    gs_distance_onsurf = _libs["grass_ogsf.8.4"].get("gs_distance_onsurf", "cdecl")
    gs_distance_onsurf.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_float), c_int]
    gs_distance_onsurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 407
if _libs["grass_ogsf.8.4"].has("gsbm_make_mask", "cdecl"):
    gsbm_make_mask = _libs["grass_ogsf.8.4"].get("gsbm_make_mask", "cdecl")
    gsbm_make_mask.argtypes = [POINTER(typbuff), c_float, c_int, c_int]
    gsbm_make_mask.restype = POINTER(struct_BM)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 408
if _libs["grass_ogsf.8.4"].has("gsbm_zero_mask", "cdecl"):
    gsbm_zero_mask = _libs["grass_ogsf.8.4"].get("gsbm_zero_mask", "cdecl")
    gsbm_zero_mask.argtypes = [POINTER(struct_BM)]
    gsbm_zero_mask.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 409
if _libs["grass_ogsf.8.4"].has("gsbm_or_masks", "cdecl"):
    gsbm_or_masks = _libs["grass_ogsf.8.4"].get("gsbm_or_masks", "cdecl")
    gsbm_or_masks.argtypes = [POINTER(struct_BM), POINTER(struct_BM)]
    gsbm_or_masks.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 410
if _libs["grass_ogsf.8.4"].has("gsbm_ornot_masks", "cdecl"):
    gsbm_ornot_masks = _libs["grass_ogsf.8.4"].get("gsbm_ornot_masks", "cdecl")
    gsbm_ornot_masks.argtypes = [POINTER(struct_BM), POINTER(struct_BM)]
    gsbm_ornot_masks.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 411
if _libs["grass_ogsf.8.4"].has("gsbm_and_masks", "cdecl"):
    gsbm_and_masks = _libs["grass_ogsf.8.4"].get("gsbm_and_masks", "cdecl")
    gsbm_and_masks.argtypes = [POINTER(struct_BM), POINTER(struct_BM)]
    gsbm_and_masks.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 412
if _libs["grass_ogsf.8.4"].has("gsbm_xor_masks", "cdecl"):
    gsbm_xor_masks = _libs["grass_ogsf.8.4"].get("gsbm_xor_masks", "cdecl")
    gsbm_xor_masks.argtypes = [POINTER(struct_BM), POINTER(struct_BM)]
    gsbm_xor_masks.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 413
if _libs["grass_ogsf.8.4"].has("gs_update_curmask", "cdecl"):
    gs_update_curmask = _libs["grass_ogsf.8.4"].get("gs_update_curmask", "cdecl")
    gs_update_curmask.argtypes = [POINTER(geosurf)]
    gs_update_curmask.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 414
if _libs["grass_ogsf.8.4"].has("print_bm", "cdecl"):
    print_bm = _libs["grass_ogsf.8.4"].get("print_bm", "cdecl")
    print_bm.argtypes = [POINTER(struct_BM)]
    print_bm.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 417
if _libs["grass_ogsf.8.4"].has("init_vars", "cdecl"):
    init_vars = _libs["grass_ogsf.8.4"].get("init_vars", "cdecl")
    init_vars.argtypes = [POINTER(geosurf)]
    init_vars.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 418
if _libs["grass_ogsf.8.4"].has("gs_calc_normals", "cdecl"):
    gs_calc_normals = _libs["grass_ogsf.8.4"].get("gs_calc_normals", "cdecl")
    gs_calc_normals.argtypes = [POINTER(geosurf)]
    gs_calc_normals.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 419
if _libs["grass_ogsf.8.4"].has("calc_norm", "cdecl"):
    calc_norm = _libs["grass_ogsf.8.4"].get("calc_norm", "cdecl")
    calc_norm.argtypes = [POINTER(geosurf), c_int, c_int, c_uint]
    calc_norm.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 422
if _libs["grass_ogsf.8.4"].has("gs_los_intersect1", "cdecl"):
    gs_los_intersect1 = _libs["grass_ogsf.8.4"].get("gs_los_intersect1", "cdecl")
    gs_los_intersect1.argtypes = [c_int, POINTER(c_float * int(3)), POINTER(c_float)]
    gs_los_intersect1.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 423
if _libs["grass_ogsf.8.4"].has("gs_los_intersect", "cdecl"):
    gs_los_intersect = _libs["grass_ogsf.8.4"].get("gs_los_intersect", "cdecl")
    gs_los_intersect.argtypes = [c_int, POINTER(POINTER(c_float)), POINTER(c_float)]
    gs_los_intersect.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 424
if _libs["grass_ogsf.8.4"].has("RayCvxPolyhedronInt", "cdecl"):
    RayCvxPolyhedronInt = _libs["grass_ogsf.8.4"].get("RayCvxPolyhedronInt", "cdecl")
    RayCvxPolyhedronInt.argtypes = [Point3, Point3, c_double, POINTER(Point4), c_int, POINTER(c_double), POINTER(c_int)]
    RayCvxPolyhedronInt.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 425
if _libs["grass_ogsf.8.4"].has("gs_get_databounds_planes", "cdecl"):
    gs_get_databounds_planes = _libs["grass_ogsf.8.4"].get("gs_get_databounds_planes", "cdecl")
    gs_get_databounds_planes.argtypes = [POINTER(Point4)]
    gs_get_databounds_planes.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 426
if _libs["grass_ogsf.8.4"].has("gs_setlos_enterdata", "cdecl"):
    gs_setlos_enterdata = _libs["grass_ogsf.8.4"].get("gs_setlos_enterdata", "cdecl")
    gs_setlos_enterdata.argtypes = [POINTER(Point3)]
    gs_setlos_enterdata.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 429
if _libs["grass_ogsf.8.4"].has("gsd_def_cplane", "cdecl"):
    gsd_def_cplane = _libs["grass_ogsf.8.4"].get("gsd_def_cplane", "cdecl")
    gsd_def_cplane.argtypes = [c_int, POINTER(c_float), POINTER(c_float)]
    gsd_def_cplane.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 430
if _libs["grass_ogsf.8.4"].has("gsd_update_cplanes", "cdecl"):
    gsd_update_cplanes = _libs["grass_ogsf.8.4"].get("gsd_update_cplanes", "cdecl")
    gsd_update_cplanes.argtypes = []
    gsd_update_cplanes.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 431
if _libs["grass_ogsf.8.4"].has("gsd_cplane_on", "cdecl"):
    gsd_cplane_on = _libs["grass_ogsf.8.4"].get("gsd_cplane_on", "cdecl")
    gsd_cplane_on.argtypes = [c_int]
    gsd_cplane_on.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 432
if _libs["grass_ogsf.8.4"].has("gsd_cplane_off", "cdecl"):
    gsd_cplane_off = _libs["grass_ogsf.8.4"].get("gsd_cplane_off", "cdecl")
    gsd_cplane_off.argtypes = [c_int]
    gsd_cplane_off.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 433
if _libs["grass_ogsf.8.4"].has("gsd_get_cplanes_state", "cdecl"):
    gsd_get_cplanes_state = _libs["grass_ogsf.8.4"].get("gsd_get_cplanes_state", "cdecl")
    gsd_get_cplanes_state.argtypes = [POINTER(c_int)]
    gsd_get_cplanes_state.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 434
if _libs["grass_ogsf.8.4"].has("gsd_get_cplanes", "cdecl"):
    gsd_get_cplanes = _libs["grass_ogsf.8.4"].get("gsd_get_cplanes", "cdecl")
    gsd_get_cplanes.argtypes = [POINTER(Point4)]
    gsd_get_cplanes.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 435
if _libs["grass_ogsf.8.4"].has("gsd_update_cpnorm", "cdecl"):
    gsd_update_cpnorm = _libs["grass_ogsf.8.4"].get("gsd_update_cpnorm", "cdecl")
    gsd_update_cpnorm.argtypes = [c_int]
    gsd_update_cpnorm.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 436
if _libs["grass_ogsf.8.4"].has("gsd_cplane_setrot", "cdecl"):
    gsd_cplane_setrot = _libs["grass_ogsf.8.4"].get("gsd_cplane_setrot", "cdecl")
    gsd_cplane_setrot.argtypes = [c_int, c_float, c_float, c_float]
    gsd_cplane_setrot.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 437
if _libs["grass_ogsf.8.4"].has("gsd_cplane_settrans", "cdecl"):
    gsd_cplane_settrans = _libs["grass_ogsf.8.4"].get("gsd_cplane_settrans", "cdecl")
    gsd_cplane_settrans.argtypes = [c_int, c_float, c_float, c_float]
    gsd_cplane_settrans.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 438
if _libs["grass_ogsf.8.4"].has("gsd_draw_cplane_fence", "cdecl"):
    gsd_draw_cplane_fence = _libs["grass_ogsf.8.4"].get("gsd_draw_cplane_fence", "cdecl")
    gsd_draw_cplane_fence.argtypes = [POINTER(geosurf), POINTER(geosurf), c_int]
    gsd_draw_cplane_fence.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 439
if _libs["grass_ogsf.8.4"].has("gsd_draw_cplane", "cdecl"):
    gsd_draw_cplane = _libs["grass_ogsf.8.4"].get("gsd_draw_cplane", "cdecl")
    gsd_draw_cplane.argtypes = [c_int]
    gsd_draw_cplane.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 442
for _lib in _libs.values():
    if not _lib.has("gsd_set_font", "cdecl"):
        continue
    gsd_set_font = _lib.get("gsd_set_font", "cdecl")
    gsd_set_font.argtypes = [String]
    gsd_set_font.restype = GLuint
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 443
if _libs["grass_ogsf.8.4"].has("gsd_get_txtwidth", "cdecl"):
    gsd_get_txtwidth = _libs["grass_ogsf.8.4"].get("gsd_get_txtwidth", "cdecl")
    gsd_get_txtwidth.argtypes = [String, c_int]
    gsd_get_txtwidth.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 444
if _libs["grass_ogsf.8.4"].has("gsd_get_txtheight", "cdecl"):
    gsd_get_txtheight = _libs["grass_ogsf.8.4"].get("gsd_get_txtheight", "cdecl")
    gsd_get_txtheight.argtypes = [c_int]
    gsd_get_txtheight.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 445
if _libs["grass_ogsf.8.4"].has("do_label_display", "cdecl"):
    do_label_display = _libs["grass_ogsf.8.4"].get("do_label_display", "cdecl")
    do_label_display.argtypes = [GLuint, POINTER(c_float), String]
    do_label_display.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 446
if _libs["grass_ogsf.8.4"].has("get_txtdescender", "cdecl"):
    get_txtdescender = _libs["grass_ogsf.8.4"].get("get_txtdescender", "cdecl")
    get_txtdescender.argtypes = []
    get_txtdescender.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 447
if _libs["grass_ogsf.8.4"].has("get_txtxoffset", "cdecl"):
    get_txtxoffset = _libs["grass_ogsf.8.4"].get("get_txtxoffset", "cdecl")
    get_txtxoffset.argtypes = []
    get_txtxoffset.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 450
if _libs["grass_ogsf.8.4"].has("GS_write_ppm", "cdecl"):
    GS_write_ppm = _libs["grass_ogsf.8.4"].get("GS_write_ppm", "cdecl")
    GS_write_ppm.argtypes = [String]
    GS_write_ppm.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 453
if _libs["grass_ogsf.8.4"].has("GS_write_tif", "cdecl"):
    GS_write_tif = _libs["grass_ogsf.8.4"].get("GS_write_tif", "cdecl")
    GS_write_tif.argtypes = [String]
    GS_write_tif.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 456
if _libs["grass_ogsf.8.4"].has("gs_put_label", "cdecl"):
    gs_put_label = _libs["grass_ogsf.8.4"].get("gs_put_label", "cdecl")
    gs_put_label.argtypes = [String, GLuint, c_int, c_ulong, POINTER(c_int)]
    gs_put_label.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 457
if _libs["grass_ogsf.8.4"].has("gsd_remove_curr", "cdecl"):
    gsd_remove_curr = _libs["grass_ogsf.8.4"].get("gsd_remove_curr", "cdecl")
    gsd_remove_curr.argtypes = []
    gsd_remove_curr.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 458
if _libs["grass_ogsf.8.4"].has("gsd_remove_all", "cdecl"):
    gsd_remove_all = _libs["grass_ogsf.8.4"].get("gsd_remove_all", "cdecl")
    gsd_remove_all.argtypes = []
    gsd_remove_all.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 459
if _libs["grass_ogsf.8.4"].has("gsd_call_label", "cdecl"):
    gsd_call_label = _libs["grass_ogsf.8.4"].get("gsd_call_label", "cdecl")
    gsd_call_label.argtypes = []
    gsd_call_label.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 462
if _libs["grass_ogsf.8.4"].has("gsd_box", "cdecl"):
    gsd_box = _libs["grass_ogsf.8.4"].get("gsd_box", "cdecl")
    gsd_box.argtypes = [POINTER(c_float), c_int, POINTER(c_float)]
    gsd_box.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 463
if _libs["grass_ogsf.8.4"].has("gsd_plus", "cdecl"):
    gsd_plus = _libs["grass_ogsf.8.4"].get("gsd_plus", "cdecl")
    gsd_plus.argtypes = [POINTER(c_float), c_int, c_float]
    gsd_plus.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 464
if _libs["grass_ogsf.8.4"].has("gsd_line_onsurf", "cdecl"):
    gsd_line_onsurf = _libs["grass_ogsf.8.4"].get("gsd_line_onsurf", "cdecl")
    gsd_line_onsurf.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float)]
    gsd_line_onsurf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 465
if _libs["grass_ogsf.8.4"].has("gsd_nline_onsurf", "cdecl"):
    gsd_nline_onsurf = _libs["grass_ogsf.8.4"].get("gsd_nline_onsurf", "cdecl")
    gsd_nline_onsurf.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_float), c_int]
    gsd_nline_onsurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 466
if _libs["grass_ogsf.8.4"].has("gsd_x", "cdecl"):
    gsd_x = _libs["grass_ogsf.8.4"].get("gsd_x", "cdecl")
    gsd_x.argtypes = [POINTER(geosurf), POINTER(c_float), c_int, c_float]
    gsd_x.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 467
if _libs["grass_ogsf.8.4"].has("gsd_diamond", "cdecl"):
    gsd_diamond = _libs["grass_ogsf.8.4"].get("gsd_diamond", "cdecl")
    gsd_diamond.argtypes = [POINTER(c_float), c_ulong, c_float]
    gsd_diamond.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 468
if _libs["grass_ogsf.8.4"].has("gsd_diamond_lines", "cdecl"):
    gsd_diamond_lines = _libs["grass_ogsf.8.4"].get("gsd_diamond_lines", "cdecl")
    gsd_diamond_lines.argtypes = []
    gsd_diamond_lines.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 469
if _libs["grass_ogsf.8.4"].has("gsd_cube", "cdecl"):
    gsd_cube = _libs["grass_ogsf.8.4"].get("gsd_cube", "cdecl")
    gsd_cube.argtypes = [POINTER(c_float), c_ulong, c_float]
    gsd_cube.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 470
if _libs["grass_ogsf.8.4"].has("gsd_draw_box", "cdecl"):
    gsd_draw_box = _libs["grass_ogsf.8.4"].get("gsd_draw_box", "cdecl")
    gsd_draw_box.argtypes = [POINTER(c_float), c_ulong, c_float]
    gsd_draw_box.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 471
if _libs["grass_ogsf.8.4"].has("gsd_drawsphere", "cdecl"):
    gsd_drawsphere = _libs["grass_ogsf.8.4"].get("gsd_drawsphere", "cdecl")
    gsd_drawsphere.argtypes = [POINTER(c_float), c_ulong, c_float]
    gsd_drawsphere.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 472
if _libs["grass_ogsf.8.4"].has("gsd_draw_asterisk", "cdecl"):
    gsd_draw_asterisk = _libs["grass_ogsf.8.4"].get("gsd_draw_asterisk", "cdecl")
    gsd_draw_asterisk.argtypes = [POINTER(c_float), c_ulong, c_float]
    gsd_draw_asterisk.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 473
if _libs["grass_ogsf.8.4"].has("gsd_draw_gyro", "cdecl"):
    gsd_draw_gyro = _libs["grass_ogsf.8.4"].get("gsd_draw_gyro", "cdecl")
    gsd_draw_gyro.argtypes = [POINTER(c_float), c_ulong, c_float]
    gsd_draw_gyro.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 474
if _libs["grass_ogsf.8.4"].has("gsd_3dcursor", "cdecl"):
    gsd_3dcursor = _libs["grass_ogsf.8.4"].get("gsd_3dcursor", "cdecl")
    gsd_3dcursor.argtypes = [POINTER(c_float)]
    gsd_3dcursor.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 475
if _libs["grass_ogsf.8.4"].has("dir_to_slope_aspect", "cdecl"):
    dir_to_slope_aspect = _libs["grass_ogsf.8.4"].get("dir_to_slope_aspect", "cdecl")
    dir_to_slope_aspect.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float), c_int]
    dir_to_slope_aspect.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 476
if _libs["grass_ogsf.8.4"].has("gsd_north_arrow", "cdecl"):
    gsd_north_arrow = _libs["grass_ogsf.8.4"].get("gsd_north_arrow", "cdecl")
    gsd_north_arrow.argtypes = [POINTER(c_float), c_float, GLuint, c_ulong, c_ulong]
    gsd_north_arrow.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 477
if _libs["grass_ogsf.8.4"].has("gsd_arrow", "cdecl"):
    gsd_arrow = _libs["grass_ogsf.8.4"].get("gsd_arrow", "cdecl")
    gsd_arrow.argtypes = [POINTER(c_float), c_ulong, c_float, POINTER(c_float), c_float, POINTER(geosurf)]
    gsd_arrow.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 478
if _libs["grass_ogsf.8.4"].has("gsd_arrow_onsurf", "cdecl"):
    gsd_arrow_onsurf = _libs["grass_ogsf.8.4"].get("gsd_arrow_onsurf", "cdecl")
    gsd_arrow_onsurf.argtypes = [POINTER(c_float), POINTER(c_float), c_ulong, c_int, POINTER(geosurf)]
    gsd_arrow_onsurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 479
if _libs["grass_ogsf.8.4"].has("gsd_3darrow", "cdecl"):
    gsd_3darrow = _libs["grass_ogsf.8.4"].get("gsd_3darrow", "cdecl")
    gsd_3darrow.argtypes = [POINTER(c_float), c_ulong, c_float, c_float, POINTER(c_float), c_float]
    gsd_3darrow.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 480
if _libs["grass_ogsf.8.4"].has("gsd_scalebar", "cdecl"):
    gsd_scalebar = _libs["grass_ogsf.8.4"].get("gsd_scalebar", "cdecl")
    gsd_scalebar.argtypes = [POINTER(c_float), c_float, GLuint, c_ulong, c_ulong]
    gsd_scalebar.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 481
if _libs["grass_ogsf.8.4"].has("gsd_scalebar_v2", "cdecl"):
    gsd_scalebar_v2 = _libs["grass_ogsf.8.4"].get("gsd_scalebar_v2", "cdecl")
    gsd_scalebar_v2.argtypes = [POINTER(c_float), c_float, GLuint, c_ulong, c_ulong]
    gsd_scalebar_v2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 482
if _libs["grass_ogsf.8.4"].has("primitive_cone", "cdecl"):
    primitive_cone = _libs["grass_ogsf.8.4"].get("primitive_cone", "cdecl")
    primitive_cone.argtypes = [c_ulong]
    primitive_cone.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 483
if _libs["grass_ogsf.8.4"].has("primitive_cylinder", "cdecl"):
    primitive_cylinder = _libs["grass_ogsf.8.4"].get("primitive_cylinder", "cdecl")
    primitive_cylinder.argtypes = [c_ulong, c_int]
    primitive_cylinder.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 486
if _libs["grass_ogsf.8.4"].has("gsd_flush", "cdecl"):
    gsd_flush = _libs["grass_ogsf.8.4"].get("gsd_flush", "cdecl")
    gsd_flush.argtypes = []
    gsd_flush.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 487
if _libs["grass_ogsf.8.4"].has("gsd_colormode", "cdecl"):
    gsd_colormode = _libs["grass_ogsf.8.4"].get("gsd_colormode", "cdecl")
    gsd_colormode.argtypes = [c_int]
    gsd_colormode.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 488
if _libs["grass_ogsf.8.4"].has("show_colormode", "cdecl"):
    show_colormode = _libs["grass_ogsf.8.4"].get("show_colormode", "cdecl")
    show_colormode.argtypes = []
    show_colormode.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 489
if _libs["grass_ogsf.8.4"].has("gsd_circ", "cdecl"):
    gsd_circ = _libs["grass_ogsf.8.4"].get("gsd_circ", "cdecl")
    gsd_circ.argtypes = [c_float, c_float, c_float]
    gsd_circ.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 490
if _libs["grass_ogsf.8.4"].has("gsd_disc", "cdecl"):
    gsd_disc = _libs["grass_ogsf.8.4"].get("gsd_disc", "cdecl")
    gsd_disc.argtypes = [c_float, c_float, c_float, c_float]
    gsd_disc.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 491
if _libs["grass_ogsf.8.4"].has("gsd_sphere", "cdecl"):
    gsd_sphere = _libs["grass_ogsf.8.4"].get("gsd_sphere", "cdecl")
    gsd_sphere.argtypes = [POINTER(c_float), c_float]
    gsd_sphere.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 492
if _libs["grass_ogsf.8.4"].has("gsd_zwritemask", "cdecl"):
    gsd_zwritemask = _libs["grass_ogsf.8.4"].get("gsd_zwritemask", "cdecl")
    gsd_zwritemask.argtypes = [c_ulong]
    gsd_zwritemask.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 493
if _libs["grass_ogsf.8.4"].has("gsd_backface", "cdecl"):
    gsd_backface = _libs["grass_ogsf.8.4"].get("gsd_backface", "cdecl")
    gsd_backface.argtypes = [c_int]
    gsd_backface.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 494
if _libs["grass_ogsf.8.4"].has("gsd_linewidth", "cdecl"):
    gsd_linewidth = _libs["grass_ogsf.8.4"].get("gsd_linewidth", "cdecl")
    gsd_linewidth.argtypes = [c_short]
    gsd_linewidth.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 495
if _libs["grass_ogsf.8.4"].has("gsd_bgnqstrip", "cdecl"):
    gsd_bgnqstrip = _libs["grass_ogsf.8.4"].get("gsd_bgnqstrip", "cdecl")
    gsd_bgnqstrip.argtypes = []
    gsd_bgnqstrip.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 496
if _libs["grass_ogsf.8.4"].has("gsd_endqstrip", "cdecl"):
    gsd_endqstrip = _libs["grass_ogsf.8.4"].get("gsd_endqstrip", "cdecl")
    gsd_endqstrip.argtypes = []
    gsd_endqstrip.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 497
if _libs["grass_ogsf.8.4"].has("gsd_bgntmesh", "cdecl"):
    gsd_bgntmesh = _libs["grass_ogsf.8.4"].get("gsd_bgntmesh", "cdecl")
    gsd_bgntmesh.argtypes = []
    gsd_bgntmesh.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 498
if _libs["grass_ogsf.8.4"].has("gsd_endtmesh", "cdecl"):
    gsd_endtmesh = _libs["grass_ogsf.8.4"].get("gsd_endtmesh", "cdecl")
    gsd_endtmesh.argtypes = []
    gsd_endtmesh.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 499
if _libs["grass_ogsf.8.4"].has("gsd_bgntstrip", "cdecl"):
    gsd_bgntstrip = _libs["grass_ogsf.8.4"].get("gsd_bgntstrip", "cdecl")
    gsd_bgntstrip.argtypes = []
    gsd_bgntstrip.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 500
if _libs["grass_ogsf.8.4"].has("gsd_endtstrip", "cdecl"):
    gsd_endtstrip = _libs["grass_ogsf.8.4"].get("gsd_endtstrip", "cdecl")
    gsd_endtstrip.argtypes = []
    gsd_endtstrip.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 501
if _libs["grass_ogsf.8.4"].has("gsd_bgntfan", "cdecl"):
    gsd_bgntfan = _libs["grass_ogsf.8.4"].get("gsd_bgntfan", "cdecl")
    gsd_bgntfan.argtypes = []
    gsd_bgntfan.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 502
if _libs["grass_ogsf.8.4"].has("gsd_endtfan", "cdecl"):
    gsd_endtfan = _libs["grass_ogsf.8.4"].get("gsd_endtfan", "cdecl")
    gsd_endtfan.argtypes = []
    gsd_endtfan.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 503
if _libs["grass_ogsf.8.4"].has("gsd_swaptmesh", "cdecl"):
    gsd_swaptmesh = _libs["grass_ogsf.8.4"].get("gsd_swaptmesh", "cdecl")
    gsd_swaptmesh.argtypes = []
    gsd_swaptmesh.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 504
if _libs["grass_ogsf.8.4"].has("gsd_bgnpolygon", "cdecl"):
    gsd_bgnpolygon = _libs["grass_ogsf.8.4"].get("gsd_bgnpolygon", "cdecl")
    gsd_bgnpolygon.argtypes = []
    gsd_bgnpolygon.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 505
if _libs["grass_ogsf.8.4"].has("gsd_endpolygon", "cdecl"):
    gsd_endpolygon = _libs["grass_ogsf.8.4"].get("gsd_endpolygon", "cdecl")
    gsd_endpolygon.argtypes = []
    gsd_endpolygon.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 506
if _libs["grass_ogsf.8.4"].has("gsd_bgnline", "cdecl"):
    gsd_bgnline = _libs["grass_ogsf.8.4"].get("gsd_bgnline", "cdecl")
    gsd_bgnline.argtypes = []
    gsd_bgnline.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 507
if _libs["grass_ogsf.8.4"].has("gsd_endline", "cdecl"):
    gsd_endline = _libs["grass_ogsf.8.4"].get("gsd_endline", "cdecl")
    gsd_endline.argtypes = []
    gsd_endline.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 508
if _libs["grass_ogsf.8.4"].has("gsd_shademodel", "cdecl"):
    gsd_shademodel = _libs["grass_ogsf.8.4"].get("gsd_shademodel", "cdecl")
    gsd_shademodel.argtypes = [c_int]
    gsd_shademodel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 509
if _libs["grass_ogsf.8.4"].has("gsd_getshademodel", "cdecl"):
    gsd_getshademodel = _libs["grass_ogsf.8.4"].get("gsd_getshademodel", "cdecl")
    gsd_getshademodel.argtypes = []
    gsd_getshademodel.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 510
if _libs["grass_ogsf.8.4"].has("gsd_bothbuffers", "cdecl"):
    gsd_bothbuffers = _libs["grass_ogsf.8.4"].get("gsd_bothbuffers", "cdecl")
    gsd_bothbuffers.argtypes = []
    gsd_bothbuffers.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 511
if _libs["grass_ogsf.8.4"].has("gsd_frontbuffer", "cdecl"):
    gsd_frontbuffer = _libs["grass_ogsf.8.4"].get("gsd_frontbuffer", "cdecl")
    gsd_frontbuffer.argtypes = []
    gsd_frontbuffer.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 512
if _libs["grass_ogsf.8.4"].has("gsd_backbuffer", "cdecl"):
    gsd_backbuffer = _libs["grass_ogsf.8.4"].get("gsd_backbuffer", "cdecl")
    gsd_backbuffer.argtypes = []
    gsd_backbuffer.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 513
if _libs["grass_ogsf.8.4"].has("gsd_swapbuffers", "cdecl"):
    gsd_swapbuffers = _libs["grass_ogsf.8.4"].get("gsd_swapbuffers", "cdecl")
    gsd_swapbuffers.argtypes = []
    gsd_swapbuffers.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 514
if _libs["grass_ogsf.8.4"].has("gsd_popmatrix", "cdecl"):
    gsd_popmatrix = _libs["grass_ogsf.8.4"].get("gsd_popmatrix", "cdecl")
    gsd_popmatrix.argtypes = []
    gsd_popmatrix.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 515
if _libs["grass_ogsf.8.4"].has("gsd_pushmatrix", "cdecl"):
    gsd_pushmatrix = _libs["grass_ogsf.8.4"].get("gsd_pushmatrix", "cdecl")
    gsd_pushmatrix.argtypes = []
    gsd_pushmatrix.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 516
if _libs["grass_ogsf.8.4"].has("gsd_scale", "cdecl"):
    gsd_scale = _libs["grass_ogsf.8.4"].get("gsd_scale", "cdecl")
    gsd_scale.argtypes = [c_float, c_float, c_float]
    gsd_scale.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 517
if _libs["grass_ogsf.8.4"].has("gsd_translate", "cdecl"):
    gsd_translate = _libs["grass_ogsf.8.4"].get("gsd_translate", "cdecl")
    gsd_translate.argtypes = [c_float, c_float, c_float]
    gsd_translate.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 518
if _libs["grass_ogsf.8.4"].has("gsd_rot", "cdecl"):
    gsd_rot = _libs["grass_ogsf.8.4"].get("gsd_rot", "cdecl")
    gsd_rot.argtypes = [c_float, c_char]
    gsd_rot.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 519
for _lib in _libs.values():
    if not _lib.has("gsd_checkwindow", "cdecl"):
        continue
    gsd_checkwindow = _lib.get("gsd_checkwindow", "cdecl")
    gsd_checkwindow.argtypes = [POINTER(c_int), POINTER(c_int), POINTER(c_double), POINTER(c_double)]
    gsd_checkwindow.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 520
if _libs["grass_ogsf.8.4"].has("gsd_checkpoint", "cdecl"):
    gsd_checkpoint = _libs["grass_ogsf.8.4"].get("gsd_checkpoint", "cdecl")
    gsd_checkpoint.argtypes = [c_float * int(4), c_int * int(4), c_int * int(4), c_double * int(16), c_double * int(16)]
    gsd_checkpoint.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 521
if _libs["grass_ogsf.8.4"].has("gsd_litvert_func", "cdecl"):
    gsd_litvert_func = _libs["grass_ogsf.8.4"].get("gsd_litvert_func", "cdecl")
    gsd_litvert_func.argtypes = [POINTER(c_float), c_ulong, POINTER(c_float)]
    gsd_litvert_func.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 522
if _libs["grass_ogsf.8.4"].has("gsd_litvert_func2", "cdecl"):
    gsd_litvert_func2 = _libs["grass_ogsf.8.4"].get("gsd_litvert_func2", "cdecl")
    gsd_litvert_func2.argtypes = [POINTER(c_float), c_ulong, POINTER(c_float)]
    gsd_litvert_func2.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 523
if _libs["grass_ogsf.8.4"].has("gsd_vert_func", "cdecl"):
    gsd_vert_func = _libs["grass_ogsf.8.4"].get("gsd_vert_func", "cdecl")
    gsd_vert_func.argtypes = [POINTER(c_float)]
    gsd_vert_func.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 524
if _libs["grass_ogsf.8.4"].has("gsd_color_func", "cdecl"):
    gsd_color_func = _libs["grass_ogsf.8.4"].get("gsd_color_func", "cdecl")
    gsd_color_func.argtypes = [c_uint]
    gsd_color_func.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 525
if _libs["grass_ogsf.8.4"].has("gsd_init_lightmodel", "cdecl"):
    gsd_init_lightmodel = _libs["grass_ogsf.8.4"].get("gsd_init_lightmodel", "cdecl")
    gsd_init_lightmodel.argtypes = []
    gsd_init_lightmodel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 526
if _libs["grass_ogsf.8.4"].has("gsd_set_material", "cdecl"):
    gsd_set_material = _libs["grass_ogsf.8.4"].get("gsd_set_material", "cdecl")
    gsd_set_material.argtypes = [c_int, c_int, c_float, c_float, c_int]
    gsd_set_material.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 527
if _libs["grass_ogsf.8.4"].has("gsd_deflight", "cdecl"):
    gsd_deflight = _libs["grass_ogsf.8.4"].get("gsd_deflight", "cdecl")
    gsd_deflight.argtypes = [c_int, POINTER(struct_lightdefs)]
    gsd_deflight.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 528
if _libs["grass_ogsf.8.4"].has("gsd_switchlight", "cdecl"):
    gsd_switchlight = _libs["grass_ogsf.8.4"].get("gsd_switchlight", "cdecl")
    gsd_switchlight.argtypes = [c_int, c_int]
    gsd_switchlight.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 529
if _libs["grass_ogsf.8.4"].has("gsd_getimage", "cdecl"):
    gsd_getimage = _libs["grass_ogsf.8.4"].get("gsd_getimage", "cdecl")
    gsd_getimage.argtypes = [POINTER(POINTER(c_ubyte)), POINTER(c_uint), POINTER(c_uint)]
    gsd_getimage.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 530
if _libs["grass_ogsf.8.4"].has("gsd_blend", "cdecl"):
    gsd_blend = _libs["grass_ogsf.8.4"].get("gsd_blend", "cdecl")
    gsd_blend.argtypes = [c_int]
    gsd_blend.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 531
if _libs["grass_ogsf.8.4"].has("gsd_def_clipplane", "cdecl"):
    gsd_def_clipplane = _libs["grass_ogsf.8.4"].get("gsd_def_clipplane", "cdecl")
    gsd_def_clipplane.argtypes = [c_int, POINTER(c_double)]
    gsd_def_clipplane.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 532
if _libs["grass_ogsf.8.4"].has("gsd_set_clipplane", "cdecl"):
    gsd_set_clipplane = _libs["grass_ogsf.8.4"].get("gsd_set_clipplane", "cdecl")
    gsd_set_clipplane.argtypes = [c_int, c_int]
    gsd_set_clipplane.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 533
if _libs["grass_ogsf.8.4"].has("gsd_finish", "cdecl"):
    gsd_finish = _libs["grass_ogsf.8.4"].get("gsd_finish", "cdecl")
    gsd_finish.argtypes = []
    gsd_finish.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 534
if _libs["grass_ogsf.8.4"].has("gsd_viewport", "cdecl"):
    gsd_viewport = _libs["grass_ogsf.8.4"].get("gsd_viewport", "cdecl")
    gsd_viewport.argtypes = [c_int, c_int, c_int, c_int]
    gsd_viewport.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 535
if _libs["grass_ogsf.8.4"].has("gsd_makelist", "cdecl"):
    gsd_makelist = _libs["grass_ogsf.8.4"].get("gsd_makelist", "cdecl")
    gsd_makelist.argtypes = []
    gsd_makelist.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 536
if _libs["grass_ogsf.8.4"].has("gsd_bgnlist", "cdecl"):
    gsd_bgnlist = _libs["grass_ogsf.8.4"].get("gsd_bgnlist", "cdecl")
    gsd_bgnlist.argtypes = [c_int, c_int]
    gsd_bgnlist.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 537
if _libs["grass_ogsf.8.4"].has("gsd_endlist", "cdecl"):
    gsd_endlist = _libs["grass_ogsf.8.4"].get("gsd_endlist", "cdecl")
    gsd_endlist.argtypes = []
    gsd_endlist.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 538
if _libs["grass_ogsf.8.4"].has("gsd_calllist", "cdecl"):
    gsd_calllist = _libs["grass_ogsf.8.4"].get("gsd_calllist", "cdecl")
    gsd_calllist.argtypes = [c_int]
    gsd_calllist.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 539
if _libs["grass_ogsf.8.4"].has("gsd_deletelist", "cdecl"):
    gsd_deletelist = _libs["grass_ogsf.8.4"].get("gsd_deletelist", "cdecl")
    gsd_deletelist.argtypes = [GLuint, c_int]
    gsd_deletelist.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 540
if _libs["grass_ogsf.8.4"].has("gsd_calllists", "cdecl"):
    gsd_calllists = _libs["grass_ogsf.8.4"].get("gsd_calllists", "cdecl")
    gsd_calllists.argtypes = [c_int]
    gsd_calllists.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 541
if _libs["grass_ogsf.8.4"].has("gsd_getwindow", "cdecl"):
    gsd_getwindow = _libs["grass_ogsf.8.4"].get("gsd_getwindow", "cdecl")
    gsd_getwindow.argtypes = [POINTER(c_int), POINTER(c_int), POINTER(c_double), POINTER(c_double)]
    gsd_getwindow.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 542
if _libs["grass_ogsf.8.4"].has("gsd_writeView", "cdecl"):
    gsd_writeView = _libs["grass_ogsf.8.4"].get("gsd_writeView", "cdecl")
    gsd_writeView.argtypes = [POINTER(POINTER(c_ubyte)), c_uint, c_uint]
    gsd_writeView.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 545
if _libs["grass_ogsf.8.4"].has("gsd_surf", "cdecl"):
    gsd_surf = _libs["grass_ogsf.8.4"].get("gsd_surf", "cdecl")
    gsd_surf.argtypes = [POINTER(geosurf)]
    gsd_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 546
if _libs["grass_ogsf.8.4"].has("gsd_surf_map", "cdecl"):
    gsd_surf_map = _libs["grass_ogsf.8.4"].get("gsd_surf_map", "cdecl")
    gsd_surf_map.argtypes = [POINTER(geosurf)]
    gsd_surf_map.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 547
if _libs["grass_ogsf.8.4"].has("gsd_surf_const", "cdecl"):
    gsd_surf_const = _libs["grass_ogsf.8.4"].get("gsd_surf_const", "cdecl")
    gsd_surf_const.argtypes = [POINTER(geosurf), c_float]
    gsd_surf_const.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 548
if _libs["grass_ogsf.8.4"].has("gsd_surf_func", "cdecl"):
    gsd_surf_func = _libs["grass_ogsf.8.4"].get("gsd_surf_func", "cdecl")
    gsd_surf_func.argtypes = [POINTER(geosurf), CFUNCTYPE(UNCHECKED(c_int), )]
    gsd_surf_func.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 549
if _libs["grass_ogsf.8.4"].has("gsd_triangulated_wall", "cdecl"):
    gsd_triangulated_wall = _libs["grass_ogsf.8.4"].get("gsd_triangulated_wall", "cdecl")
    gsd_triangulated_wall.argtypes = [c_int, c_int, POINTER(geosurf), POINTER(geosurf), POINTER(Point3), POINTER(Point3), POINTER(c_float)]
    gsd_triangulated_wall.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 551
if _libs["grass_ogsf.8.4"].has("gsd_setfc", "cdecl"):
    gsd_setfc = _libs["grass_ogsf.8.4"].get("gsd_setfc", "cdecl")
    gsd_setfc.argtypes = [c_int]
    gsd_setfc.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 552
if _libs["grass_ogsf.8.4"].has("gsd_getfc", "cdecl"):
    gsd_getfc = _libs["grass_ogsf.8.4"].get("gsd_getfc", "cdecl")
    gsd_getfc.argtypes = []
    gsd_getfc.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 553
if _libs["grass_ogsf.8.4"].has("gsd_ortho_wall", "cdecl"):
    gsd_ortho_wall = _libs["grass_ogsf.8.4"].get("gsd_ortho_wall", "cdecl")
    gsd_ortho_wall.argtypes = [c_int, c_int, POINTER(POINTER(geosurf)), POINTER(POINTER(Point3)), POINTER(c_float)]
    gsd_ortho_wall.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 554
if _libs["grass_ogsf.8.4"].has("gsd_wall", "cdecl"):
    gsd_wall = _libs["grass_ogsf.8.4"].get("gsd_wall", "cdecl")
    gsd_wall.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    gsd_wall.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 555
if _libs["grass_ogsf.8.4"].has("gsd_norm_arrows", "cdecl"):
    gsd_norm_arrows = _libs["grass_ogsf.8.4"].get("gsd_norm_arrows", "cdecl")
    gsd_norm_arrows.argtypes = [POINTER(geosurf)]
    gsd_norm_arrows.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 558
if _libs["grass_ogsf.8.4"].has("gsd_get_los", "cdecl"):
    gsd_get_los = _libs["grass_ogsf.8.4"].get("gsd_get_los", "cdecl")
    gsd_get_los.argtypes = [POINTER(c_float * int(3)), c_short, c_short]
    gsd_get_los.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 559
if _libs["grass_ogsf.8.4"].has("gsd_set_view", "cdecl"):
    gsd_set_view = _libs["grass_ogsf.8.4"].get("gsd_set_view", "cdecl")
    gsd_set_view.argtypes = [POINTER(geoview), POINTER(geodisplay)]
    gsd_set_view.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 560
if _libs["grass_ogsf.8.4"].has("gsd_check_focus", "cdecl"):
    gsd_check_focus = _libs["grass_ogsf.8.4"].get("gsd_check_focus", "cdecl")
    gsd_check_focus.argtypes = [POINTER(geoview)]
    gsd_check_focus.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 561
if _libs["grass_ogsf.8.4"].has("gsd_get_zup", "cdecl"):
    gsd_get_zup = _libs["grass_ogsf.8.4"].get("gsd_get_zup", "cdecl")
    gsd_get_zup.argtypes = [POINTER(geoview), POINTER(c_double)]
    gsd_get_zup.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 562
if _libs["grass_ogsf.8.4"].has("gsd_zup_twist", "cdecl"):
    gsd_zup_twist = _libs["grass_ogsf.8.4"].get("gsd_zup_twist", "cdecl")
    gsd_zup_twist.argtypes = [POINTER(geoview)]
    gsd_zup_twist.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 563
if _libs["grass_ogsf.8.4"].has("gsd_do_scale", "cdecl"):
    gsd_do_scale = _libs["grass_ogsf.8.4"].get("gsd_do_scale", "cdecl")
    gsd_do_scale.argtypes = [c_int]
    gsd_do_scale.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 564
if _libs["grass_ogsf.8.4"].has("gsd_real2model", "cdecl"):
    gsd_real2model = _libs["grass_ogsf.8.4"].get("gsd_real2model", "cdecl")
    gsd_real2model.argtypes = [Point3]
    gsd_real2model.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 565
if _libs["grass_ogsf.8.4"].has("gsd_model2real", "cdecl"):
    gsd_model2real = _libs["grass_ogsf.8.4"].get("gsd_model2real", "cdecl")
    gsd_model2real.argtypes = [Point3]
    gsd_model2real.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 566
if _libs["grass_ogsf.8.4"].has("gsd_model2surf", "cdecl"):
    gsd_model2surf = _libs["grass_ogsf.8.4"].get("gsd_model2surf", "cdecl")
    gsd_model2surf.argtypes = [POINTER(geosurf), Point3]
    gsd_model2surf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 567
if _libs["grass_ogsf.8.4"].has("gsd_surf2model", "cdecl"):
    gsd_surf2model = _libs["grass_ogsf.8.4"].get("gsd_surf2model", "cdecl")
    gsd_surf2model.argtypes = [Point3]
    gsd_surf2model.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 568
if _libs["grass_ogsf.8.4"].has("gsd_surf2real", "cdecl"):
    gsd_surf2real = _libs["grass_ogsf.8.4"].get("gsd_surf2real", "cdecl")
    gsd_surf2real.argtypes = [POINTER(geosurf), Point3]
    gsd_surf2real.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 569
if _libs["grass_ogsf.8.4"].has("gsd_real2surf", "cdecl"):
    gsd_real2surf = _libs["grass_ogsf.8.4"].get("gsd_real2surf", "cdecl")
    gsd_real2surf.argtypes = [POINTER(geosurf), Point3]
    gsd_real2surf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 572
if _libs["grass_ogsf.8.4"].has("gsd_wire_surf", "cdecl"):
    gsd_wire_surf = _libs["grass_ogsf.8.4"].get("gsd_wire_surf", "cdecl")
    gsd_wire_surf.argtypes = [POINTER(geosurf)]
    gsd_wire_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 573
if _libs["grass_ogsf.8.4"].has("gsd_wire_surf_map", "cdecl"):
    gsd_wire_surf_map = _libs["grass_ogsf.8.4"].get("gsd_wire_surf_map", "cdecl")
    gsd_wire_surf_map.argtypes = [POINTER(geosurf)]
    gsd_wire_surf_map.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 574
if _libs["grass_ogsf.8.4"].has("gsd_coarse_surf_map", "cdecl"):
    gsd_coarse_surf_map = _libs["grass_ogsf.8.4"].get("gsd_coarse_surf_map", "cdecl")
    gsd_coarse_surf_map.argtypes = [POINTER(geosurf)]
    gsd_coarse_surf_map.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 575
if _libs["grass_ogsf.8.4"].has("gsd_wire_surf_const", "cdecl"):
    gsd_wire_surf_const = _libs["grass_ogsf.8.4"].get("gsd_wire_surf_const", "cdecl")
    gsd_wire_surf_const.argtypes = [POINTER(geosurf), c_float]
    gsd_wire_surf_const.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 576
if _libs["grass_ogsf.8.4"].has("gsd_wire_surf_func", "cdecl"):
    gsd_wire_surf_func = _libs["grass_ogsf.8.4"].get("gsd_wire_surf_func", "cdecl")
    gsd_wire_surf_func.argtypes = [POINTER(geosurf), CFUNCTYPE(UNCHECKED(c_int), )]
    gsd_wire_surf_func.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 577
if _libs["grass_ogsf.8.4"].has("gsd_wire_arrows", "cdecl"):
    gsd_wire_arrows = _libs["grass_ogsf.8.4"].get("gsd_wire_arrows", "cdecl")
    gsd_wire_arrows.argtypes = [POINTER(geosurf)]
    gsd_wire_arrows.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 580
if _libs["grass_ogsf.8.4"].has("gsdiff_set_SDscale", "cdecl"):
    gsdiff_set_SDscale = _libs["grass_ogsf.8.4"].get("gsdiff_set_SDscale", "cdecl")
    gsdiff_set_SDscale.argtypes = [c_float]
    gsdiff_set_SDscale.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 581
if _libs["grass_ogsf.8.4"].has("gsdiff_get_SDscale", "cdecl"):
    gsdiff_get_SDscale = _libs["grass_ogsf.8.4"].get("gsdiff_get_SDscale", "cdecl")
    gsdiff_get_SDscale.argtypes = []
    gsdiff_get_SDscale.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 582
if _libs["grass_ogsf.8.4"].has("gsdiff_set_SDref", "cdecl"):
    gsdiff_set_SDref = _libs["grass_ogsf.8.4"].get("gsdiff_set_SDref", "cdecl")
    gsdiff_set_SDref.argtypes = [POINTER(geosurf)]
    gsdiff_set_SDref.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 583
if _libs["grass_ogsf.8.4"].has("gsdiff_get_SDref", "cdecl"):
    gsdiff_get_SDref = _libs["grass_ogsf.8.4"].get("gsdiff_get_SDref", "cdecl")
    gsdiff_get_SDref.argtypes = []
    gsdiff_get_SDref.restype = POINTER(geosurf)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 584
if _libs["grass_ogsf.8.4"].has("gsdiff_do_SD", "cdecl"):
    gsdiff_do_SD = _libs["grass_ogsf.8.4"].get("gsdiff_do_SD", "cdecl")
    gsdiff_do_SD.argtypes = [c_float, c_int]
    gsdiff_do_SD.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 587
if _libs["grass_ogsf.8.4"].has("gsdrape_set_surface", "cdecl"):
    gsdrape_set_surface = _libs["grass_ogsf.8.4"].get("gsdrape_set_surface", "cdecl")
    gsdrape_set_surface.argtypes = [POINTER(geosurf)]
    gsdrape_set_surface.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 588
if _libs["grass_ogsf.8.4"].has("seg_intersect_vregion", "cdecl"):
    seg_intersect_vregion = _libs["grass_ogsf.8.4"].get("seg_intersect_vregion", "cdecl")
    seg_intersect_vregion.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float)]
    seg_intersect_vregion.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 589
if _libs["grass_ogsf.8.4"].has("gsdrape_get_segments", "cdecl"):
    gsdrape_get_segments = _libs["grass_ogsf.8.4"].get("gsdrape_get_segments", "cdecl")
    gsdrape_get_segments.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_int)]
    gsdrape_get_segments.restype = POINTER(Point3)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 590
if _libs["grass_ogsf.8.4"].has("gsdrape_get_allsegments", "cdecl"):
    gsdrape_get_allsegments = _libs["grass_ogsf.8.4"].get("gsdrape_get_allsegments", "cdecl")
    gsdrape_get_allsegments.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_int)]
    gsdrape_get_allsegments.restype = POINTER(Point3)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 591
if _libs["grass_ogsf.8.4"].has("interp_first_last", "cdecl"):
    interp_first_last = _libs["grass_ogsf.8.4"].get("interp_first_last", "cdecl")
    interp_first_last.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), Point3, Point3]
    interp_first_last.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 592
if _libs["grass_ogsf.8.4"].has("_viewcell_tri_interp", "cdecl"):
    _viewcell_tri_interp = _libs["grass_ogsf.8.4"].get("_viewcell_tri_interp", "cdecl")
    _viewcell_tri_interp.argtypes = [POINTER(geosurf), Point3]
    _viewcell_tri_interp.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 593
if _libs["grass_ogsf.8.4"].has("viewcell_tri_interp", "cdecl"):
    viewcell_tri_interp = _libs["grass_ogsf.8.4"].get("viewcell_tri_interp", "cdecl")
    viewcell_tri_interp.argtypes = [POINTER(geosurf), POINTER(typbuff), Point3, c_int]
    viewcell_tri_interp.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 594
if _libs["grass_ogsf.8.4"].has("in_vregion", "cdecl"):
    in_vregion = _libs["grass_ogsf.8.4"].get("in_vregion", "cdecl")
    in_vregion.argtypes = [POINTER(geosurf), POINTER(c_float)]
    in_vregion.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 595
if _libs["grass_ogsf.8.4"].has("order_intersects", "cdecl"):
    order_intersects = _libs["grass_ogsf.8.4"].get("order_intersects", "cdecl")
    order_intersects.argtypes = [POINTER(geosurf), Point3, Point3, c_int, c_int, c_int]
    order_intersects.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 596
if _libs["grass_ogsf.8.4"].has("get_vert_intersects", "cdecl"):
    get_vert_intersects = _libs["grass_ogsf.8.4"].get("get_vert_intersects", "cdecl")
    get_vert_intersects.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    get_vert_intersects.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 597
if _libs["grass_ogsf.8.4"].has("get_horz_intersects", "cdecl"):
    get_horz_intersects = _libs["grass_ogsf.8.4"].get("get_horz_intersects", "cdecl")
    get_horz_intersects.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    get_horz_intersects.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 598
if _libs["grass_ogsf.8.4"].has("get_diag_intersects", "cdecl"):
    get_diag_intersects = _libs["grass_ogsf.8.4"].get("get_diag_intersects", "cdecl")
    get_diag_intersects.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    get_diag_intersects.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 599
if _libs["grass_ogsf.8.4"].has("segs_intersect", "cdecl"):
    segs_intersect = _libs["grass_ogsf.8.4"].get("segs_intersect", "cdecl")
    segs_intersect.argtypes = [c_float, c_float, c_float, c_float, c_float, c_float, c_float, c_float, POINTER(c_float), POINTER(c_float)]
    segs_intersect.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 601
if _libs["grass_ogsf.8.4"].has("Point_on_plane", "cdecl"):
    Point_on_plane = _libs["grass_ogsf.8.4"].get("Point_on_plane", "cdecl")
    Point_on_plane.argtypes = [Point3, Point3, Point3, Point3]
    Point_on_plane.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 602
if _libs["grass_ogsf.8.4"].has("XY_intersect_plane", "cdecl"):
    XY_intersect_plane = _libs["grass_ogsf.8.4"].get("XY_intersect_plane", "cdecl")
    XY_intersect_plane.argtypes = [POINTER(c_float), POINTER(c_float)]
    XY_intersect_plane.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 603
if _libs["grass_ogsf.8.4"].has("P3toPlane", "cdecl"):
    P3toPlane = _libs["grass_ogsf.8.4"].get("P3toPlane", "cdecl")
    P3toPlane.argtypes = [Point3, Point3, Point3, POINTER(c_float)]
    P3toPlane.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 604
if _libs["grass_ogsf.8.4"].has("V3Cross", "cdecl"):
    V3Cross = _libs["grass_ogsf.8.4"].get("V3Cross", "cdecl")
    V3Cross.argtypes = [Point3, Point3, Point3]
    V3Cross.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 607
if _libs["grass_ogsf.8.4"].has("gsds_findh", "cdecl"):
    gsds_findh = _libs["grass_ogsf.8.4"].get("gsds_findh", "cdecl")
    gsds_findh.argtypes = [String, POINTER(c_uint), POINTER(c_uint), c_int]
    gsds_findh.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 608
if _libs["grass_ogsf.8.4"].has("gsds_newh", "cdecl"):
    gsds_newh = _libs["grass_ogsf.8.4"].get("gsds_newh", "cdecl")
    gsds_newh.argtypes = [String]
    gsds_newh.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 609
if _libs["grass_ogsf.8.4"].has("gsds_get_typbuff", "cdecl"):
    gsds_get_typbuff = _libs["grass_ogsf.8.4"].get("gsds_get_typbuff", "cdecl")
    gsds_get_typbuff.argtypes = [c_int, c_uint]
    gsds_get_typbuff.restype = POINTER(typbuff)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 610
if _libs["grass_ogsf.8.4"].has("gsds_get_name", "cdecl"):
    gsds_get_name = _libs["grass_ogsf.8.4"].get("gsds_get_name", "cdecl")
    gsds_get_name.argtypes = [c_int]
    if sizeof(c_int) == sizeof(c_void_p):
        gsds_get_name.restype = ReturnString
    else:
        gsds_get_name.restype = String
        gsds_get_name.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 611
if _libs["grass_ogsf.8.4"].has("gsds_free_datah", "cdecl"):
    gsds_free_datah = _libs["grass_ogsf.8.4"].get("gsds_free_datah", "cdecl")
    gsds_free_datah.argtypes = [c_int]
    gsds_free_datah.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 612
if _libs["grass_ogsf.8.4"].has("gsds_free_data_buff", "cdecl"):
    gsds_free_data_buff = _libs["grass_ogsf.8.4"].get("gsds_free_data_buff", "cdecl")
    gsds_free_data_buff.argtypes = [c_int, c_int]
    gsds_free_data_buff.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 613
if _libs["grass_ogsf.8.4"].has("free_data_buffs", "cdecl"):
    free_data_buffs = _libs["grass_ogsf.8.4"].get("free_data_buffs", "cdecl")
    free_data_buffs.argtypes = [POINTER(dataset), c_int]
    free_data_buffs.restype = c_size_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 614
if _libs["grass_ogsf.8.4"].has("gsds_alloc_typbuff", "cdecl"):
    gsds_alloc_typbuff = _libs["grass_ogsf.8.4"].get("gsds_alloc_typbuff", "cdecl")
    gsds_alloc_typbuff.argtypes = [c_int, POINTER(c_int), c_int, c_int]
    gsds_alloc_typbuff.restype = c_size_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 615
if _libs["grass_ogsf.8.4"].has("gsds_get_changed", "cdecl"):
    gsds_get_changed = _libs["grass_ogsf.8.4"].get("gsds_get_changed", "cdecl")
    gsds_get_changed.argtypes = [c_int]
    gsds_get_changed.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 616
if _libs["grass_ogsf.8.4"].has("gsds_set_changed", "cdecl"):
    gsds_set_changed = _libs["grass_ogsf.8.4"].get("gsds_set_changed", "cdecl")
    gsds_set_changed.argtypes = [c_int, c_uint]
    gsds_set_changed.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 617
if _libs["grass_ogsf.8.4"].has("gsds_get_type", "cdecl"):
    gsds_get_type = _libs["grass_ogsf.8.4"].get("gsds_get_type", "cdecl")
    gsds_get_type.argtypes = [c_int]
    gsds_get_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 620
if _libs["grass_ogsf.8.4"].has("get_mapatt", "cdecl"):
    get_mapatt = _libs["grass_ogsf.8.4"].get("get_mapatt", "cdecl")
    get_mapatt.argtypes = [POINTER(typbuff), c_int, POINTER(c_float)]
    get_mapatt.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 623
if _libs["grass_ogsf.8.4"].has("gv_get_vect", "cdecl"):
    gv_get_vect = _libs["grass_ogsf.8.4"].get("gv_get_vect", "cdecl")
    gv_get_vect.argtypes = [c_int]
    gv_get_vect.restype = POINTER(geovect)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 624
if _libs["grass_ogsf.8.4"].has("gv_get_prev_vect", "cdecl"):
    gv_get_prev_vect = _libs["grass_ogsf.8.4"].get("gv_get_prev_vect", "cdecl")
    gv_get_prev_vect.argtypes = [c_int]
    gv_get_prev_vect.restype = POINTER(geovect)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 625
if _libs["grass_ogsf.8.4"].has("gv_num_vects", "cdecl"):
    gv_num_vects = _libs["grass_ogsf.8.4"].get("gv_num_vects", "cdecl")
    gv_num_vects.argtypes = []
    gv_num_vects.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 626
if _libs["grass_ogsf.8.4"].has("gv_get_last_vect", "cdecl"):
    gv_get_last_vect = _libs["grass_ogsf.8.4"].get("gv_get_last_vect", "cdecl")
    gv_get_last_vect.argtypes = []
    gv_get_last_vect.restype = POINTER(geovect)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 627
if _libs["grass_ogsf.8.4"].has("gv_get_new_vect", "cdecl"):
    gv_get_new_vect = _libs["grass_ogsf.8.4"].get("gv_get_new_vect", "cdecl")
    gv_get_new_vect.argtypes = []
    gv_get_new_vect.restype = POINTER(geovect)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 628
if _libs["grass_ogsf.8.4"].has("gv_update_drapesurfs", "cdecl"):
    gv_update_drapesurfs = _libs["grass_ogsf.8.4"].get("gv_update_drapesurfs", "cdecl")
    gv_update_drapesurfs.argtypes = []
    gv_update_drapesurfs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 629
if _libs["grass_ogsf.8.4"].has("gv_set_defaults", "cdecl"):
    gv_set_defaults = _libs["grass_ogsf.8.4"].get("gv_set_defaults", "cdecl")
    gv_set_defaults.argtypes = [POINTER(geovect)]
    gv_set_defaults.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 630
if _libs["grass_ogsf.8.4"].has("gv_init_vect", "cdecl"):
    gv_init_vect = _libs["grass_ogsf.8.4"].get("gv_init_vect", "cdecl")
    gv_init_vect.argtypes = [POINTER(geovect)]
    gv_init_vect.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 631
if _libs["grass_ogsf.8.4"].has("gv_delete_vect", "cdecl"):
    gv_delete_vect = _libs["grass_ogsf.8.4"].get("gv_delete_vect", "cdecl")
    gv_delete_vect.argtypes = [c_int]
    gv_delete_vect.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 632
if _libs["grass_ogsf.8.4"].has("gv_free_vect", "cdecl"):
    gv_free_vect = _libs["grass_ogsf.8.4"].get("gv_free_vect", "cdecl")
    gv_free_vect.argtypes = [POINTER(geovect)]
    gv_free_vect.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 633
if _libs["grass_ogsf.8.4"].has("gv_free_vectmem", "cdecl"):
    gv_free_vectmem = _libs["grass_ogsf.8.4"].get("gv_free_vectmem", "cdecl")
    gv_free_vectmem.argtypes = [POINTER(geovect)]
    gv_free_vectmem.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 634
if _libs["grass_ogsf.8.4"].has("gv_set_drapesurfs", "cdecl"):
    gv_set_drapesurfs = _libs["grass_ogsf.8.4"].get("gv_set_drapesurfs", "cdecl")
    gv_set_drapesurfs.argtypes = [POINTER(geovect), POINTER(c_int), c_int]
    gv_set_drapesurfs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 637
if _libs["grass_ogsf.8.4"].has("gv_line_length", "cdecl"):
    gv_line_length = _libs["grass_ogsf.8.4"].get("gv_line_length", "cdecl")
    gv_line_length.argtypes = [POINTER(geoline)]
    gv_line_length.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 638
if _libs["grass_ogsf.8.4"].has("gln_num_points", "cdecl"):
    gln_num_points = _libs["grass_ogsf.8.4"].get("gln_num_points", "cdecl")
    gln_num_points.argtypes = [POINTER(geoline)]
    gln_num_points.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 639
if _libs["grass_ogsf.8.4"].has("gv_num_points", "cdecl"):
    gv_num_points = _libs["grass_ogsf.8.4"].get("gv_num_points", "cdecl")
    gv_num_points.argtypes = [POINTER(geovect)]
    gv_num_points.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 640
if _libs["grass_ogsf.8.4"].has("gv_decimate_lines", "cdecl"):
    gv_decimate_lines = _libs["grass_ogsf.8.4"].get("gv_decimate_lines", "cdecl")
    gv_decimate_lines.argtypes = [POINTER(geovect)]
    gv_decimate_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 643
if _libs["grass_ogsf.8.4"].has("gs_clip_segment", "cdecl"):
    gs_clip_segment = _libs["grass_ogsf.8.4"].get("gs_clip_segment", "cdecl")
    gs_clip_segment.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    gs_clip_segment.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 644
if _libs["grass_ogsf.8.4"].has("gvd_vect", "cdecl"):
    gvd_vect = _libs["grass_ogsf.8.4"].get("gvd_vect", "cdecl")
    gvd_vect.argtypes = [POINTER(geovect), POINTER(geosurf), c_int]
    gvd_vect.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 645
if _libs["grass_ogsf.8.4"].has("gvd_draw_lineonsurf", "cdecl"):
    gvd_draw_lineonsurf = _libs["grass_ogsf.8.4"].get("gvd_draw_lineonsurf", "cdecl")
    gvd_draw_lineonsurf.argtypes = [POINTER(geosurf), POINTER(c_float), POINTER(c_float), c_int]
    gvd_draw_lineonsurf.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 648
if _libs["grass_ogsf.8.4"].has("gvl_get_vol", "cdecl"):
    gvl_get_vol = _libs["grass_ogsf.8.4"].get("gvl_get_vol", "cdecl")
    gvl_get_vol.argtypes = [c_int]
    gvl_get_vol.restype = POINTER(geovol)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 649
if _libs["grass_ogsf.8.4"].has("gvl_get_prev_vol", "cdecl"):
    gvl_get_prev_vol = _libs["grass_ogsf.8.4"].get("gvl_get_prev_vol", "cdecl")
    gvl_get_prev_vol.argtypes = [c_int]
    gvl_get_prev_vol.restype = POINTER(geovol)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 650
if _libs["grass_ogsf.8.4"].has("gvl_getall_vols", "cdecl"):
    gvl_getall_vols = _libs["grass_ogsf.8.4"].get("gvl_getall_vols", "cdecl")
    gvl_getall_vols.argtypes = [POINTER(POINTER(geovol))]
    gvl_getall_vols.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 651
if _libs["grass_ogsf.8.4"].has("gvl_num_vols", "cdecl"):
    gvl_num_vols = _libs["grass_ogsf.8.4"].get("gvl_num_vols", "cdecl")
    gvl_num_vols.argtypes = []
    gvl_num_vols.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 652
if _libs["grass_ogsf.8.4"].has("gvl_get_last_vol", "cdecl"):
    gvl_get_last_vol = _libs["grass_ogsf.8.4"].get("gvl_get_last_vol", "cdecl")
    gvl_get_last_vol.argtypes = []
    gvl_get_last_vol.restype = POINTER(geovol)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 653
if _libs["grass_ogsf.8.4"].has("gvl_get_new_vol", "cdecl"):
    gvl_get_new_vol = _libs["grass_ogsf.8.4"].get("gvl_get_new_vol", "cdecl")
    gvl_get_new_vol.argtypes = []
    gvl_get_new_vol.restype = POINTER(geovol)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 654
if _libs["grass_ogsf.8.4"].has("gvl_init_vol", "cdecl"):
    gvl_init_vol = _libs["grass_ogsf.8.4"].get("gvl_init_vol", "cdecl")
    gvl_init_vol.argtypes = [POINTER(geovol), c_double, c_double, c_double, c_int, c_int, c_int, c_double, c_double, c_double]
    gvl_init_vol.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 656
if _libs["grass_ogsf.8.4"].has("gvl_delete_vol", "cdecl"):
    gvl_delete_vol = _libs["grass_ogsf.8.4"].get("gvl_delete_vol", "cdecl")
    gvl_delete_vol.argtypes = [c_int]
    gvl_delete_vol.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 657
if _libs["grass_ogsf.8.4"].has("gvl_free_vol", "cdecl"):
    gvl_free_vol = _libs["grass_ogsf.8.4"].get("gvl_free_vol", "cdecl")
    gvl_free_vol.argtypes = [POINTER(geovol)]
    gvl_free_vol.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 658
if _libs["grass_ogsf.8.4"].has("gvl_free_volmem", "cdecl"):
    gvl_free_volmem = _libs["grass_ogsf.8.4"].get("gvl_free_volmem", "cdecl")
    gvl_free_volmem.argtypes = [POINTER(geovol)]
    gvl_free_volmem.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 659
if _libs["grass_ogsf.8.4"].has("print_vol_fields", "cdecl"):
    print_vol_fields = _libs["grass_ogsf.8.4"].get("print_vol_fields", "cdecl")
    print_vol_fields.argtypes = [POINTER(geovol)]
    print_vol_fields.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 660
if _libs["grass_ogsf.8.4"].has("gvl_get_xextents", "cdecl"):
    gvl_get_xextents = _libs["grass_ogsf.8.4"].get("gvl_get_xextents", "cdecl")
    gvl_get_xextents.argtypes = [POINTER(geovol), POINTER(c_float), POINTER(c_float)]
    gvl_get_xextents.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 661
if _libs["grass_ogsf.8.4"].has("gvl_get_yextents", "cdecl"):
    gvl_get_yextents = _libs["grass_ogsf.8.4"].get("gvl_get_yextents", "cdecl")
    gvl_get_yextents.argtypes = [POINTER(geovol), POINTER(c_float), POINTER(c_float)]
    gvl_get_yextents.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 662
if _libs["grass_ogsf.8.4"].has("gvl_get_zextents", "cdecl"):
    gvl_get_zextents = _libs["grass_ogsf.8.4"].get("gvl_get_zextents", "cdecl")
    gvl_get_zextents.argtypes = [POINTER(geovol), POINTER(c_float), POINTER(c_float)]
    gvl_get_zextents.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 663
if _libs["grass_ogsf.8.4"].has("gvl_get_xrange", "cdecl"):
    gvl_get_xrange = _libs["grass_ogsf.8.4"].get("gvl_get_xrange", "cdecl")
    gvl_get_xrange.argtypes = [POINTER(c_float), POINTER(c_float)]
    gvl_get_xrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 664
if _libs["grass_ogsf.8.4"].has("gvl_get_yrange", "cdecl"):
    gvl_get_yrange = _libs["grass_ogsf.8.4"].get("gvl_get_yrange", "cdecl")
    gvl_get_yrange.argtypes = [POINTER(c_float), POINTER(c_float)]
    gvl_get_yrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 665
if _libs["grass_ogsf.8.4"].has("gvl_get_zrange", "cdecl"):
    gvl_get_zrange = _libs["grass_ogsf.8.4"].get("gvl_get_zrange", "cdecl")
    gvl_get_zrange.argtypes = [POINTER(c_float), POINTER(c_float)]
    gvl_get_zrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 667
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_init", "cdecl"):
    gvl_isosurf_init = _libs["grass_ogsf.8.4"].get("gvl_isosurf_init", "cdecl")
    gvl_isosurf_init.argtypes = [POINTER(geovol_isosurf)]
    gvl_isosurf_init.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 668
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_freemem", "cdecl"):
    gvl_isosurf_freemem = _libs["grass_ogsf.8.4"].get("gvl_isosurf_freemem", "cdecl")
    gvl_isosurf_freemem.argtypes = [POINTER(geovol_isosurf)]
    gvl_isosurf_freemem.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 669
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_get_isosurf", "cdecl"):
    gvl_isosurf_get_isosurf = _libs["grass_ogsf.8.4"].get("gvl_isosurf_get_isosurf", "cdecl")
    gvl_isosurf_get_isosurf.argtypes = [c_int, c_int]
    gvl_isosurf_get_isosurf.restype = POINTER(geovol_isosurf)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 670
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_get_att_src", "cdecl"):
    gvl_isosurf_get_att_src = _libs["grass_ogsf.8.4"].get("gvl_isosurf_get_att_src", "cdecl")
    gvl_isosurf_get_att_src.argtypes = [POINTER(geovol_isosurf), c_int]
    gvl_isosurf_get_att_src.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 671
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_set_att_src", "cdecl"):
    gvl_isosurf_set_att_src = _libs["grass_ogsf.8.4"].get("gvl_isosurf_set_att_src", "cdecl")
    gvl_isosurf_set_att_src.argtypes = [POINTER(geovol_isosurf), c_int, c_int]
    gvl_isosurf_set_att_src.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 672
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_set_att_const", "cdecl"):
    gvl_isosurf_set_att_const = _libs["grass_ogsf.8.4"].get("gvl_isosurf_set_att_const", "cdecl")
    gvl_isosurf_set_att_const.argtypes = [POINTER(geovol_isosurf), c_int, c_float]
    gvl_isosurf_set_att_const.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 673
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_set_att_map", "cdecl"):
    gvl_isosurf_set_att_map = _libs["grass_ogsf.8.4"].get("gvl_isosurf_set_att_map", "cdecl")
    gvl_isosurf_set_att_map.argtypes = [POINTER(geovol_isosurf), c_int, String]
    gvl_isosurf_set_att_map.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 674
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_set_att_changed", "cdecl"):
    gvl_isosurf_set_att_changed = _libs["grass_ogsf.8.4"].get("gvl_isosurf_set_att_changed", "cdecl")
    gvl_isosurf_set_att_changed.argtypes = [POINTER(geovol_isosurf), c_int]
    gvl_isosurf_set_att_changed.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 676
if _libs["grass_ogsf.8.4"].has("gvl_slice_init", "cdecl"):
    gvl_slice_init = _libs["grass_ogsf.8.4"].get("gvl_slice_init", "cdecl")
    gvl_slice_init.argtypes = [POINTER(geovol_slice)]
    gvl_slice_init.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 677
if _libs["grass_ogsf.8.4"].has("gvl_slice_get_slice", "cdecl"):
    gvl_slice_get_slice = _libs["grass_ogsf.8.4"].get("gvl_slice_get_slice", "cdecl")
    gvl_slice_get_slice.argtypes = [c_int, c_int]
    gvl_slice_get_slice.restype = POINTER(geovol_slice)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 678
if _libs["grass_ogsf.8.4"].has("gvl_slice_freemem", "cdecl"):
    gvl_slice_freemem = _libs["grass_ogsf.8.4"].get("gvl_slice_freemem", "cdecl")
    gvl_slice_freemem.argtypes = [POINTER(geovol_slice)]
    gvl_slice_freemem.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 681
if _libs["grass_ogsf.8.4"].has("P_scale", "cdecl"):
    P_scale = _libs["grass_ogsf.8.4"].get("P_scale", "cdecl")
    P_scale.argtypes = [c_float, c_float, c_float]
    P_scale.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 682
if _libs["grass_ogsf.8.4"].has("P_transform", "cdecl"):
    P_transform = _libs["grass_ogsf.8.4"].get("P_transform", "cdecl")
    P_transform.argtypes = [c_int, POINTER(c_float * int(4)), POINTER(c_float * int(4))]
    P_transform.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 683
if _libs["grass_ogsf.8.4"].has("P_pushmatrix", "cdecl"):
    P_pushmatrix = _libs["grass_ogsf.8.4"].get("P_pushmatrix", "cdecl")
    P_pushmatrix.argtypes = []
    P_pushmatrix.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 684
if _libs["grass_ogsf.8.4"].has("P_popmatrix", "cdecl"):
    P_popmatrix = _libs["grass_ogsf.8.4"].get("P_popmatrix", "cdecl")
    P_popmatrix.argtypes = []
    P_popmatrix.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 685
if _libs["grass_ogsf.8.4"].has("P_rot", "cdecl"):
    P_rot = _libs["grass_ogsf.8.4"].get("P_rot", "cdecl")
    P_rot.argtypes = [c_float, c_char]
    P_rot.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 688
if _libs["grass_ogsf.8.4"].has("gvl_file_get_volfile", "cdecl"):
    gvl_file_get_volfile = _libs["grass_ogsf.8.4"].get("gvl_file_get_volfile", "cdecl")
    gvl_file_get_volfile.argtypes = [c_int]
    gvl_file_get_volfile.restype = POINTER(geovol_file)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 689
if _libs["grass_ogsf.8.4"].has("gvl_file_get_name", "cdecl"):
    gvl_file_get_name = _libs["grass_ogsf.8.4"].get("gvl_file_get_name", "cdecl")
    gvl_file_get_name.argtypes = [c_int]
    if sizeof(c_int) == sizeof(c_void_p):
        gvl_file_get_name.restype = ReturnString
    else:
        gvl_file_get_name.restype = String
        gvl_file_get_name.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 690
if _libs["grass_ogsf.8.4"].has("gvl_file_get_file_type", "cdecl"):
    gvl_file_get_file_type = _libs["grass_ogsf.8.4"].get("gvl_file_get_file_type", "cdecl")
    gvl_file_get_file_type.argtypes = [POINTER(geovol_file)]
    gvl_file_get_file_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 691
if _libs["grass_ogsf.8.4"].has("gvl_file_get_data_type", "cdecl"):
    gvl_file_get_data_type = _libs["grass_ogsf.8.4"].get("gvl_file_get_data_type", "cdecl")
    gvl_file_get_data_type.argtypes = [POINTER(geovol_file)]
    gvl_file_get_data_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 692
if _libs["grass_ogsf.8.4"].has("gvl_file_newh", "cdecl"):
    gvl_file_newh = _libs["grass_ogsf.8.4"].get("gvl_file_newh", "cdecl")
    gvl_file_newh.argtypes = [String, c_uint]
    gvl_file_newh.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 693
if _libs["grass_ogsf.8.4"].has("gvl_file_free_datah", "cdecl"):
    gvl_file_free_datah = _libs["grass_ogsf.8.4"].get("gvl_file_free_datah", "cdecl")
    gvl_file_free_datah.argtypes = [c_int]
    gvl_file_free_datah.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 694
if _libs["grass_ogsf.8.4"].has("gvl_file_start_read", "cdecl"):
    gvl_file_start_read = _libs["grass_ogsf.8.4"].get("gvl_file_start_read", "cdecl")
    gvl_file_start_read.argtypes = [POINTER(geovol_file)]
    gvl_file_start_read.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 695
if _libs["grass_ogsf.8.4"].has("gvl_file_end_read", "cdecl"):
    gvl_file_end_read = _libs["grass_ogsf.8.4"].get("gvl_file_end_read", "cdecl")
    gvl_file_end_read.argtypes = [POINTER(geovol_file)]
    gvl_file_end_read.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 696
if _libs["grass_ogsf.8.4"].has("gvl_file_get_value", "cdecl"):
    gvl_file_get_value = _libs["grass_ogsf.8.4"].get("gvl_file_get_value", "cdecl")
    gvl_file_get_value.argtypes = [POINTER(geovol_file), c_int, c_int, c_int, POINTER(None)]
    gvl_file_get_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 697
if _libs["grass_ogsf.8.4"].has("gvl_file_is_null_value", "cdecl"):
    gvl_file_is_null_value = _libs["grass_ogsf.8.4"].get("gvl_file_is_null_value", "cdecl")
    gvl_file_is_null_value.argtypes = [POINTER(geovol_file), POINTER(None)]
    gvl_file_is_null_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 698
if _libs["grass_ogsf.8.4"].has("gvl_file_set_mode", "cdecl"):
    gvl_file_set_mode = _libs["grass_ogsf.8.4"].get("gvl_file_set_mode", "cdecl")
    gvl_file_set_mode.argtypes = [POINTER(geovol_file), c_uint]
    gvl_file_set_mode.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 699
if _libs["grass_ogsf.8.4"].has("gvl_file_set_slices_param", "cdecl"):
    gvl_file_set_slices_param = _libs["grass_ogsf.8.4"].get("gvl_file_set_slices_param", "cdecl")
    gvl_file_set_slices_param.argtypes = [POINTER(geovol_file), c_int, c_int]
    gvl_file_set_slices_param.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 700
if _libs["grass_ogsf.8.4"].has("gvl_file_get_min_max", "cdecl"):
    gvl_file_get_min_max = _libs["grass_ogsf.8.4"].get("gvl_file_get_min_max", "cdecl")
    gvl_file_get_min_max.argtypes = [POINTER(geovol_file), POINTER(c_double), POINTER(c_double)]
    gvl_file_get_min_max.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 703
if _libs["grass_ogsf.8.4"].has("Gvl_load_colors_data", "cdecl"):
    Gvl_load_colors_data = _libs["grass_ogsf.8.4"].get("Gvl_load_colors_data", "cdecl")
    Gvl_load_colors_data.argtypes = [POINTER(POINTER(None)), String]
    Gvl_load_colors_data.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 704
if _libs["grass_ogsf.8.4"].has("Gvl_unload_colors_data", "cdecl"):
    Gvl_unload_colors_data = _libs["grass_ogsf.8.4"].get("Gvl_unload_colors_data", "cdecl")
    Gvl_unload_colors_data.argtypes = [POINTER(None)]
    Gvl_unload_colors_data.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 705
if _libs["grass_ogsf.8.4"].has("Gvl_get_color_for_value", "cdecl"):
    Gvl_get_color_for_value = _libs["grass_ogsf.8.4"].get("Gvl_get_color_for_value", "cdecl")
    Gvl_get_color_for_value.argtypes = [POINTER(None), POINTER(c_float)]
    Gvl_get_color_for_value.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 708
if _libs["grass_ogsf.8.4"].has("gvl_isosurf_calc", "cdecl"):
    gvl_isosurf_calc = _libs["grass_ogsf.8.4"].get("gvl_isosurf_calc", "cdecl")
    gvl_isosurf_calc.argtypes = [POINTER(geovol)]
    gvl_isosurf_calc.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 709
if _libs["grass_ogsf.8.4"].has("gvl_slices_calc", "cdecl"):
    gvl_slices_calc = _libs["grass_ogsf.8.4"].get("gvl_slices_calc", "cdecl")
    gvl_slices_calc.argtypes = [POINTER(geovol)]
    gvl_slices_calc.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 710
if _libs["grass_ogsf.8.4"].has("gvl_write_char", "cdecl"):
    gvl_write_char = _libs["grass_ogsf.8.4"].get("gvl_write_char", "cdecl")
    gvl_write_char.argtypes = [c_int, POINTER(POINTER(c_ubyte)), c_ubyte]
    gvl_write_char.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 711
if _libs["grass_ogsf.8.4"].has("gvl_read_char", "cdecl"):
    gvl_read_char = _libs["grass_ogsf.8.4"].get("gvl_read_char", "cdecl")
    gvl_read_char.argtypes = [c_int, POINTER(c_ubyte)]
    gvl_read_char.restype = c_ubyte

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 712
if _libs["grass_ogsf.8.4"].has("gvl_align_data", "cdecl"):
    gvl_align_data = _libs["grass_ogsf.8.4"].get("gvl_align_data", "cdecl")
    gvl_align_data.argtypes = [c_int, POINTER(POINTER(c_ubyte))]
    gvl_align_data.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 715
if _libs["grass_ogsf.8.4"].has("gvld_vol", "cdecl"):
    gvld_vol = _libs["grass_ogsf.8.4"].get("gvld_vol", "cdecl")
    gvld_vol.argtypes = [POINTER(geovol)]
    gvld_vol.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 716
if _libs["grass_ogsf.8.4"].has("gvld_wire_vol", "cdecl"):
    gvld_wire_vol = _libs["grass_ogsf.8.4"].get("gvld_wire_vol", "cdecl")
    gvld_wire_vol.argtypes = [POINTER(geovol)]
    gvld_wire_vol.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 717
if _libs["grass_ogsf.8.4"].has("gvld_isosurf", "cdecl"):
    gvld_isosurf = _libs["grass_ogsf.8.4"].get("gvld_isosurf", "cdecl")
    gvld_isosurf.argtypes = [POINTER(geovol)]
    gvld_isosurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 718
if _libs["grass_ogsf.8.4"].has("gvld_wire_isosurf", "cdecl"):
    gvld_wire_isosurf = _libs["grass_ogsf.8.4"].get("gvld_wire_isosurf", "cdecl")
    gvld_wire_isosurf.argtypes = [POINTER(geovol)]
    gvld_wire_isosurf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 719
if _libs["grass_ogsf.8.4"].has("gvld_slices", "cdecl"):
    gvld_slices = _libs["grass_ogsf.8.4"].get("gvld_slices", "cdecl")
    gvld_slices.argtypes = [POINTER(geovol)]
    gvld_slices.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 720
if _libs["grass_ogsf.8.4"].has("gvld_slice", "cdecl"):
    gvld_slice = _libs["grass_ogsf.8.4"].get("gvld_slice", "cdecl")
    gvld_slice.argtypes = [POINTER(geovol), c_int]
    gvld_slice.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 721
if _libs["grass_ogsf.8.4"].has("gvld_wire_slices", "cdecl"):
    gvld_wire_slices = _libs["grass_ogsf.8.4"].get("gvld_wire_slices", "cdecl")
    gvld_wire_slices.argtypes = [POINTER(geovol)]
    gvld_wire_slices.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 722
if _libs["grass_ogsf.8.4"].has("gvld_wind3_box", "cdecl"):
    gvld_wind3_box = _libs["grass_ogsf.8.4"].get("gvld_wind3_box", "cdecl")
    gvld_wind3_box.argtypes = [POINTER(geovol)]
    gvld_wind3_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 725
if _libs["grass_ogsf.8.4"].has("gsd_display_fringe", "cdecl"):
    gsd_display_fringe = _libs["grass_ogsf.8.4"].get("gsd_display_fringe", "cdecl")
    gsd_display_fringe.argtypes = [POINTER(geosurf), c_ulong, c_float, c_int * int(4)]
    gsd_display_fringe.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 726
if _libs["grass_ogsf.8.4"].has("gsd_fringe_horiz_poly", "cdecl"):
    gsd_fringe_horiz_poly = _libs["grass_ogsf.8.4"].get("gsd_fringe_horiz_poly", "cdecl")
    gsd_fringe_horiz_poly.argtypes = [c_float, POINTER(geosurf), c_int, c_int]
    gsd_fringe_horiz_poly.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 727
if _libs["grass_ogsf.8.4"].has("gsd_fringe_horiz_line", "cdecl"):
    gsd_fringe_horiz_line = _libs["grass_ogsf.8.4"].get("gsd_fringe_horiz_line", "cdecl")
    gsd_fringe_horiz_line.argtypes = [c_float, POINTER(geosurf), c_int, c_int]
    gsd_fringe_horiz_line.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 728
if _libs["grass_ogsf.8.4"].has("gsd_fringe_vert_poly", "cdecl"):
    gsd_fringe_vert_poly = _libs["grass_ogsf.8.4"].get("gsd_fringe_vert_poly", "cdecl")
    gsd_fringe_vert_poly.argtypes = [c_float, POINTER(geosurf), c_int, c_int]
    gsd_fringe_vert_poly.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 729
if _libs["grass_ogsf.8.4"].has("gsd_fringe_vert_line", "cdecl"):
    gsd_fringe_vert_line = _libs["grass_ogsf.8.4"].get("gsd_fringe_vert_line", "cdecl")
    gsd_fringe_vert_line.argtypes = [c_float, POINTER(geosurf), c_int, c_int]
    gsd_fringe_vert_line.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 732
if _libs["grass_ogsf.8.4"].has("gsd_put_legend", "cdecl"):
    gsd_put_legend = _libs["grass_ogsf.8.4"].get("gsd_put_legend", "cdecl")
    gsd_put_legend.argtypes = [String, GLuint, c_int, POINTER(c_int), POINTER(c_float), POINTER(c_int)]
    gsd_put_legend.restype = GLuint

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 733
if _libs["grass_ogsf.8.4"].has("gsd_bgn_legend_viewport", "cdecl"):
    gsd_bgn_legend_viewport = _libs["grass_ogsf.8.4"].get("gsd_bgn_legend_viewport", "cdecl")
    gsd_bgn_legend_viewport.argtypes = [GLint, GLint, GLint, GLint]
    gsd_bgn_legend_viewport.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 734
if _libs["grass_ogsf.8.4"].has("gsd_end_legend_viewport", "cdecl"):
    gsd_end_legend_viewport = _libs["grass_ogsf.8.4"].get("gsd_end_legend_viewport", "cdecl")
    gsd_end_legend_viewport.argtypes = []
    gsd_end_legend_viewport.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/ogsf.h: 735
if _libs["grass_ogsf.8.4"].has("gsd_make_nice_number", "cdecl"):
    gsd_make_nice_number = _libs["grass_ogsf.8.4"].get("gsd_make_nice_number", "cdecl")
    gsd_make_nice_number.argtypes = [POINTER(c_float)]
    gsd_make_nice_number.restype = c_int

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 31
try:
    GS_UNIT_SIZE = 1000.
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 33
def BETWEEN(x, a, b):
    return (((x > a) and (x < b)) or ((x > b) and (x < a)))

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 40
try:
    MAX_SURFS = 12
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 41
try:
    MAX_VECTS = 50
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 42
try:
    MAX_SITES = 50
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 43
try:
    MAX_VOLS = 12
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 44
try:
    MAX_DSP = 12
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 45
try:
    MAX_ATTS = 7
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 46
try:
    MAX_LIGHTS = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 47
try:
    MAX_CPLANES = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 48
try:
    MAX_ISOSURFS = 12
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 49
try:
    MAX_SLICES = 12
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 52
try:
    MAX_VOL_SLICES = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 53
try:
    MAX_VOL_FILES = 100
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 56
try:
    DM_GOURAUD = 0x00000100
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 57
try:
    DM_FLAT = 0x00000200
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 59
try:
    DM_FRINGE = 0x00000010
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 61
try:
    DM_WIRE = 0x00000001
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 62
try:
    DM_COL_WIRE = 0x00000002
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 63
try:
    DM_POLY = 0x00000004
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 64
try:
    DM_WIRE_POLY = 0x00000008
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 66
try:
    DM_GRID_WIRE = 0x00000400
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 67
try:
    DM_GRID_SURF = 0x00000800
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 69
try:
    WC_COLOR_ATT = 0xFF000000
except:
    pass

IFLAG = c_uint# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 71

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 74
try:
    ATT_NORM = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 75
try:
    ATT_TOPO = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 76
try:
    ATT_COLOR = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 77
try:
    ATT_MASK = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 78
try:
    ATT_TRANSP = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 79
try:
    ATT_SHINE = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 80
try:
    ATT_EMIT = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 81
def LEGAL_ATT(a):
    return ((a >= 0) and (a < MAX_ATTS))

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 84
try:
    NOTSET_ATT = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 85
try:
    MAP_ATT = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 86
try:
    CONST_ATT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 87
try:
    FUNC_ATT = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 88
def LEGAL_SRC(s):
    return ((((s == NOTSET_ATT) or (s == MAP_ATT)) or (s == CONST_ATT)) or (s == FUNC_ATT))

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 92
try:
    ST_X = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 93
try:
    ST_BOX = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 94
try:
    ST_SPHERE = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 95
try:
    ST_CUBE = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 96
try:
    ST_DIAMOND = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 97
try:
    ST_DEC_TREE = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 98
try:
    ST_CON_TREE = 7
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 99
try:
    ST_ASTER = 8
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 100
try:
    ST_GYRO = 9
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 101
try:
    ST_HISTOGRAM = 10
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 104
try:
    GSD_FRONT = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 105
try:
    GSD_BACK = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 106
try:
    GSD_BOTH = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 109
try:
    FC_OFF = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 110
try:
    FC_ABOVE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 111
try:
    FC_BELOW = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 112
try:
    FC_BLEND = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 113
try:
    FC_GREY = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 116
try:
    LT_DISCRETE = 0x00000100
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 117
try:
    LT_CONTINUOUS = 0x00000200
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 119
try:
    LT_LIST = 0x00000010
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 122
try:
    LT_RANGE_LOWSET = 0x00000001
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 123
try:
    LT_RANGE_HISET = 0x00000002
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 124
try:
    LT_RANGE_LOW_HI = 0x00000003
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 125
try:
    LT_INVERTED = 0x00000008
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 127
try:
    LT_SHOW_VALS = 0x00001000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 128
try:
    LT_SHOW_LABELS = 0x00002000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 131
try:
    VOL_FTYPE_RASTER3D = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 134
try:
    VOL_DTYPE_FLOAT = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 135
try:
    VOL_DTYPE_DOUBLE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 140
try:
    X = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 141
try:
    Y = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 142
try:
    Z = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 143
try:
    W = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 144
try:
    FROM = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 145
try:
    TO = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 148
try:
    CM_COLOR = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 149
try:
    CM_EMISSION = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 150
try:
    CM_AMBIENT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 151
try:
    CM_DIFFUSE = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 152
try:
    CM_SPECULAR = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 153
try:
    CM_AD = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 154
try:
    CM_NULL = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 156
try:
    CM_WIRE = CM_COLOR
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 158
try:
    NULL_COLOR = 0xFFFFFF
except:
    pass

GS_CHAR8 = c_char# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 161

GS_SHORT16 = c_short# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 162

GS_INT32 = c_int# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 163

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 166
try:
    ATTY_NULL = 32
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 167
try:
    ATTY_MASK = 16
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 168
try:
    ATTY_FLOAT = 8
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 169
try:
    ATTY_INT = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 170
try:
    ATTY_SHORT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 171
try:
    ATTY_CHAR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 172
try:
    ATTY_ANY = 63
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 173
def LEGAL_TYPE(t):
    return (((((t == ATTY_MASK) or (t == ATTY_FLOAT)) or (t == ATTY_INT)) or (t == ATTY_SHORT)) or (t == ATTY_CHAR))

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 177
try:
    MAXDIMS = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 179
def FUDGE(gs):
    return ((((gs.contents.zmax_nz).value) - ((gs.contents.zmin_nz).value)) / 500.)

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 180
def DOT3(a, b):
    return ((((a [X]) * (b [X])) + ((a [Y]) * (b [Y]))) + ((a [Z]) * (b [Z])))

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 183
try:
    CF_NOT_CHANGED = 0x000000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 184
try:
    CF_COLOR_PACKED = 0x000001
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 185
try:
    CF_USR_CHANGED = 0x000010
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 186
try:
    CF_CHARSCALED = 0x000100
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 188
try:
    MAX_TF = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 190
try:
    MASK_TL = 0x10000000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 191
try:
    MASK_TR = 0x01000000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 192
try:
    MASK_BR = 0x00100000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 193
try:
    MASK_BL = 0x00010000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 194
try:
    MASK_NPTS = 0x00000007
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 196
try:
    OGSF_POINT = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 197
try:
    OGSF_LINE = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 198
try:
    OGSF_POLYGON = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 200
try:
    RED_MASK = 0x000000FF
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 201
try:
    GRN_MASK = 0x0000FF00
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 202
try:
    BLU_MASK = 0x00FF0000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 491
try:
    KF_FROMX_MASK = 0x00000001
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 492
try:
    KF_FROMY_MASK = 0x00000002
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 493
try:
    KF_FROMZ_MASK = 0x00000004
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 494
try:
    KF_FROM_MASK = 0x00000007
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 496
try:
    KF_DIRX_MASK = 0x00000008
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 497
try:
    KF_DIRY_MASK = 0x00000010
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 498
try:
    KF_DIRZ_MASK = 0x00000020
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 499
try:
    KF_DIR_MASK = 0x00000038
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 501
try:
    KF_FOV_MASK = 0x00000040
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 502
try:
    KF_TWIST_MASK = 0x00000080
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 504
try:
    KF_ALL_MASK = 0x000000FF
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 506
try:
    KF_NUMFIELDS = 8
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 508
try:
    KF_LINEAR = 111
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 509
try:
    KF_SPLINE = 222
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 510
def KF_LEGAL_MODE(m):
    return ((m == KF_LINEAR) or (m == KF_SPLINE))

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 512
try:
    KF_FROMX = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 513
try:
    KF_FROMY = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 514
try:
    KF_FROMZ = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 515
try:
    KF_DIRX = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 516
try:
    KF_DIRY = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 517
try:
    KF_DIRZ = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 518
try:
    KF_FOV = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 519
try:
    KF_TWIST = 7
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 521
try:
    FM_VECT = 0x00000001
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 522
try:
    FM_SITE = 0x00000002
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 523
try:
    FM_PATH = 0x00000004
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 524
try:
    FM_VOL = 0x00000008
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 525
try:
    FM_LABEL = 0x00000010
except:
    pass

g_surf = struct_g_surf# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 256

g_vect_style = struct_g_vect_style# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 285

g_vect_style_thematic = struct_g_vect_style_thematic# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 309

line_cats = struct_line_cats# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 319

g_line = struct_g_line# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 312

g_vect = struct_g_vect# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 328

g_point = struct_g_point# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 349

g_site = struct_g_site# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 362

g_vol = struct_g_vol# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 426

lightdefs = struct_lightdefs# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 452

georot = struct_georot# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 460

view_node = struct_view_node# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 529

key_node = struct_key_node# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\ogsf.h: 531

# No inserted files

# No prefix-stripping

