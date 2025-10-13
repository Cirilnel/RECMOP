r"""Wrapper for nviz.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_nviz.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/nviz.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h -o OBJ.x86_64-w64-mingw32/nviz.py

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
_libs["grass_nviz.8.4"] = load_library("grass_nviz.8.4")

# 1 libraries
# End libraries

# No modules

# C:/msys64/mingw64/include/windef.h: 47
class struct_HDC__(Structure):
    pass

struct_HDC__.__slots__ = [
    'unused',
]
struct_HDC__._fields_ = [
    ('unused', c_int),
]

HDC = POINTER(struct_HDC__)# C:/msys64/mingw64/include/windef.h: 47

# C:/msys64/mingw64/include/windef.h: 48
class struct_HGLRC__(Structure):
    pass

struct_HGLRC__.__slots__ = [
    'unused',
]
struct_HGLRC__._fields_ = [
    ('unused', c_int),
]

HGLRC = POINTER(struct_HGLRC__)# C:/msys64/mingw64/include/windef.h: 48

GLubyte = c_ubyte# C:/msys64/mingw64/include/GL/gl.h: 29

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 75
class struct_anon_256(Structure):
    pass

struct_anon_256.__slots__ = [
    'id',
    'brt',
    'r',
    'g',
    'b',
    'ar',
    'ag',
    'ab',
    'x',
    'y',
    'z',
    'w',
]
struct_anon_256._fields_ = [
    ('id', c_int),
    ('brt', c_float),
    ('r', c_float),
    ('g', c_float),
    ('b', c_float),
    ('ar', c_float),
    ('ag', c_float),
    ('ab', c_float),
    ('x', c_float),
    ('y', c_float),
    ('z', c_float),
    ('w', c_float),
]

light_data = struct_anon_256# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 75

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 77
class struct_fringe_data(Structure):
    pass

struct_fringe_data.__slots__ = [
    'id',
    'color',
    'elev',
    'where',
]
struct_fringe_data._fields_ = [
    ('id', c_int),
    ('color', c_ulong),
    ('elev', c_float),
    ('where', c_int * int(4)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 84
class struct_arrow_data(Structure):
    pass

struct_arrow_data.__slots__ = [
    'color',
    'size',
    'where',
]
struct_arrow_data._fields_ = [
    ('color', c_ulong),
    ('size', c_float),
    ('where', c_float * int(3)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 90
class struct_scalebar_data(Structure):
    pass

struct_scalebar_data.__slots__ = [
    'id',
    'color',
    'size',
    'where',
]
struct_scalebar_data._fields_ = [
    ('id', c_int),
    ('color', c_ulong),
    ('size', c_float),
    ('where', c_float * int(3)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 125
class struct_anon_257(Structure):
    pass

struct_anon_257.__slots__ = [
    'zrange',
    'xyrange',
    'num_cplanes',
    'cur_cplane',
    'cp_on',
    'cp_trans',
    'cp_rot',
    'light',
    'num_fringes',
    'fringe',
    'draw_arrow',
    'arrow',
    'num_scalebars',
    'scalebar',
    'bgcolor',
]
struct_anon_257._fields_ = [
    ('zrange', c_float),
    ('xyrange', c_float),
    ('num_cplanes', c_int),
    ('cur_cplane', c_int),
    ('cp_on', c_int * int(6)),
    ('cp_trans', (c_float * int(3)) * int(6)),
    ('cp_rot', (c_float * int(3)) * int(6)),
    ('light', light_data * int(3)),
    ('num_fringes', c_int),
    ('fringe', POINTER(POINTER(struct_fringe_data))),
    ('draw_arrow', c_int),
    ('arrow', POINTER(struct_arrow_data)),
    ('num_scalebars', c_int),
    ('scalebar', POINTER(POINTER(struct_scalebar_data))),
    ('bgcolor', c_int),
]

nv_data = struct_anon_257# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 125

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 127
class struct_render_window(Structure):
    pass

struct_render_window.__slots__ = [
    'displayId',
    'contextId',
    'width',
    'height',
]
struct_render_window._fields_ = [
    ('displayId', HDC),
    ('contextId', HGLRC),
    ('width', c_int),
    ('height', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 5
if _libs["grass_nviz.8.4"].has("Nviz_resize_window", "cdecl"):
    Nviz_resize_window = _libs["grass_nviz.8.4"].get("Nviz_resize_window", "cdecl")
    Nviz_resize_window.argtypes = [c_int, c_int]
    Nviz_resize_window.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 6
if _libs["grass_nviz.8.4"].has("Nviz_update_ranges", "cdecl"):
    Nviz_update_ranges = _libs["grass_nviz.8.4"].get("Nviz_update_ranges", "cdecl")
    Nviz_update_ranges.argtypes = [POINTER(nv_data)]
    Nviz_update_ranges.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 7
if _libs["grass_nviz.8.4"].has("Nviz_set_viewpoint_position", "cdecl"):
    Nviz_set_viewpoint_position = _libs["grass_nviz.8.4"].get("Nviz_set_viewpoint_position", "cdecl")
    Nviz_set_viewpoint_position.argtypes = [c_double, c_double]
    Nviz_set_viewpoint_position.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 8
if _libs["grass_nviz.8.4"].has("Nviz_get_viewpoint_position", "cdecl"):
    Nviz_get_viewpoint_position = _libs["grass_nviz.8.4"].get("Nviz_get_viewpoint_position", "cdecl")
    Nviz_get_viewpoint_position.argtypes = [POINTER(c_double), POINTER(c_double)]
    Nviz_get_viewpoint_position.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 9
if _libs["grass_nviz.8.4"].has("Nviz_set_viewpoint_height", "cdecl"):
    Nviz_set_viewpoint_height = _libs["grass_nviz.8.4"].get("Nviz_set_viewpoint_height", "cdecl")
    Nviz_set_viewpoint_height.argtypes = [c_double]
    Nviz_set_viewpoint_height.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 10
if _libs["grass_nviz.8.4"].has("Nviz_get_viewpoint_height", "cdecl"):
    Nviz_get_viewpoint_height = _libs["grass_nviz.8.4"].get("Nviz_get_viewpoint_height", "cdecl")
    Nviz_get_viewpoint_height.argtypes = [POINTER(c_double)]
    Nviz_get_viewpoint_height.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 11
if _libs["grass_nviz.8.4"].has("Nviz_set_viewpoint_persp", "cdecl"):
    Nviz_set_viewpoint_persp = _libs["grass_nviz.8.4"].get("Nviz_set_viewpoint_persp", "cdecl")
    Nviz_set_viewpoint_persp.argtypes = [c_int]
    Nviz_set_viewpoint_persp.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 12
if _libs["grass_nviz.8.4"].has("Nviz_set_viewpoint_twist", "cdecl"):
    Nviz_set_viewpoint_twist = _libs["grass_nviz.8.4"].get("Nviz_set_viewpoint_twist", "cdecl")
    Nviz_set_viewpoint_twist.argtypes = [c_int]
    Nviz_set_viewpoint_twist.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 13
if _libs["grass_nviz.8.4"].has("Nviz_change_exag", "cdecl"):
    Nviz_change_exag = _libs["grass_nviz.8.4"].get("Nviz_change_exag", "cdecl")
    Nviz_change_exag.argtypes = [POINTER(nv_data), c_double]
    Nviz_change_exag.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 14
if _libs["grass_nviz.8.4"].has("Nviz_look_here", "cdecl"):
    Nviz_look_here = _libs["grass_nviz.8.4"].get("Nviz_look_here", "cdecl")
    Nviz_look_here.argtypes = [c_double, c_double]
    Nviz_look_here.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 15
if _libs["grass_nviz.8.4"].has("Nviz_get_modelview", "cdecl"):
    Nviz_get_modelview = _libs["grass_nviz.8.4"].get("Nviz_get_modelview", "cdecl")
    Nviz_get_modelview.argtypes = [POINTER(c_double)]
    Nviz_get_modelview.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 16
if _libs["grass_nviz.8.4"].has("Nviz_set_rotation", "cdecl"):
    Nviz_set_rotation = _libs["grass_nviz.8.4"].get("Nviz_set_rotation", "cdecl")
    Nviz_set_rotation.argtypes = [c_double, c_double, c_double, c_double]
    Nviz_set_rotation.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 17
if _libs["grass_nviz.8.4"].has("Nviz_unset_rotation", "cdecl"):
    Nviz_unset_rotation = _libs["grass_nviz.8.4"].get("Nviz_unset_rotation", "cdecl")
    Nviz_unset_rotation.argtypes = []
    Nviz_unset_rotation.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 18
if _libs["grass_nviz.8.4"].has("Nviz_init_rotation", "cdecl"):
    Nviz_init_rotation = _libs["grass_nviz.8.4"].get("Nviz_init_rotation", "cdecl")
    Nviz_init_rotation.argtypes = []
    Nviz_init_rotation.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 19
if _libs["grass_nviz.8.4"].has("Nviz_flythrough", "cdecl"):
    Nviz_flythrough = _libs["grass_nviz.8.4"].get("Nviz_flythrough", "cdecl")
    Nviz_flythrough.argtypes = [POINTER(nv_data), POINTER(c_float), POINTER(c_int), c_int]
    Nviz_flythrough.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 22
if _libs["grass_nviz.8.4"].has("Nviz_new_cplane", "cdecl"):
    Nviz_new_cplane = _libs["grass_nviz.8.4"].get("Nviz_new_cplane", "cdecl")
    Nviz_new_cplane.argtypes = [POINTER(nv_data), c_int]
    Nviz_new_cplane.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 23
if _libs["grass_nviz.8.4"].has("Nviz_on_cplane", "cdecl"):
    Nviz_on_cplane = _libs["grass_nviz.8.4"].get("Nviz_on_cplane", "cdecl")
    Nviz_on_cplane.argtypes = [POINTER(nv_data), c_int]
    Nviz_on_cplane.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 24
if _libs["grass_nviz.8.4"].has("Nviz_off_cplane", "cdecl"):
    Nviz_off_cplane = _libs["grass_nviz.8.4"].get("Nviz_off_cplane", "cdecl")
    Nviz_off_cplane.argtypes = [POINTER(nv_data), c_int]
    Nviz_off_cplane.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 25
if _libs["grass_nviz.8.4"].has("Nviz_draw_cplane", "cdecl"):
    Nviz_draw_cplane = _libs["grass_nviz.8.4"].get("Nviz_draw_cplane", "cdecl")
    Nviz_draw_cplane.argtypes = [POINTER(nv_data), c_int, c_int]
    Nviz_draw_cplane.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 26
if _libs["grass_nviz.8.4"].has("Nviz_num_cplanes", "cdecl"):
    Nviz_num_cplanes = _libs["grass_nviz.8.4"].get("Nviz_num_cplanes", "cdecl")
    Nviz_num_cplanes.argtypes = [POINTER(nv_data)]
    Nviz_num_cplanes.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 27
if _libs["grass_nviz.8.4"].has("Nviz_get_current_cplane", "cdecl"):
    Nviz_get_current_cplane = _libs["grass_nviz.8.4"].get("Nviz_get_current_cplane", "cdecl")
    Nviz_get_current_cplane.argtypes = [POINTER(nv_data)]
    Nviz_get_current_cplane.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 28
if _libs["grass_nviz.8.4"].has("Nviz_set_cplane_rotation", "cdecl"):
    Nviz_set_cplane_rotation = _libs["grass_nviz.8.4"].get("Nviz_set_cplane_rotation", "cdecl")
    Nviz_set_cplane_rotation.argtypes = [POINTER(nv_data), c_int, c_float, c_float, c_float]
    Nviz_set_cplane_rotation.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 29
if _libs["grass_nviz.8.4"].has("Nviz_get_cplane_rotation", "cdecl"):
    Nviz_get_cplane_rotation = _libs["grass_nviz.8.4"].get("Nviz_get_cplane_rotation", "cdecl")
    Nviz_get_cplane_rotation.argtypes = [POINTER(nv_data), c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    Nviz_get_cplane_rotation.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 30
if _libs["grass_nviz.8.4"].has("Nviz_set_cplane_translation", "cdecl"):
    Nviz_set_cplane_translation = _libs["grass_nviz.8.4"].get("Nviz_set_cplane_translation", "cdecl")
    Nviz_set_cplane_translation.argtypes = [POINTER(nv_data), c_int, c_float, c_float, c_float]
    Nviz_set_cplane_translation.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 31
if _libs["grass_nviz.8.4"].has("Nviz_get_cplane_translation", "cdecl"):
    Nviz_get_cplane_translation = _libs["grass_nviz.8.4"].get("Nviz_get_cplane_translation", "cdecl")
    Nviz_get_cplane_translation.argtypes = [POINTER(nv_data), c_int, POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    Nviz_get_cplane_translation.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 32
if _libs["grass_nviz.8.4"].has("Nviz_set_fence_color", "cdecl"):
    Nviz_set_fence_color = _libs["grass_nviz.8.4"].get("Nviz_set_fence_color", "cdecl")
    Nviz_set_fence_color.argtypes = [POINTER(nv_data), c_int]
    Nviz_set_fence_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 33
if _libs["grass_nviz.8.4"].has("Nviz_set_cplane_here", "cdecl"):
    Nviz_set_cplane_here = _libs["grass_nviz.8.4"].get("Nviz_set_cplane_here", "cdecl")
    Nviz_set_cplane_here.argtypes = [POINTER(nv_data), c_int, c_float, c_float]
    Nviz_set_cplane_here.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 36
if _libs["grass_nviz.8.4"].has("Nviz_draw_all_surf", "cdecl"):
    Nviz_draw_all_surf = _libs["grass_nviz.8.4"].get("Nviz_draw_all_surf", "cdecl")
    Nviz_draw_all_surf.argtypes = [POINTER(nv_data)]
    Nviz_draw_all_surf.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 37
if _libs["grass_nviz.8.4"].has("Nviz_draw_all_vect", "cdecl"):
    Nviz_draw_all_vect = _libs["grass_nviz.8.4"].get("Nviz_draw_all_vect", "cdecl")
    Nviz_draw_all_vect.argtypes = []
    Nviz_draw_all_vect.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 38
if _libs["grass_nviz.8.4"].has("Nviz_draw_all_site", "cdecl"):
    Nviz_draw_all_site = _libs["grass_nviz.8.4"].get("Nviz_draw_all_site", "cdecl")
    Nviz_draw_all_site.argtypes = []
    Nviz_draw_all_site.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 39
if _libs["grass_nviz.8.4"].has("Nviz_draw_all_vol", "cdecl"):
    Nviz_draw_all_vol = _libs["grass_nviz.8.4"].get("Nviz_draw_all_vol", "cdecl")
    Nviz_draw_all_vol.argtypes = []
    Nviz_draw_all_vol.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 40
if _libs["grass_nviz.8.4"].has("Nviz_draw_all", "cdecl"):
    Nviz_draw_all = _libs["grass_nviz.8.4"].get("Nviz_draw_all", "cdecl")
    Nviz_draw_all.argtypes = [POINTER(nv_data)]
    Nviz_draw_all.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 41
if _libs["grass_nviz.8.4"].has("Nviz_draw_quick", "cdecl"):
    Nviz_draw_quick = _libs["grass_nviz.8.4"].get("Nviz_draw_quick", "cdecl")
    Nviz_draw_quick.argtypes = [POINTER(nv_data), c_int]
    Nviz_draw_quick.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 42
if _libs["grass_nviz.8.4"].has("Nviz_load_image", "cdecl"):
    Nviz_load_image = _libs["grass_nviz.8.4"].get("Nviz_load_image", "cdecl")
    Nviz_load_image.argtypes = [POINTER(GLubyte), c_int, c_int, c_int]
    Nviz_load_image.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 43
if _libs["grass_nviz.8.4"].has("Nviz_draw_image", "cdecl"):
    Nviz_draw_image = _libs["grass_nviz.8.4"].get("Nviz_draw_image", "cdecl")
    Nviz_draw_image.argtypes = [c_int, c_int, c_int, c_int, c_int]
    Nviz_draw_image.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 44
if _libs["grass_nviz.8.4"].has("Nviz_set_2D", "cdecl"):
    Nviz_set_2D = _libs["grass_nviz.8.4"].get("Nviz_set_2D", "cdecl")
    Nviz_set_2D.argtypes = [c_int, c_int]
    Nviz_set_2D.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 45
if _libs["grass_nviz.8.4"].has("Nviz_del_texture", "cdecl"):
    Nviz_del_texture = _libs["grass_nviz.8.4"].get("Nviz_del_texture", "cdecl")
    Nviz_del_texture.argtypes = [c_int]
    Nviz_del_texture.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 46
if _libs["grass_nviz.8.4"].has("Nviz_get_max_texture", "cdecl"):
    Nviz_get_max_texture = _libs["grass_nviz.8.4"].get("Nviz_get_max_texture", "cdecl")
    Nviz_get_max_texture.argtypes = [POINTER(c_int)]
    Nviz_get_max_texture.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 49
if _libs["grass_nviz.8.4"].has("Nviz_get_exag_height", "cdecl"):
    Nviz_get_exag_height = _libs["grass_nviz.8.4"].get("Nviz_get_exag_height", "cdecl")
    Nviz_get_exag_height.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    Nviz_get_exag_height.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 50
if _libs["grass_nviz.8.4"].has("Nviz_get_exag", "cdecl"):
    Nviz_get_exag = _libs["grass_nviz.8.4"].get("Nviz_get_exag", "cdecl")
    Nviz_get_exag.argtypes = []
    Nviz_get_exag.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 53
if _libs["grass_nviz.8.4"].has("Nviz_set_light_position", "cdecl"):
    Nviz_set_light_position = _libs["grass_nviz.8.4"].get("Nviz_set_light_position", "cdecl")
    Nviz_set_light_position.argtypes = [POINTER(nv_data), c_int, c_double, c_double, c_double, c_double]
    Nviz_set_light_position.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 54
if _libs["grass_nviz.8.4"].has("Nviz_set_light_bright", "cdecl"):
    Nviz_set_light_bright = _libs["grass_nviz.8.4"].get("Nviz_set_light_bright", "cdecl")
    Nviz_set_light_bright.argtypes = [POINTER(nv_data), c_int, c_double]
    Nviz_set_light_bright.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 55
if _libs["grass_nviz.8.4"].has("Nviz_set_light_color", "cdecl"):
    Nviz_set_light_color = _libs["grass_nviz.8.4"].get("Nviz_set_light_color", "cdecl")
    Nviz_set_light_color.argtypes = [POINTER(nv_data), c_int, c_int, c_int, c_int]
    Nviz_set_light_color.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 56
if _libs["grass_nviz.8.4"].has("Nviz_set_light_ambient", "cdecl"):
    Nviz_set_light_ambient = _libs["grass_nviz.8.4"].get("Nviz_set_light_ambient", "cdecl")
    Nviz_set_light_ambient.argtypes = [POINTER(nv_data), c_int, c_double]
    Nviz_set_light_ambient.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 57
if _libs["grass_nviz.8.4"].has("Nviz_init_light", "cdecl"):
    Nviz_init_light = _libs["grass_nviz.8.4"].get("Nviz_init_light", "cdecl")
    Nviz_init_light.argtypes = [POINTER(nv_data), c_int]
    Nviz_init_light.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 58
if _libs["grass_nviz.8.4"].has("Nviz_new_light", "cdecl"):
    Nviz_new_light = _libs["grass_nviz.8.4"].get("Nviz_new_light", "cdecl")
    Nviz_new_light.argtypes = [POINTER(nv_data)]
    Nviz_new_light.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 59
if _libs["grass_nviz.8.4"].has("Nviz_draw_model", "cdecl"):
    Nviz_draw_model = _libs["grass_nviz.8.4"].get("Nviz_draw_model", "cdecl")
    Nviz_draw_model.argtypes = [POINTER(nv_data)]
    Nviz_draw_model.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 62
if _libs["grass_nviz.8.4"].has("Nviz_new_map_obj", "cdecl"):
    Nviz_new_map_obj = _libs["grass_nviz.8.4"].get("Nviz_new_map_obj", "cdecl")
    Nviz_new_map_obj.argtypes = [c_int, String, c_double, POINTER(nv_data)]
    Nviz_new_map_obj.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 63
if _libs["grass_nviz.8.4"].has("Nviz_set_attr", "cdecl"):
    Nviz_set_attr = _libs["grass_nviz.8.4"].get("Nviz_set_attr", "cdecl")
    Nviz_set_attr.argtypes = [c_int, c_int, c_int, c_int, String, c_double, POINTER(nv_data)]
    Nviz_set_attr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 64
if _libs["grass_nviz.8.4"].has("Nviz_set_surface_attr_default", "cdecl"):
    Nviz_set_surface_attr_default = _libs["grass_nviz.8.4"].get("Nviz_set_surface_attr_default", "cdecl")
    Nviz_set_surface_attr_default.argtypes = []
    Nviz_set_surface_attr_default.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 65
if _libs["grass_nviz.8.4"].has("Nviz_set_vpoint_attr_default", "cdecl"):
    Nviz_set_vpoint_attr_default = _libs["grass_nviz.8.4"].get("Nviz_set_vpoint_attr_default", "cdecl")
    Nviz_set_vpoint_attr_default.argtypes = [c_int]
    Nviz_set_vpoint_attr_default.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 66
if _libs["grass_nviz.8.4"].has("Nviz_set_volume_attr_default", "cdecl"):
    Nviz_set_volume_attr_default = _libs["grass_nviz.8.4"].get("Nviz_set_volume_attr_default", "cdecl")
    Nviz_set_volume_attr_default.argtypes = [c_int]
    Nviz_set_volume_attr_default.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 67
if _libs["grass_nviz.8.4"].has("Nviz_unset_attr", "cdecl"):
    Nviz_unset_attr = _libs["grass_nviz.8.4"].get("Nviz_unset_attr", "cdecl")
    Nviz_unset_attr.argtypes = [c_int, c_int, c_int]
    Nviz_unset_attr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 70
if _libs["grass_nviz.8.4"].has("Nviz_init_data", "cdecl"):
    Nviz_init_data = _libs["grass_nviz.8.4"].get("Nviz_init_data", "cdecl")
    Nviz_init_data.argtypes = [POINTER(nv_data)]
    Nviz_init_data.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 71
if _libs["grass_nviz.8.4"].has("Nviz_destroy_data", "cdecl"):
    Nviz_destroy_data = _libs["grass_nviz.8.4"].get("Nviz_destroy_data", "cdecl")
    Nviz_destroy_data.argtypes = [POINTER(nv_data)]
    Nviz_destroy_data.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 72
if _libs["grass_nviz.8.4"].has("Nviz_set_bgcolor", "cdecl"):
    Nviz_set_bgcolor = _libs["grass_nviz.8.4"].get("Nviz_set_bgcolor", "cdecl")
    Nviz_set_bgcolor.argtypes = [POINTER(nv_data), c_int]
    Nviz_set_bgcolor.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 73
if _libs["grass_nviz.8.4"].has("Nviz_get_bgcolor", "cdecl"):
    Nviz_get_bgcolor = _libs["grass_nviz.8.4"].get("Nviz_get_bgcolor", "cdecl")
    Nviz_get_bgcolor.argtypes = [POINTER(nv_data)]
    Nviz_get_bgcolor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 74
if _libs["grass_nviz.8.4"].has("Nviz_color_from_str", "cdecl"):
    Nviz_color_from_str = _libs["grass_nviz.8.4"].get("Nviz_color_from_str", "cdecl")
    Nviz_color_from_str.argtypes = [String]
    Nviz_color_from_str.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 75
if _libs["grass_nviz.8.4"].has("Nviz_new_fringe", "cdecl"):
    Nviz_new_fringe = _libs["grass_nviz.8.4"].get("Nviz_new_fringe", "cdecl")
    Nviz_new_fringe.argtypes = [POINTER(nv_data), c_int, c_ulong, c_double, c_int, c_int, c_int, c_int]
    Nviz_new_fringe.restype = POINTER(struct_fringe_data)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 77
if _libs["grass_nviz.8.4"].has("Nviz_set_fringe", "cdecl"):
    Nviz_set_fringe = _libs["grass_nviz.8.4"].get("Nviz_set_fringe", "cdecl")
    Nviz_set_fringe.argtypes = [POINTER(nv_data), c_int, c_ulong, c_double, c_int, c_int, c_int, c_int]
    Nviz_set_fringe.restype = POINTER(struct_fringe_data)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 79
if _libs["grass_nviz.8.4"].has("Nviz_draw_fringe", "cdecl"):
    Nviz_draw_fringe = _libs["grass_nviz.8.4"].get("Nviz_draw_fringe", "cdecl")
    Nviz_draw_fringe.argtypes = [POINTER(nv_data)]
    Nviz_draw_fringe.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 80
if _libs["grass_nviz.8.4"].has("Nviz_draw_arrow", "cdecl"):
    Nviz_draw_arrow = _libs["grass_nviz.8.4"].get("Nviz_draw_arrow", "cdecl")
    Nviz_draw_arrow.argtypes = [POINTER(nv_data)]
    Nviz_draw_arrow.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 81
if _libs["grass_nviz.8.4"].has("Nviz_set_arrow", "cdecl"):
    Nviz_set_arrow = _libs["grass_nviz.8.4"].get("Nviz_set_arrow", "cdecl")
    Nviz_set_arrow.argtypes = [POINTER(nv_data), c_int, c_int, c_float, c_uint]
    Nviz_set_arrow.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 82
if _libs["grass_nviz.8.4"].has("Nviz_delete_arrow", "cdecl"):
    Nviz_delete_arrow = _libs["grass_nviz.8.4"].get("Nviz_delete_arrow", "cdecl")
    Nviz_delete_arrow.argtypes = [POINTER(nv_data)]
    Nviz_delete_arrow.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 83
if _libs["grass_nviz.8.4"].has("Nviz_new_scalebar", "cdecl"):
    Nviz_new_scalebar = _libs["grass_nviz.8.4"].get("Nviz_new_scalebar", "cdecl")
    Nviz_new_scalebar.argtypes = [POINTER(nv_data), c_int, POINTER(c_float), c_float, c_uint]
    Nviz_new_scalebar.restype = POINTER(struct_scalebar_data)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 85
if _libs["grass_nviz.8.4"].has("Nviz_set_scalebar", "cdecl"):
    Nviz_set_scalebar = _libs["grass_nviz.8.4"].get("Nviz_set_scalebar", "cdecl")
    Nviz_set_scalebar.argtypes = [POINTER(nv_data), c_int, c_int, c_int, c_float, c_uint]
    Nviz_set_scalebar.restype = POINTER(struct_scalebar_data)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 87
if _libs["grass_nviz.8.4"].has("Nviz_draw_scalebar", "cdecl"):
    Nviz_draw_scalebar = _libs["grass_nviz.8.4"].get("Nviz_draw_scalebar", "cdecl")
    Nviz_draw_scalebar.argtypes = [POINTER(nv_data)]
    Nviz_draw_scalebar.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 88
if _libs["grass_nviz.8.4"].has("Nviz_delete_scalebar", "cdecl"):
    Nviz_delete_scalebar = _libs["grass_nviz.8.4"].get("Nviz_delete_scalebar", "cdecl")
    Nviz_delete_scalebar.argtypes = [POINTER(nv_data), c_int]
    Nviz_delete_scalebar.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 91
if _libs["grass_nviz.8.4"].has("Nviz_init_view", "cdecl"):
    Nviz_init_view = _libs["grass_nviz.8.4"].get("Nviz_init_view", "cdecl")
    Nviz_init_view.argtypes = [POINTER(nv_data)]
    Nviz_init_view.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 92
if _libs["grass_nviz.8.4"].has("Nviz_set_focus_state", "cdecl"):
    Nviz_set_focus_state = _libs["grass_nviz.8.4"].get("Nviz_set_focus_state", "cdecl")
    Nviz_set_focus_state.argtypes = [c_int]
    Nviz_set_focus_state.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 93
if _libs["grass_nviz.8.4"].has("Nviz_set_focus_map", "cdecl"):
    Nviz_set_focus_map = _libs["grass_nviz.8.4"].get("Nviz_set_focus_map", "cdecl")
    Nviz_set_focus_map.argtypes = [c_int, c_int]
    Nviz_set_focus_map.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 94
if _libs["grass_nviz.8.4"].has("Nviz_has_focus", "cdecl"):
    Nviz_has_focus = _libs["grass_nviz.8.4"].get("Nviz_has_focus", "cdecl")
    Nviz_has_focus.argtypes = [POINTER(nv_data)]
    Nviz_has_focus.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 95
if _libs["grass_nviz.8.4"].has("Nviz_set_focus", "cdecl"):
    Nviz_set_focus = _libs["grass_nviz.8.4"].get("Nviz_set_focus", "cdecl")
    Nviz_set_focus.argtypes = [POINTER(nv_data), c_float, c_float, c_float]
    Nviz_set_focus.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 96
if _libs["grass_nviz.8.4"].has("Nviz_get_focus", "cdecl"):
    Nviz_get_focus = _libs["grass_nviz.8.4"].get("Nviz_get_focus", "cdecl")
    Nviz_get_focus.argtypes = [POINTER(nv_data), POINTER(c_float), POINTER(c_float), POINTER(c_float)]
    Nviz_get_focus.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 97
if _libs["grass_nviz.8.4"].has("Nviz_get_xyrange", "cdecl"):
    Nviz_get_xyrange = _libs["grass_nviz.8.4"].get("Nviz_get_xyrange", "cdecl")
    Nviz_get_xyrange.argtypes = [POINTER(nv_data)]
    Nviz_get_xyrange.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 98
if _libs["grass_nviz.8.4"].has("Nviz_get_zrange", "cdecl"):
    Nviz_get_zrange = _libs["grass_nviz.8.4"].get("Nviz_get_zrange", "cdecl")
    Nviz_get_zrange.argtypes = [POINTER(nv_data), POINTER(c_float), POINTER(c_float)]
    Nviz_get_zrange.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 99
if _libs["grass_nviz.8.4"].has("Nviz_get_longdim", "cdecl"):
    Nviz_get_longdim = _libs["grass_nviz.8.4"].get("Nviz_get_longdim", "cdecl")
    Nviz_get_longdim.argtypes = [POINTER(nv_data)]
    Nviz_get_longdim.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 102
if _libs["grass_nviz.8.4"].has("Nviz_new_render_window", "cdecl"):
    Nviz_new_render_window = _libs["grass_nviz.8.4"].get("Nviz_new_render_window", "cdecl")
    Nviz_new_render_window.argtypes = []
    Nviz_new_render_window.restype = POINTER(struct_render_window)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 103
if _libs["grass_nviz.8.4"].has("Nviz_init_render_window", "cdecl"):
    Nviz_init_render_window = _libs["grass_nviz.8.4"].get("Nviz_init_render_window", "cdecl")
    Nviz_init_render_window.argtypes = [POINTER(struct_render_window)]
    Nviz_init_render_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 104
if _libs["grass_nviz.8.4"].has("Nviz_destroy_render_window", "cdecl"):
    Nviz_destroy_render_window = _libs["grass_nviz.8.4"].get("Nviz_destroy_render_window", "cdecl")
    Nviz_destroy_render_window.argtypes = [POINTER(struct_render_window)]
    Nviz_destroy_render_window.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 105
if _libs["grass_nviz.8.4"].has("Nviz_create_render_window", "cdecl"):
    Nviz_create_render_window = _libs["grass_nviz.8.4"].get("Nviz_create_render_window", "cdecl")
    Nviz_create_render_window.argtypes = [POINTER(struct_render_window), POINTER(None), c_int, c_int]
    Nviz_create_render_window.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/nviz.h: 106
if _libs["grass_nviz.8.4"].has("Nviz_make_current_render_window", "cdecl"):
    Nviz_make_current_render_window = _libs["grass_nviz.8.4"].get("Nviz_make_current_render_window", "cdecl")
    Nviz_make_current_render_window.argtypes = [POINTER(struct_render_window)]
    Nviz_make_current_render_window.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/ogsf.h: 31
try:
    GS_UNIT_SIZE = 1000.
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 42
try:
    MAP_OBJ_UNDEFINED = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 43
try:
    MAP_OBJ_SURF = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 44
try:
    MAP_OBJ_VOL = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 45
try:
    MAP_OBJ_VECT = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 46
try:
    MAP_OBJ_SITE = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 48
try:
    DRAW_COARSE = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 49
try:
    DRAW_FINE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 50
try:
    DRAW_BOTH = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 53
try:
    DRAW_QUICK_SURFACE = 0x01
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 54
try:
    DRAW_QUICK_VLINES = 0x02
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 55
try:
    DRAW_QUICK_VPOINTS = 0x04
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 56
try:
    DRAW_QUICK_VOLUME = 0x08
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 58
try:
    RANGE = (5 * GS_UNIT_SIZE)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 59
try:
    RANGE_OFFSET = (2 * GS_UNIT_SIZE)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 60
try:
    ZRANGE = (3 * GS_UNIT_SIZE)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 61
try:
    ZRANGE_OFFSET = (1 * GS_UNIT_SIZE)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 63
try:
    DEFAULT_SURF_COLOR = 0x33BBFF
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 65
try:
    FORMAT_PPM = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 66
try:
    FORMAT_TIF = 2
except:
    pass

fringe_data = struct_fringe_data# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 77

arrow_data = struct_arrow_data# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 84

scalebar_data = struct_scalebar_data# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 90

render_window = struct_render_window# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\nviz.h: 127

# No inserted files

# No prefix-stripping

