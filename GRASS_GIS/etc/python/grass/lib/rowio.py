r"""Wrapper for rowio.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_rowio.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rowio.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/rowio.h -o OBJ.x86_64-w64-mingw32/rowio.py

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
_libs["grass_rowio.8.4"] = load_library("grass_rowio.8.4")

# 1 libraries
# End libraries

# No modules

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\rowio.h: 14
class struct_ROWIO_RCB(Structure):
    pass

struct_ROWIO_RCB.__slots__ = [
    'buf',
    'age',
    'row',
    'dirty',
]
struct_ROWIO_RCB._fields_ = [
    ('buf', POINTER(None)),
    ('age', c_int),
    ('row', c_int),
    ('dirty', c_int),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\rowio.h: 20
class struct_anon_1(Structure):
    pass

struct_anon_1.__slots__ = [
    'fd',
    'nrows',
    'len',
    'cur',
    'buf',
    'getrow',
    'putrow',
    'rcb',
]
struct_anon_1._fields_ = [
    ('fd', c_int),
    ('nrows', c_int),
    ('len', c_int),
    ('cur', c_int),
    ('buf', POINTER(None)),
    ('getrow', CFUNCTYPE(UNCHECKED(c_int), c_int, POINTER(None), c_int, c_int)),
    ('putrow', CFUNCTYPE(UNCHECKED(c_int), c_int, POINTER(None), c_int, c_int)),
    ('rcb', POINTER(struct_ROWIO_RCB)),
]

ROWIO = struct_anon_1# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\rowio.h: 20

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/rowio.h: 4
if _libs["grass_rowio.8.4"].has("Rowio_fileno", "cdecl"):
    Rowio_fileno = _libs["grass_rowio.8.4"].get("Rowio_fileno", "cdecl")
    Rowio_fileno.argtypes = [POINTER(ROWIO)]
    Rowio_fileno.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/rowio.h: 5
if _libs["grass_rowio.8.4"].has("Rowio_forget", "cdecl"):
    Rowio_forget = _libs["grass_rowio.8.4"].get("Rowio_forget", "cdecl")
    Rowio_forget.argtypes = [POINTER(ROWIO), c_int]
    Rowio_forget.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/rowio.h: 6
if _libs["grass_rowio.8.4"].has("Rowio_get", "cdecl"):
    Rowio_get = _libs["grass_rowio.8.4"].get("Rowio_get", "cdecl")
    Rowio_get.argtypes = [POINTER(ROWIO), c_int]
    Rowio_get.restype = POINTER(c_ubyte)
    Rowio_get.errcheck = lambda v,*a : cast(v, c_void_p)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/rowio.h: 7
if _libs["grass_rowio.8.4"].has("Rowio_flush", "cdecl"):
    Rowio_flush = _libs["grass_rowio.8.4"].get("Rowio_flush", "cdecl")
    Rowio_flush.argtypes = [POINTER(ROWIO)]
    Rowio_flush.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/rowio.h: 8
if _libs["grass_rowio.8.4"].has("Rowio_put", "cdecl"):
    Rowio_put = _libs["grass_rowio.8.4"].get("Rowio_put", "cdecl")
    Rowio_put.argtypes = [POINTER(ROWIO), POINTER(None), c_int]
    Rowio_put.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/rowio.h: 9
if _libs["grass_rowio.8.4"].has("Rowio_release", "cdecl"):
    Rowio_release = _libs["grass_rowio.8.4"].get("Rowio_release", "cdecl")
    Rowio_release.argtypes = [POINTER(ROWIO)]
    Rowio_release.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/rowio.h: 10
if _libs["grass_rowio.8.4"].has("Rowio_setup", "cdecl"):
    Rowio_setup = _libs["grass_rowio.8.4"].get("Rowio_setup", "cdecl")
    Rowio_setup.argtypes = [POINTER(ROWIO), c_int, c_int, c_int, CFUNCTYPE(UNCHECKED(c_int), c_int, POINTER(None), c_int, c_int), CFUNCTYPE(UNCHECKED(c_int), c_int, POINTER(None), c_int, c_int)]
    Rowio_setup.restype = c_int

ROWIO_RCB = struct_ROWIO_RCB# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\rowio.h: 14

# No inserted files

# No prefix-stripping

