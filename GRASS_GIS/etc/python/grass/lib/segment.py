r"""Wrapper for segment.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_segment.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/segment.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h -o OBJ.x86_64-w64-mingw32/segment.py

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
_libs["grass_segment.8.4"] = load_library("grass_segment.8.4")

# 1 libraries
# End libraries

# No modules

off_t = c_int64# C:/msys64/mingw64/include/_mingw_off_t.h: 24

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\segment.h: 14
class struct_aq(Structure):
    pass

struct_aq.__slots__ = [
    'cur',
    'younger',
    'older',
]
struct_aq._fields_ = [
    ('cur', c_int),
    ('younger', POINTER(struct_aq)),
    ('older', POINTER(struct_aq)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\segment.h: 19
class struct_SEGMENT(Structure):
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\segment.h: 44
class struct_scb(Structure):
    pass

struct_scb.__slots__ = [
    'buf',
    'dirty',
    'age',
    'n',
]
struct_scb._fields_ = [
    ('buf', String),
    ('dirty', c_char),
    ('age', POINTER(struct_aq)),
    ('n', c_int),
]

struct_SEGMENT.__slots__ = [
    'open',
    'nrows',
    'ncols',
    'len',
    'srows',
    'scols',
    'srowscols',
    'size',
    'spr',
    'spill',
    'fast_adrs',
    'scolbits',
    'srowbits',
    'segbits',
    'fast_seek',
    'lenbits',
    'sizebits',
    'address',
    'seek',
    'fname',
    'fd',
    'scb',
    'load_idx',
    'nfreeslots',
    'freeslot',
    'agequeue',
    'youngest',
    'oldest',
    'nseg',
    'cur',
    'offset',
    'cache',
]
struct_SEGMENT._fields_ = [
    ('open', c_int),
    ('nrows', off_t),
    ('ncols', off_t),
    ('len', c_int),
    ('srows', c_int),
    ('scols', c_int),
    ('srowscols', c_int),
    ('size', c_int),
    ('spr', c_int),
    ('spill', c_int),
    ('fast_adrs', c_int),
    ('scolbits', off_t),
    ('srowbits', off_t),
    ('segbits', off_t),
    ('fast_seek', c_int),
    ('lenbits', c_int),
    ('sizebits', c_int),
    ('address', CFUNCTYPE(UNCHECKED(c_int), POINTER(struct_SEGMENT), off_t, off_t, POINTER(c_int), POINTER(c_int))),
    ('seek', CFUNCTYPE(UNCHECKED(c_int), POINTER(struct_SEGMENT), c_int, c_int)),
    ('fname', String),
    ('fd', c_int),
    ('scb', POINTER(struct_scb)),
    ('load_idx', POINTER(c_int)),
    ('nfreeslots', c_int),
    ('freeslot', POINTER(c_int)),
    ('agequeue', POINTER(struct_aq)),
    ('youngest', POINTER(struct_aq)),
    ('oldest', POINTER(struct_aq)),
    ('nseg', c_int),
    ('cur', c_int),
    ('offset', c_int),
    ('cache', String),
]

SEGMENT = struct_SEGMENT# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\segment.h: 62

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 4
if _libs["grass_segment.8.4"].has("Segment_open", "cdecl"):
    Segment_open = _libs["grass_segment.8.4"].get("Segment_open", "cdecl")
    Segment_open.argtypes = [POINTER(SEGMENT), String, off_t, off_t, c_int, c_int, c_int, c_int]
    Segment_open.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 5
if _libs["grass_segment.8.4"].has("Segment_close", "cdecl"):
    Segment_close = _libs["grass_segment.8.4"].get("Segment_close", "cdecl")
    Segment_close.argtypes = [POINTER(SEGMENT)]
    Segment_close.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 6
if _libs["grass_segment.8.4"].has("Segment_flush", "cdecl"):
    Segment_flush = _libs["grass_segment.8.4"].get("Segment_flush", "cdecl")
    Segment_flush.argtypes = [POINTER(SEGMENT)]
    Segment_flush.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 7
if _libs["grass_segment.8.4"].has("Segment_format", "cdecl"):
    Segment_format = _libs["grass_segment.8.4"].get("Segment_format", "cdecl")
    Segment_format.argtypes = [c_int, off_t, off_t, c_int, c_int, c_int]
    Segment_format.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 8
if _libs["grass_segment.8.4"].has("Segment_format_nofill", "cdecl"):
    Segment_format_nofill = _libs["grass_segment.8.4"].get("Segment_format_nofill", "cdecl")
    Segment_format_nofill.argtypes = [c_int, off_t, off_t, c_int, c_int, c_int]
    Segment_format_nofill.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 9
if _libs["grass_segment.8.4"].has("Segment_get", "cdecl"):
    Segment_get = _libs["grass_segment.8.4"].get("Segment_get", "cdecl")
    Segment_get.argtypes = [POINTER(SEGMENT), POINTER(None), off_t, off_t]
    Segment_get.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 10
if _libs["grass_segment.8.4"].has("Segment_get_row", "cdecl"):
    Segment_get_row = _libs["grass_segment.8.4"].get("Segment_get_row", "cdecl")
    Segment_get_row.argtypes = [POINTER(SEGMENT), POINTER(None), off_t]
    Segment_get_row.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 11
if _libs["grass_segment.8.4"].has("Segment_init", "cdecl"):
    Segment_init = _libs["grass_segment.8.4"].get("Segment_init", "cdecl")
    Segment_init.argtypes = [POINTER(SEGMENT), c_int, c_int]
    Segment_init.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 12
if _libs["grass_segment.8.4"].has("Segment_put", "cdecl"):
    Segment_put = _libs["grass_segment.8.4"].get("Segment_put", "cdecl")
    Segment_put.argtypes = [POINTER(SEGMENT), POINTER(None), off_t, off_t]
    Segment_put.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 13
if _libs["grass_segment.8.4"].has("Segment_put_row", "cdecl"):
    Segment_put_row = _libs["grass_segment.8.4"].get("Segment_put_row", "cdecl")
    Segment_put_row.argtypes = [POINTER(SEGMENT), POINTER(None), off_t]
    Segment_put_row.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/segment.h: 14
if _libs["grass_segment.8.4"].has("Segment_release", "cdecl"):
    Segment_release = _libs["grass_segment.8.4"].get("Segment_release", "cdecl")
    Segment_release.argtypes = [POINTER(SEGMENT)]
    Segment_release.restype = c_int

aq = struct_aq# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\segment.h: 14

SEGMENT = struct_SEGMENT# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\segment.h: 19

scb = struct_scb# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\segment.h: 44

# No inserted files

# No prefix-stripping

