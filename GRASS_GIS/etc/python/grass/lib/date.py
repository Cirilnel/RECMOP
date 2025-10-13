r"""Wrapper for datetime.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_datetime.8.4 C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/datetime.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h -o OBJ.x86_64-w64-mingw32/date.py

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
_libs["grass_datetime.8.4"] = load_library("grass_datetime.8.4")

# 1 libraries
# End libraries

# No modules

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 26
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

DateTime = struct_DateTime# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 26

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 5
if _libs["grass_datetime.8.4"].has("datetime_is_between", "cdecl"):
    datetime_is_between = _libs["grass_datetime.8.4"].get("datetime_is_between", "cdecl")
    datetime_is_between.argtypes = [c_int, c_int, c_int]
    datetime_is_between.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 8
if _libs["grass_datetime.8.4"].has("datetime_change_from_to", "cdecl"):
    datetime_change_from_to = _libs["grass_datetime.8.4"].get("datetime_change_from_to", "cdecl")
    datetime_change_from_to.argtypes = [POINTER(DateTime), c_int, c_int, c_int]
    datetime_change_from_to.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 11
if _libs["grass_datetime.8.4"].has("datetime_copy", "cdecl"):
    datetime_copy = _libs["grass_datetime.8.4"].get("datetime_copy", "cdecl")
    datetime_copy.argtypes = [POINTER(DateTime), POINTER(DateTime)]
    datetime_copy.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 14
if _libs["grass_datetime.8.4"].has("datetime_difference", "cdecl"):
    datetime_difference = _libs["grass_datetime.8.4"].get("datetime_difference", "cdecl")
    datetime_difference.argtypes = [POINTER(DateTime), POINTER(DateTime), POINTER(DateTime)]
    datetime_difference.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 17
if _libs["grass_datetime.8.4"].has("datetime_error", "cdecl"):
    datetime_error = _libs["grass_datetime.8.4"].get("datetime_error", "cdecl")
    datetime_error.argtypes = [c_int, String]
    datetime_error.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 18
if _libs["grass_datetime.8.4"].has("datetime_error_code", "cdecl"):
    datetime_error_code = _libs["grass_datetime.8.4"].get("datetime_error_code", "cdecl")
    datetime_error_code.argtypes = []
    datetime_error_code.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 19
if _libs["grass_datetime.8.4"].has("datetime_error_msg", "cdecl"):
    datetime_error_msg = _libs["grass_datetime.8.4"].get("datetime_error_msg", "cdecl")
    datetime_error_msg.argtypes = []
    if sizeof(c_int) == sizeof(c_void_p):
        datetime_error_msg.restype = ReturnString
    else:
        datetime_error_msg.restype = String
        datetime_error_msg.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 20
if _libs["grass_datetime.8.4"].has("datetime_clear_error", "cdecl"):
    datetime_clear_error = _libs["grass_datetime.8.4"].get("datetime_clear_error", "cdecl")
    datetime_clear_error.argtypes = []
    datetime_clear_error.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 23
if _libs["grass_datetime.8.4"].has("datetime_format", "cdecl"):
    datetime_format = _libs["grass_datetime.8.4"].get("datetime_format", "cdecl")
    datetime_format.argtypes = [POINTER(DateTime), String]
    datetime_format.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 26
if _libs["grass_datetime.8.4"].has("datetime_increment", "cdecl"):
    datetime_increment = _libs["grass_datetime.8.4"].get("datetime_increment", "cdecl")
    datetime_increment.argtypes = [POINTER(DateTime), POINTER(DateTime)]
    datetime_increment.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 29
if _libs["grass_datetime.8.4"].has("datetime_is_valid_increment", "cdecl"):
    datetime_is_valid_increment = _libs["grass_datetime.8.4"].get("datetime_is_valid_increment", "cdecl")
    datetime_is_valid_increment.argtypes = [POINTER(DateTime), POINTER(DateTime)]
    datetime_is_valid_increment.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 30
if _libs["grass_datetime.8.4"].has("datetime_check_increment", "cdecl"):
    datetime_check_increment = _libs["grass_datetime.8.4"].get("datetime_check_increment", "cdecl")
    datetime_check_increment.argtypes = [POINTER(DateTime), POINTER(DateTime)]
    datetime_check_increment.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 33
if _libs["grass_datetime.8.4"].has("datetime_get_increment_type", "cdecl"):
    datetime_get_increment_type = _libs["grass_datetime.8.4"].get("datetime_get_increment_type", "cdecl")
    datetime_get_increment_type.argtypes = [POINTER(DateTime), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    datetime_get_increment_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 35
if _libs["grass_datetime.8.4"].has("datetime_set_increment_type", "cdecl"):
    datetime_set_increment_type = _libs["grass_datetime.8.4"].get("datetime_set_increment_type", "cdecl")
    datetime_set_increment_type.argtypes = [POINTER(DateTime), POINTER(DateTime)]
    datetime_set_increment_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 38
if _libs["grass_datetime.8.4"].has("datetime_get_local_timezone", "cdecl"):
    datetime_get_local_timezone = _libs["grass_datetime.8.4"].get("datetime_get_local_timezone", "cdecl")
    datetime_get_local_timezone.argtypes = [POINTER(c_int)]
    datetime_get_local_timezone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 39
if _libs["grass_datetime.8.4"].has("datetime_get_local_time", "cdecl"):
    datetime_get_local_time = _libs["grass_datetime.8.4"].get("datetime_get_local_time", "cdecl")
    datetime_get_local_time.argtypes = [POINTER(DateTime)]
    datetime_get_local_time.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 42
if _libs["grass_datetime.8.4"].has("datetime_days_in_month", "cdecl"):
    datetime_days_in_month = _libs["grass_datetime.8.4"].get("datetime_days_in_month", "cdecl")
    datetime_days_in_month.argtypes = [c_int, c_int, c_int]
    datetime_days_in_month.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 43
if _libs["grass_datetime.8.4"].has("datetime_is_leap_year", "cdecl"):
    datetime_is_leap_year = _libs["grass_datetime.8.4"].get("datetime_is_leap_year", "cdecl")
    datetime_is_leap_year.argtypes = [c_int, c_int]
    datetime_is_leap_year.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 44
if _libs["grass_datetime.8.4"].has("datetime_days_in_year", "cdecl"):
    datetime_days_in_year = _libs["grass_datetime.8.4"].get("datetime_days_in_year", "cdecl")
    datetime_days_in_year.argtypes = [c_int, c_int]
    datetime_days_in_year.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 47
if _libs["grass_datetime.8.4"].has("datetime_is_same", "cdecl"):
    datetime_is_same = _libs["grass_datetime.8.4"].get("datetime_is_same", "cdecl")
    datetime_is_same.argtypes = [POINTER(DateTime), POINTER(DateTime)]
    datetime_is_same.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 50
if _libs["grass_datetime.8.4"].has("datetime_scan", "cdecl"):
    datetime_scan = _libs["grass_datetime.8.4"].get("datetime_scan", "cdecl")
    datetime_scan.argtypes = [POINTER(DateTime), String]
    datetime_scan.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 53
if _libs["grass_datetime.8.4"].has("datetime_is_positive", "cdecl"):
    datetime_is_positive = _libs["grass_datetime.8.4"].get("datetime_is_positive", "cdecl")
    datetime_is_positive.argtypes = [POINTER(DateTime)]
    datetime_is_positive.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 54
if _libs["grass_datetime.8.4"].has("datetime_is_negative", "cdecl"):
    datetime_is_negative = _libs["grass_datetime.8.4"].get("datetime_is_negative", "cdecl")
    datetime_is_negative.argtypes = [POINTER(DateTime)]
    datetime_is_negative.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 55
if _libs["grass_datetime.8.4"].has("datetime_set_positive", "cdecl"):
    datetime_set_positive = _libs["grass_datetime.8.4"].get("datetime_set_positive", "cdecl")
    datetime_set_positive.argtypes = [POINTER(DateTime)]
    datetime_set_positive.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 56
if _libs["grass_datetime.8.4"].has("datetime_set_negative", "cdecl"):
    datetime_set_negative = _libs["grass_datetime.8.4"].get("datetime_set_negative", "cdecl")
    datetime_set_negative.argtypes = [POINTER(DateTime)]
    datetime_set_negative.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 57
if _libs["grass_datetime.8.4"].has("datetime_invert_sign", "cdecl"):
    datetime_invert_sign = _libs["grass_datetime.8.4"].get("datetime_invert_sign", "cdecl")
    datetime_invert_sign.argtypes = [POINTER(DateTime)]
    datetime_invert_sign.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 60
if _libs["grass_datetime.8.4"].has("datetime_set_type", "cdecl"):
    datetime_set_type = _libs["grass_datetime.8.4"].get("datetime_set_type", "cdecl")
    datetime_set_type.argtypes = [POINTER(DateTime), c_int, c_int, c_int, c_int]
    datetime_set_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 61
if _libs["grass_datetime.8.4"].has("datetime_get_type", "cdecl"):
    datetime_get_type = _libs["grass_datetime.8.4"].get("datetime_get_type", "cdecl")
    datetime_get_type.argtypes = [POINTER(DateTime), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    datetime_get_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 63
if _libs["grass_datetime.8.4"].has("datetime_is_valid_type", "cdecl"):
    datetime_is_valid_type = _libs["grass_datetime.8.4"].get("datetime_is_valid_type", "cdecl")
    datetime_is_valid_type.argtypes = [POINTER(DateTime)]
    datetime_is_valid_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 64
if _libs["grass_datetime.8.4"].has("datetime_check_type", "cdecl"):
    datetime_check_type = _libs["grass_datetime.8.4"].get("datetime_check_type", "cdecl")
    datetime_check_type.argtypes = [POINTER(DateTime)]
    datetime_check_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 65
if _libs["grass_datetime.8.4"].has("datetime_in_interval_year_month", "cdecl"):
    datetime_in_interval_year_month = _libs["grass_datetime.8.4"].get("datetime_in_interval_year_month", "cdecl")
    datetime_in_interval_year_month.argtypes = [c_int]
    datetime_in_interval_year_month.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 66
if _libs["grass_datetime.8.4"].has("datetime_in_interval_day_second", "cdecl"):
    datetime_in_interval_day_second = _libs["grass_datetime.8.4"].get("datetime_in_interval_day_second", "cdecl")
    datetime_in_interval_day_second.argtypes = [c_int]
    datetime_in_interval_day_second.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 67
if _libs["grass_datetime.8.4"].has("datetime_is_absolute", "cdecl"):
    datetime_is_absolute = _libs["grass_datetime.8.4"].get("datetime_is_absolute", "cdecl")
    datetime_is_absolute.argtypes = [POINTER(DateTime)]
    datetime_is_absolute.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 68
if _libs["grass_datetime.8.4"].has("datetime_is_relative", "cdecl"):
    datetime_is_relative = _libs["grass_datetime.8.4"].get("datetime_is_relative", "cdecl")
    datetime_is_relative.argtypes = [POINTER(DateTime)]
    datetime_is_relative.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 71
if _libs["grass_datetime.8.4"].has("datetime_check_timezone", "cdecl"):
    datetime_check_timezone = _libs["grass_datetime.8.4"].get("datetime_check_timezone", "cdecl")
    datetime_check_timezone.argtypes = [POINTER(DateTime), c_int]
    datetime_check_timezone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 72
if _libs["grass_datetime.8.4"].has("datetime_get_timezone", "cdecl"):
    datetime_get_timezone = _libs["grass_datetime.8.4"].get("datetime_get_timezone", "cdecl")
    datetime_get_timezone.argtypes = [POINTER(DateTime), POINTER(c_int)]
    datetime_get_timezone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 73
if _libs["grass_datetime.8.4"].has("datetime_set_timezone", "cdecl"):
    datetime_set_timezone = _libs["grass_datetime.8.4"].get("datetime_set_timezone", "cdecl")
    datetime_set_timezone.argtypes = [POINTER(DateTime), c_int]
    datetime_set_timezone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 74
if _libs["grass_datetime.8.4"].has("datetime_unset_timezone", "cdecl"):
    datetime_unset_timezone = _libs["grass_datetime.8.4"].get("datetime_unset_timezone", "cdecl")
    datetime_unset_timezone.argtypes = [POINTER(DateTime)]
    datetime_unset_timezone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 75
if _libs["grass_datetime.8.4"].has("datetime_is_valid_timezone", "cdecl"):
    datetime_is_valid_timezone = _libs["grass_datetime.8.4"].get("datetime_is_valid_timezone", "cdecl")
    datetime_is_valid_timezone.argtypes = [c_int]
    datetime_is_valid_timezone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 78
if _libs["grass_datetime.8.4"].has("datetime_change_timezone", "cdecl"):
    datetime_change_timezone = _libs["grass_datetime.8.4"].get("datetime_change_timezone", "cdecl")
    datetime_change_timezone.argtypes = [POINTER(DateTime), c_int]
    datetime_change_timezone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 79
if _libs["grass_datetime.8.4"].has("datetime_change_to_utc", "cdecl"):
    datetime_change_to_utc = _libs["grass_datetime.8.4"].get("datetime_change_to_utc", "cdecl")
    datetime_change_to_utc.argtypes = [POINTER(DateTime)]
    datetime_change_to_utc.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 80
if _libs["grass_datetime.8.4"].has("datetime_decompose_timezone", "cdecl"):
    datetime_decompose_timezone = _libs["grass_datetime.8.4"].get("datetime_decompose_timezone", "cdecl")
    datetime_decompose_timezone.argtypes = [c_int, POINTER(c_int), POINTER(c_int)]
    datetime_decompose_timezone.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 83
if _libs["grass_datetime.8.4"].has("datetime_check_year", "cdecl"):
    datetime_check_year = _libs["grass_datetime.8.4"].get("datetime_check_year", "cdecl")
    datetime_check_year.argtypes = [POINTER(DateTime), c_int]
    datetime_check_year.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 84
if _libs["grass_datetime.8.4"].has("datetime_check_month", "cdecl"):
    datetime_check_month = _libs["grass_datetime.8.4"].get("datetime_check_month", "cdecl")
    datetime_check_month.argtypes = [POINTER(DateTime), c_int]
    datetime_check_month.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 85
if _libs["grass_datetime.8.4"].has("datetime_check_day", "cdecl"):
    datetime_check_day = _libs["grass_datetime.8.4"].get("datetime_check_day", "cdecl")
    datetime_check_day.argtypes = [POINTER(DateTime), c_int]
    datetime_check_day.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 86
if _libs["grass_datetime.8.4"].has("datetime_check_hour", "cdecl"):
    datetime_check_hour = _libs["grass_datetime.8.4"].get("datetime_check_hour", "cdecl")
    datetime_check_hour.argtypes = [POINTER(DateTime), c_int]
    datetime_check_hour.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 87
if _libs["grass_datetime.8.4"].has("datetime_check_minute", "cdecl"):
    datetime_check_minute = _libs["grass_datetime.8.4"].get("datetime_check_minute", "cdecl")
    datetime_check_minute.argtypes = [POINTER(DateTime), c_int]
    datetime_check_minute.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 88
if _libs["grass_datetime.8.4"].has("datetime_check_second", "cdecl"):
    datetime_check_second = _libs["grass_datetime.8.4"].get("datetime_check_second", "cdecl")
    datetime_check_second.argtypes = [POINTER(DateTime), c_double]
    datetime_check_second.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 89
if _libs["grass_datetime.8.4"].has("datetime_check_fracsec", "cdecl"):
    datetime_check_fracsec = _libs["grass_datetime.8.4"].get("datetime_check_fracsec", "cdecl")
    datetime_check_fracsec.argtypes = [POINTER(DateTime), c_int]
    datetime_check_fracsec.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 90
if _libs["grass_datetime.8.4"].has("datetime_get_year", "cdecl"):
    datetime_get_year = _libs["grass_datetime.8.4"].get("datetime_get_year", "cdecl")
    datetime_get_year.argtypes = [POINTER(DateTime), POINTER(c_int)]
    datetime_get_year.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 91
if _libs["grass_datetime.8.4"].has("datetime_set_year", "cdecl"):
    datetime_set_year = _libs["grass_datetime.8.4"].get("datetime_set_year", "cdecl")
    datetime_set_year.argtypes = [POINTER(DateTime), c_int]
    datetime_set_year.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 92
if _libs["grass_datetime.8.4"].has("datetime_get_month", "cdecl"):
    datetime_get_month = _libs["grass_datetime.8.4"].get("datetime_get_month", "cdecl")
    datetime_get_month.argtypes = [POINTER(DateTime), POINTER(c_int)]
    datetime_get_month.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 93
if _libs["grass_datetime.8.4"].has("datetime_set_month", "cdecl"):
    datetime_set_month = _libs["grass_datetime.8.4"].get("datetime_set_month", "cdecl")
    datetime_set_month.argtypes = [POINTER(DateTime), c_int]
    datetime_set_month.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 94
if _libs["grass_datetime.8.4"].has("datetime_get_day", "cdecl"):
    datetime_get_day = _libs["grass_datetime.8.4"].get("datetime_get_day", "cdecl")
    datetime_get_day.argtypes = [POINTER(DateTime), POINTER(c_int)]
    datetime_get_day.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 95
if _libs["grass_datetime.8.4"].has("datetime_set_day", "cdecl"):
    datetime_set_day = _libs["grass_datetime.8.4"].get("datetime_set_day", "cdecl")
    datetime_set_day.argtypes = [POINTER(DateTime), c_int]
    datetime_set_day.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 96
if _libs["grass_datetime.8.4"].has("datetime_get_hour", "cdecl"):
    datetime_get_hour = _libs["grass_datetime.8.4"].get("datetime_get_hour", "cdecl")
    datetime_get_hour.argtypes = [POINTER(DateTime), POINTER(c_int)]
    datetime_get_hour.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 97
if _libs["grass_datetime.8.4"].has("datetime_set_hour", "cdecl"):
    datetime_set_hour = _libs["grass_datetime.8.4"].get("datetime_set_hour", "cdecl")
    datetime_set_hour.argtypes = [POINTER(DateTime), c_int]
    datetime_set_hour.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 98
if _libs["grass_datetime.8.4"].has("datetime_get_minute", "cdecl"):
    datetime_get_minute = _libs["grass_datetime.8.4"].get("datetime_get_minute", "cdecl")
    datetime_get_minute.argtypes = [POINTER(DateTime), POINTER(c_int)]
    datetime_get_minute.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 99
if _libs["grass_datetime.8.4"].has("datetime_set_minute", "cdecl"):
    datetime_set_minute = _libs["grass_datetime.8.4"].get("datetime_set_minute", "cdecl")
    datetime_set_minute.argtypes = [POINTER(DateTime), c_int]
    datetime_set_minute.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 100
if _libs["grass_datetime.8.4"].has("datetime_get_second", "cdecl"):
    datetime_get_second = _libs["grass_datetime.8.4"].get("datetime_get_second", "cdecl")
    datetime_get_second.argtypes = [POINTER(DateTime), POINTER(c_double)]
    datetime_get_second.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 101
if _libs["grass_datetime.8.4"].has("datetime_set_second", "cdecl"):
    datetime_set_second = _libs["grass_datetime.8.4"].get("datetime_set_second", "cdecl")
    datetime_set_second.argtypes = [POINTER(DateTime), c_double]
    datetime_set_second.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 102
if _libs["grass_datetime.8.4"].has("datetime_get_fracsec", "cdecl"):
    datetime_get_fracsec = _libs["grass_datetime.8.4"].get("datetime_get_fracsec", "cdecl")
    datetime_get_fracsec.argtypes = [POINTER(DateTime), POINTER(c_int)]
    datetime_get_fracsec.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/datetime.h: 103
if _libs["grass_datetime.8.4"].has("datetime_set_fracsec", "cdecl"):
    datetime_set_fracsec = _libs["grass_datetime.8.4"].get("datetime_set_fracsec", "cdecl")
    datetime_set_fracsec.argtypes = [POINTER(DateTime), c_int]
    datetime_set_fracsec.restype = c_int

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 4
try:
    DATETIME_ABSOLUTE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 5
try:
    DATETIME_RELATIVE = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 10
try:
    DATETIME_YEAR = 101
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 11
try:
    DATETIME_MONTH = 102
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 12
try:
    DATETIME_DAY = 103
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 13
try:
    DATETIME_HOUR = 104
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 14
try:
    DATETIME_MINUTE = 105
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 15
try:
    DATETIME_SECOND = 106
except:
    pass

DateTime = struct_DateTime# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\datetime.h: 26

# No inserted files

# No prefix-stripping

