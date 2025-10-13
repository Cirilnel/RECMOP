r"""Wrapper for vedit.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_vedit.8.4 -IC:/osgeo4w/include -IC:/osgeo4w/include -IC:/osgeo4w/include C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vedit.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h -o OBJ.x86_64-w64-mingw32/vedit.py

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
_libs["grass_vedit.8.4"] = load_library("grass_vedit.8.4")

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

off_t = c_int64# C:/msys64/mingw64/include/_mingw_off_t.h: 24

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/gis.h: 708
class struct_ilist(Structure):
    pass

struct_ilist.__slots__ = [
    'value',
    'n_values',
    'alloc_values',
]
struct_ilist._fields_ = [
    ('value', POINTER(c_int)),
    ('n_values', c_int),
    ('alloc_values', c_int),
]

enum_anon_7 = c_int# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_FeatureType = enum_anon_7# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

dglByte_t = c_ubyte# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/type.h: 35

dglInt32_t = c_long# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/type.h: 36

dglInt64_t = c_longlong# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/type.h: 37

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/heap.h: 32
class union__dglHeapData(Union):
    pass

union__dglHeapData.__slots__ = [
    'pv',
    'n',
    'un',
    'l',
    'ul',
]
union__dglHeapData._fields_ = [
    ('pv', POINTER(None)),
    ('n', c_int),
    ('un', c_uint),
    ('l', c_long),
    ('ul', c_ulong),
]

dglHeapData_u = union__dglHeapData# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/heap.h: 32

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/heap.h: 39
class struct__dglHeapNode(Structure):
    pass

struct__dglHeapNode.__slots__ = [
    'key',
    'value',
    'flags',
]
struct__dglHeapNode._fields_ = [
    ('key', c_long),
    ('value', dglHeapData_u),
    ('flags', c_ubyte),
]

dglHeapNode_s = struct__dglHeapNode# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/heap.h: 39

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/heap.h: 49
class struct__dglHeap(Structure):
    pass

struct__dglHeap.__slots__ = [
    'index',
    'count',
    'block',
    'pnode',
]
struct__dglHeap._fields_ = [
    ('index', c_long),
    ('count', c_long),
    ('block', c_long),
    ('pnode', POINTER(dglHeapNode_s)),
]

dglHeap_s = struct__dglHeap# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/heap.h: 49

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/tree.h: 151
class struct__dglTreeEdgePri32(Structure):
    pass

struct__dglTreeEdgePri32.__slots__ = [
    'nKey',
    'cnData',
    'pnData',
]
struct__dglTreeEdgePri32._fields_ = [
    ('nKey', dglInt32_t),
    ('cnData', dglInt32_t),
    ('pnData', POINTER(dglInt32_t)),
]

dglTreeEdgePri32_s = struct__dglTreeEdgePri32# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/tree.h: 151

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/graph.h: 122
class struct_anon_9(Structure):
    pass

struct_anon_9.__slots__ = [
    'pvAVL',
]
struct_anon_9._fields_ = [
    ('pvAVL', POINTER(None)),
]

dglNodePrioritizer_s = struct_anon_9# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/graph.h: 122

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/graph.h: 132
class struct_anon_10(Structure):
    pass

struct_anon_10.__slots__ = [
    'cEdge',
    'iEdge',
    'pEdgePri32Item',
    'pvAVL',
]
struct_anon_10._fields_ = [
    ('cEdge', c_int),
    ('iEdge', c_int),
    ('pEdgePri32Item', POINTER(dglTreeEdgePri32_s)),
    ('pvAVL', POINTER(None)),
]

dglEdgePrioritizer_s = struct_anon_10# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/graph.h: 132

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/graph.h: 174
class struct__dglGraph(Structure):
    pass

struct__dglGraph.__slots__ = [
    'iErrno',
    'Version',
    'Endian',
    'NodeAttrSize',
    'EdgeAttrSize',
    'aOpaqueSet',
    'cNode',
    'cHead',
    'cTail',
    'cAlone',
    'cEdge',
    'nnCost',
    'Flags',
    'nFamily',
    'nOptions',
    'pNodeTree',
    'pEdgeTree',
    'pNodeBuffer',
    'iNodeBuffer',
    'pEdgeBuffer',
    'iEdgeBuffer',
    'edgePrioritizer',
    'nodePrioritizer',
]
struct__dglGraph._fields_ = [
    ('iErrno', c_int),
    ('Version', dglByte_t),
    ('Endian', dglByte_t),
    ('NodeAttrSize', dglInt32_t),
    ('EdgeAttrSize', dglInt32_t),
    ('aOpaqueSet', dglInt32_t * int(16)),
    ('cNode', dglInt32_t),
    ('cHead', dglInt32_t),
    ('cTail', dglInt32_t),
    ('cAlone', dglInt32_t),
    ('cEdge', dglInt32_t),
    ('nnCost', dglInt64_t),
    ('Flags', dglInt32_t),
    ('nFamily', dglInt32_t),
    ('nOptions', dglInt32_t),
    ('pNodeTree', POINTER(None)),
    ('pEdgeTree', POINTER(None)),
    ('pNodeBuffer', POINTER(dglByte_t)),
    ('iNodeBuffer', dglInt32_t),
    ('pEdgeBuffer', POINTER(dglByte_t)),
    ('iEdgeBuffer', dglInt32_t),
    ('edgePrioritizer', dglEdgePrioritizer_s),
    ('nodePrioritizer', dglNodePrioritizer_s),
]

dglGraph_s = struct__dglGraph# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/graph.h: 174

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/graph.h: 218
class struct_anon_11(Structure):
    pass

struct_anon_11.__slots__ = [
    'nStartNode',
    'NodeHeap',
    'pvVisited',
    'pvPredist',
]
struct_anon_11._fields_ = [
    ('nStartNode', dglInt32_t),
    ('NodeHeap', dglHeap_s),
    ('pvVisited', POINTER(None)),
    ('pvPredist', POINTER(None)),
]

dglSPCache_s = struct_anon_11# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dgl/graph.h: 218

RectReal = c_double# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 26

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 54
class struct_RTree_Rect(Structure):
    pass

struct_RTree_Rect.__slots__ = [
    'boundary',
]
struct_RTree_Rect._fields_ = [
    ('boundary', POINTER(RectReal)),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 72
class struct_RTree_Node(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 60
class union_RTree_Child(Union):
    pass

union_RTree_Child.__slots__ = [
    'id',
    'ptr',
    'pos',
]
union_RTree_Child._fields_ = [
    ('id', c_int),
    ('ptr', POINTER(struct_RTree_Node)),
    ('pos', off_t),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 66
class struct_RTree_Branch(Structure):
    pass

struct_RTree_Branch.__slots__ = [
    'rect',
    'child',
]
struct_RTree_Branch._fields_ = [
    ('rect', struct_RTree_Rect),
    ('child', union_RTree_Child),
]

struct_RTree_Node.__slots__ = [
    'count',
    'level',
    'branch',
]
struct_RTree_Node._fields_ = [
    ('count', c_int),
    ('level', c_int),
    ('branch', POINTER(struct_RTree_Branch)),
]

SearchHitCallback = CFUNCTYPE(UNCHECKED(c_int), c_int, POINTER(struct_RTree_Rect), POINTER(None))# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 86

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 123
class struct_RTree(Structure):
    pass

rt_search_fn = CFUNCTYPE(UNCHECKED(c_int), POINTER(struct_RTree), POINTER(struct_RTree_Rect), POINTER(SearchHitCallback), POINTER(None))# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 90

rt_insert_fn = CFUNCTYPE(UNCHECKED(c_int), POINTER(struct_RTree_Rect), union_RTree_Child, c_int, POINTER(struct_RTree))# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 92

rt_delete_fn = CFUNCTYPE(UNCHECKED(c_int), POINTER(struct_RTree_Rect), union_RTree_Child, POINTER(struct_RTree))# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 94

rt_valid_child_fn = CFUNCTYPE(UNCHECKED(c_int), POINTER(union_RTree_Child))# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 96

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 100
class struct_nstack(Structure):
    pass

struct_nstack.__slots__ = [
    'sn',
    'branch_id',
    'pos',
]
struct_nstack._fields_ = [
    ('sn', POINTER(struct_RTree_Node)),
    ('branch_id', c_int),
    ('pos', off_t),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 107
class struct_NodeBuffer(Structure):
    pass

struct_NodeBuffer.__slots__ = [
    'n',
    'pos',
    'dirty',
]
struct_NodeBuffer._fields_ = [
    ('n', struct_RTree_Node),
    ('pos', off_t),
    ('dirty', c_char),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 114
class struct_RTree_PartitionVars(Structure):
    pass

struct_RTree_PartitionVars.__slots__ = [
    'partition',
    'total',
    'minfill',
    'taken',
    'count',
    'cover',
    'area',
]
struct_RTree_PartitionVars._fields_ = [
    ('partition', c_int * int((9 + 1))),
    ('total', c_int),
    ('minfill', c_int),
    ('taken', c_int * int((9 + 1))),
    ('count', c_int * int(2)),
    ('cover', struct_RTree_Rect * int(2)),
    ('area', RectReal * int(2)),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/rtree.h: 150
class struct__recycle(Structure):
    pass

struct__recycle.__slots__ = [
    'avail',
    'alloc',
    'pos',
]
struct__recycle._fields_ = [
    ('avail', c_int),
    ('alloc', c_int),
    ('pos', POINTER(off_t)),
]

struct_RTree.__slots__ = [
    'fd',
    'ndims',
    'nsides',
    'ndims_alloc',
    'nsides_alloc',
    'nodesize',
    'branchsize',
    'rectsize',
    'n_nodes',
    'n_leafs',
    'rootlevel',
    'nodecard',
    'leafcard',
    'min_node_fill',
    'min_leaf_fill',
    'minfill_node_split',
    'minfill_leaf_split',
    'overflow',
    'free_nodes',
    'nb',
    'used',
    'insert_rect',
    'delete_rect',
    'search_rect',
    'valid_child',
    'root',
    'ns',
    'p',
    'BranchBuf',
    'tmpb1',
    'tmpb2',
    'c',
    'BranchCount',
    'rect_0',
    'rect_1',
    'upperrect',
    'orect',
    'center_n',
    'rootpos',
]
struct_RTree._fields_ = [
    ('fd', c_int),
    ('ndims', c_ubyte),
    ('nsides', c_ubyte),
    ('ndims_alloc', c_ubyte),
    ('nsides_alloc', c_ubyte),
    ('nodesize', c_int),
    ('branchsize', c_int),
    ('rectsize', c_int),
    ('n_nodes', c_int),
    ('n_leafs', c_int),
    ('rootlevel', c_int),
    ('nodecard', c_int),
    ('leafcard', c_int),
    ('min_node_fill', c_int),
    ('min_leaf_fill', c_int),
    ('minfill_node_split', c_int),
    ('minfill_leaf_split', c_int),
    ('overflow', c_char),
    ('free_nodes', struct__recycle),
    ('nb', POINTER(POINTER(struct_NodeBuffer))),
    ('used', POINTER(POINTER(c_int))),
    ('insert_rect', POINTER(rt_insert_fn)),
    ('delete_rect', POINTER(rt_delete_fn)),
    ('search_rect', POINTER(rt_search_fn)),
    ('valid_child', POINTER(rt_valid_child_fn)),
    ('root', POINTER(struct_RTree_Node)),
    ('ns', POINTER(struct_nstack)),
    ('p', struct_RTree_PartitionVars),
    ('BranchBuf', POINTER(struct_RTree_Branch)),
    ('tmpb1', struct_RTree_Branch),
    ('tmpb2', struct_RTree_Branch),
    ('c', struct_RTree_Branch),
    ('BranchCount', c_int),
    ('rect_0', struct_RTree_Rect),
    ('rect_1', struct_RTree_Rect),
    ('upperrect', struct_RTree_Rect),
    ('orect', struct_RTree_Rect),
    ('center_n', POINTER(RectReal)),
    ('rootpos', off_t),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dbmi.h: 152
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

dbDbmscap = struct__dbmscap# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dbmi.h: 157

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dbmi.h: 169
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

dbDriver = struct__db_driver# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/dbmi.h: 169

OGRFeatureH = POINTER(None)# C:/osgeo4w/include/ogr_api.h: 410

OGRLayerH = POINTER(None)# C:/osgeo4w/include/ogr_api.h: 676

OGRDataSourceH = POINTER(None)# C:/osgeo4w/include/ogr_api.h: 678

OGRSFDriverH = POINTER(None)# C:/osgeo4w/include/ogr_api.h: 680

# C:/osgeo4w/include/libpq-fe.h: 186
class struct_pg_conn(Structure):
    pass

PGconn = struct_pg_conn# C:/osgeo4w/include/libpq-fe.h: 186

# C:/osgeo4w/include/libpq-fe.h: 198
class struct_pg_result(Structure):
    pass

PGresult = struct_pg_result# C:/osgeo4w/include/libpq-fe.h: 198

plus_t = c_int# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 41

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 46
class struct_site_att(Structure):
    pass

struct_site_att.__slots__ = [
    'cat',
    'dbl',
    'str',
]
struct_site_att._fields_ = [
    ('cat', c_int),
    ('dbl', POINTER(c_double)),
    ('str', POINTER(POINTER(c_char))),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 64
class struct_bound_box(Structure):
    pass

struct_bound_box.__slots__ = [
    'N',
    'S',
    'E',
    'W',
    'T',
    'B',
]
struct_bound_box._fields_ = [
    ('N', c_double),
    ('S', c_double),
    ('E', c_double),
    ('W', c_double),
    ('T', c_double),
    ('B', c_double),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 94
class struct_gvfile(Structure):
    pass

struct_gvfile.__slots__ = [
    'file',
    'start',
    'current',
    'end',
    'size',
    'alloc',
    'loaded',
]
struct_gvfile._fields_ = [
    ('file', POINTER(FILE)),
    ('start', String),
    ('current', String),
    ('end', String),
    ('size', off_t),
    ('alloc', off_t),
    ('loaded', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 131
class struct_field_info(Structure):
    pass

struct_field_info.__slots__ = [
    'number',
    'name',
    'driver',
    'database',
    'table',
    'key',
]
struct_field_info._fields_ = [
    ('number', c_int),
    ('name', String),
    ('driver', String),
    ('database', String),
    ('table', String),
    ('key', String),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 161
class struct_dblinks(Structure):
    pass

struct_dblinks.__slots__ = [
    'field',
    'alloc_fields',
    'n_fields',
]
struct_dblinks._fields_ = [
    ('field', POINTER(struct_field_info)),
    ('alloc_fields', c_int),
    ('n_fields', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 181
class struct_Port_info(Structure):
    pass

struct_Port_info.__slots__ = [
    'byte_order',
    'off_t_size',
    'dbl_cnvrt',
    'flt_cnvrt',
    'lng_cnvrt',
    'int_cnvrt',
    'shrt_cnvrt',
    'off_t_cnvrt',
    'dbl_quick',
    'flt_quick',
    'lng_quick',
    'int_quick',
    'shrt_quick',
    'off_t_quick',
]
struct_Port_info._fields_ = [
    ('byte_order', c_int),
    ('off_t_size', c_int),
    ('dbl_cnvrt', c_ubyte * int(8)),
    ('flt_cnvrt', c_ubyte * int(4)),
    ('lng_cnvrt', c_ubyte * int(4)),
    ('int_cnvrt', c_ubyte * int(4)),
    ('shrt_cnvrt', c_ubyte * int(2)),
    ('off_t_cnvrt', c_ubyte * int(8)),
    ('dbl_quick', c_int),
    ('flt_quick', c_int),
    ('lng_quick', c_int),
    ('int_quick', c_int),
    ('shrt_quick', c_int),
    ('off_t_quick', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 266
class struct_recycle(Structure):
    pass

struct_recycle.__slots__ = [
    'dummy',
]
struct_recycle._fields_ = [
    ('dummy', c_char),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 271
class struct_Version_info(Structure):
    pass

struct_Version_info.__slots__ = [
    'major',
    'minor',
    'back_major',
    'back_minor',
]
struct_Version_info._fields_ = [
    ('major', c_int),
    ('minor', c_int),
    ('back_major', c_int),
    ('back_minor', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 287
class struct_dig_head(Structure):
    pass

struct_dig_head.__slots__ = [
    'organization',
    'date',
    'user_name',
    'map_name',
    'source_date',
    'orig_scale',
    'comment',
    'proj',
    'plani_zone',
    'digit_thresh',
    'coor_version',
    'with_z',
    'size',
    'head_size',
    'port',
    'last_offset',
    'recycle',
]
struct_dig_head._fields_ = [
    ('organization', String),
    ('date', String),
    ('user_name', String),
    ('map_name', String),
    ('source_date', String),
    ('orig_scale', c_long),
    ('comment', String),
    ('proj', c_int),
    ('plani_zone', c_int),
    ('digit_thresh', c_double),
    ('coor_version', struct_Version_info),
    ('with_z', c_int),
    ('size', off_t),
    ('head_size', c_long),
    ('port', struct_Port_info),
    ('last_offset', off_t),
    ('recycle', POINTER(struct_recycle)),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 388
class struct_Format_info_offset(Structure):
    pass

struct_Format_info_offset.__slots__ = [
    'array',
    'array_num',
    'array_alloc',
]
struct_Format_info_offset._fields_ = [
    ('array', POINTER(c_int)),
    ('array_num', c_int),
    ('array_alloc', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1651
class struct_line_pnts(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 450
class struct_Format_info_cache(Structure):
    pass

struct_Format_info_cache.__slots__ = [
    'lines',
    'lines_types',
    'lines_cats',
    'lines_alloc',
    'lines_num',
    'lines_next',
    'fid',
    'sf_type',
    'ctype',
]
struct_Format_info_cache._fields_ = [
    ('lines', POINTER(POINTER(struct_line_pnts))),
    ('lines_types', POINTER(c_int)),
    ('lines_cats', POINTER(c_int)),
    ('lines_alloc', c_int),
    ('lines_num', c_int),
    ('lines_next', c_int),
    ('fid', c_long),
    ('sf_type', SF_FeatureType),
    ('ctype', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 505
class struct_Format_info_ogr(Structure):
    pass

struct_Format_info_ogr.__slots__ = [
    'driver_name',
    'dsn',
    'layer_name',
    'where',
    'driver',
    'ds',
    'layer',
    'dbdriver',
    'dsn_options',
    'layer_options',
    'cache',
    'feature_cache',
    'offset',
    'next_line',
]
struct_Format_info_ogr._fields_ = [
    ('driver_name', String),
    ('dsn', String),
    ('layer_name', String),
    ('where', String),
    ('driver', OGRSFDriverH),
    ('ds', OGRDataSourceH),
    ('layer', OGRLayerH),
    ('dbdriver', POINTER(dbDriver)),
    ('dsn_options', POINTER(POINTER(c_char))),
    ('layer_options', POINTER(POINTER(c_char))),
    ('cache', struct_Format_info_cache),
    ('feature_cache', OGRFeatureH),
    ('offset', struct_Format_info_offset),
    ('next_line', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 590
class struct_Format_info_pg(Structure):
    pass

struct_Format_info_pg.__slots__ = [
    'conninfo',
    'db_name',
    'schema_name',
    'table_name',
    'where',
    'fid_column',
    'geom_column',
    'feature_type',
    'coor_dim',
    'srid',
    'dbdriver',
    'fi',
    'inTransaction',
    'conn',
    'res',
    'cursor_name',
    'cursor_fid',
    'next_line',
    'cache',
    'offset',
    'topogeom_column',
    'toposchema_name',
    'toposchema_id',
    'topo_geo_only',
]
struct_Format_info_pg._fields_ = [
    ('conninfo', String),
    ('db_name', String),
    ('schema_name', String),
    ('table_name', String),
    ('where', String),
    ('fid_column', String),
    ('geom_column', String),
    ('feature_type', SF_FeatureType),
    ('coor_dim', c_int),
    ('srid', c_int),
    ('dbdriver', POINTER(dbDriver)),
    ('fi', POINTER(struct_field_info)),
    ('inTransaction', c_int),
    ('conn', POINTER(PGconn)),
    ('res', POINTER(PGresult)),
    ('cursor_name', String),
    ('cursor_fid', c_int),
    ('next_line', c_int),
    ('cache', struct_Format_info_cache),
    ('offset', struct_Format_info_offset),
    ('topogeom_column', String),
    ('toposchema_name', String),
    ('toposchema_id', c_int),
    ('topo_geo_only', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 700
class struct_Format_info(Structure):
    pass

struct_Format_info.__slots__ = [
    'i',
    'ogr',
    'pg',
]
struct_Format_info._fields_ = [
    ('i', c_int),
    ('ogr', struct_Format_info_ogr),
    ('pg', struct_Format_info_pg),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 718
class struct_Cat_index(Structure):
    pass

struct_Cat_index.__slots__ = [
    'field',
    'n_cats',
    'a_cats',
    'cat',
    'n_ucats',
    'n_types',
    'type',
    'offset',
]
struct_Cat_index._fields_ = [
    ('field', c_int),
    ('n_cats', c_int),
    ('a_cats', c_int),
    ('cat', POINTER(c_int * int(3))),
    ('n_ucats', c_int),
    ('n_types', c_int),
    ('type', (c_int * int(2)) * int(7)),
    ('offset', off_t),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 771
class struct_anon_67(Structure):
    pass

struct_anon_67.__slots__ = [
    'topo',
    'spidx',
    'cidx',
]
struct_anon_67._fields_ = [
    ('topo', struct_Version_info),
    ('spidx', struct_Version_info),
    ('cidx', struct_Version_info),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1433
class struct_P_node(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1553
class struct_P_line(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1583
class struct_P_area(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1623
class struct_P_isle(Structure):
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1157
class struct_anon_68(Structure):
    pass

struct_anon_68.__slots__ = [
    'do_uplist',
    'uplines',
    'uplines_offset',
    'alloc_uplines',
    'n_uplines',
    'upnodes',
    'alloc_upnodes',
    'n_upnodes',
]
struct_anon_68._fields_ = [
    ('do_uplist', c_int),
    ('uplines', POINTER(c_int)),
    ('uplines_offset', POINTER(off_t)),
    ('alloc_uplines', c_int),
    ('n_uplines', c_int),
    ('upnodes', POINTER(c_int)),
    ('alloc_upnodes', c_int),
    ('n_upnodes', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 769
class struct_Plus_head(Structure):
    pass

struct_Plus_head.__slots__ = [
    'version',
    'with_z',
    'spidx_with_z',
    'off_t_size',
    'head_size',
    'spidx_head_size',
    'cidx_head_size',
    'release_support',
    'port',
    'spidx_port',
    'cidx_port',
    'mode',
    'built',
    'box',
    'Node',
    'Line',
    'Area',
    'Isle',
    'n_plines',
    'n_llines',
    'n_blines',
    'n_clines',
    'n_flines',
    'n_klines',
    'n_vfaces',
    'n_hfaces',
    'n_nodes',
    'n_edges',
    'n_lines',
    'n_areas',
    'n_isles',
    'n_faces',
    'n_volumes',
    'n_holes',
    'alloc_nodes',
    'alloc_edges',
    'alloc_lines',
    'alloc_areas',
    'alloc_isles',
    'alloc_faces',
    'alloc_volumes',
    'alloc_holes',
    'Node_offset',
    'Edge_offset',
    'Line_offset',
    'Area_offset',
    'Isle_offset',
    'Volume_offset',
    'Hole_offset',
    'Spidx_built',
    'Spidx_new',
    'Spidx_file',
    'spidx_fp',
    'Node_spidx_offset',
    'Line_spidx_offset',
    'Area_spidx_offset',
    'Isle_spidx_offset',
    'Face_spidx_offset',
    'Volume_spidx_offset',
    'Hole_spidx_offset',
    'Node_spidx',
    'Line_spidx',
    'Area_spidx',
    'Isle_spidx',
    'Face_spidx',
    'Volume_spidx',
    'Hole_spidx',
    'update_cidx',
    'n_cidx',
    'a_cidx',
    'cidx',
    'cidx_up_to_date',
    'coor_size',
    'coor_mtime',
    'uplist',
]
struct_Plus_head._fields_ = [
    ('version', struct_anon_67),
    ('with_z', c_int),
    ('spidx_with_z', c_int),
    ('off_t_size', c_int),
    ('head_size', c_long),
    ('spidx_head_size', c_long),
    ('cidx_head_size', c_long),
    ('release_support', c_int),
    ('port', struct_Port_info),
    ('spidx_port', struct_Port_info),
    ('cidx_port', struct_Port_info),
    ('mode', c_int),
    ('built', c_int),
    ('box', struct_bound_box),
    ('Node', POINTER(POINTER(struct_P_node))),
    ('Line', POINTER(POINTER(struct_P_line))),
    ('Area', POINTER(POINTER(struct_P_area))),
    ('Isle', POINTER(POINTER(struct_P_isle))),
    ('n_plines', plus_t),
    ('n_llines', plus_t),
    ('n_blines', plus_t),
    ('n_clines', plus_t),
    ('n_flines', plus_t),
    ('n_klines', plus_t),
    ('n_vfaces', plus_t),
    ('n_hfaces', plus_t),
    ('n_nodes', plus_t),
    ('n_edges', plus_t),
    ('n_lines', plus_t),
    ('n_areas', plus_t),
    ('n_isles', plus_t),
    ('n_faces', plus_t),
    ('n_volumes', plus_t),
    ('n_holes', plus_t),
    ('alloc_nodes', plus_t),
    ('alloc_edges', plus_t),
    ('alloc_lines', plus_t),
    ('alloc_areas', plus_t),
    ('alloc_isles', plus_t),
    ('alloc_faces', plus_t),
    ('alloc_volumes', plus_t),
    ('alloc_holes', plus_t),
    ('Node_offset', off_t),
    ('Edge_offset', off_t),
    ('Line_offset', off_t),
    ('Area_offset', off_t),
    ('Isle_offset', off_t),
    ('Volume_offset', off_t),
    ('Hole_offset', off_t),
    ('Spidx_built', c_int),
    ('Spidx_new', c_int),
    ('Spidx_file', c_int),
    ('spidx_fp', struct_gvfile),
    ('Node_spidx_offset', off_t),
    ('Line_spidx_offset', off_t),
    ('Area_spidx_offset', off_t),
    ('Isle_spidx_offset', off_t),
    ('Face_spidx_offset', off_t),
    ('Volume_spidx_offset', off_t),
    ('Hole_spidx_offset', off_t),
    ('Node_spidx', POINTER(struct_RTree)),
    ('Line_spidx', POINTER(struct_RTree)),
    ('Area_spidx', POINTER(struct_RTree)),
    ('Isle_spidx', POINTER(struct_RTree)),
    ('Face_spidx', POINTER(struct_RTree)),
    ('Volume_spidx', POINTER(struct_RTree)),
    ('Hole_spidx', POINTER(struct_RTree)),
    ('update_cidx', c_int),
    ('n_cidx', c_int),
    ('a_cidx', c_int),
    ('cidx', POINTER(struct_Cat_index)),
    ('cidx_up_to_date', c_int),
    ('coor_size', off_t),
    ('coor_mtime', c_long),
    ('uplist', struct_anon_68),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1204
class struct_Graph_info(Structure):
    pass

struct_Graph_info.__slots__ = [
    'line_type',
    'graph_s',
    'spCache',
    'edge_fcosts',
    'edge_bcosts',
    'node_costs',
    'cost_multip',
]
struct_Graph_info._fields_ = [
    ('line_type', c_int),
    ('graph_s', dglGraph_s),
    ('spCache', dglSPCache_s),
    ('edge_fcosts', POINTER(c_double)),
    ('edge_bcosts', POINTER(c_double)),
    ('node_costs', POINTER(c_double)),
    ('cost_multip', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1343
class struct_anon_69(Structure):
    pass

struct_anon_69.__slots__ = [
    'region_flag',
    'box',
    'type_flag',
    'type',
    'field_flag',
    'field',
]
struct_anon_69._fields_ = [
    ('region_flag', c_int),
    ('box', struct_bound_box),
    ('type_flag', c_int),
    ('type', c_int),
    ('field_flag', c_int),
    ('field', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1243
class struct_Map_info(Structure):
    pass

struct_Map_info.__slots__ = [
    'format',
    'temporary',
    'dblnk',
    'plus',
    'open',
    'mode',
    'level',
    'head_only',
    'support_updated',
    'name',
    'mapset',
    'location',
    'gisdbase',
    'next_line',
    'constraint',
    'proj',
    'hist_fp',
    'dgraph',
    'head',
    'dig_fp',
    'fInfo',
    'site_att',
    'n_site_att',
    'n_site_dbl',
    'n_site_str',
]
struct_Map_info._fields_ = [
    ('format', c_int),
    ('temporary', c_int),
    ('dblnk', POINTER(struct_dblinks)),
    ('plus', struct_Plus_head),
    ('open', c_int),
    ('mode', c_int),
    ('level', c_int),
    ('head_only', c_int),
    ('support_updated', c_int),
    ('name', String),
    ('mapset', String),
    ('location', String),
    ('gisdbase', String),
    ('next_line', plus_t),
    ('constraint', struct_anon_69),
    ('proj', c_int),
    ('hist_fp', POINTER(FILE)),
    ('dgraph', struct_Graph_info),
    ('head', struct_dig_head),
    ('dig_fp', struct_gvfile),
    ('fInfo', struct_Format_info),
    ('site_att', POINTER(struct_site_att)),
    ('n_site_att', c_int),
    ('n_site_dbl', c_int),
    ('n_site_str', c_int),
]

struct_P_node.__slots__ = [
    'x',
    'y',
    'z',
    'alloc_lines',
    'n_lines',
    'lines',
    'angles',
]
struct_P_node._fields_ = [
    ('x', c_double),
    ('y', c_double),
    ('z', c_double),
    ('alloc_lines', plus_t),
    ('n_lines', plus_t),
    ('lines', POINTER(plus_t)),
    ('angles', POINTER(c_float)),
]

struct_P_line.__slots__ = [
    'type',
    'offset',
    'topo',
]
struct_P_line._fields_ = [
    ('type', c_char),
    ('offset', off_t),
    ('topo', POINTER(None)),
]

struct_P_area.__slots__ = [
    'n_lines',
    'alloc_lines',
    'lines',
    'centroid',
    'n_isles',
    'alloc_isles',
    'isles',
]
struct_P_area._fields_ = [
    ('n_lines', plus_t),
    ('alloc_lines', plus_t),
    ('lines', POINTER(plus_t)),
    ('centroid', plus_t),
    ('n_isles', plus_t),
    ('alloc_isles', plus_t),
    ('isles', POINTER(plus_t)),
]

struct_P_isle.__slots__ = [
    'n_lines',
    'alloc_lines',
    'lines',
    'area',
]
struct_P_isle._fields_ = [
    ('n_lines', plus_t),
    ('alloc_lines', plus_t),
    ('lines', POINTER(plus_t)),
    ('area', plus_t),
]

struct_line_pnts.__slots__ = [
    'x',
    'y',
    'z',
    'n_points',
    'alloc_points',
]
struct_line_pnts._fields_ = [
    ('x', POINTER(c_double)),
    ('y', POINTER(c_double)),
    ('z', POINTER(c_double)),
    ('n_points', c_int),
    ('alloc_points', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1697
class struct_cat_list(Structure):
    pass

struct_cat_list.__slots__ = [
    'field',
    'min',
    'max',
    'n_ranges',
    'alloc_ranges',
]
struct_cat_list._fields_ = [
    ('field', c_int),
    ('min', POINTER(c_int)),
    ('max', POINTER(c_int)),
    ('n_ranges', c_int),
    ('alloc_ranges', c_int),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 45
class struct_rpoint(Structure):
    pass

struct_rpoint.__slots__ = [
    'x',
    'y',
]
struct_rpoint._fields_ = [
    ('x', c_int),
    ('y', c_int),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 50
class struct_robject(Structure):
    pass

struct_robject.__slots__ = [
    'fid',
    'type',
    'npoints',
    'point',
]
struct_robject._fields_ = [
    ('fid', c_int),
    ('type', c_int),
    ('npoints', c_int),
    ('point', POINTER(struct_rpoint)),
]

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 58
class struct_robject_list(Structure):
    pass

struct_robject_list.__slots__ = [
    'nitems',
    'item',
]
struct_robject_list._fields_ = [
    ('nitems', c_int),
    ('item', POINTER(POINTER(struct_robject))),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 5
if _libs["grass_vedit.8.4"].has("Vedit_split_lines", "cdecl"):
    Vedit_split_lines = _libs["grass_vedit.8.4"].get("Vedit_split_lines", "cdecl")
    Vedit_split_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), POINTER(struct_line_pnts), c_double, POINTER(struct_ilist)]
    Vedit_split_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 7
if _libs["grass_vedit.8.4"].has("Vedit_connect_lines", "cdecl"):
    Vedit_connect_lines = _libs["grass_vedit.8.4"].get("Vedit_connect_lines", "cdecl")
    Vedit_connect_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), c_double]
    Vedit_connect_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 10
if _libs["grass_vedit.8.4"].has("Vedit_extend_lines", "cdecl"):
    Vedit_extend_lines = _libs["grass_vedit.8.4"].get("Vedit_extend_lines", "cdecl")
    Vedit_extend_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), c_int, c_int, c_double]
    Vedit_extend_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 13
if _libs["grass_vedit.8.4"].has("Vedit_modify_cats", "cdecl"):
    Vedit_modify_cats = _libs["grass_vedit.8.4"].get("Vedit_modify_cats", "cdecl")
    Vedit_modify_cats.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), c_int, c_int, POINTER(struct_cat_list)]
    Vedit_modify_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 17
if _libs["grass_vedit.8.4"].has("Vedit_copy_lines", "cdecl"):
    Vedit_copy_lines = _libs["grass_vedit.8.4"].get("Vedit_copy_lines", "cdecl")
    Vedit_copy_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info), POINTER(struct_ilist)]
    Vedit_copy_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 20
if _libs["grass_vedit.8.4"].has("Vedit_chtype_lines", "cdecl"):
    Vedit_chtype_lines = _libs["grass_vedit.8.4"].get("Vedit_chtype_lines", "cdecl")
    Vedit_chtype_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist)]
    Vedit_chtype_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 23
if _libs["grass_vedit.8.4"].has("Vedit_delete_lines", "cdecl"):
    Vedit_delete_lines = _libs["grass_vedit.8.4"].get("Vedit_delete_lines", "cdecl")
    Vedit_delete_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist)]
    Vedit_delete_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 24
if _libs["grass_vedit.8.4"].has("Vedit_delete_area_centroid", "cdecl"):
    Vedit_delete_area_centroid = _libs["grass_vedit.8.4"].get("Vedit_delete_area_centroid", "cdecl")
    Vedit_delete_area_centroid.argtypes = [POINTER(struct_Map_info), c_int]
    Vedit_delete_area_centroid.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 25
if _libs["grass_vedit.8.4"].has("Vedit_delete_area", "cdecl"):
    Vedit_delete_area = _libs["grass_vedit.8.4"].get("Vedit_delete_area", "cdecl")
    Vedit_delete_area.argtypes = [POINTER(struct_Map_info), c_int]
    Vedit_delete_area.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 26
if _libs["grass_vedit.8.4"].has("Vedit_delete_areas_cat", "cdecl"):
    Vedit_delete_areas_cat = _libs["grass_vedit.8.4"].get("Vedit_delete_areas_cat", "cdecl")
    Vedit_delete_areas_cat.argtypes = [POINTER(struct_Map_info), c_int, c_int]
    Vedit_delete_areas_cat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 29
if _libs["grass_vedit.8.4"].has("Vedit_get_min_distance", "cdecl"):
    Vedit_get_min_distance = _libs["grass_vedit.8.4"].get("Vedit_get_min_distance", "cdecl")
    Vedit_get_min_distance.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), c_int, POINTER(c_int)]
    Vedit_get_min_distance.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 33
if _libs["grass_vedit.8.4"].has("Vedit_flip_lines", "cdecl"):
    Vedit_flip_lines = _libs["grass_vedit.8.4"].get("Vedit_flip_lines", "cdecl")
    Vedit_flip_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist)]
    Vedit_flip_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 36
if _libs["grass_vedit.8.4"].has("Vedit_merge_lines", "cdecl"):
    Vedit_merge_lines = _libs["grass_vedit.8.4"].get("Vedit_merge_lines", "cdecl")
    Vedit_merge_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist)]
    Vedit_merge_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 39
if _libs["grass_vedit.8.4"].has("Vedit_move_lines", "cdecl"):
    Vedit_move_lines = _libs["grass_vedit.8.4"].get("Vedit_move_lines", "cdecl")
    Vedit_move_lines.argtypes = [POINTER(struct_Map_info), POINTER(POINTER(struct_Map_info)), c_int, POINTER(struct_ilist), c_double, c_double, c_double, c_int, c_double]
    Vedit_move_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 43
if _libs["grass_vedit.8.4"].has("Vedit_render_map", "cdecl"):
    Vedit_render_map = _libs["grass_vedit.8.4"].get("Vedit_render_map", "cdecl")
    Vedit_render_map.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box), c_int, c_double, c_double, c_int, c_int, c_double]
    Vedit_render_map.restype = POINTER(struct_robject_list)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 47
if _libs["grass_vedit.8.4"].has("Vedit_select_by_query", "cdecl"):
    Vedit_select_by_query = _libs["grass_vedit.8.4"].get("Vedit_select_by_query", "cdecl")
    Vedit_select_by_query.argtypes = [POINTER(struct_Map_info), c_int, c_int, c_double, c_int, POINTER(struct_ilist)]
    Vedit_select_by_query.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 51
if _libs["grass_vedit.8.4"].has("Vedit_snap_point", "cdecl"):
    Vedit_snap_point = _libs["grass_vedit.8.4"].get("Vedit_snap_point", "cdecl")
    Vedit_snap_point.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double), c_double, c_int]
    Vedit_snap_point.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 53
if _libs["grass_vedit.8.4"].has("Vedit_snap_line", "cdecl"):
    Vedit_snap_line = _libs["grass_vedit.8.4"].get("Vedit_snap_line", "cdecl")
    Vedit_snap_line.argtypes = [POINTER(struct_Map_info), POINTER(POINTER(struct_Map_info)), c_int, c_int, POINTER(struct_line_pnts), c_double, c_int]
    Vedit_snap_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 55
if _libs["grass_vedit.8.4"].has("Vedit_snap_lines", "cdecl"):
    Vedit_snap_lines = _libs["grass_vedit.8.4"].get("Vedit_snap_lines", "cdecl")
    Vedit_snap_lines.argtypes = [POINTER(struct_Map_info), POINTER(POINTER(struct_Map_info)), c_int, POINTER(struct_ilist), c_double, c_int]
    Vedit_snap_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 59
if _libs["grass_vedit.8.4"].has("Vedit_move_vertex", "cdecl"):
    Vedit_move_vertex = _libs["grass_vedit.8.4"].get("Vedit_move_vertex", "cdecl")
    Vedit_move_vertex.argtypes = [POINTER(struct_Map_info), POINTER(POINTER(struct_Map_info)), c_int, POINTER(struct_ilist), POINTER(struct_line_pnts), c_double, c_double, c_double, c_double, c_double, c_int, c_int]
    Vedit_move_vertex.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 62
if _libs["grass_vedit.8.4"].has("Vedit_add_vertex", "cdecl"):
    Vedit_add_vertex = _libs["grass_vedit.8.4"].get("Vedit_add_vertex", "cdecl")
    Vedit_add_vertex.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), POINTER(struct_line_pnts), c_double]
    Vedit_add_vertex.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 64
if _libs["grass_vedit.8.4"].has("Vedit_remove_vertex", "cdecl"):
    Vedit_remove_vertex = _libs["grass_vedit.8.4"].get("Vedit_remove_vertex", "cdecl")
    Vedit_remove_vertex.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), POINTER(struct_line_pnts), c_double]
    Vedit_remove_vertex.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vedit.h: 68
if _libs["grass_vedit.8.4"].has("Vedit_bulk_labeling", "cdecl"):
    Vedit_bulk_labeling = _libs["grass_vedit.8.4"].get("Vedit_bulk_labeling", "cdecl")
    Vedit_bulk_labeling.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), c_double, c_double, c_double, c_double, c_double, c_double]
    Vedit_bulk_labeling.restype = c_int

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 7
try:
    NO_SNAP = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 8
try:
    SNAP = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 9
try:
    SNAPVERTEX = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 11
try:
    QUERY_UNKNOWN = (-1)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 12
try:
    QUERY_LENGTH = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 13
try:
    QUERY_DANGLE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 16
try:
    TYPE_POINT = 0x01
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 17
try:
    TYPE_LINE = 0x02
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 18
try:
    TYPE_BOUNDARYNO = 0x04
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 19
try:
    TYPE_BOUNDARYTWO = 0x08
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 20
try:
    TYPE_BOUNDARYONE = 0x10
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 21
try:
    TYPE_CENTROIDIN = 0x20
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 22
try:
    TYPE_CENTROIDOUT = 0x40
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 23
try:
    TYPE_CENTROIDDUP = 0x80
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 24
try:
    TYPE_NODEONE = 0x100
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 25
try:
    TYPE_NODETWO = 0x200
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 26
try:
    TYPE_VERTEX = 0x400
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 27
try:
    TYPE_AREA = 0x800
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 28
try:
    TYPE_ISLE = 0x1000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 29
try:
    TYPE_DIRECTION = 0x2000
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 31
try:
    DRAW_POINT = 0x01
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 32
try:
    DRAW_LINE = 0x02
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 33
try:
    DRAW_BOUNDARYNO = 0x04
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 34
try:
    DRAW_BOUNDARYTWO = 0x08
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 35
try:
    DRAW_BOUNDARYONE = 0x10
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 36
try:
    DRAW_CENTROIDIN = 0x20
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 37
try:
    DRAW_CENTROIDOUT = 0x40
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 38
try:
    DRAW_CENTROIDDUP = 0x80
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 39
try:
    DRAW_NODEONE = 0x100
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 40
try:
    DRAW_NODETWO = 0x200
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 41
try:
    DRAW_VERTEX = 0x400
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 42
try:
    DRAW_AREA = 0x800
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 43
try:
    DRAW_DIRECTION = 0x1000
except:
    pass

rpoint = struct_rpoint# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 45

robject = struct_robject# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 50

robject_list = struct_robject_list# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vedit.h: 58

# No inserted files

# No prefix-stripping

