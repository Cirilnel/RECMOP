r"""Wrapper for vector.h

Generated with:
./run.py --no-embed-preamble C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32 --cpp x86_64-w64-mingw32-gcc -E -I/c/osgeo4w/include -D_FILE_OFFSET_BITS=64     -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -I/usr/src/grass841/dist.x86_64-w64-mingw32/include -D__GLIBC_HAVE_LONG_LONG -lgrass_vector.8.4 -IC:/osgeo4w/include -IC:/osgeo4w/include -IC:/osgeo4w/include C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vector.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h -o OBJ.x86_64-w64-mingw32/vector.py

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
_libs["grass_vector.8.4"] = load_library("grass_vector.8.4")

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

enum_overlay_operator = c_int# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 210

GV_O_AND = 0# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 210

GV_O_OVERLAP = (GV_O_AND + 1)# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 210

OVERLAY_OPERATOR = enum_overlay_operator# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 212

enum_anon_7 = c_int# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_GEOMETRY = 0# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_POINT = 1# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_LINESTRING = 2# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_POLYGON = 3# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_MULTIPOINT = 4# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_MULTILINESTRING = 5# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_MULTIPOLYGON = 6# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_GEOMETRYCOLLECTION = 7# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_NONE = 100# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_LINEARRING = 101# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_POINT25D = 0x80000001# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_LINESTRING25D = 0x80000002# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_POLYGON25D = 0x80000003# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_MULTIPOINT25D = 0x80000004# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_MULTILINESTRING25D = 0x80000005# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_MULTIPOLYGON25D = 0x80000006# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

SF_GEOMETRYCOLLECTION25D = 0x80000007# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 261

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

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 371
class struct_Coor_info(Structure):
    pass

struct_Coor_info.__slots__ = [
    'size',
    'mtime',
]
struct_Coor_info._fields_ = [
    ('size', off_t),
    ('mtime', c_long),
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

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1478
class struct_P_topo_l(Structure):
    pass

struct_P_topo_l.__slots__ = [
    'N1',
    'N2',
]
struct_P_topo_l._fields_ = [
    ('N1', plus_t),
    ('N2', plus_t),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1492
class struct_P_topo_b(Structure):
    pass

struct_P_topo_b.__slots__ = [
    'N1',
    'N2',
    'left',
    'right',
]
struct_P_topo_b._fields_ = [
    ('N1', plus_t),
    ('N2', plus_t),
    ('left', plus_t),
    ('right', plus_t),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1514
class struct_P_topo_c(Structure):
    pass

struct_P_topo_c.__slots__ = [
    'area',
]
struct_P_topo_c._fields_ = [
    ('area', plus_t),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1524
class struct_P_topo_f(Structure):
    pass

struct_P_topo_f.__slots__ = [
    'E',
    'left',
    'right',
]
struct_P_topo_f._fields_ = [
    ('E', plus_t * int(3)),
    ('left', plus_t),
    ('right', plus_t),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1543
class struct_P_topo_k(Structure):
    pass

struct_P_topo_k.__slots__ = [
    'volume',
]
struct_P_topo_k._fields_ = [
    ('volume', plus_t),
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

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1677
class struct_line_cats(Structure):
    pass

struct_line_cats.__slots__ = [
    'field',
    'cat',
    'n_cats',
    'alloc_cats',
]
struct_line_cats._fields_ = [
    ('field', POINTER(c_int)),
    ('cat', POINTER(c_int)),
    ('n_cats', c_int),
    ('alloc_cats', c_int),
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

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1723
class struct_boxlist(Structure):
    pass

struct_boxlist.__slots__ = [
    'id',
    'box',
    'have_boxes',
    'n_values',
    'alloc_values',
]
struct_boxlist._fields_ = [
    ('id', POINTER(c_int)),
    ('box', POINTER(struct_bound_box)),
    ('have_boxes', c_int),
    ('n_values', c_int),
    ('alloc_values', c_int),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1751
class struct_varray(Structure):
    pass

struct_varray.__slots__ = [
    'size',
    'c',
]
struct_varray._fields_ = [
    ('size', c_int),
    ('c', POINTER(c_int)),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1770
class struct_spatial_index(Structure):
    pass

struct_spatial_index.__slots__ = [
    'si_tree',
    'name',
]
struct_spatial_index._fields_ = [
    ('si_tree', POINTER(struct_RTree)),
    ('name', String),
]

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 13
for _lib in _libs.values():
    if not _lib.has("dig_alloc_space", "cdecl"):
        continue
    dig_alloc_space = _lib.get("dig_alloc_space", "cdecl")
    dig_alloc_space.argtypes = [c_int, POINTER(c_int), c_int, POINTER(None), c_int]
    dig_alloc_space.restype = POINTER(c_ubyte)
    dig_alloc_space.errcheck = lambda v,*a : cast(v, c_void_p)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 15
for _lib in _libs.values():
    if not _lib.has("dig__alloc_space", "cdecl"):
        continue
    dig__alloc_space = _lib.get("dig__alloc_space", "cdecl")
    dig__alloc_space.argtypes = [c_int, POINTER(c_int), c_int, POINTER(None), c_int]
    dig__alloc_space.restype = POINTER(c_ubyte)
    dig__alloc_space.errcheck = lambda v,*a : cast(v, c_void_p)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 18
for _lib in _libs.values():
    if not _lib.has("dig_falloc", "cdecl"):
        continue
    dig_falloc = _lib.get("dig_falloc", "cdecl")
    dig_falloc.argtypes = [c_int, c_int]
    dig_falloc.restype = POINTER(c_ubyte)
    dig_falloc.errcheck = lambda v,*a : cast(v, c_void_p)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 19
for _lib in _libs.values():
    if not _lib.has("dig_frealloc", "cdecl"):
        continue
    dig_frealloc = _lib.get("dig_frealloc", "cdecl")
    dig_frealloc.argtypes = [POINTER(None), c_int, c_int, c_int]
    dig_frealloc.restype = POINTER(c_ubyte)
    dig_frealloc.errcheck = lambda v,*a : cast(v, c_void_p)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 21
for _lib in _libs.values():
    if not _lib.has("dig__falloc", "cdecl"):
        continue
    dig__falloc = _lib.get("dig__falloc", "cdecl")
    dig__falloc.argtypes = [c_int, c_int]
    dig__falloc.restype = POINTER(c_ubyte)
    dig__falloc.errcheck = lambda v,*a : cast(v, c_void_p)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 22
for _lib in _libs.values():
    if not _lib.has("dig__frealloc", "cdecl"):
        continue
    dig__frealloc = _lib.get("dig__frealloc", "cdecl")
    dig__frealloc.argtypes = [POINTER(None), c_int, c_int, c_int]
    dig__frealloc.restype = POINTER(c_ubyte)
    dig__frealloc.errcheck = lambda v,*a : cast(v, c_void_p)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 26
for _lib in _libs.values():
    if not _lib.has("dig_calc_begin_angle", "cdecl"):
        continue
    dig_calc_begin_angle = _lib.get("dig_calc_begin_angle", "cdecl")
    dig_calc_begin_angle.argtypes = [POINTER(struct_line_pnts), c_double]
    dig_calc_begin_angle.restype = c_float
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 27
for _lib in _libs.values():
    if not _lib.has("dig_calc_end_angle", "cdecl"):
        continue
    dig_calc_end_angle = _lib.get("dig_calc_end_angle", "cdecl")
    dig_calc_end_angle.argtypes = [POINTER(struct_line_pnts), c_double]
    dig_calc_end_angle.restype = c_float
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 28
for _lib in _libs.values():
    if not _lib.has("dig_line_degenerate", "cdecl"):
        continue
    dig_line_degenerate = _lib.get("dig_line_degenerate", "cdecl")
    dig_line_degenerate.argtypes = [POINTER(struct_line_pnts)]
    dig_line_degenerate.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 29
for _lib in _libs.values():
    if not _lib.has("dig_is_line_degenerate", "cdecl"):
        continue
    dig_is_line_degenerate = _lib.get("dig_is_line_degenerate", "cdecl")
    dig_is_line_degenerate.argtypes = [POINTER(struct_line_pnts), c_double]
    dig_is_line_degenerate.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 32
for _lib in _libs.values():
    if not _lib.has("dig_box_copy", "cdecl"):
        continue
    dig_box_copy = _lib.get("dig_box_copy", "cdecl")
    dig_box_copy.argtypes = [POINTER(struct_bound_box), POINTER(struct_bound_box)]
    dig_box_copy.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 33
for _lib in _libs.values():
    if not _lib.has("dig_box_extend", "cdecl"):
        continue
    dig_box_extend = _lib.get("dig_box_extend", "cdecl")
    dig_box_extend.argtypes = [POINTER(struct_bound_box), POINTER(struct_bound_box)]
    dig_box_extend.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 34
for _lib in _libs.values():
    if not _lib.has("dig_line_box", "cdecl"):
        continue
    dig_line_box = _lib.get("dig_line_box", "cdecl")
    dig_line_box.argtypes = [POINTER(struct_line_pnts), POINTER(struct_bound_box)]
    dig_line_box.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 47
for _lib in _libs.values():
    if not _lib.has("dig_cidx_init", "cdecl"):
        continue
    dig_cidx_init = _lib.get("dig_cidx_init", "cdecl")
    dig_cidx_init.argtypes = [POINTER(struct_Plus_head)]
    dig_cidx_init.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 48
for _lib in _libs.values():
    if not _lib.has("dig_cidx_free", "cdecl"):
        continue
    dig_cidx_free = _lib.get("dig_cidx_free", "cdecl")
    dig_cidx_free.argtypes = [POINTER(struct_Plus_head)]
    dig_cidx_free.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 49
for _lib in _libs.values():
    if not _lib.has("dig_cidx_add_cat", "cdecl"):
        continue
    dig_cidx_add_cat = _lib.get("dig_cidx_add_cat", "cdecl")
    dig_cidx_add_cat.argtypes = [POINTER(struct_Plus_head), c_int, c_int, c_int, c_int]
    dig_cidx_add_cat.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 50
for _lib in _libs.values():
    if not _lib.has("dig_cidx_add_cat_sorted", "cdecl"):
        continue
    dig_cidx_add_cat_sorted = _lib.get("dig_cidx_add_cat_sorted", "cdecl")
    dig_cidx_add_cat_sorted.argtypes = [POINTER(struct_Plus_head), c_int, c_int, c_int, c_int]
    dig_cidx_add_cat_sorted.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 51
for _lib in _libs.values():
    if not _lib.has("dig_cidx_del_cat", "cdecl"):
        continue
    dig_cidx_del_cat = _lib.get("dig_cidx_del_cat", "cdecl")
    dig_cidx_del_cat.argtypes = [POINTER(struct_Plus_head), c_int, c_int, c_int, c_int]
    dig_cidx_del_cat.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 52
for _lib in _libs.values():
    if not _lib.has("dig_cidx_sort", "cdecl"):
        continue
    dig_cidx_sort = _lib.get("dig_cidx_sort", "cdecl")
    dig_cidx_sort.argtypes = [POINTER(struct_Plus_head)]
    dig_cidx_sort.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 55
for _lib in _libs.values():
    if not _lib.has("dig_write_cidx_head", "cdecl"):
        continue
    dig_write_cidx_head = _lib.get("dig_write_cidx_head", "cdecl")
    dig_write_cidx_head.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_write_cidx_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 56
for _lib in _libs.values():
    if not _lib.has("dig_read_cidx_head", "cdecl"):
        continue
    dig_read_cidx_head = _lib.get("dig_read_cidx_head", "cdecl")
    dig_read_cidx_head.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_read_cidx_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 57
for _lib in _libs.values():
    if not _lib.has("dig_write_cidx", "cdecl"):
        continue
    dig_write_cidx = _lib.get("dig_write_cidx", "cdecl")
    dig_write_cidx.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_write_cidx.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 58
for _lib in _libs.values():
    if not _lib.has("dig_read_cidx", "cdecl"):
        continue
    dig_read_cidx = _lib.get("dig_read_cidx", "cdecl")
    dig_read_cidx.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head), c_int]
    dig_read_cidx.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 62
for _lib in _libs.values():
    if not _lib.has("dig_ftell", "cdecl"):
        continue
    dig_ftell = _lib.get("dig_ftell", "cdecl")
    dig_ftell.argtypes = [POINTER(struct_gvfile)]
    dig_ftell.restype = off_t
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 63
for _lib in _libs.values():
    if not _lib.has("dig_fseek", "cdecl"):
        continue
    dig_fseek = _lib.get("dig_fseek", "cdecl")
    dig_fseek.argtypes = [POINTER(struct_gvfile), off_t, c_int]
    dig_fseek.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 64
for _lib in _libs.values():
    if not _lib.has("dig_rewind", "cdecl"):
        continue
    dig_rewind = _lib.get("dig_rewind", "cdecl")
    dig_rewind.argtypes = [POINTER(struct_gvfile)]
    dig_rewind.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 65
for _lib in _libs.values():
    if not _lib.has("dig_fflush", "cdecl"):
        continue
    dig_fflush = _lib.get("dig_fflush", "cdecl")
    dig_fflush.argtypes = [POINTER(struct_gvfile)]
    dig_fflush.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 66
for _lib in _libs.values():
    if not _lib.has("dig_fread", "cdecl"):
        continue
    dig_fread = _lib.get("dig_fread", "cdecl")
    dig_fread.argtypes = [POINTER(None), c_size_t, c_size_t, POINTER(struct_gvfile)]
    dig_fread.restype = c_size_t
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 67
for _lib in _libs.values():
    if not _lib.has("dig_fwrite", "cdecl"):
        continue
    dig_fwrite = _lib.get("dig_fwrite", "cdecl")
    dig_fwrite.argtypes = [POINTER(None), c_size_t, c_size_t, POINTER(struct_gvfile)]
    dig_fwrite.restype = c_size_t
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 69
for _lib in _libs.values():
    if not _lib.has("dig_file_init", "cdecl"):
        continue
    dig_file_init = _lib.get("dig_file_init", "cdecl")
    dig_file_init.argtypes = [POINTER(struct_gvfile)]
    dig_file_init.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 70
for _lib in _libs.values():
    if not _lib.has("dig_file_load", "cdecl"):
        continue
    dig_file_load = _lib.get("dig_file_load", "cdecl")
    dig_file_load.argtypes = [POINTER(struct_gvfile)]
    dig_file_load.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 71
for _lib in _libs.values():
    if not _lib.has("dig_file_free", "cdecl"):
        continue
    dig_file_free = _lib.get("dig_file_free", "cdecl")
    dig_file_free.argtypes = [POINTER(struct_gvfile)]
    dig_file_free.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 74
for _lib in _libs.values():
    if not _lib.has("dig_write_frmt_ascii", "cdecl"):
        continue
    dig_write_frmt_ascii = _lib.get("dig_write_frmt_ascii", "cdecl")
    dig_write_frmt_ascii.argtypes = [POINTER(FILE), POINTER(struct_Format_info), c_int]
    dig_write_frmt_ascii.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 75
for _lib in _libs.values():
    if not _lib.has("dig_read_frmt_ascii", "cdecl"):
        continue
    dig_read_frmt_ascii = _lib.get("dig_read_frmt_ascii", "cdecl")
    dig_read_frmt_ascii.argtypes = [POINTER(FILE), POINTER(struct_Format_info)]
    dig_read_frmt_ascii.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 78
for _lib in _libs.values():
    if not _lib.has("dig__write_head", "cdecl"):
        continue
    dig__write_head = _lib.get("dig__write_head", "cdecl")
    dig__write_head.argtypes = [POINTER(struct_Map_info)]
    dig__write_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 79
for _lib in _libs.values():
    if not _lib.has("dig__read_head", "cdecl"):
        continue
    dig__read_head = _lib.get("dig__read_head", "cdecl")
    dig__read_head.argtypes = [POINTER(struct_Map_info)]
    dig__read_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 82
for _lib in _libs.values():
    if not _lib.has("dig_x_intersect", "cdecl"):
        continue
    dig_x_intersect = _lib.get("dig_x_intersect", "cdecl")
    dig_x_intersect.argtypes = [c_double, c_double, c_double, c_double, c_double]
    dig_x_intersect.restype = c_double
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 85
for _lib in _libs.values():
    if not _lib.has("dig_test_for_intersection", "cdecl"):
        continue
    dig_test_for_intersection = _lib.get("dig_test_for_intersection", "cdecl")
    dig_test_for_intersection.argtypes = [c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double]
    dig_test_for_intersection.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 87
for _lib in _libs.values():
    if not _lib.has("dig_find_intersection", "cdecl"):
        continue
    dig_find_intersection = _lib.get("dig_find_intersection", "cdecl")
    dig_find_intersection.argtypes = [c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, POINTER(c_double), POINTER(c_double)]
    dig_find_intersection.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 91
for _lib in _libs.values():
    if not _lib.has("dig_distance2_point_to_line", "cdecl"):
        continue
    dig_distance2_point_to_line = _lib.get("dig_distance2_point_to_line", "cdecl")
    dig_distance2_point_to_line.argtypes = [c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_int)]
    dig_distance2_point_to_line.restype = c_double
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 95
for _lib in _libs.values():
    if not _lib.has("dig_set_distance_to_line_tolerance", "cdecl"):
        continue
    dig_set_distance_to_line_tolerance = _lib.get("dig_set_distance_to_line_tolerance", "cdecl")
    dig_set_distance_to_line_tolerance.argtypes = [c_double]
    dig_set_distance_to_line_tolerance.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 98
for _lib in _libs.values():
    if not _lib.has("dig_init_boxlist", "cdecl"):
        continue
    dig_init_boxlist = _lib.get("dig_init_boxlist", "cdecl")
    dig_init_boxlist.argtypes = [POINTER(struct_boxlist), c_int]
    dig_init_boxlist.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 99
for _lib in _libs.values():
    if not _lib.has("dig_boxlist_add", "cdecl"):
        continue
    dig_boxlist_add = _lib.get("dig_boxlist_add", "cdecl")
    dig_boxlist_add.argtypes = [POINTER(struct_boxlist), c_int, POINTER(struct_bound_box)]
    dig_boxlist_add.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 102
for _lib in _libs.values():
    if not _lib.has("dig_init_plus", "cdecl"):
        continue
    dig_init_plus = _lib.get("dig_init_plus", "cdecl")
    dig_init_plus.argtypes = [POINTER(struct_Plus_head)]
    dig_init_plus.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 103
for _lib in _libs.values():
    if not _lib.has("dig_free_plus_nodes", "cdecl"):
        continue
    dig_free_plus_nodes = _lib.get("dig_free_plus_nodes", "cdecl")
    dig_free_plus_nodes.argtypes = [POINTER(struct_Plus_head)]
    dig_free_plus_nodes.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 104
for _lib in _libs.values():
    if not _lib.has("dig_free_plus_lines", "cdecl"):
        continue
    dig_free_plus_lines = _lib.get("dig_free_plus_lines", "cdecl")
    dig_free_plus_lines.argtypes = [POINTER(struct_Plus_head)]
    dig_free_plus_lines.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 105
for _lib in _libs.values():
    if not _lib.has("dig_free_plus_areas", "cdecl"):
        continue
    dig_free_plus_areas = _lib.get("dig_free_plus_areas", "cdecl")
    dig_free_plus_areas.argtypes = [POINTER(struct_Plus_head)]
    dig_free_plus_areas.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 106
for _lib in _libs.values():
    if not _lib.has("dig_free_plus_isles", "cdecl"):
        continue
    dig_free_plus_isles = _lib.get("dig_free_plus_isles", "cdecl")
    dig_free_plus_isles.argtypes = [POINTER(struct_Plus_head)]
    dig_free_plus_isles.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 107
for _lib in _libs.values():
    if not _lib.has("dig_free_plus", "cdecl"):
        continue
    dig_free_plus = _lib.get("dig_free_plus", "cdecl")
    dig_free_plus.argtypes = [POINTER(struct_Plus_head)]
    dig_free_plus.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 108
for _lib in _libs.values():
    if not _lib.has("dig_load_plus", "cdecl"):
        continue
    dig_load_plus = _lib.get("dig_load_plus", "cdecl")
    dig_load_plus.argtypes = [POINTER(struct_Plus_head), POINTER(struct_gvfile), c_int]
    dig_load_plus.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 109
for _lib in _libs.values():
    if not _lib.has("dig_write_plus_file", "cdecl"):
        continue
    dig_write_plus_file = _lib.get("dig_write_plus_file", "cdecl")
    dig_write_plus_file.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_write_plus_file.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 110
for _lib in _libs.values():
    if not _lib.has("dig_write_nodes", "cdecl"):
        continue
    dig_write_nodes = _lib.get("dig_write_nodes", "cdecl")
    dig_write_nodes.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_write_nodes.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 111
for _lib in _libs.values():
    if not _lib.has("dig_write_lines", "cdecl"):
        continue
    dig_write_lines = _lib.get("dig_write_lines", "cdecl")
    dig_write_lines.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_write_lines.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 112
for _lib in _libs.values():
    if not _lib.has("dig_write_areas", "cdecl"):
        continue
    dig_write_areas = _lib.get("dig_write_areas", "cdecl")
    dig_write_areas.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_write_areas.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 113
for _lib in _libs.values():
    if not _lib.has("dig_write_isles", "cdecl"):
        continue
    dig_write_isles = _lib.get("dig_write_isles", "cdecl")
    dig_write_isles.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_write_isles.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 116
for _lib in _libs.values():
    if not _lib.has("dig_add_area", "cdecl"):
        continue
    dig_add_area = _lib.get("dig_add_area", "cdecl")
    dig_add_area.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(plus_t), POINTER(struct_bound_box)]
    dig_add_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 117
for _lib in _libs.values():
    if not _lib.has("dig_area_add_isle", "cdecl"):
        continue
    dig_area_add_isle = _lib.get("dig_area_add_isle", "cdecl")
    dig_area_add_isle.argtypes = [POINTER(struct_Plus_head), c_int, c_int]
    dig_area_add_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 118
for _lib in _libs.values():
    if not _lib.has("dig_area_del_isle", "cdecl"):
        continue
    dig_area_del_isle = _lib.get("dig_area_del_isle", "cdecl")
    dig_area_del_isle.argtypes = [POINTER(struct_Plus_head), c_int, c_int]
    dig_area_del_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 119
for _lib in _libs.values():
    if not _lib.has("dig_del_area", "cdecl"):
        continue
    dig_del_area = _lib.get("dig_del_area", "cdecl")
    dig_del_area.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_del_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 120
for _lib in _libs.values():
    if not _lib.has("dig_add_isle", "cdecl"):
        continue
    dig_add_isle = _lib.get("dig_add_isle", "cdecl")
    dig_add_isle.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(plus_t), POINTER(struct_bound_box)]
    dig_add_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 121
for _lib in _libs.values():
    if not _lib.has("dig_del_isle", "cdecl"):
        continue
    dig_del_isle = _lib.get("dig_del_isle", "cdecl")
    dig_del_isle.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_del_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 122
for _lib in _libs.values():
    if not _lib.has("dig_build_area_with_line", "cdecl"):
        continue
    dig_build_area_with_line = _lib.get("dig_build_area_with_line", "cdecl")
    dig_build_area_with_line.argtypes = [POINTER(struct_Plus_head), plus_t, c_int, POINTER(POINTER(plus_t))]
    dig_build_area_with_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 123
for _lib in _libs.values():
    if not _lib.has("dig_angle_next_line", "cdecl"):
        continue
    dig_angle_next_line = _lib.get("dig_angle_next_line", "cdecl")
    dig_angle_next_line.argtypes = [POINTER(struct_Plus_head), plus_t, c_int, c_int, POINTER(c_float)]
    dig_angle_next_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 124
for _lib in _libs.values():
    if not _lib.has("dig_node_angle_check", "cdecl"):
        continue
    dig_node_angle_check = _lib.get("dig_node_angle_check", "cdecl")
    dig_node_angle_check.argtypes = [POINTER(struct_Plus_head), c_int, c_int]
    dig_node_angle_check.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 125
for _lib in _libs.values():
    if not _lib.has("dig_area_get_box", "cdecl"):
        continue
    dig_area_get_box = _lib.get("dig_area_get_box", "cdecl")
    dig_area_get_box.argtypes = [POINTER(struct_Plus_head), plus_t, POINTER(struct_bound_box)]
    dig_area_get_box.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 126
for _lib in _libs.values():
    if not _lib.has("dig_isle_get_box", "cdecl"):
        continue
    dig_isle_get_box = _lib.get("dig_isle_get_box", "cdecl")
    dig_isle_get_box.argtypes = [POINTER(struct_Plus_head), plus_t, POINTER(struct_bound_box)]
    dig_isle_get_box.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 129
for _lib in _libs.values():
    if not _lib.has("dig_add_line", "cdecl"):
        continue
    dig_add_line = _lib.get("dig_add_line", "cdecl")
    dig_add_line.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_line_pnts), POINTER(struct_bound_box), off_t]
    dig_add_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 131
for _lib in _libs.values():
    if not _lib.has("dig_restore_line", "cdecl"):
        continue
    dig_restore_line = _lib.get("dig_restore_line", "cdecl")
    dig_restore_line.argtypes = [POINTER(struct_Plus_head), c_int, c_int, POINTER(struct_line_pnts), POINTER(struct_bound_box), off_t]
    dig_restore_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 133
for _lib in _libs.values():
    if not _lib.has("dig_del_line", "cdecl"):
        continue
    dig_del_line = _lib.get("dig_del_line", "cdecl")
    dig_del_line.argtypes = [POINTER(struct_Plus_head), c_int, c_double, c_double, c_double]
    dig_del_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 134
for _lib in _libs.values():
    if not _lib.has("dig_line_get_area", "cdecl"):
        continue
    dig_line_get_area = _lib.get("dig_line_get_area", "cdecl")
    dig_line_get_area.argtypes = [POINTER(struct_Plus_head), plus_t, c_int]
    dig_line_get_area.restype = plus_t
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 135
for _lib in _libs.values():
    if not _lib.has("dig_line_set_area", "cdecl"):
        continue
    dig_line_set_area = _lib.get("dig_line_set_area", "cdecl")
    dig_line_set_area.argtypes = [POINTER(struct_Plus_head), plus_t, c_int, plus_t]
    dig_line_set_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 138
for _lib in _libs.values():
    if not _lib.has("dig_add_node", "cdecl"):
        continue
    dig_add_node = _lib.get("dig_add_node", "cdecl")
    dig_add_node.argtypes = [POINTER(struct_Plus_head), c_double, c_double, c_double]
    dig_add_node.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 139
for _lib in _libs.values():
    if not _lib.has("dig_which_node", "cdecl"):
        continue
    dig_which_node = _lib.get("dig_which_node", "cdecl")
    dig_which_node.argtypes = [POINTER(struct_Plus_head), c_double, c_double, c_double]
    dig_which_node.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 141
for _lib in _libs.values():
    if not _lib.has("dig_node_add_line", "cdecl"):
        continue
    dig_node_add_line = _lib.get("dig_node_add_line", "cdecl")
    dig_node_add_line.argtypes = [POINTER(struct_Plus_head), c_int, c_int, POINTER(struct_line_pnts), c_int]
    dig_node_add_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 143
for _lib in _libs.values():
    if not _lib.has("dig_node_line_angle", "cdecl"):
        continue
    dig_node_line_angle = _lib.get("dig_node_line_angle", "cdecl")
    dig_node_line_angle.argtypes = [POINTER(struct_Plus_head), c_int, c_int]
    dig_node_line_angle.restype = c_float
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 146
for _lib in _libs.values():
    if not _lib.has("dig_Rd_P_node", "cdecl"):
        continue
    dig_Rd_P_node = _lib.get("dig_Rd_P_node", "cdecl")
    dig_Rd_P_node.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_gvfile)]
    dig_Rd_P_node.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 147
for _lib in _libs.values():
    if not _lib.has("dig_Wr_P_node", "cdecl"):
        continue
    dig_Wr_P_node = _lib.get("dig_Wr_P_node", "cdecl")
    dig_Wr_P_node.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_gvfile)]
    dig_Wr_P_node.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 148
for _lib in _libs.values():
    if not _lib.has("dig_Rd_P_line", "cdecl"):
        continue
    dig_Rd_P_line = _lib.get("dig_Rd_P_line", "cdecl")
    dig_Rd_P_line.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_gvfile)]
    dig_Rd_P_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 149
for _lib in _libs.values():
    if not _lib.has("dig_Wr_P_line", "cdecl"):
        continue
    dig_Wr_P_line = _lib.get("dig_Wr_P_line", "cdecl")
    dig_Wr_P_line.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_gvfile)]
    dig_Wr_P_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 150
for _lib in _libs.values():
    if not _lib.has("dig_Rd_P_area", "cdecl"):
        continue
    dig_Rd_P_area = _lib.get("dig_Rd_P_area", "cdecl")
    dig_Rd_P_area.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_gvfile)]
    dig_Rd_P_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 151
for _lib in _libs.values():
    if not _lib.has("dig_Wr_P_area", "cdecl"):
        continue
    dig_Wr_P_area = _lib.get("dig_Wr_P_area", "cdecl")
    dig_Wr_P_area.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_gvfile)]
    dig_Wr_P_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 152
for _lib in _libs.values():
    if not _lib.has("dig_Rd_P_isle", "cdecl"):
        continue
    dig_Rd_P_isle = _lib.get("dig_Rd_P_isle", "cdecl")
    dig_Rd_P_isle.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_gvfile)]
    dig_Rd_P_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 153
for _lib in _libs.values():
    if not _lib.has("dig_Wr_P_isle", "cdecl"):
        continue
    dig_Wr_P_isle = _lib.get("dig_Wr_P_isle", "cdecl")
    dig_Wr_P_isle.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_gvfile)]
    dig_Wr_P_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 154
for _lib in _libs.values():
    if not _lib.has("dig_Rd_Plus_head", "cdecl"):
        continue
    dig_Rd_Plus_head = _lib.get("dig_Rd_Plus_head", "cdecl")
    dig_Rd_Plus_head.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_Rd_Plus_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 155
for _lib in _libs.values():
    if not _lib.has("dig_Wr_Plus_head", "cdecl"):
        continue
    dig_Wr_Plus_head = _lib.get("dig_Wr_Plus_head", "cdecl")
    dig_Wr_Plus_head.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_Wr_Plus_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 158
for _lib in _libs.values():
    if not _lib.has("dig_find_area_poly", "cdecl"):
        continue
    dig_find_area_poly = _lib.get("dig_find_area_poly", "cdecl")
    dig_find_area_poly.argtypes = [POINTER(struct_line_pnts), POINTER(c_double)]
    dig_find_area_poly.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 159
for _lib in _libs.values():
    if not _lib.has("dig_find_poly_orientation", "cdecl"):
        continue
    dig_find_poly_orientation = _lib.get("dig_find_poly_orientation", "cdecl")
    dig_find_poly_orientation.argtypes = [POINTER(struct_line_pnts)]
    dig_find_poly_orientation.restype = c_double
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 160
for _lib in _libs.values():
    if not _lib.has("dig_get_poly_points", "cdecl"):
        continue
    dig_get_poly_points = _lib.get("dig_get_poly_points", "cdecl")
    dig_get_poly_points.argtypes = [c_int, POINTER(POINTER(struct_line_pnts)), POINTER(c_int), POINTER(struct_line_pnts)]
    dig_get_poly_points.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 163
for _lib in _libs.values():
    if not _lib.has("dig_init_portable", "cdecl"):
        continue
    dig_init_portable = _lib.get("dig_init_portable", "cdecl")
    dig_init_portable.argtypes = [POINTER(struct_Port_info), c_int]
    dig_init_portable.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 164
for _lib in _libs.values():
    if not _lib.has("dig__byte_order_out", "cdecl"):
        continue
    dig__byte_order_out = _lib.get("dig__byte_order_out", "cdecl")
    dig__byte_order_out.argtypes = []
    dig__byte_order_out.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 167
for _lib in _libs.values():
    if not _lib.has("dig_set_cur_port", "cdecl"):
        continue
    dig_set_cur_port = _lib.get("dig_set_cur_port", "cdecl")
    dig_set_cur_port.argtypes = [POINTER(struct_Port_info)]
    dig_set_cur_port.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 169
for _lib in _libs.values():
    if not _lib.has("dig__fread_port_D", "cdecl"):
        continue
    dig__fread_port_D = _lib.get("dig__fread_port_D", "cdecl")
    dig__fread_port_D.argtypes = [POINTER(c_double), c_size_t, POINTER(struct_gvfile)]
    dig__fread_port_D.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 170
for _lib in _libs.values():
    if not _lib.has("dig__fread_port_F", "cdecl"):
        continue
    dig__fread_port_F = _lib.get("dig__fread_port_F", "cdecl")
    dig__fread_port_F.argtypes = [POINTER(c_float), c_size_t, POINTER(struct_gvfile)]
    dig__fread_port_F.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 171
for _lib in _libs.values():
    if not _lib.has("dig__fread_port_O", "cdecl"):
        continue
    dig__fread_port_O = _lib.get("dig__fread_port_O", "cdecl")
    dig__fread_port_O.argtypes = [POINTER(off_t), c_size_t, POINTER(struct_gvfile), c_size_t]
    dig__fread_port_O.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 172
for _lib in _libs.values():
    if not _lib.has("dig__fread_port_L", "cdecl"):
        continue
    dig__fread_port_L = _lib.get("dig__fread_port_L", "cdecl")
    dig__fread_port_L.argtypes = [POINTER(c_long), c_size_t, POINTER(struct_gvfile)]
    dig__fread_port_L.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 173
for _lib in _libs.values():
    if not _lib.has("dig__fread_port_S", "cdecl"):
        continue
    dig__fread_port_S = _lib.get("dig__fread_port_S", "cdecl")
    dig__fread_port_S.argtypes = [POINTER(c_short), c_size_t, POINTER(struct_gvfile)]
    dig__fread_port_S.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 174
for _lib in _libs.values():
    if not _lib.has("dig__fread_port_I", "cdecl"):
        continue
    dig__fread_port_I = _lib.get("dig__fread_port_I", "cdecl")
    dig__fread_port_I.argtypes = [POINTER(c_int), c_size_t, POINTER(struct_gvfile)]
    dig__fread_port_I.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 175
for _lib in _libs.values():
    if not _lib.has("dig__fread_port_P", "cdecl"):
        continue
    dig__fread_port_P = _lib.get("dig__fread_port_P", "cdecl")
    dig__fread_port_P.argtypes = [POINTER(plus_t), c_size_t, POINTER(struct_gvfile)]
    dig__fread_port_P.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 176
for _lib in _libs.values():
    if not _lib.has("dig__fread_port_C", "cdecl"):
        continue
    dig__fread_port_C = _lib.get("dig__fread_port_C", "cdecl")
    dig__fread_port_C.argtypes = [String, c_size_t, POINTER(struct_gvfile)]
    dig__fread_port_C.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 177
for _lib in _libs.values():
    if not _lib.has("dig__fwrite_port_D", "cdecl"):
        continue
    dig__fwrite_port_D = _lib.get("dig__fwrite_port_D", "cdecl")
    dig__fwrite_port_D.argtypes = [POINTER(c_double), c_size_t, POINTER(struct_gvfile)]
    dig__fwrite_port_D.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 178
for _lib in _libs.values():
    if not _lib.has("dig__fwrite_port_F", "cdecl"):
        continue
    dig__fwrite_port_F = _lib.get("dig__fwrite_port_F", "cdecl")
    dig__fwrite_port_F.argtypes = [POINTER(c_float), c_size_t, POINTER(struct_gvfile)]
    dig__fwrite_port_F.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 179
for _lib in _libs.values():
    if not _lib.has("dig__fwrite_port_O", "cdecl"):
        continue
    dig__fwrite_port_O = _lib.get("dig__fwrite_port_O", "cdecl")
    dig__fwrite_port_O.argtypes = [POINTER(off_t), c_size_t, POINTER(struct_gvfile), c_size_t]
    dig__fwrite_port_O.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 180
for _lib in _libs.values():
    if not _lib.has("dig__fwrite_port_L", "cdecl"):
        continue
    dig__fwrite_port_L = _lib.get("dig__fwrite_port_L", "cdecl")
    dig__fwrite_port_L.argtypes = [POINTER(c_long), c_size_t, POINTER(struct_gvfile)]
    dig__fwrite_port_L.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 181
for _lib in _libs.values():
    if not _lib.has("dig__fwrite_port_S", "cdecl"):
        continue
    dig__fwrite_port_S = _lib.get("dig__fwrite_port_S", "cdecl")
    dig__fwrite_port_S.argtypes = [POINTER(c_short), c_size_t, POINTER(struct_gvfile)]
    dig__fwrite_port_S.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 182
for _lib in _libs.values():
    if not _lib.has("dig__fwrite_port_I", "cdecl"):
        continue
    dig__fwrite_port_I = _lib.get("dig__fwrite_port_I", "cdecl")
    dig__fwrite_port_I.argtypes = [POINTER(c_int), c_size_t, POINTER(struct_gvfile)]
    dig__fwrite_port_I.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 183
for _lib in _libs.values():
    if not _lib.has("dig__fwrite_port_P", "cdecl"):
        continue
    dig__fwrite_port_P = _lib.get("dig__fwrite_port_P", "cdecl")
    dig__fwrite_port_P.argtypes = [POINTER(plus_t), c_size_t, POINTER(struct_gvfile)]
    dig__fwrite_port_P.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 184
for _lib in _libs.values():
    if not _lib.has("dig__fwrite_port_C", "cdecl"):
        continue
    dig__fwrite_port_C = _lib.get("dig__fwrite_port_C", "cdecl")
    dig__fwrite_port_C.argtypes = [String, c_size_t, POINTER(struct_gvfile)]
    dig__fwrite_port_C.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 191
for _lib in _libs.values():
    if not _lib.has("dig_prune", "cdecl"):
        continue
    dig_prune = _lib.get("dig_prune", "cdecl")
    dig_prune.argtypes = [POINTER(struct_line_pnts), c_double]
    dig_prune.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 195
for _lib in _libs.values():
    if not _lib.has("dig_spidx_init", "cdecl"):
        continue
    dig_spidx_init = _lib.get("dig_spidx_init", "cdecl")
    dig_spidx_init.argtypes = [POINTER(struct_Plus_head)]
    dig_spidx_init.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 196
for _lib in _libs.values():
    if not _lib.has("dig_spidx_free_nodes", "cdecl"):
        continue
    dig_spidx_free_nodes = _lib.get("dig_spidx_free_nodes", "cdecl")
    dig_spidx_free_nodes.argtypes = [POINTER(struct_Plus_head)]
    dig_spidx_free_nodes.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 197
for _lib in _libs.values():
    if not _lib.has("dig_spidx_free_lines", "cdecl"):
        continue
    dig_spidx_free_lines = _lib.get("dig_spidx_free_lines", "cdecl")
    dig_spidx_free_lines.argtypes = [POINTER(struct_Plus_head)]
    dig_spidx_free_lines.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 198
for _lib in _libs.values():
    if not _lib.has("dig_spidx_free_areas", "cdecl"):
        continue
    dig_spidx_free_areas = _lib.get("dig_spidx_free_areas", "cdecl")
    dig_spidx_free_areas.argtypes = [POINTER(struct_Plus_head)]
    dig_spidx_free_areas.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 199
for _lib in _libs.values():
    if not _lib.has("dig_spidx_free_isles", "cdecl"):
        continue
    dig_spidx_free_isles = _lib.get("dig_spidx_free_isles", "cdecl")
    dig_spidx_free_isles.argtypes = [POINTER(struct_Plus_head)]
    dig_spidx_free_isles.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 200
for _lib in _libs.values():
    if not _lib.has("dig_spidx_free", "cdecl"):
        continue
    dig_spidx_free = _lib.get("dig_spidx_free", "cdecl")
    dig_spidx_free.argtypes = [POINTER(struct_Plus_head)]
    dig_spidx_free.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 202
for _lib in _libs.values():
    if not _lib.has("dig_spidx_add_node", "cdecl"):
        continue
    dig_spidx_add_node = _lib.get("dig_spidx_add_node", "cdecl")
    dig_spidx_add_node.argtypes = [POINTER(struct_Plus_head), c_int, c_double, c_double, c_double]
    dig_spidx_add_node.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 203
for _lib in _libs.values():
    if not _lib.has("dig_spidx_add_line", "cdecl"):
        continue
    dig_spidx_add_line = _lib.get("dig_spidx_add_line", "cdecl")
    dig_spidx_add_line.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_bound_box)]
    dig_spidx_add_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 204
for _lib in _libs.values():
    if not _lib.has("dig_spidx_add_area", "cdecl"):
        continue
    dig_spidx_add_area = _lib.get("dig_spidx_add_area", "cdecl")
    dig_spidx_add_area.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_bound_box)]
    dig_spidx_add_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 205
for _lib in _libs.values():
    if not _lib.has("dig_spidx_add_isle", "cdecl"):
        continue
    dig_spidx_add_isle = _lib.get("dig_spidx_add_isle", "cdecl")
    dig_spidx_add_isle.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_bound_box)]
    dig_spidx_add_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 207
for _lib in _libs.values():
    if not _lib.has("dig_spidx_del_node", "cdecl"):
        continue
    dig_spidx_del_node = _lib.get("dig_spidx_del_node", "cdecl")
    dig_spidx_del_node.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_spidx_del_node.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 208
for _lib in _libs.values():
    if not _lib.has("dig_spidx_del_line", "cdecl"):
        continue
    dig_spidx_del_line = _lib.get("dig_spidx_del_line", "cdecl")
    dig_spidx_del_line.argtypes = [POINTER(struct_Plus_head), c_int, c_double, c_double, c_double]
    dig_spidx_del_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 209
for _lib in _libs.values():
    if not _lib.has("dig_spidx_del_area", "cdecl"):
        continue
    dig_spidx_del_area = _lib.get("dig_spidx_del_area", "cdecl")
    dig_spidx_del_area.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_spidx_del_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 210
for _lib in _libs.values():
    if not _lib.has("dig_spidx_del_isle", "cdecl"):
        continue
    dig_spidx_del_isle = _lib.get("dig_spidx_del_isle", "cdecl")
    dig_spidx_del_isle.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_spidx_del_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 212
for _lib in _libs.values():
    if not _lib.has("dig_select_nodes", "cdecl"):
        continue
    dig_select_nodes = _lib.get("dig_select_nodes", "cdecl")
    dig_select_nodes.argtypes = [POINTER(struct_Plus_head), POINTER(struct_bound_box), POINTER(struct_ilist)]
    dig_select_nodes.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 214
for _lib in _libs.values():
    if not _lib.has("dig_select_lines", "cdecl"):
        continue
    dig_select_lines = _lib.get("dig_select_lines", "cdecl")
    dig_select_lines.argtypes = [POINTER(struct_Plus_head), POINTER(struct_bound_box), POINTER(struct_boxlist)]
    dig_select_lines.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 216
for _lib in _libs.values():
    if not _lib.has("dig_select_areas", "cdecl"):
        continue
    dig_select_areas = _lib.get("dig_select_areas", "cdecl")
    dig_select_areas.argtypes = [POINTER(struct_Plus_head), POINTER(struct_bound_box), POINTER(struct_boxlist)]
    dig_select_areas.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 218
for _lib in _libs.values():
    if not _lib.has("dig_select_isles", "cdecl"):
        continue
    dig_select_isles = _lib.get("dig_select_isles", "cdecl")
    dig_select_isles.argtypes = [POINTER(struct_Plus_head), POINTER(struct_bound_box), POINTER(struct_boxlist)]
    dig_select_isles.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 220
for _lib in _libs.values():
    if not _lib.has("dig_find_node", "cdecl"):
        continue
    dig_find_node = _lib.get("dig_find_node", "cdecl")
    dig_find_node.argtypes = [POINTER(struct_Plus_head), c_double, c_double, c_double]
    dig_find_node.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 221
for _lib in _libs.values():
    if not _lib.has("dig_find_line_box", "cdecl"):
        continue
    dig_find_line_box = _lib.get("dig_find_line_box", "cdecl")
    dig_find_line_box.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_bound_box)]
    dig_find_line_box.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 222
for _lib in _libs.values():
    if not _lib.has("dig_find_area_box", "cdecl"):
        continue
    dig_find_area_box = _lib.get("dig_find_area_box", "cdecl")
    dig_find_area_box.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_bound_box)]
    dig_find_area_box.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 223
for _lib in _libs.values():
    if not _lib.has("dig_find_isle_box", "cdecl"):
        continue
    dig_find_isle_box = _lib.get("dig_find_isle_box", "cdecl")
    dig_find_isle_box.argtypes = [POINTER(struct_Plus_head), c_int, POINTER(struct_bound_box)]
    dig_find_isle_box.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 226
for _lib in _libs.values():
    if not _lib.has("dig_Rd_spidx_head", "cdecl"):
        continue
    dig_Rd_spidx_head = _lib.get("dig_Rd_spidx_head", "cdecl")
    dig_Rd_spidx_head.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_Rd_spidx_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 227
for _lib in _libs.values():
    if not _lib.has("dig_Wr_spidx_head", "cdecl"):
        continue
    dig_Wr_spidx_head = _lib.get("dig_Wr_spidx_head", "cdecl")
    dig_Wr_spidx_head.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_Wr_spidx_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 228
for _lib in _libs.values():
    if not _lib.has("dig_Wr_spidx", "cdecl"):
        continue
    dig_Wr_spidx = _lib.get("dig_Wr_spidx", "cdecl")
    dig_Wr_spidx.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_Wr_spidx.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 229
for _lib in _libs.values():
    if not _lib.has("dig_Rd_spidx", "cdecl"):
        continue
    dig_Rd_spidx = _lib.get("dig_Rd_spidx", "cdecl")
    dig_Rd_spidx.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_Rd_spidx.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 231
for _lib in _libs.values():
    if not _lib.has("dig_dump_spidx", "cdecl"):
        continue
    dig_dump_spidx = _lib.get("dig_dump_spidx", "cdecl")
    dig_dump_spidx.argtypes = [POINTER(FILE), POINTER(struct_Plus_head)]
    dig_dump_spidx.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 233
for _lib in _libs.values():
    if not _lib.has("rtree_search", "cdecl"):
        continue
    rtree_search = _lib.get("rtree_search", "cdecl")
    rtree_search.argtypes = [POINTER(struct_RTree), POINTER(struct_RTree_Rect), SearchHitCallback, POINTER(None), POINTER(struct_Plus_head)]
    rtree_search.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 237
for _lib in _libs.values():
    if not _lib.has("dig_node_alloc_line", "cdecl"):
        continue
    dig_node_alloc_line = _lib.get("dig_node_alloc_line", "cdecl")
    dig_node_alloc_line.argtypes = [POINTER(struct_P_node), c_int]
    dig_node_alloc_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 238
for _lib in _libs.values():
    if not _lib.has("dig_alloc_nodes", "cdecl"):
        continue
    dig_alloc_nodes = _lib.get("dig_alloc_nodes", "cdecl")
    dig_alloc_nodes.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_alloc_nodes.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 239
for _lib in _libs.values():
    if not _lib.has("dig_alloc_lines", "cdecl"):
        continue
    dig_alloc_lines = _lib.get("dig_alloc_lines", "cdecl")
    dig_alloc_lines.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_alloc_lines.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 240
for _lib in _libs.values():
    if not _lib.has("dig_alloc_areas", "cdecl"):
        continue
    dig_alloc_areas = _lib.get("dig_alloc_areas", "cdecl")
    dig_alloc_areas.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_alloc_areas.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 241
for _lib in _libs.values():
    if not _lib.has("dig_alloc_isles", "cdecl"):
        continue
    dig_alloc_isles = _lib.get("dig_alloc_isles", "cdecl")
    dig_alloc_isles.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_alloc_isles.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 242
for _lib in _libs.values():
    if not _lib.has("dig_alloc_node", "cdecl"):
        continue
    dig_alloc_node = _lib.get("dig_alloc_node", "cdecl")
    dig_alloc_node.argtypes = []
    dig_alloc_node.restype = POINTER(struct_P_node)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 243
for _lib in _libs.values():
    if not _lib.has("dig_alloc_line", "cdecl"):
        continue
    dig_alloc_line = _lib.get("dig_alloc_line", "cdecl")
    dig_alloc_line.argtypes = []
    dig_alloc_line.restype = POINTER(struct_P_line)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 244
for _lib in _libs.values():
    if not _lib.has("dig_alloc_topo", "cdecl"):
        continue
    dig_alloc_topo = _lib.get("dig_alloc_topo", "cdecl")
    dig_alloc_topo.argtypes = [c_char]
    dig_alloc_topo.restype = POINTER(c_ubyte)
    dig_alloc_topo.errcheck = lambda v,*a : cast(v, c_void_p)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 245
for _lib in _libs.values():
    if not _lib.has("dig_alloc_area", "cdecl"):
        continue
    dig_alloc_area = _lib.get("dig_alloc_area", "cdecl")
    dig_alloc_area.argtypes = []
    dig_alloc_area.restype = POINTER(struct_P_area)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 246
for _lib in _libs.values():
    if not _lib.has("dig_alloc_isle", "cdecl"):
        continue
    dig_alloc_isle = _lib.get("dig_alloc_isle", "cdecl")
    dig_alloc_isle.argtypes = []
    dig_alloc_isle.restype = POINTER(struct_P_isle)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 247
for _lib in _libs.values():
    if not _lib.has("dig_free_node", "cdecl"):
        continue
    dig_free_node = _lib.get("dig_free_node", "cdecl")
    dig_free_node.argtypes = [POINTER(struct_P_node)]
    dig_free_node.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 248
for _lib in _libs.values():
    if not _lib.has("dig_free_line", "cdecl"):
        continue
    dig_free_line = _lib.get("dig_free_line", "cdecl")
    dig_free_line.argtypes = [POINTER(struct_P_line)]
    dig_free_line.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 249
for _lib in _libs.values():
    if not _lib.has("dig_free_area", "cdecl"):
        continue
    dig_free_area = _lib.get("dig_free_area", "cdecl")
    dig_free_area.argtypes = [POINTER(struct_P_area)]
    dig_free_area.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 250
for _lib in _libs.values():
    if not _lib.has("dig_free_isle", "cdecl"):
        continue
    dig_free_isle = _lib.get("dig_free_isle", "cdecl")
    dig_free_isle.argtypes = [POINTER(struct_P_isle)]
    dig_free_isle.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 251
for _lib in _libs.values():
    if not _lib.has("dig_alloc_points", "cdecl"):
        continue
    dig_alloc_points = _lib.get("dig_alloc_points", "cdecl")
    dig_alloc_points.argtypes = [POINTER(struct_line_pnts), c_int]
    dig_alloc_points.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 252
for _lib in _libs.values():
    if not _lib.has("dig_alloc_cats", "cdecl"):
        continue
    dig_alloc_cats = _lib.get("dig_alloc_cats", "cdecl")
    dig_alloc_cats.argtypes = [POINTER(struct_line_cats), c_int]
    dig_alloc_cats.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 253
for _lib in _libs.values():
    if not _lib.has("dig_area_alloc_line", "cdecl"):
        continue
    dig_area_alloc_line = _lib.get("dig_area_alloc_line", "cdecl")
    dig_area_alloc_line.argtypes = [POINTER(struct_P_area), c_int]
    dig_area_alloc_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 254
for _lib in _libs.values():
    if not _lib.has("dig_area_alloc_isle", "cdecl"):
        continue
    dig_area_alloc_isle = _lib.get("dig_area_alloc_isle", "cdecl")
    dig_area_alloc_isle.argtypes = [POINTER(struct_P_area), c_int]
    dig_area_alloc_isle.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 255
for _lib in _libs.values():
    if not _lib.has("dig_isle_alloc_line", "cdecl"):
        continue
    dig_isle_alloc_line = _lib.get("dig_isle_alloc_line", "cdecl")
    dig_isle_alloc_line.argtypes = [POINTER(struct_P_isle), c_int]
    dig_isle_alloc_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 256
for _lib in _libs.values():
    if not _lib.has("dig_out_of_memory", "cdecl"):
        continue
    dig_out_of_memory = _lib.get("dig_out_of_memory", "cdecl")
    dig_out_of_memory.argtypes = []
    dig_out_of_memory.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 260
for _lib in _libs.values():
    if not _lib.has("dig_type_to_store", "cdecl"):
        continue
    dig_type_to_store = _lib.get("dig_type_to_store", "cdecl")
    dig_type_to_store.argtypes = [c_int]
    dig_type_to_store.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 261
for _lib in _libs.values():
    if not _lib.has("dig_type_from_store", "cdecl"):
        continue
    dig_type_from_store = _lib.get("dig_type_from_store", "cdecl")
    dig_type_from_store.argtypes = [c_int]
    dig_type_from_store.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 265
for _lib in _libs.values():
    if not _lib.has("dig_line_reset_updated", "cdecl"):
        continue
    dig_line_reset_updated = _lib.get("dig_line_reset_updated", "cdecl")
    dig_line_reset_updated.argtypes = [POINTER(struct_Plus_head)]
    dig_line_reset_updated.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 266
for _lib in _libs.values():
    if not _lib.has("dig_line_add_updated", "cdecl"):
        continue
    dig_line_add_updated = _lib.get("dig_line_add_updated", "cdecl")
    dig_line_add_updated.argtypes = [POINTER(struct_Plus_head), c_int, off_t]
    dig_line_add_updated.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 267
for _lib in _libs.values():
    if not _lib.has("dig_node_reset_updated", "cdecl"):
        continue
    dig_node_reset_updated = _lib.get("dig_node_reset_updated", "cdecl")
    dig_node_reset_updated.argtypes = [POINTER(struct_Plus_head)]
    dig_node_reset_updated.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 268
for _lib in _libs.values():
    if not _lib.has("dig_node_add_updated", "cdecl"):
        continue
    dig_node_add_updated = _lib.get("dig_node_add_updated", "cdecl")
    dig_node_add_updated.argtypes = [POINTER(struct_Plus_head), c_int]
    dig_node_add_updated.restype = None
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 274
for _lib in _libs.values():
    if not _lib.has("color_name", "cdecl"):
        continue
    color_name = _lib.get("color_name", "cdecl")
    color_name.argtypes = [c_int]
    if sizeof(c_int) == sizeof(c_void_p):
        color_name.restype = ReturnString
    else:
        color_name.restype = String
        color_name.errcheck = ReturnString
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 276
for _lib in _libs.values():
    if not _lib.has("dig_float_point", "cdecl"):
        continue
    dig_float_point = _lib.get("dig_float_point", "cdecl")
    dig_float_point.argtypes = [String, c_int, c_double]
    if sizeof(c_int) == sizeof(c_void_p):
        dig_float_point.restype = ReturnString
    else:
        dig_float_point.restype = String
        dig_float_point.errcheck = ReturnString
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 281
for _lib in _libs.values():
    if not _lib.has("dig_unit_conversion", "cdecl"):
        continue
    dig_unit_conversion = _lib.get("dig_unit_conversion", "cdecl")
    dig_unit_conversion.argtypes = []
    dig_unit_conversion.restype = c_double
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 284
for _lib in _libs.values():
    if not _lib.has("dig__double_convert", "cdecl"):
        continue
    dig__double_convert = _lib.get("dig__double_convert", "cdecl")
    dig__double_convert.argtypes = [POINTER(c_double), POINTER(c_double), c_int, POINTER(struct_dig_head)]
    dig__double_convert.restype = POINTER(c_double)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 285
for _lib in _libs.values():
    if not _lib.has("dig__float_convert", "cdecl"):
        continue
    dig__float_convert = _lib.get("dig__float_convert", "cdecl")
    dig__float_convert.argtypes = [POINTER(c_float), POINTER(c_float), c_int, POINTER(struct_dig_head)]
    dig__float_convert.restype = POINTER(c_float)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 286
for _lib in _libs.values():
    if not _lib.has("dig__short_convert", "cdecl"):
        continue
    dig__short_convert = _lib.get("dig__short_convert", "cdecl")
    dig__short_convert.argtypes = [POINTER(c_short), POINTER(c_short), c_int, POINTER(struct_dig_head)]
    dig__short_convert.restype = POINTER(c_short)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 287
for _lib in _libs.values():
    if not _lib.has("dig__long_convert", "cdecl"):
        continue
    dig__long_convert = _lib.get("dig__long_convert", "cdecl")
    dig__long_convert.argtypes = [POINTER(c_long), POINTER(c_long), c_int, POINTER(struct_dig_head)]
    dig__long_convert.restype = POINTER(c_long)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 288
for _lib in _libs.values():
    if not _lib.has("dig__int_convert", "cdecl"):
        continue
    dig__int_convert = _lib.get("dig__int_convert", "cdecl")
    dig__int_convert.argtypes = [POINTER(c_int), POINTER(c_long), c_int, POINTER(struct_dig_head)]
    dig__int_convert.restype = POINTER(c_long)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 289
for _lib in _libs.values():
    if not _lib.has("dig__plus_t_convert", "cdecl"):
        continue
    dig__plus_t_convert = _lib.get("dig__plus_t_convert", "cdecl")
    dig__plus_t_convert.argtypes = [POINTER(plus_t), POINTER(c_long), c_int, POINTER(struct_dig_head)]
    dig__plus_t_convert.restype = POINTER(c_long)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 290
for _lib in _libs.values():
    if not _lib.has("dig__long_convert_to_int", "cdecl"):
        continue
    dig__long_convert_to_int = _lib.get("dig__long_convert_to_int", "cdecl")
    dig__long_convert_to_int.argtypes = [POINTER(c_long), POINTER(c_int), c_int, POINTER(struct_dig_head)]
    dig__long_convert_to_int.restype = POINTER(c_int)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 291
for _lib in _libs.values():
    if not _lib.has("dig__long_convert_to_plus_t", "cdecl"):
        continue
    dig__long_convert_to_plus_t = _lib.get("dig__long_convert_to_plus_t", "cdecl")
    dig__long_convert_to_plus_t.argtypes = [POINTER(c_long), POINTER(plus_t), c_int, POINTER(struct_dig_head)]
    dig__long_convert_to_plus_t.restype = POINTER(plus_t)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 292
for _lib in _libs.values():
    if not _lib.has("dig__convert_buffer", "cdecl"):
        continue
    dig__convert_buffer = _lib.get("dig__convert_buffer", "cdecl")
    dig__convert_buffer.argtypes = [c_int]
    if sizeof(c_int) == sizeof(c_void_p):
        dig__convert_buffer.restype = ReturnString
    else:
        dig__convert_buffer.restype = String
        dig__convert_buffer.errcheck = ReturnString
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 294
for _lib in _libs.values():
    if not _lib.has("dig_get_cont_lines", "cdecl"):
        continue
    dig_get_cont_lines = _lib.get("dig_get_cont_lines", "cdecl")
    dig_get_cont_lines.argtypes = [POINTER(struct_Map_info), plus_t, c_double, c_int]
    dig_get_cont_lines.restype = POINTER(POINTER(plus_t))
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 295
for _lib in _libs.values():
    if not _lib.has("dig_get_next_cont_line", "cdecl"):
        continue
    dig_get_next_cont_line = _lib.get("dig_get_next_cont_line", "cdecl")
    dig_get_next_cont_line.argtypes = [POINTER(struct_Map_info), plus_t, c_double, c_int]
    dig_get_next_cont_line.restype = plus_t
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 297
for _lib in _libs.values():
    if not _lib.has("dig_get_head", "cdecl"):
        continue
    dig_get_head = _lib.get("dig_get_head", "cdecl")
    dig_get_head.argtypes = []
    dig_get_head.restype = POINTER(struct_dig_head)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 298
for _lib in _libs.values():
    if not _lib.has("dig__get_head", "cdecl"):
        continue
    dig__get_head = _lib.get("dig__get_head", "cdecl")
    dig__get_head.argtypes = []
    dig__get_head.restype = POINTER(struct_dig_head)
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 302
for _lib in _libs.values():
    if not _lib.has("dig_start_clock", "cdecl"):
        continue
    dig_start_clock = _lib.get("dig_start_clock", "cdecl")
    dig_start_clock.argtypes = [POINTER(c_long)]
    dig_start_clock.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 303
for _lib in _libs.values():
    if not _lib.has("dig_stop_clock", "cdecl"):
        continue
    dig_stop_clock = _lib.get("dig_stop_clock", "cdecl")
    dig_stop_clock.argtypes = [POINTER(c_long)]
    dig_stop_clock.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 304
for _lib in _libs.values():
    if not _lib.has("dig_stop_clock_str", "cdecl"):
        continue
    dig_stop_clock_str = _lib.get("dig_stop_clock_str", "cdecl")
    dig_stop_clock_str.argtypes = [POINTER(c_long)]
    if sizeof(c_int) == sizeof(c_void_p):
        dig_stop_clock_str.restype = ReturnString
    else:
        dig_stop_clock_str.restype = String
        dig_stop_clock_str.errcheck = ReturnString
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 305
for _lib in _libs.values():
    if not _lib.has("dig_write_file_checks", "cdecl"):
        continue
    dig_write_file_checks = _lib.get("dig_write_file_checks", "cdecl")
    dig_write_file_checks.argtypes = [POINTER(struct_gvfile), POINTER(struct_Plus_head)]
    dig_write_file_checks.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 306
for _lib in _libs.values():
    if not _lib.has("dig_do_file_checks", "cdecl"):
        continue
    dig_do_file_checks = _lib.get("dig_do_file_checks", "cdecl")
    dig_do_file_checks.argtypes = [POINTER(struct_Map_info), String, String]
    dig_do_file_checks.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 312
for _lib in _libs.values():
    if not _lib.has("dig_map_to_head", "cdecl"):
        continue
    dig_map_to_head = _lib.get("dig_map_to_head", "cdecl")
    dig_map_to_head.argtypes = [POINTER(struct_Map_info), POINTER(struct_Plus_head)]
    dig_map_to_head.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 313
for _lib in _libs.values():
    if not _lib.has("dig_head_to_map", "cdecl"):
        continue
    dig_head_to_map = _lib.get("dig_head_to_map", "cdecl")
    dig_head_to_map.argtypes = [POINTER(struct_Plus_head), POINTER(struct_Map_info)]
    dig_head_to_map.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 314
for _lib in _libs.values():
    if not _lib.has("dig_spindex_init", "cdecl"):
        continue
    dig_spindex_init = _lib.get("dig_spindex_init", "cdecl")
    dig_spindex_init.argtypes = [POINTER(struct_Plus_head)]
    dig_spindex_init.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 322
for _lib in _libs.values():
    if not _lib.has("dig_point_to_area", "cdecl"):
        continue
    dig_point_to_area = _lib.get("dig_point_to_area", "cdecl")
    dig_point_to_area.argtypes = [POINTER(struct_Map_info), c_double, c_double]
    dig_point_to_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 323
for _lib in _libs.values():
    if not _lib.has("dig_point_to_next_area", "cdecl"):
        continue
    dig_point_to_next_area = _lib.get("dig_point_to_next_area", "cdecl")
    dig_point_to_next_area.argtypes = [POINTER(struct_Map_info), c_double, c_double, POINTER(c_double)]
    dig_point_to_next_area.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 324
for _lib in _libs.values():
    if not _lib.has("dig_point_to_line", "cdecl"):
        continue
    dig_point_to_line = _lib.get("dig_point_to_line", "cdecl")
    dig_point_to_line.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_char]
    dig_point_to_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 327
for _lib in _libs.values():
    if not _lib.has("dig_check_dist", "cdecl"):
        continue
    dig_check_dist = _lib.get("dig_check_dist", "cdecl")
    dig_check_dist.argtypes = [POINTER(struct_Map_info), c_int, c_double, c_double, POINTER(c_double)]
    dig_check_dist.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 328
for _lib in _libs.values():
    if not _lib.has("dig__check_dist", "cdecl"):
        continue
    dig__check_dist = _lib.get("dig__check_dist", "cdecl")
    dig__check_dist.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), c_double, c_double, POINTER(c_double)]
    dig__check_dist.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 331
for _lib in _libs.values():
    if not _lib.has("dig_point_by_line", "cdecl"):
        continue
    dig_point_by_line = _lib.get("dig_point_by_line", "cdecl")
    dig_point_by_line.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_double, c_char]
    dig_point_by_line.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 333
for _lib in _libs.values():
    if not _lib.has("dig_write_head_ascii", "cdecl"):
        continue
    dig_write_head_ascii = _lib.get("dig_write_head_ascii", "cdecl")
    dig_write_head_ascii.argtypes = [POINTER(FILE), POINTER(struct_dig_head)]
    dig_write_head_ascii.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 334
for _lib in _libs.values():
    if not _lib.has("dig_read_head_ascii", "cdecl"):
        continue
    dig_read_head_ascii = _lib.get("dig_read_head_ascii", "cdecl")
    dig_read_head_ascii.argtypes = [POINTER(FILE), POINTER(struct_dig_head)]
    dig_read_head_ascii.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 336
for _lib in _libs.values():
    if not _lib.has("dig_struct_copy", "cdecl"):
        continue
    dig_struct_copy = _lib.get("dig_struct_copy", "cdecl")
    dig_struct_copy.argtypes = [POINTER(None), POINTER(None), c_int]
    dig_struct_copy.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_externs.h: 337
for _lib in _libs.values():
    if not _lib.has("dig_rmcr", "cdecl"):
        continue
    dig_rmcr = _lib.get("dig_rmcr", "cdecl")
    dig_rmcr.argtypes = [String]
    dig_rmcr.restype = c_int
    break

# C:/osgeo4w/include/geos_c.h: 140
class struct_GEOSGeom_t(Structure):
    pass

GEOSGeometry = struct_GEOSGeom_t# C:/osgeo4w/include/geos_c.h: 140

# C:/osgeo4w/include/geos_c.h: 155
class struct_GEOSCoordSeq_t(Structure):
    pass

GEOSCoordSequence = struct_GEOSCoordSeq_t# C:/osgeo4w/include/geos_c.h: 155

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 8
if _libs["grass_vector.8.4"].has("Vect_new_line_struct", "cdecl"):
    Vect_new_line_struct = _libs["grass_vector.8.4"].get("Vect_new_line_struct", "cdecl")
    Vect_new_line_struct.argtypes = []
    Vect_new_line_struct.restype = POINTER(struct_line_pnts)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 9
if _libs["grass_vector.8.4"].has("Vect_append_point", "cdecl"):
    Vect_append_point = _libs["grass_vector.8.4"].get("Vect_append_point", "cdecl")
    Vect_append_point.argtypes = [POINTER(struct_line_pnts), c_double, c_double, c_double]
    Vect_append_point.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 10
if _libs["grass_vector.8.4"].has("Vect_append_points", "cdecl"):
    Vect_append_points = _libs["grass_vector.8.4"].get("Vect_append_points", "cdecl")
    Vect_append_points.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), c_int]
    Vect_append_points.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 11
if _libs["grass_vector.8.4"].has("Vect_line_insert_point", "cdecl"):
    Vect_line_insert_point = _libs["grass_vector.8.4"].get("Vect_line_insert_point", "cdecl")
    Vect_line_insert_point.argtypes = [POINTER(struct_line_pnts), c_int, c_double, c_double, c_double]
    Vect_line_insert_point.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 12
if _libs["grass_vector.8.4"].has("Vect_line_delete_point", "cdecl"):
    Vect_line_delete_point = _libs["grass_vector.8.4"].get("Vect_line_delete_point", "cdecl")
    Vect_line_delete_point.argtypes = [POINTER(struct_line_pnts), c_int]
    Vect_line_delete_point.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 13
if _libs["grass_vector.8.4"].has("Vect_line_get_point", "cdecl"):
    Vect_line_get_point = _libs["grass_vector.8.4"].get("Vect_line_get_point", "cdecl")
    Vect_line_get_point.argtypes = [POINTER(struct_line_pnts), c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    Vect_line_get_point.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 15
if _libs["grass_vector.8.4"].has("Vect_get_num_line_points", "cdecl"):
    Vect_get_num_line_points = _libs["grass_vector.8.4"].get("Vect_get_num_line_points", "cdecl")
    Vect_get_num_line_points.argtypes = [POINTER(struct_line_pnts)]
    Vect_get_num_line_points.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 16
if _libs["grass_vector.8.4"].has("Vect_line_prune", "cdecl"):
    Vect_line_prune = _libs["grass_vector.8.4"].get("Vect_line_prune", "cdecl")
    Vect_line_prune.argtypes = [POINTER(struct_line_pnts)]
    Vect_line_prune.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 17
if _libs["grass_vector.8.4"].has("Vect_line_prune_thresh", "cdecl"):
    Vect_line_prune_thresh = _libs["grass_vector.8.4"].get("Vect_line_prune_thresh", "cdecl")
    Vect_line_prune_thresh.argtypes = [POINTER(struct_line_pnts), c_double]
    Vect_line_prune_thresh.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 18
if _libs["grass_vector.8.4"].has("Vect_line_reverse", "cdecl"):
    Vect_line_reverse = _libs["grass_vector.8.4"].get("Vect_line_reverse", "cdecl")
    Vect_line_reverse.argtypes = [POINTER(struct_line_pnts)]
    Vect_line_reverse.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 19
if _libs["grass_vector.8.4"].has("Vect_copy_xyz_to_pnts", "cdecl"):
    Vect_copy_xyz_to_pnts = _libs["grass_vector.8.4"].get("Vect_copy_xyz_to_pnts", "cdecl")
    Vect_copy_xyz_to_pnts.argtypes = [POINTER(struct_line_pnts), POINTER(c_double), POINTER(c_double), POINTER(c_double), c_int]
    Vect_copy_xyz_to_pnts.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 21
if _libs["grass_vector.8.4"].has("Vect_copy_pnts_to_xyz", "cdecl"):
    Vect_copy_pnts_to_xyz = _libs["grass_vector.8.4"].get("Vect_copy_pnts_to_xyz", "cdecl")
    Vect_copy_pnts_to_xyz.argtypes = [POINTER(struct_line_pnts), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_int)]
    Vect_copy_pnts_to_xyz.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 23
if _libs["grass_vector.8.4"].has("Vect_reset_line", "cdecl"):
    Vect_reset_line = _libs["grass_vector.8.4"].get("Vect_reset_line", "cdecl")
    Vect_reset_line.argtypes = [POINTER(struct_line_pnts)]
    Vect_reset_line.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 24
if _libs["grass_vector.8.4"].has("Vect_destroy_line_struct", "cdecl"):
    Vect_destroy_line_struct = _libs["grass_vector.8.4"].get("Vect_destroy_line_struct", "cdecl")
    Vect_destroy_line_struct.argtypes = [POINTER(struct_line_pnts)]
    Vect_destroy_line_struct.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 25
if _libs["grass_vector.8.4"].has("Vect_point_on_line", "cdecl"):
    Vect_point_on_line = _libs["grass_vector.8.4"].get("Vect_point_on_line", "cdecl")
    Vect_point_on_line.argtypes = [POINTER(struct_line_pnts), c_double, POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    Vect_point_on_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 27
if _libs["grass_vector.8.4"].has("Vect_line_segment", "cdecl"):
    Vect_line_segment = _libs["grass_vector.8.4"].get("Vect_line_segment", "cdecl")
    Vect_line_segment.argtypes = [POINTER(struct_line_pnts), c_double, c_double, POINTER(struct_line_pnts)]
    Vect_line_segment.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 29
if _libs["grass_vector.8.4"].has("Vect_line_length", "cdecl"):
    Vect_line_length = _libs["grass_vector.8.4"].get("Vect_line_length", "cdecl")
    Vect_line_length.argtypes = [POINTER(struct_line_pnts)]
    Vect_line_length.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 30
if _libs["grass_vector.8.4"].has("Vect_line_geodesic_length", "cdecl"):
    Vect_line_geodesic_length = _libs["grass_vector.8.4"].get("Vect_line_geodesic_length", "cdecl")
    Vect_line_geodesic_length.argtypes = [POINTER(struct_line_pnts)]
    Vect_line_geodesic_length.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 31
if _libs["grass_vector.8.4"].has("Vect_line_distance", "cdecl"):
    Vect_line_distance = _libs["grass_vector.8.4"].get("Vect_line_distance", "cdecl")
    Vect_line_distance.argtypes = [POINTER(struct_line_pnts), c_double, c_double, c_double, c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    Vect_line_distance.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 34
if _libs["grass_vector.8.4"].has("Vect_line_geodesic_distance", "cdecl"):
    Vect_line_geodesic_distance = _libs["grass_vector.8.4"].get("Vect_line_geodesic_distance", "cdecl")
    Vect_line_geodesic_distance.argtypes = [POINTER(struct_line_pnts), c_double, c_double, c_double, c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    Vect_line_geodesic_distance.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 37
if _libs["grass_vector.8.4"].has("Vect_line_box", "cdecl"):
    Vect_line_box = _libs["grass_vector.8.4"].get("Vect_line_box", "cdecl")
    Vect_line_box.argtypes = [POINTER(struct_line_pnts), POINTER(struct_bound_box)]
    Vect_line_box.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 38
if _libs["grass_vector.8.4"].has("Vect_line_parallel", "cdecl"):
    Vect_line_parallel = _libs["grass_vector.8.4"].get("Vect_line_parallel", "cdecl")
    Vect_line_parallel.argtypes = [POINTER(struct_line_pnts), c_double, c_double, c_int, POINTER(struct_line_pnts)]
    Vect_line_parallel.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 40
if _libs["grass_vector.8.4"].has("Vect_line_parallel2", "cdecl"):
    Vect_line_parallel2 = _libs["grass_vector.8.4"].get("Vect_line_parallel2", "cdecl")
    Vect_line_parallel2.argtypes = [POINTER(struct_line_pnts), c_double, c_double, c_double, c_int, c_int, c_double, POINTER(struct_line_pnts)]
    Vect_line_parallel2.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 42
if _libs["grass_vector.8.4"].has("Vect_line_buffer", "cdecl"):
    Vect_line_buffer = _libs["grass_vector.8.4"].get("Vect_line_buffer", "cdecl")
    Vect_line_buffer.argtypes = [POINTER(struct_line_pnts), c_double, c_double, POINTER(struct_line_pnts)]
    Vect_line_buffer.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 44
if _libs["grass_vector.8.4"].has("Vect_line_buffer2", "cdecl"):
    Vect_line_buffer2 = _libs["grass_vector.8.4"].get("Vect_line_buffer2", "cdecl")
    Vect_line_buffer2.argtypes = [POINTER(struct_line_pnts), c_double, c_double, c_double, c_int, c_int, c_double, POINTER(POINTER(struct_line_pnts)), POINTER(POINTER(POINTER(struct_line_pnts))), POINTER(c_int)]
    Vect_line_buffer2.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 47
if _libs["grass_vector.8.4"].has("Vect_area_buffer2", "cdecl"):
    Vect_area_buffer2 = _libs["grass_vector.8.4"].get("Vect_area_buffer2", "cdecl")
    Vect_area_buffer2.argtypes = [POINTER(struct_Map_info), c_int, c_double, c_double, c_double, c_int, c_int, c_double, POINTER(POINTER(struct_line_pnts)), POINTER(POINTER(POINTER(struct_line_pnts))), POINTER(c_int)]
    Vect_area_buffer2.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 50
if _libs["grass_vector.8.4"].has("Vect_point_buffer2", "cdecl"):
    Vect_point_buffer2 = _libs["grass_vector.8.4"].get("Vect_point_buffer2", "cdecl")
    Vect_point_buffer2.argtypes = [c_double, c_double, c_double, c_double, c_double, c_int, c_double, POINTER(POINTER(struct_line_pnts))]
    Vect_point_buffer2.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 54
if _libs["grass_vector.8.4"].has("Vect_new_cats_struct", "cdecl"):
    Vect_new_cats_struct = _libs["grass_vector.8.4"].get("Vect_new_cats_struct", "cdecl")
    Vect_new_cats_struct.argtypes = []
    Vect_new_cats_struct.restype = POINTER(struct_line_cats)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 55
if _libs["grass_vector.8.4"].has("Vect_cat_set", "cdecl"):
    Vect_cat_set = _libs["grass_vector.8.4"].get("Vect_cat_set", "cdecl")
    Vect_cat_set.argtypes = [POINTER(struct_line_cats), c_int, c_int]
    Vect_cat_set.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 56
if _libs["grass_vector.8.4"].has("Vect_cat_get", "cdecl"):
    Vect_cat_get = _libs["grass_vector.8.4"].get("Vect_cat_get", "cdecl")
    Vect_cat_get.argtypes = [POINTER(struct_line_cats), c_int, POINTER(c_int)]
    Vect_cat_get.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 57
if _libs["grass_vector.8.4"].has("Vect_cat_del", "cdecl"):
    Vect_cat_del = _libs["grass_vector.8.4"].get("Vect_cat_del", "cdecl")
    Vect_cat_del.argtypes = [POINTER(struct_line_cats), c_int]
    Vect_cat_del.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 58
if _libs["grass_vector.8.4"].has("Vect_field_cat_del", "cdecl"):
    Vect_field_cat_del = _libs["grass_vector.8.4"].get("Vect_field_cat_del", "cdecl")
    Vect_field_cat_del.argtypes = [POINTER(struct_line_cats), c_int, c_int]
    Vect_field_cat_del.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 59
if _libs["grass_vector.8.4"].has("Vect_field_cat_get", "cdecl"):
    Vect_field_cat_get = _libs["grass_vector.8.4"].get("Vect_field_cat_get", "cdecl")
    Vect_field_cat_get.argtypes = [POINTER(struct_line_cats), c_int, POINTER(struct_ilist)]
    Vect_field_cat_get.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 60
if _libs["grass_vector.8.4"].has("Vect_cat_in_array", "cdecl"):
    Vect_cat_in_array = _libs["grass_vector.8.4"].get("Vect_cat_in_array", "cdecl")
    Vect_cat_in_array.argtypes = [c_int, POINTER(c_int), c_int]
    Vect_cat_in_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 61
if _libs["grass_vector.8.4"].has("Vect_reset_cats", "cdecl"):
    Vect_reset_cats = _libs["grass_vector.8.4"].get("Vect_reset_cats", "cdecl")
    Vect_reset_cats.argtypes = [POINTER(struct_line_cats)]
    Vect_reset_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 62
if _libs["grass_vector.8.4"].has("Vect_destroy_cats_struct", "cdecl"):
    Vect_destroy_cats_struct = _libs["grass_vector.8.4"].get("Vect_destroy_cats_struct", "cdecl")
    Vect_destroy_cats_struct.argtypes = [POINTER(struct_line_cats)]
    Vect_destroy_cats_struct.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 63
if _libs["grass_vector.8.4"].has("Vect_get_area_cats", "cdecl"):
    Vect_get_area_cats = _libs["grass_vector.8.4"].get("Vect_get_area_cats", "cdecl")
    Vect_get_area_cats.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_cats)]
    Vect_get_area_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 64
if _libs["grass_vector.8.4"].has("Vect_get_area_cat", "cdecl"):
    Vect_get_area_cat = _libs["grass_vector.8.4"].get("Vect_get_area_cat", "cdecl")
    Vect_get_area_cat.argtypes = [POINTER(struct_Map_info), c_int, c_int]
    Vect_get_area_cat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 65
if _libs["grass_vector.8.4"].has("Vect_get_line_cat", "cdecl"):
    Vect_get_line_cat = _libs["grass_vector.8.4"].get("Vect_get_line_cat", "cdecl")
    Vect_get_line_cat.argtypes = [POINTER(struct_Map_info), c_int, c_int]
    Vect_get_line_cat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 66
if _libs["grass_vector.8.4"].has("Vect_cats_set_constraint", "cdecl"):
    Vect_cats_set_constraint = _libs["grass_vector.8.4"].get("Vect_cats_set_constraint", "cdecl")
    Vect_cats_set_constraint.argtypes = [POINTER(struct_Map_info), c_int, String, String]
    Vect_cats_set_constraint.restype = POINTER(struct_cat_list)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 68
if _libs["grass_vector.8.4"].has("Vect_cats_in_constraint", "cdecl"):
    Vect_cats_in_constraint = _libs["grass_vector.8.4"].get("Vect_cats_in_constraint", "cdecl")
    Vect_cats_in_constraint.argtypes = [POINTER(struct_line_cats), c_int, POINTER(struct_cat_list)]
    Vect_cats_in_constraint.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 71
if _libs["grass_vector.8.4"].has("Vect_new_cat_list", "cdecl"):
    Vect_new_cat_list = _libs["grass_vector.8.4"].get("Vect_new_cat_list", "cdecl")
    Vect_new_cat_list.argtypes = []
    Vect_new_cat_list.restype = POINTER(struct_cat_list)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 72
if _libs["grass_vector.8.4"].has("Vect_str_to_cat_list", "cdecl"):
    Vect_str_to_cat_list = _libs["grass_vector.8.4"].get("Vect_str_to_cat_list", "cdecl")
    Vect_str_to_cat_list.argtypes = [String, POINTER(struct_cat_list)]
    Vect_str_to_cat_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 73
if _libs["grass_vector.8.4"].has("Vect_array_to_cat_list", "cdecl"):
    Vect_array_to_cat_list = _libs["grass_vector.8.4"].get("Vect_array_to_cat_list", "cdecl")
    Vect_array_to_cat_list.argtypes = [POINTER(c_int), c_int, POINTER(struct_cat_list)]
    Vect_array_to_cat_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 74
if _libs["grass_vector.8.4"].has("Vect_cat_list_to_array", "cdecl"):
    Vect_cat_list_to_array = _libs["grass_vector.8.4"].get("Vect_cat_list_to_array", "cdecl")
    Vect_cat_list_to_array.argtypes = [POINTER(struct_cat_list), POINTER(POINTER(c_int)), POINTER(c_int)]
    Vect_cat_list_to_array.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 75
if _libs["grass_vector.8.4"].has("Vect_cat_in_cat_list", "cdecl"):
    Vect_cat_in_cat_list = _libs["grass_vector.8.4"].get("Vect_cat_in_cat_list", "cdecl")
    Vect_cat_in_cat_list.argtypes = [c_int, POINTER(struct_cat_list)]
    Vect_cat_in_cat_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 76
if _libs["grass_vector.8.4"].has("Vect_destroy_cat_list", "cdecl"):
    Vect_destroy_cat_list = _libs["grass_vector.8.4"].get("Vect_destroy_cat_list", "cdecl")
    Vect_destroy_cat_list.argtypes = [POINTER(struct_cat_list)]
    Vect_destroy_cat_list.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 79
if _libs["grass_vector.8.4"].has("Vect_new_varray", "cdecl"):
    Vect_new_varray = _libs["grass_vector.8.4"].get("Vect_new_varray", "cdecl")
    Vect_new_varray.argtypes = [c_int]
    Vect_new_varray.restype = POINTER(struct_varray)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 80
if _libs["grass_vector.8.4"].has("Vect_set_varray_from_cat_string", "cdecl"):
    Vect_set_varray_from_cat_string = _libs["grass_vector.8.4"].get("Vect_set_varray_from_cat_string", "cdecl")
    Vect_set_varray_from_cat_string.argtypes = [POINTER(struct_Map_info), c_int, String, c_int, c_int, POINTER(struct_varray)]
    Vect_set_varray_from_cat_string.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 82
if _libs["grass_vector.8.4"].has("Vect_set_varray_from_cat_list", "cdecl"):
    Vect_set_varray_from_cat_list = _libs["grass_vector.8.4"].get("Vect_set_varray_from_cat_list", "cdecl")
    Vect_set_varray_from_cat_list.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_cat_list), c_int, c_int, POINTER(struct_varray)]
    Vect_set_varray_from_cat_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 84
if _libs["grass_vector.8.4"].has("Vect_set_varray_from_db", "cdecl"):
    Vect_set_varray_from_db = _libs["grass_vector.8.4"].get("Vect_set_varray_from_db", "cdecl")
    Vect_set_varray_from_db.argtypes = [POINTER(struct_Map_info), c_int, String, c_int, c_int, POINTER(struct_varray)]
    Vect_set_varray_from_db.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 88
if _libs["grass_vector.8.4"].has("Vect_new_dblinks_struct", "cdecl"):
    Vect_new_dblinks_struct = _libs["grass_vector.8.4"].get("Vect_new_dblinks_struct", "cdecl")
    Vect_new_dblinks_struct.argtypes = []
    Vect_new_dblinks_struct.restype = POINTER(struct_dblinks)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 89
if _libs["grass_vector.8.4"].has("Vect_reset_dblinks", "cdecl"):
    Vect_reset_dblinks = _libs["grass_vector.8.4"].get("Vect_reset_dblinks", "cdecl")
    Vect_reset_dblinks.argtypes = [POINTER(struct_dblinks)]
    Vect_reset_dblinks.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 90
if _libs["grass_vector.8.4"].has("Vect_add_dblink", "cdecl"):
    Vect_add_dblink = _libs["grass_vector.8.4"].get("Vect_add_dblink", "cdecl")
    Vect_add_dblink.argtypes = [POINTER(struct_dblinks), c_int, String, String, String, String, String]
    Vect_add_dblink.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 92
if _libs["grass_vector.8.4"].has("Vect_check_dblink", "cdecl"):
    Vect_check_dblink = _libs["grass_vector.8.4"].get("Vect_check_dblink", "cdecl")
    Vect_check_dblink.argtypes = [POINTER(struct_dblinks), c_int, String]
    Vect_check_dblink.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 93
if _libs["grass_vector.8.4"].has("Vect_map_add_dblink", "cdecl"):
    Vect_map_add_dblink = _libs["grass_vector.8.4"].get("Vect_map_add_dblink", "cdecl")
    Vect_map_add_dblink.argtypes = [POINTER(struct_Map_info), c_int, String, String, String, String, String]
    Vect_map_add_dblink.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 95
if _libs["grass_vector.8.4"].has("Vect_map_del_dblink", "cdecl"):
    Vect_map_del_dblink = _libs["grass_vector.8.4"].get("Vect_map_del_dblink", "cdecl")
    Vect_map_del_dblink.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_map_del_dblink.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 96
if _libs["grass_vector.8.4"].has("Vect_copy_map_dblinks", "cdecl"):
    Vect_copy_map_dblinks = _libs["grass_vector.8.4"].get("Vect_copy_map_dblinks", "cdecl")
    Vect_copy_map_dblinks.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info), c_int]
    Vect_copy_map_dblinks.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 97
if _libs["grass_vector.8.4"].has("Vect_map_check_dblink", "cdecl"):
    Vect_map_check_dblink = _libs["grass_vector.8.4"].get("Vect_map_check_dblink", "cdecl")
    Vect_map_check_dblink.argtypes = [POINTER(struct_Map_info), c_int, String]
    Vect_map_check_dblink.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 98
if _libs["grass_vector.8.4"].has("Vect_read_dblinks", "cdecl"):
    Vect_read_dblinks = _libs["grass_vector.8.4"].get("Vect_read_dblinks", "cdecl")
    Vect_read_dblinks.argtypes = [POINTER(struct_Map_info)]
    Vect_read_dblinks.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 99
if _libs["grass_vector.8.4"].has("Vect_write_dblinks", "cdecl"):
    Vect_write_dblinks = _libs["grass_vector.8.4"].get("Vect_write_dblinks", "cdecl")
    Vect_write_dblinks.argtypes = [POINTER(struct_Map_info)]
    Vect_write_dblinks.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 100
if _libs["grass_vector.8.4"].has("Vect_default_field_info", "cdecl"):
    Vect_default_field_info = _libs["grass_vector.8.4"].get("Vect_default_field_info", "cdecl")
    Vect_default_field_info.argtypes = [POINTER(struct_Map_info), c_int, String, c_int]
    Vect_default_field_info.restype = POINTER(struct_field_info)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 102
if _libs["grass_vector.8.4"].has("Vect_get_dblink", "cdecl"):
    Vect_get_dblink = _libs["grass_vector.8.4"].get("Vect_get_dblink", "cdecl")
    Vect_get_dblink.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_dblink.restype = POINTER(struct_field_info)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 103
if _libs["grass_vector.8.4"].has("Vect_get_field", "cdecl"):
    Vect_get_field = _libs["grass_vector.8.4"].get("Vect_get_field", "cdecl")
    Vect_get_field.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_field.restype = POINTER(struct_field_info)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 104
if _libs["grass_vector.8.4"].has("Vect_get_field_by_name", "cdecl"):
    Vect_get_field_by_name = _libs["grass_vector.8.4"].get("Vect_get_field_by_name", "cdecl")
    Vect_get_field_by_name.argtypes = [POINTER(struct_Map_info), String]
    Vect_get_field_by_name.restype = POINTER(struct_field_info)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 105
if _libs["grass_vector.8.4"].has("Vect_get_field2", "cdecl"):
    Vect_get_field2 = _libs["grass_vector.8.4"].get("Vect_get_field2", "cdecl")
    Vect_get_field2.argtypes = [POINTER(struct_Map_info), String]
    Vect_get_field2.restype = POINTER(struct_field_info)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 106
if _libs["grass_vector.8.4"].has("Vect_get_field_number", "cdecl"):
    Vect_get_field_number = _libs["grass_vector.8.4"].get("Vect_get_field_number", "cdecl")
    Vect_get_field_number.argtypes = [POINTER(struct_Map_info), String]
    Vect_get_field_number.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 107
if _libs["grass_vector.8.4"].has("Vect_set_db_updated", "cdecl"):
    Vect_set_db_updated = _libs["grass_vector.8.4"].get("Vect_set_db_updated", "cdecl")
    Vect_set_db_updated.argtypes = [POINTER(struct_Map_info)]
    Vect_set_db_updated.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 108
if _libs["grass_vector.8.4"].has("Vect_get_column_names", "cdecl"):
    Vect_get_column_names = _libs["grass_vector.8.4"].get("Vect_get_column_names", "cdecl")
    Vect_get_column_names.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_column_names.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 109
if _libs["grass_vector.8.4"].has("Vect_get_column_types", "cdecl"):
    Vect_get_column_types = _libs["grass_vector.8.4"].get("Vect_get_column_types", "cdecl")
    Vect_get_column_types.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_column_types.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 110
if _libs["grass_vector.8.4"].has("Vect_get_column_names_types", "cdecl"):
    Vect_get_column_names_types = _libs["grass_vector.8.4"].get("Vect_get_column_names_types", "cdecl")
    Vect_get_column_names_types.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_column_names_types.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 111
if _libs["grass_vector.8.4"].has("Vect_destroy_field_info", "cdecl"):
    Vect_destroy_field_info = _libs["grass_vector.8.4"].get("Vect_destroy_field_info", "cdecl")
    Vect_destroy_field_info.argtypes = [POINTER(struct_field_info)]
    Vect_destroy_field_info.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 114
if _libs["grass_vector.8.4"].has("Vect_new_list", "cdecl"):
    Vect_new_list = _libs["grass_vector.8.4"].get("Vect_new_list", "cdecl")
    Vect_new_list.argtypes = []
    Vect_new_list.restype = POINTER(struct_ilist)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 115
if _libs["grass_vector.8.4"].has("Vect_list_append", "cdecl"):
    Vect_list_append = _libs["grass_vector.8.4"].get("Vect_list_append", "cdecl")
    Vect_list_append.argtypes = [POINTER(struct_ilist), c_int]
    Vect_list_append.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 116
if _libs["grass_vector.8.4"].has("Vect_list_append_list", "cdecl"):
    Vect_list_append_list = _libs["grass_vector.8.4"].get("Vect_list_append_list", "cdecl")
    Vect_list_append_list.argtypes = [POINTER(struct_ilist), POINTER(struct_ilist)]
    Vect_list_append_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 117
if _libs["grass_vector.8.4"].has("Vect_list_delete", "cdecl"):
    Vect_list_delete = _libs["grass_vector.8.4"].get("Vect_list_delete", "cdecl")
    Vect_list_delete.argtypes = [POINTER(struct_ilist), c_int]
    Vect_list_delete.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 118
if _libs["grass_vector.8.4"].has("Vect_list_delete_list", "cdecl"):
    Vect_list_delete_list = _libs["grass_vector.8.4"].get("Vect_list_delete_list", "cdecl")
    Vect_list_delete_list.argtypes = [POINTER(struct_ilist), POINTER(struct_ilist)]
    Vect_list_delete_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 119
if _libs["grass_vector.8.4"].has("Vect_val_in_list", "cdecl"):
    Vect_val_in_list = _libs["grass_vector.8.4"].get("Vect_val_in_list", "cdecl")
    Vect_val_in_list.argtypes = [POINTER(struct_ilist), c_int]
    Vect_val_in_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 120
if _libs["grass_vector.8.4"].has("Vect_reset_list", "cdecl"):
    Vect_reset_list = _libs["grass_vector.8.4"].get("Vect_reset_list", "cdecl")
    Vect_reset_list.argtypes = [POINTER(struct_ilist)]
    Vect_reset_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 121
if _libs["grass_vector.8.4"].has("Vect_destroy_list", "cdecl"):
    Vect_destroy_list = _libs["grass_vector.8.4"].get("Vect_destroy_list", "cdecl")
    Vect_destroy_list.argtypes = [POINTER(struct_ilist)]
    Vect_destroy_list.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 124
if _libs["grass_vector.8.4"].has("Vect_new_boxlist", "cdecl"):
    Vect_new_boxlist = _libs["grass_vector.8.4"].get("Vect_new_boxlist", "cdecl")
    Vect_new_boxlist.argtypes = [c_int]
    Vect_new_boxlist.restype = POINTER(struct_boxlist)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 125
if _libs["grass_vector.8.4"].has("Vect_boxlist_append", "cdecl"):
    Vect_boxlist_append = _libs["grass_vector.8.4"].get("Vect_boxlist_append", "cdecl")
    Vect_boxlist_append.argtypes = [POINTER(struct_boxlist), c_int, POINTER(struct_bound_box)]
    Vect_boxlist_append.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 126
if _libs["grass_vector.8.4"].has("Vect_boxlist_append_boxlist", "cdecl"):
    Vect_boxlist_append_boxlist = _libs["grass_vector.8.4"].get("Vect_boxlist_append_boxlist", "cdecl")
    Vect_boxlist_append_boxlist.argtypes = [POINTER(struct_boxlist), POINTER(struct_boxlist)]
    Vect_boxlist_append_boxlist.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 127
if _libs["grass_vector.8.4"].has("Vect_boxlist_delete", "cdecl"):
    Vect_boxlist_delete = _libs["grass_vector.8.4"].get("Vect_boxlist_delete", "cdecl")
    Vect_boxlist_delete.argtypes = [POINTER(struct_boxlist), c_int]
    Vect_boxlist_delete.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 128
if _libs["grass_vector.8.4"].has("Vect_boxlist_delete_boxlist", "cdecl"):
    Vect_boxlist_delete_boxlist = _libs["grass_vector.8.4"].get("Vect_boxlist_delete_boxlist", "cdecl")
    Vect_boxlist_delete_boxlist.argtypes = [POINTER(struct_boxlist), POINTER(struct_boxlist)]
    Vect_boxlist_delete_boxlist.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 129
if _libs["grass_vector.8.4"].has("Vect_val_in_boxlist", "cdecl"):
    Vect_val_in_boxlist = _libs["grass_vector.8.4"].get("Vect_val_in_boxlist", "cdecl")
    Vect_val_in_boxlist.argtypes = [POINTER(struct_boxlist), c_int]
    Vect_val_in_boxlist.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 130
if _libs["grass_vector.8.4"].has("Vect_reset_boxlist", "cdecl"):
    Vect_reset_boxlist = _libs["grass_vector.8.4"].get("Vect_reset_boxlist", "cdecl")
    Vect_reset_boxlist.argtypes = [POINTER(struct_boxlist)]
    Vect_reset_boxlist.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 131
if _libs["grass_vector.8.4"].has("Vect_destroy_boxlist", "cdecl"):
    Vect_destroy_boxlist = _libs["grass_vector.8.4"].get("Vect_destroy_boxlist", "cdecl")
    Vect_destroy_boxlist.argtypes = [POINTER(struct_boxlist)]
    Vect_destroy_boxlist.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 134
if _libs["grass_vector.8.4"].has("Vect_point_in_box", "cdecl"):
    Vect_point_in_box = _libs["grass_vector.8.4"].get("Vect_point_in_box", "cdecl")
    Vect_point_in_box.argtypes = [c_double, c_double, c_double, POINTER(struct_bound_box)]
    Vect_point_in_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 135
if _libs["grass_vector.8.4"].has("Vect_point_in_box_2d", "cdecl"):
    Vect_point_in_box_2d = _libs["grass_vector.8.4"].get("Vect_point_in_box_2d", "cdecl")
    Vect_point_in_box_2d.argtypes = [c_double, c_double, POINTER(struct_bound_box)]
    Vect_point_in_box_2d.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 136
if _libs["grass_vector.8.4"].has("Vect_box_overlap", "cdecl"):
    Vect_box_overlap = _libs["grass_vector.8.4"].get("Vect_box_overlap", "cdecl")
    Vect_box_overlap.argtypes = [POINTER(struct_bound_box), POINTER(struct_bound_box)]
    Vect_box_overlap.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 137
if _libs["grass_vector.8.4"].has("Vect_box_copy", "cdecl"):
    Vect_box_copy = _libs["grass_vector.8.4"].get("Vect_box_copy", "cdecl")
    Vect_box_copy.argtypes = [POINTER(struct_bound_box), POINTER(struct_bound_box)]
    Vect_box_copy.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 138
if _libs["grass_vector.8.4"].has("Vect_box_extend", "cdecl"):
    Vect_box_extend = _libs["grass_vector.8.4"].get("Vect_box_extend", "cdecl")
    Vect_box_extend.argtypes = [POINTER(struct_bound_box), POINTER(struct_bound_box)]
    Vect_box_extend.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 139
if _libs["grass_vector.8.4"].has("Vect_box_clip", "cdecl"):
    Vect_box_clip = _libs["grass_vector.8.4"].get("Vect_box_clip", "cdecl")
    Vect_box_clip.argtypes = [POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(struct_bound_box)]
    Vect_box_clip.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 141
if _libs["grass_vector.8.4"].has("Vect_region_box", "cdecl"):
    Vect_region_box = _libs["grass_vector.8.4"].get("Vect_region_box", "cdecl")
    Vect_region_box.argtypes = [POINTER(struct_Cell_head), POINTER(struct_bound_box)]
    Vect_region_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 144
if _libs["grass_vector.8.4"].has("Vect_cidx_get_num_fields", "cdecl"):
    Vect_cidx_get_num_fields = _libs["grass_vector.8.4"].get("Vect_cidx_get_num_fields", "cdecl")
    Vect_cidx_get_num_fields.argtypes = [POINTER(struct_Map_info)]
    Vect_cidx_get_num_fields.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 145
if _libs["grass_vector.8.4"].has("Vect_cidx_get_field_number", "cdecl"):
    Vect_cidx_get_field_number = _libs["grass_vector.8.4"].get("Vect_cidx_get_field_number", "cdecl")
    Vect_cidx_get_field_number.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_cidx_get_field_number.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 146
if _libs["grass_vector.8.4"].has("Vect_cidx_get_field_index", "cdecl"):
    Vect_cidx_get_field_index = _libs["grass_vector.8.4"].get("Vect_cidx_get_field_index", "cdecl")
    Vect_cidx_get_field_index.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_cidx_get_field_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 147
if _libs["grass_vector.8.4"].has("Vect_cidx_get_num_unique_cats_by_index", "cdecl"):
    Vect_cidx_get_num_unique_cats_by_index = _libs["grass_vector.8.4"].get("Vect_cidx_get_num_unique_cats_by_index", "cdecl")
    Vect_cidx_get_num_unique_cats_by_index.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_cidx_get_num_unique_cats_by_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 148
if _libs["grass_vector.8.4"].has("Vect_cidx_get_num_cats_by_index", "cdecl"):
    Vect_cidx_get_num_cats_by_index = _libs["grass_vector.8.4"].get("Vect_cidx_get_num_cats_by_index", "cdecl")
    Vect_cidx_get_num_cats_by_index.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_cidx_get_num_cats_by_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 149
if _libs["grass_vector.8.4"].has("Vect_cidx_get_num_types_by_index", "cdecl"):
    Vect_cidx_get_num_types_by_index = _libs["grass_vector.8.4"].get("Vect_cidx_get_num_types_by_index", "cdecl")
    Vect_cidx_get_num_types_by_index.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_cidx_get_num_types_by_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 150
if _libs["grass_vector.8.4"].has("Vect_cidx_get_type_count_by_index", "cdecl"):
    Vect_cidx_get_type_count_by_index = _libs["grass_vector.8.4"].get("Vect_cidx_get_type_count_by_index", "cdecl")
    Vect_cidx_get_type_count_by_index.argtypes = [POINTER(struct_Map_info), c_int, c_int, POINTER(c_int), POINTER(c_int)]
    Vect_cidx_get_type_count_by_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 152
if _libs["grass_vector.8.4"].has("Vect_cidx_get_type_count", "cdecl"):
    Vect_cidx_get_type_count = _libs["grass_vector.8.4"].get("Vect_cidx_get_type_count", "cdecl")
    Vect_cidx_get_type_count.argtypes = [POINTER(struct_Map_info), c_int, c_int]
    Vect_cidx_get_type_count.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 153
if _libs["grass_vector.8.4"].has("Vect_cidx_get_cat_by_index", "cdecl"):
    Vect_cidx_get_cat_by_index = _libs["grass_vector.8.4"].get("Vect_cidx_get_cat_by_index", "cdecl")
    Vect_cidx_get_cat_by_index.argtypes = [POINTER(struct_Map_info), c_int, c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    Vect_cidx_get_cat_by_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 155
if _libs["grass_vector.8.4"].has("Vect_cidx_get_unique_cats_by_index", "cdecl"):
    Vect_cidx_get_unique_cats_by_index = _libs["grass_vector.8.4"].get("Vect_cidx_get_unique_cats_by_index", "cdecl")
    Vect_cidx_get_unique_cats_by_index.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_ilist)]
    Vect_cidx_get_unique_cats_by_index.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 156
if _libs["grass_vector.8.4"].has("Vect_cidx_find_next", "cdecl"):
    Vect_cidx_find_next = _libs["grass_vector.8.4"].get("Vect_cidx_find_next", "cdecl")
    Vect_cidx_find_next.argtypes = [POINTER(struct_Map_info), c_int, c_int, c_int, c_int, POINTER(c_int), POINTER(c_int)]
    Vect_cidx_find_next.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 157
if _libs["grass_vector.8.4"].has("Vect_cidx_find_all", "cdecl"):
    Vect_cidx_find_all = _libs["grass_vector.8.4"].get("Vect_cidx_find_all", "cdecl")
    Vect_cidx_find_all.argtypes = [POINTER(struct_Map_info), c_int, c_int, c_int, POINTER(struct_ilist)]
    Vect_cidx_find_all.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 158
if _libs["grass_vector.8.4"].has("Vect_cidx_dump", "cdecl"):
    Vect_cidx_dump = _libs["grass_vector.8.4"].get("Vect_cidx_dump", "cdecl")
    Vect_cidx_dump.argtypes = [POINTER(struct_Map_info), POINTER(FILE)]
    Vect_cidx_dump.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 159
if _libs["grass_vector.8.4"].has("Vect_cidx_save", "cdecl"):
    Vect_cidx_save = _libs["grass_vector.8.4"].get("Vect_cidx_save", "cdecl")
    Vect_cidx_save.argtypes = [POINTER(struct_Map_info)]
    Vect_cidx_save.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 160
if _libs["grass_vector.8.4"].has("Vect_cidx_open", "cdecl"):
    Vect_cidx_open = _libs["grass_vector.8.4"].get("Vect_cidx_open", "cdecl")
    Vect_cidx_open.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_cidx_open.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 163
if _libs["grass_vector.8.4"].has("Vect_new_map_struct", "cdecl"):
    Vect_new_map_struct = _libs["grass_vector.8.4"].get("Vect_new_map_struct", "cdecl")
    Vect_new_map_struct.argtypes = []
    Vect_new_map_struct.restype = POINTER(struct_Map_info)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 164
if _libs["grass_vector.8.4"].has("Vect_destroy_map_struct", "cdecl"):
    Vect_destroy_map_struct = _libs["grass_vector.8.4"].get("Vect_destroy_map_struct", "cdecl")
    Vect_destroy_map_struct.argtypes = [POINTER(struct_Map_info)]
    Vect_destroy_map_struct.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 167
if _libs["grass_vector.8.4"].has("Vect_read_header", "cdecl"):
    Vect_read_header = _libs["grass_vector.8.4"].get("Vect_read_header", "cdecl")
    Vect_read_header.argtypes = [POINTER(struct_Map_info)]
    Vect_read_header.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 168
if _libs["grass_vector.8.4"].has("Vect_write_header", "cdecl"):
    Vect_write_header = _libs["grass_vector.8.4"].get("Vect_write_header", "cdecl")
    Vect_write_header.argtypes = [POINTER(struct_Map_info)]
    Vect_write_header.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 169
if _libs["grass_vector.8.4"].has("Vect_get_name", "cdecl"):
    Vect_get_name = _libs["grass_vector.8.4"].get("Vect_get_name", "cdecl")
    Vect_get_name.argtypes = [POINTER(struct_Map_info)]
    Vect_get_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 170
if _libs["grass_vector.8.4"].has("Vect_get_mapset", "cdecl"):
    Vect_get_mapset = _libs["grass_vector.8.4"].get("Vect_get_mapset", "cdecl")
    Vect_get_mapset.argtypes = [POINTER(struct_Map_info)]
    Vect_get_mapset.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 171
if _libs["grass_vector.8.4"].has("Vect_get_full_name", "cdecl"):
    Vect_get_full_name = _libs["grass_vector.8.4"].get("Vect_get_full_name", "cdecl")
    Vect_get_full_name.argtypes = [POINTER(struct_Map_info)]
    Vect_get_full_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 172
if _libs["grass_vector.8.4"].has("Vect_get_finfo_dsn_name", "cdecl"):
    Vect_get_finfo_dsn_name = _libs["grass_vector.8.4"].get("Vect_get_finfo_dsn_name", "cdecl")
    Vect_get_finfo_dsn_name.argtypes = [POINTER(struct_Map_info)]
    Vect_get_finfo_dsn_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 173
if _libs["grass_vector.8.4"].has("Vect_get_finfo_layer_name", "cdecl"):
    Vect_get_finfo_layer_name = _libs["grass_vector.8.4"].get("Vect_get_finfo_layer_name", "cdecl")
    Vect_get_finfo_layer_name.argtypes = [POINTER(struct_Map_info)]
    if sizeof(c_int) == sizeof(c_void_p):
        Vect_get_finfo_layer_name.restype = ReturnString
    else:
        Vect_get_finfo_layer_name.restype = String
        Vect_get_finfo_layer_name.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 174
if _libs["grass_vector.8.4"].has("Vect_get_finfo_format_info", "cdecl"):
    Vect_get_finfo_format_info = _libs["grass_vector.8.4"].get("Vect_get_finfo_format_info", "cdecl")
    Vect_get_finfo_format_info.argtypes = [POINTER(struct_Map_info)]
    Vect_get_finfo_format_info.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 175
if _libs["grass_vector.8.4"].has("Vect_get_finfo_geometry_type", "cdecl"):
    Vect_get_finfo_geometry_type = _libs["grass_vector.8.4"].get("Vect_get_finfo_geometry_type", "cdecl")
    Vect_get_finfo_geometry_type.argtypes = [POINTER(struct_Map_info)]
    Vect_get_finfo_geometry_type.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 176
if _libs["grass_vector.8.4"].has("Vect_get_finfo", "cdecl"):
    Vect_get_finfo = _libs["grass_vector.8.4"].get("Vect_get_finfo", "cdecl")
    Vect_get_finfo.argtypes = [POINTER(struct_Map_info)]
    Vect_get_finfo.restype = POINTER(struct_Format_info)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 177
if _libs["grass_vector.8.4"].has("Vect_get_finfo_topology_info", "cdecl"):
    Vect_get_finfo_topology_info = _libs["grass_vector.8.4"].get("Vect_get_finfo_topology_info", "cdecl")
    Vect_get_finfo_topology_info.argtypes = [POINTER(struct_Map_info), POINTER(POINTER(c_char)), POINTER(POINTER(c_char)), POINTER(c_int)]
    Vect_get_finfo_topology_info.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 178
if _libs["grass_vector.8.4"].has("Vect_is_3d", "cdecl"):
    Vect_is_3d = _libs["grass_vector.8.4"].get("Vect_is_3d", "cdecl")
    Vect_is_3d.argtypes = [POINTER(struct_Map_info)]
    Vect_is_3d.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 179
if _libs["grass_vector.8.4"].has("Vect_set_organization", "cdecl"):
    Vect_set_organization = _libs["grass_vector.8.4"].get("Vect_set_organization", "cdecl")
    Vect_set_organization.argtypes = [POINTER(struct_Map_info), String]
    Vect_set_organization.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 180
if _libs["grass_vector.8.4"].has("Vect_get_organization", "cdecl"):
    Vect_get_organization = _libs["grass_vector.8.4"].get("Vect_get_organization", "cdecl")
    Vect_get_organization.argtypes = [POINTER(struct_Map_info)]
    Vect_get_organization.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 181
if _libs["grass_vector.8.4"].has("Vect_set_date", "cdecl"):
    Vect_set_date = _libs["grass_vector.8.4"].get("Vect_set_date", "cdecl")
    Vect_set_date.argtypes = [POINTER(struct_Map_info), String]
    Vect_set_date.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 182
if _libs["grass_vector.8.4"].has("Vect_get_date", "cdecl"):
    Vect_get_date = _libs["grass_vector.8.4"].get("Vect_get_date", "cdecl")
    Vect_get_date.argtypes = [POINTER(struct_Map_info)]
    Vect_get_date.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 183
if _libs["grass_vector.8.4"].has("Vect_set_person", "cdecl"):
    Vect_set_person = _libs["grass_vector.8.4"].get("Vect_set_person", "cdecl")
    Vect_set_person.argtypes = [POINTER(struct_Map_info), String]
    Vect_set_person.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 184
if _libs["grass_vector.8.4"].has("Vect_get_person", "cdecl"):
    Vect_get_person = _libs["grass_vector.8.4"].get("Vect_get_person", "cdecl")
    Vect_get_person.argtypes = [POINTER(struct_Map_info)]
    Vect_get_person.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 185
if _libs["grass_vector.8.4"].has("Vect_set_map_name", "cdecl"):
    Vect_set_map_name = _libs["grass_vector.8.4"].get("Vect_set_map_name", "cdecl")
    Vect_set_map_name.argtypes = [POINTER(struct_Map_info), String]
    Vect_set_map_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 186
if _libs["grass_vector.8.4"].has("Vect_get_map_name", "cdecl"):
    Vect_get_map_name = _libs["grass_vector.8.4"].get("Vect_get_map_name", "cdecl")
    Vect_get_map_name.argtypes = [POINTER(struct_Map_info)]
    Vect_get_map_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 187
if _libs["grass_vector.8.4"].has("Vect_set_map_date", "cdecl"):
    Vect_set_map_date = _libs["grass_vector.8.4"].get("Vect_set_map_date", "cdecl")
    Vect_set_map_date.argtypes = [POINTER(struct_Map_info), String]
    Vect_set_map_date.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 188
if _libs["grass_vector.8.4"].has("Vect_get_map_date", "cdecl"):
    Vect_get_map_date = _libs["grass_vector.8.4"].get("Vect_get_map_date", "cdecl")
    Vect_get_map_date.argtypes = [POINTER(struct_Map_info)]
    Vect_get_map_date.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 189
if _libs["grass_vector.8.4"].has("Vect_set_comment", "cdecl"):
    Vect_set_comment = _libs["grass_vector.8.4"].get("Vect_set_comment", "cdecl")
    Vect_set_comment.argtypes = [POINTER(struct_Map_info), String]
    Vect_set_comment.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 190
if _libs["grass_vector.8.4"].has("Vect_get_comment", "cdecl"):
    Vect_get_comment = _libs["grass_vector.8.4"].get("Vect_get_comment", "cdecl")
    Vect_get_comment.argtypes = [POINTER(struct_Map_info)]
    Vect_get_comment.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 191
if _libs["grass_vector.8.4"].has("Vect_set_scale", "cdecl"):
    Vect_set_scale = _libs["grass_vector.8.4"].get("Vect_set_scale", "cdecl")
    Vect_set_scale.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_set_scale.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 192
if _libs["grass_vector.8.4"].has("Vect_get_scale", "cdecl"):
    Vect_get_scale = _libs["grass_vector.8.4"].get("Vect_get_scale", "cdecl")
    Vect_get_scale.argtypes = [POINTER(struct_Map_info)]
    Vect_get_scale.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 193
if _libs["grass_vector.8.4"].has("Vect_set_zone", "cdecl"):
    Vect_set_zone = _libs["grass_vector.8.4"].get("Vect_set_zone", "cdecl")
    Vect_set_zone.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_set_zone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 194
if _libs["grass_vector.8.4"].has("Vect_get_zone", "cdecl"):
    Vect_get_zone = _libs["grass_vector.8.4"].get("Vect_get_zone", "cdecl")
    Vect_get_zone.argtypes = [POINTER(struct_Map_info)]
    Vect_get_zone.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 195
if _libs["grass_vector.8.4"].has("Vect_get_proj", "cdecl"):
    Vect_get_proj = _libs["grass_vector.8.4"].get("Vect_get_proj", "cdecl")
    Vect_get_proj.argtypes = [POINTER(struct_Map_info)]
    Vect_get_proj.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 196
if _libs["grass_vector.8.4"].has("Vect_set_proj", "cdecl"):
    Vect_set_proj = _libs["grass_vector.8.4"].get("Vect_set_proj", "cdecl")
    Vect_set_proj.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_set_proj.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 197
if _libs["grass_vector.8.4"].has("Vect_get_proj_name", "cdecl"):
    Vect_get_proj_name = _libs["grass_vector.8.4"].get("Vect_get_proj_name", "cdecl")
    Vect_get_proj_name.argtypes = [POINTER(struct_Map_info)]
    Vect_get_proj_name.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 198
if _libs["grass_vector.8.4"].has("Vect_set_thresh", "cdecl"):
    Vect_set_thresh = _libs["grass_vector.8.4"].get("Vect_set_thresh", "cdecl")
    Vect_set_thresh.argtypes = [POINTER(struct_Map_info), c_double]
    Vect_set_thresh.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 199
if _libs["grass_vector.8.4"].has("Vect_get_thresh", "cdecl"):
    Vect_get_thresh = _libs["grass_vector.8.4"].get("Vect_get_thresh", "cdecl")
    Vect_get_thresh.argtypes = [POINTER(struct_Map_info)]
    Vect_get_thresh.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 200
if _libs["grass_vector.8.4"].has("Vect_get_constraint_box", "cdecl"):
    Vect_get_constraint_box = _libs["grass_vector.8.4"].get("Vect_get_constraint_box", "cdecl")
    Vect_get_constraint_box.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box)]
    Vect_get_constraint_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 203
if _libs["grass_vector.8.4"].has("Vect_level", "cdecl"):
    Vect_level = _libs["grass_vector.8.4"].get("Vect_level", "cdecl")
    Vect_level.argtypes = [POINTER(struct_Map_info)]
    Vect_level.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 206
if _libs["grass_vector.8.4"].has("Vect_get_map_box1", "cdecl"):
    Vect_get_map_box1 = _libs["grass_vector.8.4"].get("Vect_get_map_box1", "cdecl")
    Vect_get_map_box1.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box)]
    Vect_get_map_box1.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 209
if _libs["grass_vector.8.4"].has("Vect_get_line_type", "cdecl"):
    Vect_get_line_type = _libs["grass_vector.8.4"].get("Vect_get_line_type", "cdecl")
    Vect_get_line_type.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_line_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 210
if _libs["grass_vector.8.4"].has("Vect_get_num_nodes", "cdecl"):
    Vect_get_num_nodes = _libs["grass_vector.8.4"].get("Vect_get_num_nodes", "cdecl")
    Vect_get_num_nodes.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_nodes.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 211
if _libs["grass_vector.8.4"].has("Vect_get_num_primitives", "cdecl"):
    Vect_get_num_primitives = _libs["grass_vector.8.4"].get("Vect_get_num_primitives", "cdecl")
    Vect_get_num_primitives.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_num_primitives.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 212
if _libs["grass_vector.8.4"].has("Vect_get_num_lines", "cdecl"):
    Vect_get_num_lines = _libs["grass_vector.8.4"].get("Vect_get_num_lines", "cdecl")
    Vect_get_num_lines.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_lines.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 213
if _libs["grass_vector.8.4"].has("Vect_get_num_areas", "cdecl"):
    Vect_get_num_areas = _libs["grass_vector.8.4"].get("Vect_get_num_areas", "cdecl")
    Vect_get_num_areas.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_areas.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 214
if _libs["grass_vector.8.4"].has("Vect_get_num_faces", "cdecl"):
    Vect_get_num_faces = _libs["grass_vector.8.4"].get("Vect_get_num_faces", "cdecl")
    Vect_get_num_faces.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_faces.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 215
if _libs["grass_vector.8.4"].has("Vect_get_num_kernels", "cdecl"):
    Vect_get_num_kernels = _libs["grass_vector.8.4"].get("Vect_get_num_kernels", "cdecl")
    Vect_get_num_kernels.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_kernels.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 216
if _libs["grass_vector.8.4"].has("Vect_get_num_volumes", "cdecl"):
    Vect_get_num_volumes = _libs["grass_vector.8.4"].get("Vect_get_num_volumes", "cdecl")
    Vect_get_num_volumes.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_volumes.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 217
if _libs["grass_vector.8.4"].has("Vect_get_num_islands", "cdecl"):
    Vect_get_num_islands = _libs["grass_vector.8.4"].get("Vect_get_num_islands", "cdecl")
    Vect_get_num_islands.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_islands.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 218
if _libs["grass_vector.8.4"].has("Vect_get_num_holes", "cdecl"):
    Vect_get_num_holes = _libs["grass_vector.8.4"].get("Vect_get_num_holes", "cdecl")
    Vect_get_num_holes.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_holes.restype = plus_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 219
if _libs["grass_vector.8.4"].has("Vect_get_line_box", "cdecl"):
    Vect_get_line_box = _libs["grass_vector.8.4"].get("Vect_get_line_box", "cdecl")
    Vect_get_line_box.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_bound_box)]
    Vect_get_line_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 220
if _libs["grass_vector.8.4"].has("Vect_get_area_box", "cdecl"):
    Vect_get_area_box = _libs["grass_vector.8.4"].get("Vect_get_area_box", "cdecl")
    Vect_get_area_box.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_bound_box)]
    Vect_get_area_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 221
if _libs["grass_vector.8.4"].has("Vect_get_isle_box", "cdecl"):
    Vect_get_isle_box = _libs["grass_vector.8.4"].get("Vect_get_isle_box", "cdecl")
    Vect_get_isle_box.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_bound_box)]
    Vect_get_isle_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 222
if _libs["grass_vector.8.4"].has("Vect_get_map_box", "cdecl"):
    Vect_get_map_box = _libs["grass_vector.8.4"].get("Vect_get_map_box", "cdecl")
    Vect_get_map_box.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box)]
    Vect_get_map_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 223
if _libs["grass_vector.8.4"].has("V__map_overlap", "cdecl"):
    V__map_overlap = _libs["grass_vector.8.4"].get("V__map_overlap", "cdecl")
    V__map_overlap.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_double]
    V__map_overlap.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 224
if _libs["grass_vector.8.4"].has("Vect_set_release_support", "cdecl"):
    Vect_set_release_support = _libs["grass_vector.8.4"].get("Vect_set_release_support", "cdecl")
    Vect_set_release_support.argtypes = [POINTER(struct_Map_info)]
    Vect_set_release_support.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 225
if _libs["grass_vector.8.4"].has("Vect_set_category_index_update", "cdecl"):
    Vect_set_category_index_update = _libs["grass_vector.8.4"].get("Vect_set_category_index_update", "cdecl")
    Vect_set_category_index_update.argtypes = [POINTER(struct_Map_info)]
    Vect_set_category_index_update.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 228
if _libs["grass_vector.8.4"].has("Vect_check_input_output_name", "cdecl"):
    Vect_check_input_output_name = _libs["grass_vector.8.4"].get("Vect_check_input_output_name", "cdecl")
    Vect_check_input_output_name.argtypes = [String, String, c_int]
    Vect_check_input_output_name.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 229
if _libs["grass_vector.8.4"].has("Vect_legal_filename", "cdecl"):
    Vect_legal_filename = _libs["grass_vector.8.4"].get("Vect_legal_filename", "cdecl")
    Vect_legal_filename.argtypes = [String]
    Vect_legal_filename.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 230
if _libs["grass_vector.8.4"].has("Vect_set_open_level", "cdecl"):
    Vect_set_open_level = _libs["grass_vector.8.4"].get("Vect_set_open_level", "cdecl")
    Vect_set_open_level.argtypes = [c_int]
    Vect_set_open_level.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 231
if _libs["grass_vector.8.4"].has("Vect_open_old", "cdecl"):
    Vect_open_old = _libs["grass_vector.8.4"].get("Vect_open_old", "cdecl")
    Vect_open_old.argtypes = [POINTER(struct_Map_info), String, String]
    Vect_open_old.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 232
if _libs["grass_vector.8.4"].has("Vect_open_tmp_old", "cdecl"):
    Vect_open_tmp_old = _libs["grass_vector.8.4"].get("Vect_open_tmp_old", "cdecl")
    Vect_open_tmp_old.argtypes = [POINTER(struct_Map_info), String, String]
    Vect_open_tmp_old.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 233
if _libs["grass_vector.8.4"].has("Vect_open_old2", "cdecl"):
    Vect_open_old2 = _libs["grass_vector.8.4"].get("Vect_open_old2", "cdecl")
    Vect_open_old2.argtypes = [POINTER(struct_Map_info), String, String, String]
    Vect_open_old2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 234
if _libs["grass_vector.8.4"].has("Vect_open_old_head", "cdecl"):
    Vect_open_old_head = _libs["grass_vector.8.4"].get("Vect_open_old_head", "cdecl")
    Vect_open_old_head.argtypes = [POINTER(struct_Map_info), String, String]
    Vect_open_old_head.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 235
if _libs["grass_vector.8.4"].has("Vect_open_old_head2", "cdecl"):
    Vect_open_old_head2 = _libs["grass_vector.8.4"].get("Vect_open_old_head2", "cdecl")
    Vect_open_old_head2.argtypes = [POINTER(struct_Map_info), String, String, String]
    Vect_open_old_head2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 237
if _libs["grass_vector.8.4"].has("Vect_open_new", "cdecl"):
    Vect_open_new = _libs["grass_vector.8.4"].get("Vect_open_new", "cdecl")
    Vect_open_new.argtypes = [POINTER(struct_Map_info), String, c_int]
    Vect_open_new.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 238
if _libs["grass_vector.8.4"].has("Vect_open_tmp_new", "cdecl"):
    Vect_open_tmp_new = _libs["grass_vector.8.4"].get("Vect_open_tmp_new", "cdecl")
    Vect_open_tmp_new.argtypes = [POINTER(struct_Map_info), String, c_int]
    Vect_open_tmp_new.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 239
if _libs["grass_vector.8.4"].has("Vect_open_update", "cdecl"):
    Vect_open_update = _libs["grass_vector.8.4"].get("Vect_open_update", "cdecl")
    Vect_open_update.argtypes = [POINTER(struct_Map_info), String, String]
    Vect_open_update.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 240
if _libs["grass_vector.8.4"].has("Vect_open_tmp_update", "cdecl"):
    Vect_open_tmp_update = _libs["grass_vector.8.4"].get("Vect_open_tmp_update", "cdecl")
    Vect_open_tmp_update.argtypes = [POINTER(struct_Map_info), String, String]
    Vect_open_tmp_update.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 241
if _libs["grass_vector.8.4"].has("Vect_open_update2", "cdecl"):
    Vect_open_update2 = _libs["grass_vector.8.4"].get("Vect_open_update2", "cdecl")
    Vect_open_update2.argtypes = [POINTER(struct_Map_info), String, String, String]
    Vect_open_update2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 243
if _libs["grass_vector.8.4"].has("Vect_open_update_head", "cdecl"):
    Vect_open_update_head = _libs["grass_vector.8.4"].get("Vect_open_update_head", "cdecl")
    Vect_open_update_head.argtypes = [POINTER(struct_Map_info), String, String]
    Vect_open_update_head.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 244
if _libs["grass_vector.8.4"].has("Vect_copy_head_data", "cdecl"):
    Vect_copy_head_data = _libs["grass_vector.8.4"].get("Vect_copy_head_data", "cdecl")
    Vect_copy_head_data.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info)]
    Vect_copy_head_data.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 245
if _libs["grass_vector.8.4"].has("Vect_build", "cdecl"):
    Vect_build = _libs["grass_vector.8.4"].get("Vect_build", "cdecl")
    Vect_build.argtypes = [POINTER(struct_Map_info)]
    Vect_build.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 246
if _libs["grass_vector.8.4"].has("Vect_topo_check", "cdecl"):
    Vect_topo_check = _libs["grass_vector.8.4"].get("Vect_topo_check", "cdecl")
    Vect_topo_check.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info)]
    Vect_topo_check.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 247
if _libs["grass_vector.8.4"].has("Vect_get_built", "cdecl"):
    Vect_get_built = _libs["grass_vector.8.4"].get("Vect_get_built", "cdecl")
    Vect_get_built.argtypes = [POINTER(struct_Map_info)]
    Vect_get_built.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 248
if _libs["grass_vector.8.4"].has("Vect_build_partial", "cdecl"):
    Vect_build_partial = _libs["grass_vector.8.4"].get("Vect_build_partial", "cdecl")
    Vect_build_partial.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_build_partial.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 249
if _libs["grass_vector.8.4"].has("Vect_set_constraint_region", "cdecl"):
    Vect_set_constraint_region = _libs["grass_vector.8.4"].get("Vect_set_constraint_region", "cdecl")
    Vect_set_constraint_region.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_double, c_double, c_double]
    Vect_set_constraint_region.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 251
if _libs["grass_vector.8.4"].has("Vect_set_constraint_type", "cdecl"):
    Vect_set_constraint_type = _libs["grass_vector.8.4"].get("Vect_set_constraint_type", "cdecl")
    Vect_set_constraint_type.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_set_constraint_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 252
if _libs["grass_vector.8.4"].has("Vect_set_constraint_field", "cdecl"):
    Vect_set_constraint_field = _libs["grass_vector.8.4"].get("Vect_set_constraint_field", "cdecl")
    Vect_set_constraint_field.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_set_constraint_field.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 253
if _libs["grass_vector.8.4"].has("Vect_remove_constraints", "cdecl"):
    Vect_remove_constraints = _libs["grass_vector.8.4"].get("Vect_remove_constraints", "cdecl")
    Vect_remove_constraints.argtypes = [POINTER(struct_Map_info)]
    Vect_remove_constraints.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 254
if _libs["grass_vector.8.4"].has("Vect_rewind", "cdecl"):
    Vect_rewind = _libs["grass_vector.8.4"].get("Vect_rewind", "cdecl")
    Vect_rewind.argtypes = [POINTER(struct_Map_info)]
    Vect_rewind.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 255
if _libs["grass_vector.8.4"].has("Vect_close", "cdecl"):
    Vect_close = _libs["grass_vector.8.4"].get("Vect_close", "cdecl")
    Vect_close.argtypes = [POINTER(struct_Map_info)]
    Vect_close.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 256
if _libs["grass_vector.8.4"].has("Vect_set_error_handler_io", "cdecl"):
    Vect_set_error_handler_io = _libs["grass_vector.8.4"].get("Vect_set_error_handler_io", "cdecl")
    Vect_set_error_handler_io.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info)]
    Vect_set_error_handler_io.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 260
if _libs["grass_vector.8.4"].has("Vect_get_next_line_id", "cdecl"):
    Vect_get_next_line_id = _libs["grass_vector.8.4"].get("Vect_get_next_line_id", "cdecl")
    Vect_get_next_line_id.argtypes = [POINTER(struct_Map_info)]
    Vect_get_next_line_id.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 261
if _libs["grass_vector.8.4"].has("Vect_read_next_line", "cdecl"):
    Vect_read_next_line = _libs["grass_vector.8.4"].get("Vect_read_next_line", "cdecl")
    Vect_read_next_line.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    Vect_read_next_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 263
if _libs["grass_vector.8.4"].has("Vect_write_line", "cdecl"):
    Vect_write_line = _libs["grass_vector.8.4"].get("Vect_write_line", "cdecl")
    Vect_write_line.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    Vect_write_line.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 265
if _libs["grass_vector.8.4"].has("Vect_rewrite_line", "cdecl"):
    Vect_rewrite_line = _libs["grass_vector.8.4"].get("Vect_rewrite_line", "cdecl")
    Vect_rewrite_line.argtypes = [POINTER(struct_Map_info), off_t, c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    Vect_rewrite_line.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 267
if _libs["grass_vector.8.4"].has("Vect_delete_line", "cdecl"):
    Vect_delete_line = _libs["grass_vector.8.4"].get("Vect_delete_line", "cdecl")
    Vect_delete_line.argtypes = [POINTER(struct_Map_info), off_t]
    Vect_delete_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 268
if _libs["grass_vector.8.4"].has("Vect_restore_line", "cdecl"):
    Vect_restore_line = _libs["grass_vector.8.4"].get("Vect_restore_line", "cdecl")
    Vect_restore_line.argtypes = [POINTER(struct_Map_info), off_t, off_t]
    Vect_restore_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 270
if _libs["grass_vector.8.4"].has("Vect_get_num_dblinks", "cdecl"):
    Vect_get_num_dblinks = _libs["grass_vector.8.4"].get("Vect_get_num_dblinks", "cdecl")
    Vect_get_num_dblinks.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_dblinks.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 273
if _libs["grass_vector.8.4"].has("Vect_read_line", "cdecl"):
    Vect_read_line = _libs["grass_vector.8.4"].get("Vect_read_line", "cdecl")
    Vect_read_line.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats), c_int]
    Vect_read_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 276
if _libs["grass_vector.8.4"].has("Vect_line_alive", "cdecl"):
    Vect_line_alive = _libs["grass_vector.8.4"].get("Vect_line_alive", "cdecl")
    Vect_line_alive.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_line_alive.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 277
if _libs["grass_vector.8.4"].has("Vect_node_alive", "cdecl"):
    Vect_node_alive = _libs["grass_vector.8.4"].get("Vect_node_alive", "cdecl")
    Vect_node_alive.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_node_alive.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 278
if _libs["grass_vector.8.4"].has("Vect_area_alive", "cdecl"):
    Vect_area_alive = _libs["grass_vector.8.4"].get("Vect_area_alive", "cdecl")
    Vect_area_alive.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_area_alive.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 279
if _libs["grass_vector.8.4"].has("Vect_isle_alive", "cdecl"):
    Vect_isle_alive = _libs["grass_vector.8.4"].get("Vect_isle_alive", "cdecl")
    Vect_isle_alive.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_isle_alive.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 280
if _libs["grass_vector.8.4"].has("Vect_get_line_nodes", "cdecl"):
    Vect_get_line_nodes = _libs["grass_vector.8.4"].get("Vect_get_line_nodes", "cdecl")
    Vect_get_line_nodes.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_int), POINTER(c_int)]
    Vect_get_line_nodes.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 281
if _libs["grass_vector.8.4"].has("Vect_get_line_areas", "cdecl"):
    Vect_get_line_areas = _libs["grass_vector.8.4"].get("Vect_get_line_areas", "cdecl")
    Vect_get_line_areas.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_int), POINTER(c_int)]
    Vect_get_line_areas.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 282
if _libs["grass_vector.8.4"].has("Vect_get_line_offset", "cdecl"):
    Vect_get_line_offset = _libs["grass_vector.8.4"].get("Vect_get_line_offset", "cdecl")
    Vect_get_line_offset.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_line_offset.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 284
if _libs["grass_vector.8.4"].has("Vect_get_node_coor", "cdecl"):
    Vect_get_node_coor = _libs["grass_vector.8.4"].get("Vect_get_node_coor", "cdecl")
    Vect_get_node_coor.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    Vect_get_node_coor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 285
if _libs["grass_vector.8.4"].has("Vect_get_node_n_lines", "cdecl"):
    Vect_get_node_n_lines = _libs["grass_vector.8.4"].get("Vect_get_node_n_lines", "cdecl")
    Vect_get_node_n_lines.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_node_n_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 286
if _libs["grass_vector.8.4"].has("Vect_get_node_line", "cdecl"):
    Vect_get_node_line = _libs["grass_vector.8.4"].get("Vect_get_node_line", "cdecl")
    Vect_get_node_line.argtypes = [POINTER(struct_Map_info), c_int, c_int]
    Vect_get_node_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 287
if _libs["grass_vector.8.4"].has("Vect_get_node_line_angle", "cdecl"):
    Vect_get_node_line_angle = _libs["grass_vector.8.4"].get("Vect_get_node_line_angle", "cdecl")
    Vect_get_node_line_angle.argtypes = [POINTER(struct_Map_info), c_int, c_int]
    Vect_get_node_line_angle.restype = c_float

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 289
if _libs["grass_vector.8.4"].has("Vect_get_area_points", "cdecl"):
    Vect_get_area_points = _libs["grass_vector.8.4"].get("Vect_get_area_points", "cdecl")
    Vect_get_area_points.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts)]
    Vect_get_area_points.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 290
if _libs["grass_vector.8.4"].has("Vect_get_area_centroid", "cdecl"):
    Vect_get_area_centroid = _libs["grass_vector.8.4"].get("Vect_get_area_centroid", "cdecl")
    Vect_get_area_centroid.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_area_centroid.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 291
if _libs["grass_vector.8.4"].has("Vect_get_area_num_isles", "cdecl"):
    Vect_get_area_num_isles = _libs["grass_vector.8.4"].get("Vect_get_area_num_isles", "cdecl")
    Vect_get_area_num_isles.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_area_num_isles.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 292
if _libs["grass_vector.8.4"].has("Vect_get_area_isle", "cdecl"):
    Vect_get_area_isle = _libs["grass_vector.8.4"].get("Vect_get_area_isle", "cdecl")
    Vect_get_area_isle.argtypes = [POINTER(struct_Map_info), c_int, c_int]
    Vect_get_area_isle.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 293
if _libs["grass_vector.8.4"].has("Vect_get_area_perimeter", "cdecl"):
    Vect_get_area_perimeter = _libs["grass_vector.8.4"].get("Vect_get_area_perimeter", "cdecl")
    Vect_get_area_perimeter.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_area_perimeter.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 294
if _libs["grass_vector.8.4"].has("Vect_get_area_area", "cdecl"):
    Vect_get_area_area = _libs["grass_vector.8.4"].get("Vect_get_area_area", "cdecl")
    Vect_get_area_area.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_area_area.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 295
if _libs["grass_vector.8.4"].has("Vect_get_area_boundaries", "cdecl"):
    Vect_get_area_boundaries = _libs["grass_vector.8.4"].get("Vect_get_area_boundaries", "cdecl")
    Vect_get_area_boundaries.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_ilist)]
    Vect_get_area_boundaries.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 297
if _libs["grass_vector.8.4"].has("Vect_get_isle_points", "cdecl"):
    Vect_get_isle_points = _libs["grass_vector.8.4"].get("Vect_get_isle_points", "cdecl")
    Vect_get_isle_points.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts)]
    Vect_get_isle_points.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 298
if _libs["grass_vector.8.4"].has("Vect_get_isle_area", "cdecl"):
    Vect_get_isle_area = _libs["grass_vector.8.4"].get("Vect_get_isle_area", "cdecl")
    Vect_get_isle_area.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_isle_area.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 299
if _libs["grass_vector.8.4"].has("Vect_get_isle_boundaries", "cdecl"):
    Vect_get_isle_boundaries = _libs["grass_vector.8.4"].get("Vect_get_isle_boundaries", "cdecl")
    Vect_get_isle_boundaries.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_ilist)]
    Vect_get_isle_boundaries.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 301
if _libs["grass_vector.8.4"].has("Vect_get_centroid_area", "cdecl"):
    Vect_get_centroid_area = _libs["grass_vector.8.4"].get("Vect_get_centroid_area", "cdecl")
    Vect_get_centroid_area.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_centroid_area.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 304
if _libs["grass_vector.8.4"].has("Vect_get_num_updated_lines", "cdecl"):
    Vect_get_num_updated_lines = _libs["grass_vector.8.4"].get("Vect_get_num_updated_lines", "cdecl")
    Vect_get_num_updated_lines.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_updated_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 305
if _libs["grass_vector.8.4"].has("Vect_get_updated_line", "cdecl"):
    Vect_get_updated_line = _libs["grass_vector.8.4"].get("Vect_get_updated_line", "cdecl")
    Vect_get_updated_line.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_updated_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 306
if _libs["grass_vector.8.4"].has("Vect_get_updated_line_offset", "cdecl"):
    Vect_get_updated_line_offset = _libs["grass_vector.8.4"].get("Vect_get_updated_line_offset", "cdecl")
    Vect_get_updated_line_offset.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_updated_line_offset.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 307
if _libs["grass_vector.8.4"].has("Vect_get_num_updated_nodes", "cdecl"):
    Vect_get_num_updated_nodes = _libs["grass_vector.8.4"].get("Vect_get_num_updated_nodes", "cdecl")
    Vect_get_num_updated_nodes.argtypes = [POINTER(struct_Map_info)]
    Vect_get_num_updated_nodes.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 308
if _libs["grass_vector.8.4"].has("Vect_get_updated_node", "cdecl"):
    Vect_get_updated_node = _libs["grass_vector.8.4"].get("Vect_get_updated_node", "cdecl")
    Vect_get_updated_node.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_updated_node.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 309
if _libs["grass_vector.8.4"].has("Vect_set_updated", "cdecl"):
    Vect_set_updated = _libs["grass_vector.8.4"].get("Vect_set_updated", "cdecl")
    Vect_set_updated.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_set_updated.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 310
if _libs["grass_vector.8.4"].has("Vect_reset_updated", "cdecl"):
    Vect_reset_updated = _libs["grass_vector.8.4"].get("Vect_reset_updated", "cdecl")
    Vect_reset_updated.argtypes = [POINTER(struct_Map_info)]
    Vect_reset_updated.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 313
if _libs["grass_vector.8.4"].has("Vect_hist_command", "cdecl"):
    Vect_hist_command = _libs["grass_vector.8.4"].get("Vect_hist_command", "cdecl")
    Vect_hist_command.argtypes = [POINTER(struct_Map_info)]
    Vect_hist_command.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 314
if _libs["grass_vector.8.4"].has("Vect_hist_write", "cdecl"):
    Vect_hist_write = _libs["grass_vector.8.4"].get("Vect_hist_write", "cdecl")
    Vect_hist_write.argtypes = [POINTER(struct_Map_info), String]
    Vect_hist_write.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 315
if _libs["grass_vector.8.4"].has("Vect_hist_copy", "cdecl"):
    Vect_hist_copy = _libs["grass_vector.8.4"].get("Vect_hist_copy", "cdecl")
    Vect_hist_copy.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info)]
    Vect_hist_copy.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 316
if _libs["grass_vector.8.4"].has("Vect_hist_rewind", "cdecl"):
    Vect_hist_rewind = _libs["grass_vector.8.4"].get("Vect_hist_rewind", "cdecl")
    Vect_hist_rewind.argtypes = [POINTER(struct_Map_info)]
    Vect_hist_rewind.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 317
if _libs["grass_vector.8.4"].has("Vect_hist_read", "cdecl"):
    Vect_hist_read = _libs["grass_vector.8.4"].get("Vect_hist_read", "cdecl")
    Vect_hist_read.argtypes = [String, c_int, POINTER(struct_Map_info)]
    if sizeof(c_int) == sizeof(c_void_p):
        Vect_hist_read.restype = ReturnString
    else:
        Vect_hist_read.restype = String
        Vect_hist_read.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 320
if _libs["grass_vector.8.4"].has("Vect_select_lines_by_box", "cdecl"):
    Vect_select_lines_by_box = _libs["grass_vector.8.4"].get("Vect_select_lines_by_box", "cdecl")
    Vect_select_lines_by_box.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box), c_int, POINTER(struct_boxlist)]
    Vect_select_lines_by_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 322
if _libs["grass_vector.8.4"].has("Vect_select_areas_by_box", "cdecl"):
    Vect_select_areas_by_box = _libs["grass_vector.8.4"].get("Vect_select_areas_by_box", "cdecl")
    Vect_select_areas_by_box.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box), POINTER(struct_boxlist)]
    Vect_select_areas_by_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 324
if _libs["grass_vector.8.4"].has("Vect_select_isles_by_box", "cdecl"):
    Vect_select_isles_by_box = _libs["grass_vector.8.4"].get("Vect_select_isles_by_box", "cdecl")
    Vect_select_isles_by_box.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box), POINTER(struct_boxlist)]
    Vect_select_isles_by_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 326
if _libs["grass_vector.8.4"].has("Vect_select_nodes_by_box", "cdecl"):
    Vect_select_nodes_by_box = _libs["grass_vector.8.4"].get("Vect_select_nodes_by_box", "cdecl")
    Vect_select_nodes_by_box.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box), POINTER(struct_ilist)]
    Vect_select_nodes_by_box.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 328
if _libs["grass_vector.8.4"].has("Vect_find_node", "cdecl"):
    Vect_find_node = _libs["grass_vector.8.4"].get("Vect_find_node", "cdecl")
    Vect_find_node.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_double, c_int]
    Vect_find_node.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 329
if _libs["grass_vector.8.4"].has("Vect_find_line", "cdecl"):
    Vect_find_line = _libs["grass_vector.8.4"].get("Vect_find_line", "cdecl")
    Vect_find_line.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_int, c_double, c_int, c_int]
    Vect_find_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 331
if _libs["grass_vector.8.4"].has("Vect_find_line_list", "cdecl"):
    Vect_find_line_list = _libs["grass_vector.8.4"].get("Vect_find_line_list", "cdecl")
    Vect_find_line_list.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_int, c_double, c_int, POINTER(struct_ilist), POINTER(struct_ilist)]
    Vect_find_line_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 333
if _libs["grass_vector.8.4"].has("Vect_find_area", "cdecl"):
    Vect_find_area = _libs["grass_vector.8.4"].get("Vect_find_area", "cdecl")
    Vect_find_area.argtypes = [POINTER(struct_Map_info), c_double, c_double]
    Vect_find_area.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 334
if _libs["grass_vector.8.4"].has("Vect_find_island", "cdecl"):
    Vect_find_island = _libs["grass_vector.8.4"].get("Vect_find_island", "cdecl")
    Vect_find_island.argtypes = [POINTER(struct_Map_info), c_double, c_double]
    Vect_find_island.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 335
if _libs["grass_vector.8.4"].has("Vect_select_lines_by_polygon", "cdecl"):
    Vect_select_lines_by_polygon = _libs["grass_vector.8.4"].get("Vect_select_lines_by_polygon", "cdecl")
    Vect_select_lines_by_polygon.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), c_int, POINTER(POINTER(struct_line_pnts)), c_int, POINTER(struct_ilist)]
    Vect_select_lines_by_polygon.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 337
if _libs["grass_vector.8.4"].has("Vect_select_areas_by_polygon", "cdecl"):
    Vect_select_areas_by_polygon = _libs["grass_vector.8.4"].get("Vect_select_areas_by_polygon", "cdecl")
    Vect_select_areas_by_polygon.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), c_int, POINTER(POINTER(struct_line_pnts)), POINTER(struct_ilist)]
    Vect_select_areas_by_polygon.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 341
if _libs["grass_vector.8.4"].has("Vect_tin_get_z", "cdecl"):
    Vect_tin_get_z = _libs["grass_vector.8.4"].get("Vect_tin_get_z", "cdecl")
    Vect_tin_get_z.argtypes = [POINTER(struct_Map_info), c_double, c_double, POINTER(c_double), POINTER(c_double), POINTER(c_double)]
    Vect_tin_get_z.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 345
if _libs["grass_vector.8.4"].has("Vect_find_poly_centroid", "cdecl"):
    Vect_find_poly_centroid = _libs["grass_vector.8.4"].get("Vect_find_poly_centroid", "cdecl")
    Vect_find_poly_centroid.argtypes = [POINTER(struct_line_pnts), POINTER(c_double), POINTER(c_double)]
    Vect_find_poly_centroid.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 346
for _lib in _libs.values():
    if not _lib.has("Vect__intersect_line_with_poly", "cdecl"):
        continue
    Vect__intersect_line_with_poly = _lib.get("Vect__intersect_line_with_poly", "cdecl")
    Vect__intersect_line_with_poly.argtypes = [POINTER(struct_line_pnts), c_double, POINTER(struct_line_pnts)]
    Vect__intersect_line_with_poly.restype = c_int
    break

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 348
if _libs["grass_vector.8.4"].has("Vect_get_point_in_area", "cdecl"):
    Vect_get_point_in_area = _libs["grass_vector.8.4"].get("Vect_get_point_in_area", "cdecl")
    Vect_get_point_in_area.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_double), POINTER(c_double)]
    Vect_get_point_in_area.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 349
if _libs["grass_vector.8.4"].has("Vect_get_point_in_poly", "cdecl"):
    Vect_get_point_in_poly = _libs["grass_vector.8.4"].get("Vect_get_point_in_poly", "cdecl")
    Vect_get_point_in_poly.argtypes = [POINTER(struct_line_pnts), POINTER(c_double), POINTER(c_double)]
    Vect_get_point_in_poly.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 350
if _libs["grass_vector.8.4"].has("Vect_get_point_in_poly_isl", "cdecl"):
    Vect_get_point_in_poly_isl = _libs["grass_vector.8.4"].get("Vect_get_point_in_poly_isl", "cdecl")
    Vect_get_point_in_poly_isl.argtypes = [POINTER(struct_line_pnts), POINTER(POINTER(struct_line_pnts)), c_int, POINTER(c_double), POINTER(c_double)]
    Vect_get_point_in_poly_isl.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 353
if _libs["grass_vector.8.4"].has("Vect_point_in_area", "cdecl"):
    Vect_point_in_area = _libs["grass_vector.8.4"].get("Vect_point_in_area", "cdecl")
    Vect_point_in_area.argtypes = [c_double, c_double, POINTER(struct_Map_info), c_int, POINTER(struct_bound_box)]
    Vect_point_in_area.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 355
if _libs["grass_vector.8.4"].has("Vect_point_in_area_outer_ring", "cdecl"):
    Vect_point_in_area_outer_ring = _libs["grass_vector.8.4"].get("Vect_point_in_area_outer_ring", "cdecl")
    Vect_point_in_area_outer_ring.argtypes = [c_double, c_double, POINTER(struct_Map_info), c_int, POINTER(struct_bound_box)]
    Vect_point_in_area_outer_ring.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 357
if _libs["grass_vector.8.4"].has("Vect_point_in_island", "cdecl"):
    Vect_point_in_island = _libs["grass_vector.8.4"].get("Vect_point_in_island", "cdecl")
    Vect_point_in_island.argtypes = [c_double, c_double, POINTER(struct_Map_info), c_int, POINTER(struct_bound_box)]
    Vect_point_in_island.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 359
if _libs["grass_vector.8.4"].has("Vect_point_in_poly", "cdecl"):
    Vect_point_in_poly = _libs["grass_vector.8.4"].get("Vect_point_in_poly", "cdecl")
    Vect_point_in_poly.argtypes = [c_double, c_double, POINTER(struct_line_pnts)]
    Vect_point_in_poly.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 362
if _libs["grass_vector.8.4"].has("Vect_break_lines", "cdecl"):
    Vect_break_lines = _libs["grass_vector.8.4"].get("Vect_break_lines", "cdecl")
    Vect_break_lines.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_Map_info)]
    Vect_break_lines.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 363
if _libs["grass_vector.8.4"].has("Vect_break_lines_list", "cdecl"):
    Vect_break_lines_list = _libs["grass_vector.8.4"].get("Vect_break_lines_list", "cdecl")
    Vect_break_lines_list.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), POINTER(struct_ilist), c_int, POINTER(struct_Map_info)]
    Vect_break_lines_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 365
if _libs["grass_vector.8.4"].has("Vect_check_line_breaks", "cdecl"):
    Vect_check_line_breaks = _libs["grass_vector.8.4"].get("Vect_check_line_breaks", "cdecl")
    Vect_check_line_breaks.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_Map_info)]
    Vect_check_line_breaks.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 366
if _libs["grass_vector.8.4"].has("Vect_check_line_breaks_list", "cdecl"):
    Vect_check_line_breaks_list = _libs["grass_vector.8.4"].get("Vect_check_line_breaks_list", "cdecl")
    Vect_check_line_breaks_list.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), POINTER(struct_ilist), c_int, POINTER(struct_Map_info)]
    Vect_check_line_breaks_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 368
if _libs["grass_vector.8.4"].has("Vect_merge_lines", "cdecl"):
    Vect_merge_lines = _libs["grass_vector.8.4"].get("Vect_merge_lines", "cdecl")
    Vect_merge_lines.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_int), POINTER(struct_Map_info)]
    Vect_merge_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 369
if _libs["grass_vector.8.4"].has("Vect_break_polygons", "cdecl"):
    Vect_break_polygons = _libs["grass_vector.8.4"].get("Vect_break_polygons", "cdecl")
    Vect_break_polygons.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_Map_info)]
    Vect_break_polygons.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 370
if _libs["grass_vector.8.4"].has("Vect_remove_duplicates", "cdecl"):
    Vect_remove_duplicates = _libs["grass_vector.8.4"].get("Vect_remove_duplicates", "cdecl")
    Vect_remove_duplicates.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_Map_info)]
    Vect_remove_duplicates.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 371
if _libs["grass_vector.8.4"].has("Vect_line_check_duplicate", "cdecl"):
    Vect_line_check_duplicate = _libs["grass_vector.8.4"].get("Vect_line_check_duplicate", "cdecl")
    Vect_line_check_duplicate.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), c_int]
    Vect_line_check_duplicate.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 373
if _libs["grass_vector.8.4"].has("Vect_snap_lines", "cdecl"):
    Vect_snap_lines = _libs["grass_vector.8.4"].get("Vect_snap_lines", "cdecl")
    Vect_snap_lines.argtypes = [POINTER(struct_Map_info), c_int, c_double, POINTER(struct_Map_info)]
    Vect_snap_lines.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 374
if _libs["grass_vector.8.4"].has("Vect_snap_lines_list", "cdecl"):
    Vect_snap_lines_list = _libs["grass_vector.8.4"].get("Vect_snap_lines_list", "cdecl")
    Vect_snap_lines_list.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), c_double, POINTER(struct_Map_info)]
    Vect_snap_lines_list.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 376
if _libs["grass_vector.8.4"].has("Vect_snap_line", "cdecl"):
    Vect_snap_line = _libs["grass_vector.8.4"].get("Vect_snap_line", "cdecl")
    Vect_snap_line.argtypes = [POINTER(struct_Map_info), POINTER(struct_ilist), POINTER(struct_line_pnts), c_double, c_int, POINTER(c_int), POINTER(c_int)]
    Vect_snap_line.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 378
if _libs["grass_vector.8.4"].has("Vect_remove_dangles", "cdecl"):
    Vect_remove_dangles = _libs["grass_vector.8.4"].get("Vect_remove_dangles", "cdecl")
    Vect_remove_dangles.argtypes = [POINTER(struct_Map_info), c_int, c_double, POINTER(struct_Map_info)]
    Vect_remove_dangles.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 379
if _libs["grass_vector.8.4"].has("Vect_chtype_dangles", "cdecl"):
    Vect_chtype_dangles = _libs["grass_vector.8.4"].get("Vect_chtype_dangles", "cdecl")
    Vect_chtype_dangles.argtypes = [POINTER(struct_Map_info), c_double, POINTER(struct_Map_info)]
    Vect_chtype_dangles.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 380
if _libs["grass_vector.8.4"].has("Vect_select_dangles", "cdecl"):
    Vect_select_dangles = _libs["grass_vector.8.4"].get("Vect_select_dangles", "cdecl")
    Vect_select_dangles.argtypes = [POINTER(struct_Map_info), c_int, c_double, POINTER(struct_ilist)]
    Vect_select_dangles.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 381
if _libs["grass_vector.8.4"].has("Vect_remove_bridges", "cdecl"):
    Vect_remove_bridges = _libs["grass_vector.8.4"].get("Vect_remove_bridges", "cdecl")
    Vect_remove_bridges.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info), POINTER(c_int), POINTER(c_int)]
    Vect_remove_bridges.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 382
if _libs["grass_vector.8.4"].has("Vect_chtype_bridges", "cdecl"):
    Vect_chtype_bridges = _libs["grass_vector.8.4"].get("Vect_chtype_bridges", "cdecl")
    Vect_chtype_bridges.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info), POINTER(c_int), POINTER(c_int)]
    Vect_chtype_bridges.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 383
if _libs["grass_vector.8.4"].has("Vect_remove_small_areas", "cdecl"):
    Vect_remove_small_areas = _libs["grass_vector.8.4"].get("Vect_remove_small_areas", "cdecl")
    Vect_remove_small_areas.argtypes = [POINTER(struct_Map_info), c_double, POINTER(struct_Map_info), POINTER(c_double)]
    Vect_remove_small_areas.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 385
if _libs["grass_vector.8.4"].has("Vect_clean_small_angles_at_nodes", "cdecl"):
    Vect_clean_small_angles_at_nodes = _libs["grass_vector.8.4"].get("Vect_clean_small_angles_at_nodes", "cdecl")
    Vect_clean_small_angles_at_nodes.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_Map_info)]
    Vect_clean_small_angles_at_nodes.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 388
if _libs["grass_vector.8.4"].has("Vect_overlay_str_to_operator", "cdecl"):
    Vect_overlay_str_to_operator = _libs["grass_vector.8.4"].get("Vect_overlay_str_to_operator", "cdecl")
    Vect_overlay_str_to_operator.argtypes = [String]
    Vect_overlay_str_to_operator.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 389
if _libs["grass_vector.8.4"].has("Vect_overlay", "cdecl"):
    Vect_overlay = _libs["grass_vector.8.4"].get("Vect_overlay", "cdecl")
    Vect_overlay.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_ilist), POINTER(struct_ilist), POINTER(struct_Map_info), c_int, POINTER(struct_ilist), POINTER(struct_ilist), c_int, POINTER(struct_Map_info)]
    Vect_overlay.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 392
if _libs["grass_vector.8.4"].has("Vect_overlay_and", "cdecl"):
    Vect_overlay_and = _libs["grass_vector.8.4"].get("Vect_overlay_and", "cdecl")
    Vect_overlay_and.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_ilist), POINTER(struct_ilist), POINTER(struct_Map_info), c_int, POINTER(struct_ilist), POINTER(struct_ilist), POINTER(struct_Map_info)]
    Vect_overlay_and.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 397
if _libs["grass_vector.8.4"].has("Vect_graph_init", "cdecl"):
    Vect_graph_init = _libs["grass_vector.8.4"].get("Vect_graph_init", "cdecl")
    Vect_graph_init.argtypes = [POINTER(dglGraph_s), c_int]
    Vect_graph_init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 398
if _libs["grass_vector.8.4"].has("Vect_graph_build", "cdecl"):
    Vect_graph_build = _libs["grass_vector.8.4"].get("Vect_graph_build", "cdecl")
    Vect_graph_build.argtypes = [POINTER(dglGraph_s)]
    Vect_graph_build.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 399
if _libs["grass_vector.8.4"].has("Vect_graph_add_edge", "cdecl"):
    Vect_graph_add_edge = _libs["grass_vector.8.4"].get("Vect_graph_add_edge", "cdecl")
    Vect_graph_add_edge.argtypes = [POINTER(dglGraph_s), c_int, c_int, c_double, c_int]
    Vect_graph_add_edge.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 400
if _libs["grass_vector.8.4"].has("Vect_graph_set_node_costs", "cdecl"):
    Vect_graph_set_node_costs = _libs["grass_vector.8.4"].get("Vect_graph_set_node_costs", "cdecl")
    Vect_graph_set_node_costs.argtypes = [POINTER(dglGraph_s), c_int, c_double]
    Vect_graph_set_node_costs.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 401
if _libs["grass_vector.8.4"].has("Vect_graph_shortest_path", "cdecl"):
    Vect_graph_shortest_path = _libs["grass_vector.8.4"].get("Vect_graph_shortest_path", "cdecl")
    Vect_graph_shortest_path.argtypes = [POINTER(dglGraph_s), c_int, c_int, POINTER(struct_ilist), POINTER(c_double)]
    Vect_graph_shortest_path.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 404
if _libs["grass_vector.8.4"].has("Vect_net_build_graph", "cdecl"):
    Vect_net_build_graph = _libs["grass_vector.8.4"].get("Vect_net_build_graph", "cdecl")
    Vect_net_build_graph.argtypes = [POINTER(struct_Map_info), c_int, c_int, c_int, String, String, String, c_int, c_int]
    Vect_net_build_graph.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 406
if _libs["grass_vector.8.4"].has("Vect_net_ttb_build_graph", "cdecl"):
    Vect_net_ttb_build_graph = _libs["grass_vector.8.4"].get("Vect_net_ttb_build_graph", "cdecl")
    Vect_net_ttb_build_graph.argtypes = [POINTER(struct_Map_info), c_int, c_int, c_int, c_int, c_int, String, String, String, c_int, c_int]
    Vect_net_ttb_build_graph.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 409
if _libs["grass_vector.8.4"].has("Vect_net_shortest_path", "cdecl"):
    Vect_net_shortest_path = _libs["grass_vector.8.4"].get("Vect_net_shortest_path", "cdecl")
    Vect_net_shortest_path.argtypes = [POINTER(struct_Map_info), c_int, c_int, POINTER(struct_ilist), POINTER(c_double)]
    Vect_net_shortest_path.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 411
if _libs["grass_vector.8.4"].has("Vect_net_ttb_shortest_path", "cdecl"):
    Vect_net_ttb_shortest_path = _libs["grass_vector.8.4"].get("Vect_net_ttb_shortest_path", "cdecl")
    Vect_net_ttb_shortest_path.argtypes = [POINTER(struct_Map_info), c_int, c_int, c_int, c_int, c_int, POINTER(struct_ilist), POINTER(c_double)]
    Vect_net_ttb_shortest_path.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 413
if _libs["grass_vector.8.4"].has("Vect_net_get_graph", "cdecl"):
    Vect_net_get_graph = _libs["grass_vector.8.4"].get("Vect_net_get_graph", "cdecl")
    Vect_net_get_graph.argtypes = [POINTER(struct_Map_info)]
    Vect_net_get_graph.restype = POINTER(dglGraph_s)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 414
if _libs["grass_vector.8.4"].has("Vect_net_get_line_cost", "cdecl"):
    Vect_net_get_line_cost = _libs["grass_vector.8.4"].get("Vect_net_get_line_cost", "cdecl")
    Vect_net_get_line_cost.argtypes = [POINTER(struct_Map_info), c_int, c_int, POINTER(c_double)]
    Vect_net_get_line_cost.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 415
if _libs["grass_vector.8.4"].has("Vect_net_get_node_cost", "cdecl"):
    Vect_net_get_node_cost = _libs["grass_vector.8.4"].get("Vect_net_get_node_cost", "cdecl")
    Vect_net_get_node_cost.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_double)]
    Vect_net_get_node_cost.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 416
if _libs["grass_vector.8.4"].has("Vect_net_nearest_nodes", "cdecl"):
    Vect_net_nearest_nodes = _libs["grass_vector.8.4"].get("Vect_net_nearest_nodes", "cdecl")
    Vect_net_nearest_nodes.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_int, c_double, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_double), POINTER(c_double), POINTER(struct_line_pnts), POINTER(struct_line_pnts), POINTER(c_double)]
    Vect_net_nearest_nodes.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 419
if _libs["grass_vector.8.4"].has("Vect_net_shortest_path_coor", "cdecl"):
    Vect_net_shortest_path_coor = _libs["grass_vector.8.4"].get("Vect_net_shortest_path_coor", "cdecl")
    Vect_net_shortest_path_coor.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, POINTER(c_double), POINTER(struct_line_pnts), POINTER(struct_ilist), POINTER(struct_ilist), POINTER(struct_line_pnts), POINTER(struct_line_pnts), POINTER(c_double), POINTER(c_double)]
    Vect_net_shortest_path_coor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 424
if _libs["grass_vector.8.4"].has("Vect_net_ttb_shortest_path_coor", "cdecl"):
    Vect_net_ttb_shortest_path_coor = _libs["grass_vector.8.4"].get("Vect_net_ttb_shortest_path_coor", "cdecl")
    Vect_net_ttb_shortest_path_coor.argtypes = [POINTER(struct_Map_info), c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_int, POINTER(c_double), POINTER(struct_line_pnts), POINTER(struct_ilist), POINTER(struct_ilist), POINTER(struct_line_pnts), POINTER(struct_line_pnts), POINTER(c_double), POINTER(c_double)]
    Vect_net_ttb_shortest_path_coor.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 432
if _libs["grass_vector.8.4"].has("Vect_topo_dump", "cdecl"):
    Vect_topo_dump = _libs["grass_vector.8.4"].get("Vect_topo_dump", "cdecl")
    Vect_topo_dump.argtypes = [POINTER(struct_Map_info), POINTER(FILE)]
    Vect_topo_dump.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 433
if _libs["grass_vector.8.4"].has("Vect_points_distance", "cdecl"):
    Vect_points_distance = _libs["grass_vector.8.4"].get("Vect_points_distance", "cdecl")
    Vect_points_distance.argtypes = [c_double, c_double, c_double, c_double, c_double, c_double, c_int]
    Vect_points_distance.restype = c_double

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 435
if _libs["grass_vector.8.4"].has("Vect_option_to_types", "cdecl"):
    Vect_option_to_types = _libs["grass_vector.8.4"].get("Vect_option_to_types", "cdecl")
    Vect_option_to_types.argtypes = [POINTER(struct_Option)]
    Vect_option_to_types.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 436
if _libs["grass_vector.8.4"].has("Vect_copy_map_lines", "cdecl"):
    Vect_copy_map_lines = _libs["grass_vector.8.4"].get("Vect_copy_map_lines", "cdecl")
    Vect_copy_map_lines.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info)]
    Vect_copy_map_lines.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 437
if _libs["grass_vector.8.4"].has("Vect_copy_map_lines_field", "cdecl"):
    Vect_copy_map_lines_field = _libs["grass_vector.8.4"].get("Vect_copy_map_lines_field", "cdecl")
    Vect_copy_map_lines_field.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_Map_info)]
    Vect_copy_map_lines_field.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 438
if _libs["grass_vector.8.4"].has("Vect_copy", "cdecl"):
    Vect_copy = _libs["grass_vector.8.4"].get("Vect_copy", "cdecl")
    Vect_copy.argtypes = [String, String, String]
    Vect_copy.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 439
if _libs["grass_vector.8.4"].has("Vect_rename", "cdecl"):
    Vect_rename = _libs["grass_vector.8.4"].get("Vect_rename", "cdecl")
    Vect_rename.argtypes = [String, String]
    Vect_rename.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 440
if _libs["grass_vector.8.4"].has("Vect_copy_table", "cdecl"):
    Vect_copy_table = _libs["grass_vector.8.4"].get("Vect_copy_table", "cdecl")
    Vect_copy_table.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info), c_int, c_int, String, c_int]
    Vect_copy_table.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 442
if _libs["grass_vector.8.4"].has("Vect_copy_table_by_cat_list", "cdecl"):
    Vect_copy_table_by_cat_list = _libs["grass_vector.8.4"].get("Vect_copy_table_by_cat_list", "cdecl")
    Vect_copy_table_by_cat_list.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info), c_int, c_int, String, c_int, POINTER(struct_cat_list)]
    Vect_copy_table_by_cat_list.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 444
if _libs["grass_vector.8.4"].has("Vect_copy_table_by_cats", "cdecl"):
    Vect_copy_table_by_cats = _libs["grass_vector.8.4"].get("Vect_copy_table_by_cats", "cdecl")
    Vect_copy_table_by_cats.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info), c_int, c_int, String, c_int, POINTER(c_int), c_int]
    Vect_copy_table_by_cats.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 446
if _libs["grass_vector.8.4"].has("Vect_copy_tables", "cdecl"):
    Vect_copy_tables = _libs["grass_vector.8.4"].get("Vect_copy_tables", "cdecl")
    Vect_copy_tables.argtypes = [POINTER(struct_Map_info), POINTER(struct_Map_info), c_int]
    Vect_copy_tables.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 447
if _libs["grass_vector.8.4"].has("Vect_delete", "cdecl"):
    Vect_delete = _libs["grass_vector.8.4"].get("Vect_delete", "cdecl")
    Vect_delete.argtypes = [String]
    Vect_delete.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 448
if _libs["grass_vector.8.4"].has("Vect_segment_intersection", "cdecl"):
    Vect_segment_intersection = _libs["grass_vector.8.4"].get("Vect_segment_intersection", "cdecl")
    Vect_segment_intersection.argtypes = [c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), POINTER(c_double), c_int]
    Vect_segment_intersection.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 452
if _libs["grass_vector.8.4"].has("Vect_line_intersection", "cdecl"):
    Vect_line_intersection = _libs["grass_vector.8.4"].get("Vect_line_intersection", "cdecl")
    Vect_line_intersection.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), POINTER(struct_bound_box), POINTER(struct_bound_box), POINTER(POINTER(POINTER(struct_line_pnts))), POINTER(POINTER(POINTER(struct_line_pnts))), POINTER(c_int), POINTER(c_int), c_int]
    Vect_line_intersection.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 456
if _libs["grass_vector.8.4"].has("Vect_line_intersection2", "cdecl"):
    Vect_line_intersection2 = _libs["grass_vector.8.4"].get("Vect_line_intersection2", "cdecl")
    Vect_line_intersection2.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), POINTER(struct_bound_box), POINTER(struct_bound_box), POINTER(POINTER(POINTER(struct_line_pnts))), POINTER(POINTER(POINTER(struct_line_pnts))), POINTER(c_int), POINTER(c_int), c_int]
    Vect_line_intersection2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 460
if _libs["grass_vector.8.4"].has("Vect_line_check_intersection", "cdecl"):
    Vect_line_check_intersection = _libs["grass_vector.8.4"].get("Vect_line_check_intersection", "cdecl")
    Vect_line_check_intersection.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), c_int]
    Vect_line_check_intersection.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 461
if _libs["grass_vector.8.4"].has("Vect_line_check_intersection2", "cdecl"):
    Vect_line_check_intersection2 = _libs["grass_vector.8.4"].get("Vect_line_check_intersection2", "cdecl")
    Vect_line_check_intersection2.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), c_int]
    Vect_line_check_intersection2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 462
if _libs["grass_vector.8.4"].has("Vect_line_get_intersections", "cdecl"):
    Vect_line_get_intersections = _libs["grass_vector.8.4"].get("Vect_line_get_intersections", "cdecl")
    Vect_line_get_intersections.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), POINTER(struct_line_pnts), c_int]
    Vect_line_get_intersections.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 464
if _libs["grass_vector.8.4"].has("Vect_line_get_intersections2", "cdecl"):
    Vect_line_get_intersections2 = _libs["grass_vector.8.4"].get("Vect_line_get_intersections2", "cdecl")
    Vect_line_get_intersections2.argtypes = [POINTER(struct_line_pnts), POINTER(struct_line_pnts), POINTER(struct_line_pnts), c_int]
    Vect_line_get_intersections2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 466
if _libs["grass_vector.8.4"].has("Vect_subst_var", "cdecl"):
    Vect_subst_var = _libs["grass_vector.8.4"].get("Vect_subst_var", "cdecl")
    Vect_subst_var.argtypes = [String, POINTER(struct_Map_info)]
    if sizeof(c_int) == sizeof(c_void_p):
        Vect_subst_var.restype = ReturnString
    else:
        Vect_subst_var.restype = String
        Vect_subst_var.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 469
if _libs["grass_vector.8.4"].has("Vect_spatial_index_init", "cdecl"):
    Vect_spatial_index_init = _libs["grass_vector.8.4"].get("Vect_spatial_index_init", "cdecl")
    Vect_spatial_index_init.argtypes = [POINTER(struct_spatial_index), c_int]
    Vect_spatial_index_init.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 470
if _libs["grass_vector.8.4"].has("Vect_spatial_index_destroy", "cdecl"):
    Vect_spatial_index_destroy = _libs["grass_vector.8.4"].get("Vect_spatial_index_destroy", "cdecl")
    Vect_spatial_index_destroy.argtypes = [POINTER(struct_spatial_index)]
    Vect_spatial_index_destroy.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 471
if _libs["grass_vector.8.4"].has("Vect_spatial_index_add_item", "cdecl"):
    Vect_spatial_index_add_item = _libs["grass_vector.8.4"].get("Vect_spatial_index_add_item", "cdecl")
    Vect_spatial_index_add_item.argtypes = [POINTER(struct_spatial_index), c_int, POINTER(struct_bound_box)]
    Vect_spatial_index_add_item.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 473
if _libs["grass_vector.8.4"].has("Vect_spatial_index_del_item", "cdecl"):
    Vect_spatial_index_del_item = _libs["grass_vector.8.4"].get("Vect_spatial_index_del_item", "cdecl")
    Vect_spatial_index_del_item.argtypes = [POINTER(struct_spatial_index), c_int, POINTER(struct_bound_box)]
    Vect_spatial_index_del_item.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 475
if _libs["grass_vector.8.4"].has("Vect_spatial_index_select", "cdecl"):
    Vect_spatial_index_select = _libs["grass_vector.8.4"].get("Vect_spatial_index_select", "cdecl")
    Vect_spatial_index_select.argtypes = [POINTER(struct_spatial_index), POINTER(struct_bound_box), POINTER(struct_ilist)]
    Vect_spatial_index_select.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 479
if _libs["grass_vector.8.4"].has("Vect_read_ascii", "cdecl"):
    Vect_read_ascii = _libs["grass_vector.8.4"].get("Vect_read_ascii", "cdecl")
    Vect_read_ascii.argtypes = [POINTER(FILE), POINTER(struct_Map_info)]
    Vect_read_ascii.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 480
if _libs["grass_vector.8.4"].has("Vect_read_ascii_head", "cdecl"):
    Vect_read_ascii_head = _libs["grass_vector.8.4"].get("Vect_read_ascii_head", "cdecl")
    Vect_read_ascii_head.argtypes = [POINTER(FILE), POINTER(struct_Map_info)]
    Vect_read_ascii_head.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 481
if _libs["grass_vector.8.4"].has("Vect_write_ascii", "cdecl"):
    Vect_write_ascii = _libs["grass_vector.8.4"].get("Vect_write_ascii", "cdecl")
    Vect_write_ascii.argtypes = [POINTER(FILE), POINTER(FILE), POINTER(struct_Map_info), c_int, c_int, c_int, String, c_int, c_int, c_int, POINTER(struct_cat_list), String, POINTER(POINTER(c_char)), c_int]
    Vect_write_ascii.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 484
if _libs["grass_vector.8.4"].has("Vect_write_ascii_head", "cdecl"):
    Vect_write_ascii_head = _libs["grass_vector.8.4"].get("Vect_write_ascii_head", "cdecl")
    Vect_write_ascii_head.argtypes = [POINTER(FILE), POINTER(struct_Map_info)]
    Vect_write_ascii_head.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 487
if _libs["grass_vector.8.4"].has("Vect_sfa_get_line_type", "cdecl"):
    Vect_sfa_get_line_type = _libs["grass_vector.8.4"].get("Vect_sfa_get_line_type", "cdecl")
    Vect_sfa_get_line_type.argtypes = [POINTER(struct_line_pnts), c_int, c_int]
    Vect_sfa_get_line_type.restype = SF_FeatureType

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 488
if _libs["grass_vector.8.4"].has("Vect_sfa_get_type", "cdecl"):
    Vect_sfa_get_type = _libs["grass_vector.8.4"].get("Vect_sfa_get_type", "cdecl")
    Vect_sfa_get_type.argtypes = [SF_FeatureType]
    Vect_sfa_get_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 489
if _libs["grass_vector.8.4"].has("Vect_sfa_check_line_type", "cdecl"):
    Vect_sfa_check_line_type = _libs["grass_vector.8.4"].get("Vect_sfa_check_line_type", "cdecl")
    Vect_sfa_check_line_type.argtypes = [POINTER(struct_line_pnts), c_int, SF_FeatureType, c_int]
    Vect_sfa_check_line_type.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 491
if _libs["grass_vector.8.4"].has("Vect_sfa_line_dimension", "cdecl"):
    Vect_sfa_line_dimension = _libs["grass_vector.8.4"].get("Vect_sfa_line_dimension", "cdecl")
    Vect_sfa_line_dimension.argtypes = [c_int]
    Vect_sfa_line_dimension.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 492
if _libs["grass_vector.8.4"].has("Vect_sfa_line_geometry_type", "cdecl"):
    Vect_sfa_line_geometry_type = _libs["grass_vector.8.4"].get("Vect_sfa_line_geometry_type", "cdecl")
    Vect_sfa_line_geometry_type.argtypes = [POINTER(struct_line_pnts), c_int]
    if sizeof(c_int) == sizeof(c_void_p):
        Vect_sfa_line_geometry_type.restype = ReturnString
    else:
        Vect_sfa_line_geometry_type.restype = String
        Vect_sfa_line_geometry_type.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 493
if _libs["grass_vector.8.4"].has("Vect_sfa_line_astext", "cdecl"):
    Vect_sfa_line_astext = _libs["grass_vector.8.4"].get("Vect_sfa_line_astext", "cdecl")
    Vect_sfa_line_astext.argtypes = [POINTER(struct_line_pnts), c_int, c_int, c_int, POINTER(FILE)]
    Vect_sfa_line_astext.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 494
if _libs["grass_vector.8.4"].has("Vect_sfa_is_line_simple", "cdecl"):
    Vect_sfa_is_line_simple = _libs["grass_vector.8.4"].get("Vect_sfa_is_line_simple", "cdecl")
    Vect_sfa_is_line_simple.argtypes = [POINTER(struct_line_pnts), c_int, c_int]
    Vect_sfa_is_line_simple.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 495
if _libs["grass_vector.8.4"].has("Vect_sfa_is_line_closed", "cdecl"):
    Vect_sfa_is_line_closed = _libs["grass_vector.8.4"].get("Vect_sfa_is_line_closed", "cdecl")
    Vect_sfa_is_line_closed.argtypes = [POINTER(struct_line_pnts), c_int, c_int]
    Vect_sfa_is_line_closed.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 496
if _libs["grass_vector.8.4"].has("Vect_sfa_get_num_features", "cdecl"):
    Vect_sfa_get_num_features = _libs["grass_vector.8.4"].get("Vect_sfa_get_num_features", "cdecl")
    Vect_sfa_get_num_features.argtypes = [POINTER(struct_Map_info)]
    Vect_sfa_get_num_features.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 501
if _libs["grass_vector.8.4"].has("Vect_print_header", "cdecl"):
    Vect_print_header = _libs["grass_vector.8.4"].get("Vect_print_header", "cdecl")
    Vect_print_header.argtypes = [POINTER(struct_Map_info)]
    Vect_print_header.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 502
if _libs["grass_vector.8.4"].has("Vect__init_head", "cdecl"):
    Vect__init_head = _libs["grass_vector.8.4"].get("Vect__init_head", "cdecl")
    Vect__init_head.argtypes = [POINTER(struct_Map_info)]
    Vect__init_head.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 505
if _libs["grass_vector.8.4"].has("Vect_coor_info", "cdecl"):
    Vect_coor_info = _libs["grass_vector.8.4"].get("Vect_coor_info", "cdecl")
    Vect_coor_info.argtypes = [POINTER(struct_Map_info), POINTER(struct_Coor_info)]
    Vect_coor_info.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 506
if _libs["grass_vector.8.4"].has("Vect_maptype_info", "cdecl"):
    Vect_maptype_info = _libs["grass_vector.8.4"].get("Vect_maptype_info", "cdecl")
    Vect_maptype_info.argtypes = [POINTER(struct_Map_info)]
    Vect_maptype_info.restype = c_char_p

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 507
if _libs["grass_vector.8.4"].has("Vect_maptype", "cdecl"):
    Vect_maptype = _libs["grass_vector.8.4"].get("Vect_maptype", "cdecl")
    Vect_maptype.argtypes = [POINTER(struct_Map_info)]
    Vect_maptype.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 508
if _libs["grass_vector.8.4"].has("Vect_open_topo", "cdecl"):
    Vect_open_topo = _libs["grass_vector.8.4"].get("Vect_open_topo", "cdecl")
    Vect_open_topo.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_open_topo.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 509
if _libs["grass_vector.8.4"].has("Vect_save_topo", "cdecl"):
    Vect_save_topo = _libs["grass_vector.8.4"].get("Vect_save_topo", "cdecl")
    Vect_save_topo.argtypes = [POINTER(struct_Map_info)]
    Vect_save_topo.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 510
if _libs["grass_vector.8.4"].has("Vect_open_sidx", "cdecl"):
    Vect_open_sidx = _libs["grass_vector.8.4"].get("Vect_open_sidx", "cdecl")
    Vect_open_sidx.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_open_sidx.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 511
if _libs["grass_vector.8.4"].has("Vect_save_sidx", "cdecl"):
    Vect_save_sidx = _libs["grass_vector.8.4"].get("Vect_save_sidx", "cdecl")
    Vect_save_sidx.argtypes = [POINTER(struct_Map_info)]
    Vect_save_sidx.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 512
if _libs["grass_vector.8.4"].has("Vect_sidx_dump", "cdecl"):
    Vect_sidx_dump = _libs["grass_vector.8.4"].get("Vect_sidx_dump", "cdecl")
    Vect_sidx_dump.argtypes = [POINTER(struct_Map_info), POINTER(FILE)]
    Vect_sidx_dump.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 513
if _libs["grass_vector.8.4"].has("Vect_build_sidx_from_topo", "cdecl"):
    Vect_build_sidx_from_topo = _libs["grass_vector.8.4"].get("Vect_build_sidx_from_topo", "cdecl")
    Vect_build_sidx_from_topo.argtypes = [POINTER(struct_Map_info)]
    Vect_build_sidx_from_topo.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 514
if _libs["grass_vector.8.4"].has("Vect_build_sidx", "cdecl"):
    Vect_build_sidx = _libs["grass_vector.8.4"].get("Vect_build_sidx", "cdecl")
    Vect_build_sidx.argtypes = [POINTER(struct_Map_info)]
    Vect_build_sidx.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 515
if _libs["grass_vector.8.4"].has("Vect_open_fidx", "cdecl"):
    Vect_open_fidx = _libs["grass_vector.8.4"].get("Vect_open_fidx", "cdecl")
    Vect_open_fidx.argtypes = [POINTER(struct_Map_info), POINTER(struct_Format_info_offset)]
    Vect_open_fidx.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 516
if _libs["grass_vector.8.4"].has("Vect_save_fidx", "cdecl"):
    Vect_save_fidx = _libs["grass_vector.8.4"].get("Vect_save_fidx", "cdecl")
    Vect_save_fidx.argtypes = [POINTER(struct_Map_info), POINTER(struct_Format_info_offset)]
    Vect_save_fidx.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 517
if _libs["grass_vector.8.4"].has("Vect_fidx_dump", "cdecl"):
    Vect_fidx_dump = _libs["grass_vector.8.4"].get("Vect_fidx_dump", "cdecl")
    Vect_fidx_dump.argtypes = [POINTER(struct_Map_info), POINTER(FILE)]
    Vect_fidx_dump.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 518
if _libs["grass_vector.8.4"].has("Vect_save_frmt", "cdecl"):
    Vect_save_frmt = _libs["grass_vector.8.4"].get("Vect_save_frmt", "cdecl")
    Vect_save_frmt.argtypes = [POINTER(struct_Map_info)]
    Vect_save_frmt.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 520
if _libs["grass_vector.8.4"].has("Vect__write_head", "cdecl"):
    Vect__write_head = _libs["grass_vector.8.4"].get("Vect__write_head", "cdecl")
    Vect__write_head.argtypes = [POINTER(struct_Map_info)]
    Vect__write_head.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 521
if _libs["grass_vector.8.4"].has("Vect__read_head", "cdecl"):
    Vect__read_head = _libs["grass_vector.8.4"].get("Vect__read_head", "cdecl")
    Vect__read_head.argtypes = [POINTER(struct_Map_info)]
    Vect__read_head.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 522
if _libs["grass_vector.8.4"].has("V1_open_old_nat", "cdecl"):
    V1_open_old_nat = _libs["grass_vector.8.4"].get("V1_open_old_nat", "cdecl")
    V1_open_old_nat.argtypes = [POINTER(struct_Map_info), c_int]
    V1_open_old_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 523
if _libs["grass_vector.8.4"].has("V1_open_old_ogr", "cdecl"):
    V1_open_old_ogr = _libs["grass_vector.8.4"].get("V1_open_old_ogr", "cdecl")
    V1_open_old_ogr.argtypes = [POINTER(struct_Map_info), c_int]
    V1_open_old_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 524
if _libs["grass_vector.8.4"].has("V1_open_old_pg", "cdecl"):
    V1_open_old_pg = _libs["grass_vector.8.4"].get("V1_open_old_pg", "cdecl")
    V1_open_old_pg.argtypes = [POINTER(struct_Map_info), c_int]
    V1_open_old_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 525
if _libs["grass_vector.8.4"].has("V2_open_old_ogr", "cdecl"):
    V2_open_old_ogr = _libs["grass_vector.8.4"].get("V2_open_old_ogr", "cdecl")
    V2_open_old_ogr.argtypes = [POINTER(struct_Map_info)]
    V2_open_old_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 526
if _libs["grass_vector.8.4"].has("V2_open_old_pg", "cdecl"):
    V2_open_old_pg = _libs["grass_vector.8.4"].get("V2_open_old_pg", "cdecl")
    V2_open_old_pg.argtypes = [POINTER(struct_Map_info)]
    V2_open_old_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 527
if _libs["grass_vector.8.4"].has("V1_open_new_nat", "cdecl"):
    V1_open_new_nat = _libs["grass_vector.8.4"].get("V1_open_new_nat", "cdecl")
    V1_open_new_nat.argtypes = [POINTER(struct_Map_info), String, c_int]
    V1_open_new_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 528
if _libs["grass_vector.8.4"].has("V1_open_new_ogr", "cdecl"):
    V1_open_new_ogr = _libs["grass_vector.8.4"].get("V1_open_new_ogr", "cdecl")
    V1_open_new_ogr.argtypes = [POINTER(struct_Map_info), String, c_int]
    V1_open_new_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 529
if _libs["grass_vector.8.4"].has("V1_open_new_pg", "cdecl"):
    V1_open_new_pg = _libs["grass_vector.8.4"].get("V1_open_new_pg", "cdecl")
    V1_open_new_pg.argtypes = [POINTER(struct_Map_info), String, c_int]
    V1_open_new_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 530
if _libs["grass_vector.8.4"].has("V1_rewind_nat", "cdecl"):
    V1_rewind_nat = _libs["grass_vector.8.4"].get("V1_rewind_nat", "cdecl")
    V1_rewind_nat.argtypes = [POINTER(struct_Map_info)]
    V1_rewind_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 531
if _libs["grass_vector.8.4"].has("V1_rewind_ogr", "cdecl"):
    V1_rewind_ogr = _libs["grass_vector.8.4"].get("V1_rewind_ogr", "cdecl")
    V1_rewind_ogr.argtypes = [POINTER(struct_Map_info)]
    V1_rewind_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 532
if _libs["grass_vector.8.4"].has("V1_rewind_pg", "cdecl"):
    V1_rewind_pg = _libs["grass_vector.8.4"].get("V1_rewind_pg", "cdecl")
    V1_rewind_pg.argtypes = [POINTER(struct_Map_info)]
    V1_rewind_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 533
if _libs["grass_vector.8.4"].has("V2_rewind_nat", "cdecl"):
    V2_rewind_nat = _libs["grass_vector.8.4"].get("V2_rewind_nat", "cdecl")
    V2_rewind_nat.argtypes = [POINTER(struct_Map_info)]
    V2_rewind_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 534
if _libs["grass_vector.8.4"].has("V2_rewind_ogr", "cdecl"):
    V2_rewind_ogr = _libs["grass_vector.8.4"].get("V2_rewind_ogr", "cdecl")
    V2_rewind_ogr.argtypes = [POINTER(struct_Map_info)]
    V2_rewind_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 535
if _libs["grass_vector.8.4"].has("V2_rewind_pg", "cdecl"):
    V2_rewind_pg = _libs["grass_vector.8.4"].get("V2_rewind_pg", "cdecl")
    V2_rewind_pg.argtypes = [POINTER(struct_Map_info)]
    V2_rewind_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 536
if _libs["grass_vector.8.4"].has("V1_close_nat", "cdecl"):
    V1_close_nat = _libs["grass_vector.8.4"].get("V1_close_nat", "cdecl")
    V1_close_nat.argtypes = [POINTER(struct_Map_info)]
    V1_close_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 537
if _libs["grass_vector.8.4"].has("V1_close_ogr", "cdecl"):
    V1_close_ogr = _libs["grass_vector.8.4"].get("V1_close_ogr", "cdecl")
    V1_close_ogr.argtypes = [POINTER(struct_Map_info)]
    V1_close_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 538
if _libs["grass_vector.8.4"].has("V1_close_pg", "cdecl"):
    V1_close_pg = _libs["grass_vector.8.4"].get("V1_close_pg", "cdecl")
    V1_close_pg.argtypes = [POINTER(struct_Map_info)]
    V1_close_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 539
if _libs["grass_vector.8.4"].has("V2_close_ogr", "cdecl"):
    V2_close_ogr = _libs["grass_vector.8.4"].get("V2_close_ogr", "cdecl")
    V2_close_ogr.argtypes = [POINTER(struct_Map_info)]
    V2_close_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 540
if _libs["grass_vector.8.4"].has("V2_close_pg", "cdecl"):
    V2_close_pg = _libs["grass_vector.8.4"].get("V2_close_pg", "cdecl")
    V2_close_pg.argtypes = [POINTER(struct_Map_info)]
    V2_close_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 543
if _libs["grass_vector.8.4"].has("V1_read_line_nat", "cdecl"):
    V1_read_line_nat = _libs["grass_vector.8.4"].get("V1_read_line_nat", "cdecl")
    V1_read_line_nat.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats), off_t]
    V1_read_line_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 545
if _libs["grass_vector.8.4"].has("V1_read_line_ogr", "cdecl"):
    V1_read_line_ogr = _libs["grass_vector.8.4"].get("V1_read_line_ogr", "cdecl")
    V1_read_line_ogr.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats), off_t]
    V1_read_line_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 547
if _libs["grass_vector.8.4"].has("V1_read_line_pg", "cdecl"):
    V1_read_line_pg = _libs["grass_vector.8.4"].get("V1_read_line_pg", "cdecl")
    V1_read_line_pg.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats), off_t]
    V1_read_line_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 549
if _libs["grass_vector.8.4"].has("V2_read_line_nat", "cdecl"):
    V2_read_line_nat = _libs["grass_vector.8.4"].get("V2_read_line_nat", "cdecl")
    V2_read_line_nat.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats), c_int]
    V2_read_line_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 551
if _libs["grass_vector.8.4"].has("V2_read_line_sfa", "cdecl"):
    V2_read_line_sfa = _libs["grass_vector.8.4"].get("V2_read_line_sfa", "cdecl")
    V2_read_line_sfa.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats), c_int]
    V2_read_line_sfa.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 553
if _libs["grass_vector.8.4"].has("V2_read_line_pg", "cdecl"):
    V2_read_line_pg = _libs["grass_vector.8.4"].get("V2_read_line_pg", "cdecl")
    V2_read_line_pg.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats), c_int]
    V2_read_line_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 555
if _libs["grass_vector.8.4"].has("V1_read_next_line_nat", "cdecl"):
    V1_read_next_line_nat = _libs["grass_vector.8.4"].get("V1_read_next_line_nat", "cdecl")
    V1_read_next_line_nat.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_read_next_line_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 557
if _libs["grass_vector.8.4"].has("V1_read_next_line_ogr", "cdecl"):
    V1_read_next_line_ogr = _libs["grass_vector.8.4"].get("V1_read_next_line_ogr", "cdecl")
    V1_read_next_line_ogr.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_read_next_line_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 559
if _libs["grass_vector.8.4"].has("V1_read_next_line_pg", "cdecl"):
    V1_read_next_line_pg = _libs["grass_vector.8.4"].get("V1_read_next_line_pg", "cdecl")
    V1_read_next_line_pg.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_read_next_line_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 561
if _libs["grass_vector.8.4"].has("V2_read_next_line_nat", "cdecl"):
    V2_read_next_line_nat = _libs["grass_vector.8.4"].get("V2_read_next_line_nat", "cdecl")
    V2_read_next_line_nat.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_read_next_line_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 563
if _libs["grass_vector.8.4"].has("V2_read_next_line_ogr", "cdecl"):
    V2_read_next_line_ogr = _libs["grass_vector.8.4"].get("V2_read_next_line_ogr", "cdecl")
    V2_read_next_line_ogr.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_read_next_line_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 565
if _libs["grass_vector.8.4"].has("V2_read_next_line_pg", "cdecl"):
    V2_read_next_line_pg = _libs["grass_vector.8.4"].get("V2_read_next_line_pg", "cdecl")
    V2_read_next_line_pg.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_read_next_line_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 567
if _libs["grass_vector.8.4"].has("V1_delete_line_nat", "cdecl"):
    V1_delete_line_nat = _libs["grass_vector.8.4"].get("V1_delete_line_nat", "cdecl")
    V1_delete_line_nat.argtypes = [POINTER(struct_Map_info), off_t]
    V1_delete_line_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 568
if _libs["grass_vector.8.4"].has("V1_delete_line_ogr", "cdecl"):
    V1_delete_line_ogr = _libs["grass_vector.8.4"].get("V1_delete_line_ogr", "cdecl")
    V1_delete_line_ogr.argtypes = [POINTER(struct_Map_info), off_t]
    V1_delete_line_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 569
if _libs["grass_vector.8.4"].has("V1_delete_line_pg", "cdecl"):
    V1_delete_line_pg = _libs["grass_vector.8.4"].get("V1_delete_line_pg", "cdecl")
    V1_delete_line_pg.argtypes = [POINTER(struct_Map_info), off_t]
    V1_delete_line_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 570
if _libs["grass_vector.8.4"].has("V2_delete_line_nat", "cdecl"):
    V2_delete_line_nat = _libs["grass_vector.8.4"].get("V2_delete_line_nat", "cdecl")
    V2_delete_line_nat.argtypes = [POINTER(struct_Map_info), off_t]
    V2_delete_line_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 571
if _libs["grass_vector.8.4"].has("V2_delete_line_sfa", "cdecl"):
    V2_delete_line_sfa = _libs["grass_vector.8.4"].get("V2_delete_line_sfa", "cdecl")
    V2_delete_line_sfa.argtypes = [POINTER(struct_Map_info), off_t]
    V2_delete_line_sfa.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 572
if _libs["grass_vector.8.4"].has("V2_delete_line_pg", "cdecl"):
    V2_delete_line_pg = _libs["grass_vector.8.4"].get("V2_delete_line_pg", "cdecl")
    V2_delete_line_pg.argtypes = [POINTER(struct_Map_info), off_t]
    V2_delete_line_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 573
if _libs["grass_vector.8.4"].has("V1_restore_line_nat", "cdecl"):
    V1_restore_line_nat = _libs["grass_vector.8.4"].get("V1_restore_line_nat", "cdecl")
    V1_restore_line_nat.argtypes = [POINTER(struct_Map_info), off_t, off_t]
    V1_restore_line_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 574
if _libs["grass_vector.8.4"].has("V2_restore_line_nat", "cdecl"):
    V2_restore_line_nat = _libs["grass_vector.8.4"].get("V2_restore_line_nat", "cdecl")
    V2_restore_line_nat.argtypes = [POINTER(struct_Map_info), off_t, off_t]
    V2_restore_line_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 575
if _libs["grass_vector.8.4"].has("V1_write_line_nat", "cdecl"):
    V1_write_line_nat = _libs["grass_vector.8.4"].get("V1_write_line_nat", "cdecl")
    V1_write_line_nat.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_write_line_nat.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 577
if _libs["grass_vector.8.4"].has("V1_write_line_ogr", "cdecl"):
    V1_write_line_ogr = _libs["grass_vector.8.4"].get("V1_write_line_ogr", "cdecl")
    V1_write_line_ogr.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_write_line_ogr.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 579
if _libs["grass_vector.8.4"].has("V1_write_line_pg", "cdecl"):
    V1_write_line_pg = _libs["grass_vector.8.4"].get("V1_write_line_pg", "cdecl")
    V1_write_line_pg.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_write_line_pg.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 581
if _libs["grass_vector.8.4"].has("V2_write_line_nat", "cdecl"):
    V2_write_line_nat = _libs["grass_vector.8.4"].get("V2_write_line_nat", "cdecl")
    V2_write_line_nat.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_write_line_nat.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 583
if _libs["grass_vector.8.4"].has("V2_write_line_sfa", "cdecl"):
    V2_write_line_sfa = _libs["grass_vector.8.4"].get("V2_write_line_sfa", "cdecl")
    V2_write_line_sfa.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_write_line_sfa.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 585
if _libs["grass_vector.8.4"].has("V2_write_line_pg", "cdecl"):
    V2_write_line_pg = _libs["grass_vector.8.4"].get("V2_write_line_pg", "cdecl")
    V2_write_line_pg.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_write_line_pg.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 587
if _libs["grass_vector.8.4"].has("V1_rewrite_line_nat", "cdecl"):
    V1_rewrite_line_nat = _libs["grass_vector.8.4"].get("V1_rewrite_line_nat", "cdecl")
    V1_rewrite_line_nat.argtypes = [POINTER(struct_Map_info), off_t, c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_rewrite_line_nat.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 589
if _libs["grass_vector.8.4"].has("V1_rewrite_line_ogr", "cdecl"):
    V1_rewrite_line_ogr = _libs["grass_vector.8.4"].get("V1_rewrite_line_ogr", "cdecl")
    V1_rewrite_line_ogr.argtypes = [POINTER(struct_Map_info), off_t, c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_rewrite_line_ogr.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 591
if _libs["grass_vector.8.4"].has("V1_rewrite_line_pg", "cdecl"):
    V1_rewrite_line_pg = _libs["grass_vector.8.4"].get("V1_rewrite_line_pg", "cdecl")
    V1_rewrite_line_pg.argtypes = [POINTER(struct_Map_info), off_t, c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V1_rewrite_line_pg.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 593
if _libs["grass_vector.8.4"].has("V2_rewrite_line_nat", "cdecl"):
    V2_rewrite_line_nat = _libs["grass_vector.8.4"].get("V2_rewrite_line_nat", "cdecl")
    V2_rewrite_line_nat.argtypes = [POINTER(struct_Map_info), off_t, c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_rewrite_line_nat.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 595
if _libs["grass_vector.8.4"].has("V2_rewrite_line_sfa", "cdecl"):
    V2_rewrite_line_sfa = _libs["grass_vector.8.4"].get("V2_rewrite_line_sfa", "cdecl")
    V2_rewrite_line_sfa.argtypes = [POINTER(struct_Map_info), off_t, c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_rewrite_line_sfa.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 597
if _libs["grass_vector.8.4"].has("V2_rewrite_line_pg", "cdecl"):
    V2_rewrite_line_pg = _libs["grass_vector.8.4"].get("V2_rewrite_line_pg", "cdecl")
    V2_rewrite_line_pg.argtypes = [POINTER(struct_Map_info), off_t, c_int, POINTER(struct_line_pnts), POINTER(struct_line_cats)]
    V2_rewrite_line_pg.restype = off_t

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 601
if _libs["grass_vector.8.4"].has("Vect_build_nat", "cdecl"):
    Vect_build_nat = _libs["grass_vector.8.4"].get("Vect_build_nat", "cdecl")
    Vect_build_nat.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_build_nat.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 602
if _libs["grass_vector.8.4"].has("Vect__build_downgrade", "cdecl"):
    Vect__build_downgrade = _libs["grass_vector.8.4"].get("Vect__build_downgrade", "cdecl")
    Vect__build_downgrade.argtypes = [POINTER(struct_Map_info), c_int]
    Vect__build_downgrade.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 603
if _libs["grass_vector.8.4"].has("Vect__build_sfa", "cdecl"):
    Vect__build_sfa = _libs["grass_vector.8.4"].get("Vect__build_sfa", "cdecl")
    Vect__build_sfa.argtypes = [POINTER(struct_Map_info), c_int]
    Vect__build_sfa.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 604
if _libs["grass_vector.8.4"].has("Vect_build_ogr", "cdecl"):
    Vect_build_ogr = _libs["grass_vector.8.4"].get("Vect_build_ogr", "cdecl")
    Vect_build_ogr.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_build_ogr.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 605
if _libs["grass_vector.8.4"].has("Vect_build_pg", "cdecl"):
    Vect_build_pg = _libs["grass_vector.8.4"].get("Vect_build_pg", "cdecl")
    Vect_build_pg.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_build_pg.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 606
if _libs["grass_vector.8.4"].has("Vect_build_line_area", "cdecl"):
    Vect_build_line_area = _libs["grass_vector.8.4"].get("Vect_build_line_area", "cdecl")
    Vect_build_line_area.argtypes = [POINTER(struct_Map_info), c_int, c_int]
    Vect_build_line_area.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 607
if _libs["grass_vector.8.4"].has("Vect_isle_find_area", "cdecl"):
    Vect_isle_find_area = _libs["grass_vector.8.4"].get("Vect_isle_find_area", "cdecl")
    Vect_isle_find_area.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_bound_box)]
    Vect_isle_find_area.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 608
if _libs["grass_vector.8.4"].has("Vect_attach_isle", "cdecl"):
    Vect_attach_isle = _libs["grass_vector.8.4"].get("Vect_attach_isle", "cdecl")
    Vect_attach_isle.argtypes = [POINTER(struct_Map_info), c_int, POINTER(struct_bound_box)]
    Vect_attach_isle.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 609
if _libs["grass_vector.8.4"].has("Vect_attach_isles", "cdecl"):
    Vect_attach_isles = _libs["grass_vector.8.4"].get("Vect_attach_isles", "cdecl")
    Vect_attach_isles.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box)]
    Vect_attach_isles.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 610
if _libs["grass_vector.8.4"].has("Vect_attach_centroids", "cdecl"):
    Vect_attach_centroids = _libs["grass_vector.8.4"].get("Vect_attach_centroids", "cdecl")
    Vect_attach_centroids.argtypes = [POINTER(struct_Map_info), POINTER(struct_bound_box)]
    Vect_attach_centroids.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 614
if _libs["grass_vector.8.4"].has("Vect_read_line_geos", "cdecl"):
    Vect_read_line_geos = _libs["grass_vector.8.4"].get("Vect_read_line_geos", "cdecl")
    Vect_read_line_geos.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_int)]
    Vect_read_line_geos.restype = POINTER(GEOSGeometry)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 615
if _libs["grass_vector.8.4"].has("Vect_line_to_geos", "cdecl"):
    Vect_line_to_geos = _libs["grass_vector.8.4"].get("Vect_line_to_geos", "cdecl")
    Vect_line_to_geos.argtypes = [POINTER(struct_line_pnts), c_int, c_int]
    Vect_line_to_geos.restype = POINTER(GEOSGeometry)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 616
if _libs["grass_vector.8.4"].has("Vect_read_area_geos", "cdecl"):
    Vect_read_area_geos = _libs["grass_vector.8.4"].get("Vect_read_area_geos", "cdecl")
    Vect_read_area_geos.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_read_area_geos.restype = POINTER(GEOSGeometry)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 617
if _libs["grass_vector.8.4"].has("Vect_get_area_points_geos", "cdecl"):
    Vect_get_area_points_geos = _libs["grass_vector.8.4"].get("Vect_get_area_points_geos", "cdecl")
    Vect_get_area_points_geos.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_area_points_geos.restype = POINTER(GEOSCoordSequence)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 618
if _libs["grass_vector.8.4"].has("Vect_get_isle_points_geos", "cdecl"):
    Vect_get_isle_points_geos = _libs["grass_vector.8.4"].get("Vect_get_isle_points_geos", "cdecl")
    Vect_get_isle_points_geos.argtypes = [POINTER(struct_Map_info), c_int]
    Vect_get_isle_points_geos.restype = POINTER(GEOSCoordSequence)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 619
if _libs["grass_vector.8.4"].has("Vect_line_to_wkt", "cdecl"):
    Vect_line_to_wkt = _libs["grass_vector.8.4"].get("Vect_line_to_wkt", "cdecl")
    Vect_line_to_wkt.argtypes = [POINTER(struct_line_pnts), c_int, c_bool]
    if sizeof(c_int) == sizeof(c_void_p):
        Vect_line_to_wkt.restype = ReturnString
    else:
        Vect_line_to_wkt.restype = String
        Vect_line_to_wkt.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 620
if _libs["grass_vector.8.4"].has("Vect_line_to_wkt2", "cdecl"):
    Vect_line_to_wkt2 = _libs["grass_vector.8.4"].get("Vect_line_to_wkt2", "cdecl")
    Vect_line_to_wkt2.argtypes = [POINTER(struct_line_pnts), c_int, c_bool, c_bool]
    if sizeof(c_int) == sizeof(c_void_p):
        Vect_line_to_wkt2.restype = ReturnString
    else:
        Vect_line_to_wkt2.restype = String
        Vect_line_to_wkt2.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 621
if _libs["grass_vector.8.4"].has("Vect_line_to_wkb", "cdecl"):
    Vect_line_to_wkb = _libs["grass_vector.8.4"].get("Vect_line_to_wkb", "cdecl")
    Vect_line_to_wkb.argtypes = [POINTER(struct_line_pnts), c_int, c_int, POINTER(c_size_t)]
    Vect_line_to_wkb.restype = POINTER(c_ubyte)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 622
if _libs["grass_vector.8.4"].has("Vect_read_area_to_wkt", "cdecl"):
    Vect_read_area_to_wkt = _libs["grass_vector.8.4"].get("Vect_read_area_to_wkt", "cdecl")
    Vect_read_area_to_wkt.argtypes = [POINTER(struct_Map_info), c_int]
    if sizeof(c_int) == sizeof(c_void_p):
        Vect_read_area_to_wkt.restype = ReturnString
    else:
        Vect_read_area_to_wkt.restype = String
        Vect_read_area_to_wkt.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 623
if _libs["grass_vector.8.4"].has("Vect_read_area_to_wkt2", "cdecl"):
    Vect_read_area_to_wkt2 = _libs["grass_vector.8.4"].get("Vect_read_area_to_wkt2", "cdecl")
    Vect_read_area_to_wkt2.argtypes = [POINTER(struct_Map_info), c_int, c_bool]
    if sizeof(c_int) == sizeof(c_void_p):
        Vect_read_area_to_wkt2.restype = ReturnString
    else:
        Vect_read_area_to_wkt2.restype = String
        Vect_read_area_to_wkt2.errcheck = ReturnString

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 624
if _libs["grass_vector.8.4"].has("Vect_read_area_to_wkb", "cdecl"):
    Vect_read_area_to_wkb = _libs["grass_vector.8.4"].get("Vect_read_area_to_wkb", "cdecl")
    Vect_read_area_to_wkb.argtypes = [POINTER(struct_Map_info), c_int, POINTER(c_size_t)]
    Vect_read_area_to_wkb.restype = POINTER(c_ubyte)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 625
if _libs["grass_vector.8.4"].has("Vect_read_line_to_wkb", "cdecl"):
    Vect_read_line_to_wkb = _libs["grass_vector.8.4"].get("Vect_read_line_to_wkb", "cdecl")
    Vect_read_line_to_wkb.argtypes = [POINTER(struct_Map_info), POINTER(struct_line_pnts), POINTER(struct_line_cats), c_int, POINTER(c_size_t), POINTER(c_int)]
    Vect_read_line_to_wkb.restype = POINTER(c_ubyte)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 630
if _libs["grass_vector.8.4"].has("Vect_read_colors", "cdecl"):
    Vect_read_colors = _libs["grass_vector.8.4"].get("Vect_read_colors", "cdecl")
    Vect_read_colors.argtypes = [String, String, POINTER(struct_Colors)]
    Vect_read_colors.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 631
if _libs["grass_vector.8.4"].has("Vect_remove_colors", "cdecl"):
    Vect_remove_colors = _libs["grass_vector.8.4"].get("Vect_remove_colors", "cdecl")
    Vect_remove_colors.argtypes = [String, String]
    Vect_remove_colors.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 632
if _libs["grass_vector.8.4"].has("Vect_write_colors", "cdecl"):
    Vect_write_colors = _libs["grass_vector.8.4"].get("Vect_write_colors", "cdecl")
    Vect_write_colors.argtypes = [String, String, POINTER(struct_Colors)]
    Vect_write_colors.restype = None

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/defs/vector.h: 635
if _libs["grass_vector.8.4"].has("RTreeSearch2", "cdecl"):
    RTreeSearch2 = _libs["grass_vector.8.4"].get("RTreeSearch2", "cdecl")
    RTreeSearch2.argtypes = [POINTER(struct_RTree), POINTER(struct_RTree_Rect), POINTER(struct_ilist)]
    RTreeSearch2.restype = c_int

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 8
try:
    GV_DIRECTORY = 'vector'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 10
try:
    GV_FRMT_ELEMENT = 'frmt'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 12
try:
    GV_COOR_ELEMENT = 'coor'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 14
try:
    GV_HEAD_ELEMENT = 'head'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 16
try:
    GV_DBLN_ELEMENT = 'dbln'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 18
try:
    GV_HIST_ELEMENT = 'hist'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 20
try:
    GV_TOPO_ELEMENT = 'topo'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 22
try:
    GV_SIDX_ELEMENT = 'sidx'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 24
try:
    GV_CIDX_ELEMENT = 'cidx'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 26
try:
    GV_FIDX_ELEMENT = 'fidx'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 28
try:
    GV_COLR_ELEMENT = 'colr'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 30
try:
    GV_COLR2_DIRECTORY = 'vcolr2'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 32
try:
    GV_TIMESTAMP_ELEMENT = 'timestamp'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 45
try:
    PORT_DOUBLE = 8
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 46
try:
    PORT_FLOAT = 4
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 47
try:
    PORT_LONG = 4
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 48
try:
    PORT_INT = 4
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 49
try:
    PORT_SHORT = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 50
try:
    PORT_CHAR = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 51
try:
    PORT_OFF_T = 8
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 57
try:
    DBL_SIZ = 8
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 58
try:
    FLT_SIZ = 4
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 59
try:
    LNG_SIZ = 4
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 60
try:
    SHRT_SIZ = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 66
try:
    PORT_DOUBLE_MAX = 1.7976931348623157e+308
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 67
try:
    PORT_DOUBLE_MIN = 2.2250738585072014e-308
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 68
try:
    PORT_FLOAT_MAX = 3.40282347e+38
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 69
try:
    PORT_FLOAT_MIN = 1.17549435e-38
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 70
try:
    PORT_LONG_MAX = 2147483647
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 71
try:
    PORT_LONG_MIN = (-2147483647)
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 72
try:
    PORT_INT_MAX = 2147483647
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 73
try:
    PORT_INT_MIN = (-2147483647)
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 74
try:
    PORT_SHORT_MAX = 32767
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 75
try:
    PORT_SHORT_MIN = (-32768)
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 76
try:
    PORT_CHAR_MAX = 127
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 77
try:
    PORT_CHAR_MIN = (-128)
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 83
try:
    GV_FORMAT_NATIVE = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 85
try:
    GV_FORMAT_OGR = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 87
try:
    GV_FORMAT_OGR_DIRECT = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 89
try:
    GV_FORMAT_POSTGIS = 3
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 92
try:
    GV_TOPO_NATIVE = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 94
try:
    GV_TOPO_PSEUDO = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 96
try:
    GV_TOPO_POSTGIS = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 99
try:
    GV_1TABLE = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 101
try:
    GV_MTABLE = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 104
try:
    GV_MODE_READ = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 106
try:
    GV_MODE_WRITE = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 108
try:
    GV_MODE_RW = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 111
try:
    VECT_OPEN_CODE = 0x5522AA22
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 113
try:
    VECT_CLOSED_CODE = 0x22AA2255
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 116
try:
    LEVEL_1 = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 118
try:
    LEVEL_2 = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 120
try:
    LEVEL_3 = 3
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 123
try:
    GV_BUILD_NONE = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 125
try:
    GV_BUILD_BASE = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 127
try:
    GV_BUILD_AREAS = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 129
try:
    GV_BUILD_ATTACH_ISLES = 3
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 131
try:
    GV_BUILD_CENTROIDS = 4
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 134
try:
    GV_BUILD_ALL = GV_BUILD_CENTROIDS
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 137
def VECT_OPEN(Map):
    return (((Map.contents.open).value) == VECT_OPEN_CODE)

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 140
try:
    GV_MEMORY_ALWAYS = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 141
try:
    GV_MEMORY_NEVER = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 142
try:
    GV_MEMORY_AUTO = 3
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 145
try:
    GV_COOR_HEAD_SIZE = 14
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 147
try:
    GRASS_V_VERSION = '5.0'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 150
try:
    GV_COOR_VER_MAJOR = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 151
try:
    GV_COOR_VER_MINOR = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 152
try:
    GV_TOPO_VER_MAJOR = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 153
try:
    GV_TOPO_VER_MINOR = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 154
try:
    GV_SIDX_VER_MAJOR = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 155
try:
    GV_SIDX_VER_MINOR = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 156
try:
    GV_CIDX_VER_MAJOR = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 157
try:
    GV_CIDX_VER_MINOR = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 161
try:
    GV_COOR_EARLIEST_MAJOR = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 162
try:
    GV_COOR_EARLIEST_MINOR = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 163
try:
    GV_TOPO_EARLIEST_MAJOR = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 164
try:
    GV_TOPO_EARLIEST_MINOR = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 165
try:
    GV_SIDX_EARLIEST_MAJOR = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 166
try:
    GV_SIDX_EARLIEST_MINOR = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 167
try:
    GV_CIDX_EARLIEST_MAJOR = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 168
try:
    GV_CIDX_EARLIEST_MINOR = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 171
try:
    WITHOUT_Z = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 172
try:
    WITH_Z = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 175
try:
    GV_LEFT = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 176
try:
    GV_RIGHT = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 179
try:
    GV_FORWARD = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 180
try:
    GV_BACKWARD = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 183
try:
    GV_POINT = 0x01
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 184
try:
    GV_LINE = 0x02
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 185
try:
    GV_BOUNDARY = 0x04
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 186
try:
    GV_CENTROID = 0x08
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 187
try:
    GV_FACE = 0x10
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 188
try:
    GV_KERNEL = 0x20
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 189
try:
    GV_AREA = 0x40
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 190
try:
    GV_VOLUME = 0x80
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 192
try:
    GV_POINTS = (GV_POINT | GV_CENTROID)
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 193
try:
    GV_LINES = (GV_LINE | GV_BOUNDARY)
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 197
try:
    GV_STORE_POINT = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 198
try:
    GV_STORE_LINE = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 199
try:
    GV_STORE_BOUNDARY = 3
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 200
try:
    GV_STORE_CENTROID = 4
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 201
try:
    GV_STORE_FACE = 5
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 202
try:
    GV_STORE_KERNEL = 6
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 203
try:
    GV_STORE_AREA = 7
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 204
try:
    GV_STORE_VOLUME = 8
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 207
try:
    GV_ON_AND = 'AND'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 208
try:
    GV_ON_OVERLAP = 'OVERLAP'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 215
try:
    GV_NCATS_MAX = PORT_INT_MAX
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 217
try:
    GV_FIELD_MAX = PORT_INT_MAX
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 219
try:
    GV_CAT_MAX = PORT_INT_MAX
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 222
try:
    GV_ASCII_FORMAT_POINT = 0
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 224
try:
    GV_ASCII_FORMAT_STD = 1
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 226
try:
    GV_ASCII_FORMAT_WKT = 2
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 268
try:
    HEADSTR = 50
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 271
try:
    GV_PG_FID_COLUMN = 'fid'
except:
    pass

# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_defines.h: 273
try:
    GV_PG_GEOMETRY_COLUMN = 'geom'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 8
try:
    GV_DIRECTORY = 'vector'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 10
try:
    GV_FRMT_ELEMENT = 'frmt'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 12
try:
    GV_COOR_ELEMENT = 'coor'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 14
try:
    GV_HEAD_ELEMENT = 'head'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 16
try:
    GV_DBLN_ELEMENT = 'dbln'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 18
try:
    GV_HIST_ELEMENT = 'hist'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 20
try:
    GV_TOPO_ELEMENT = 'topo'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 22
try:
    GV_SIDX_ELEMENT = 'sidx'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 24
try:
    GV_CIDX_ELEMENT = 'cidx'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 26
try:
    GV_FIDX_ELEMENT = 'fidx'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 28
try:
    GV_COLR_ELEMENT = 'colr'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 30
try:
    GV_COLR2_DIRECTORY = 'vcolr2'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 32
try:
    GV_TIMESTAMP_ELEMENT = 'timestamp'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 45
try:
    PORT_DOUBLE = 8
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 46
try:
    PORT_FLOAT = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 47
try:
    PORT_LONG = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 48
try:
    PORT_INT = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 49
try:
    PORT_SHORT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 50
try:
    PORT_CHAR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 51
try:
    PORT_OFF_T = 8
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 57
try:
    DBL_SIZ = 8
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 58
try:
    FLT_SIZ = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 59
try:
    LNG_SIZ = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 60
try:
    SHRT_SIZ = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 66
try:
    PORT_DOUBLE_MAX = 1.7976931348623157e+308
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 67
try:
    PORT_DOUBLE_MIN = 2.2250738585072014e-308
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 68
try:
    PORT_FLOAT_MAX = 3.40282347e+38
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 69
try:
    PORT_FLOAT_MIN = 1.17549435e-38
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 70
try:
    PORT_LONG_MAX = 2147483647
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 71
try:
    PORT_LONG_MIN = (-2147483647)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 72
try:
    PORT_INT_MAX = 2147483647
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 73
try:
    PORT_INT_MIN = (-2147483647)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 74
try:
    PORT_SHORT_MAX = 32767
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 75
try:
    PORT_SHORT_MIN = (-32768)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 76
try:
    PORT_CHAR_MAX = 127
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 77
try:
    PORT_CHAR_MIN = (-128)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 83
try:
    GV_FORMAT_NATIVE = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 85
try:
    GV_FORMAT_OGR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 87
try:
    GV_FORMAT_OGR_DIRECT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 89
try:
    GV_FORMAT_POSTGIS = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 92
try:
    GV_TOPO_NATIVE = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 94
try:
    GV_TOPO_PSEUDO = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 96
try:
    GV_TOPO_POSTGIS = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 99
try:
    GV_1TABLE = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 101
try:
    GV_MTABLE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 104
try:
    GV_MODE_READ = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 106
try:
    GV_MODE_WRITE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 108
try:
    GV_MODE_RW = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 111
try:
    VECT_OPEN_CODE = 0x5522AA22
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 113
try:
    VECT_CLOSED_CODE = 0x22AA2255
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 116
try:
    LEVEL_1 = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 118
try:
    LEVEL_2 = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 120
try:
    LEVEL_3 = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 123
try:
    GV_BUILD_NONE = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 125
try:
    GV_BUILD_BASE = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 127
try:
    GV_BUILD_AREAS = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 129
try:
    GV_BUILD_ATTACH_ISLES = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 131
try:
    GV_BUILD_CENTROIDS = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 134
try:
    GV_BUILD_ALL = GV_BUILD_CENTROIDS
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 137
def VECT_OPEN(Map):
    return (((Map.contents.open).value) == VECT_OPEN_CODE)

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 140
try:
    GV_MEMORY_ALWAYS = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 141
try:
    GV_MEMORY_NEVER = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 142
try:
    GV_MEMORY_AUTO = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 145
try:
    GV_COOR_HEAD_SIZE = 14
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 147
try:
    GRASS_V_VERSION = '5.0'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 150
try:
    GV_COOR_VER_MAJOR = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 151
try:
    GV_COOR_VER_MINOR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 152
try:
    GV_TOPO_VER_MAJOR = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 153
try:
    GV_TOPO_VER_MINOR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 154
try:
    GV_SIDX_VER_MAJOR = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 155
try:
    GV_SIDX_VER_MINOR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 156
try:
    GV_CIDX_VER_MAJOR = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 157
try:
    GV_CIDX_VER_MINOR = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 161
try:
    GV_COOR_EARLIEST_MAJOR = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 162
try:
    GV_COOR_EARLIEST_MINOR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 163
try:
    GV_TOPO_EARLIEST_MAJOR = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 164
try:
    GV_TOPO_EARLIEST_MINOR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 165
try:
    GV_SIDX_EARLIEST_MAJOR = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 166
try:
    GV_SIDX_EARLIEST_MINOR = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 167
try:
    GV_CIDX_EARLIEST_MAJOR = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 168
try:
    GV_CIDX_EARLIEST_MINOR = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 171
try:
    WITHOUT_Z = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 172
try:
    WITH_Z = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 175
try:
    GV_LEFT = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 176
try:
    GV_RIGHT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 179
try:
    GV_FORWARD = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 180
try:
    GV_BACKWARD = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 183
try:
    GV_POINT = 0x01
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 184
try:
    GV_LINE = 0x02
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 185
try:
    GV_BOUNDARY = 0x04
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 186
try:
    GV_CENTROID = 0x08
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 187
try:
    GV_FACE = 0x10
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 188
try:
    GV_KERNEL = 0x20
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 189
try:
    GV_AREA = 0x40
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 190
try:
    GV_VOLUME = 0x80
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 192
try:
    GV_POINTS = (GV_POINT | GV_CENTROID)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 193
try:
    GV_LINES = (GV_LINE | GV_BOUNDARY)
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 197
try:
    GV_STORE_POINT = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 198
try:
    GV_STORE_LINE = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 199
try:
    GV_STORE_BOUNDARY = 3
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 200
try:
    GV_STORE_CENTROID = 4
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 201
try:
    GV_STORE_FACE = 5
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 202
try:
    GV_STORE_KERNEL = 6
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 203
try:
    GV_STORE_AREA = 7
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 204
try:
    GV_STORE_VOLUME = 8
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 207
try:
    GV_ON_AND = 'AND'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 208
try:
    GV_ON_OVERLAP = 'OVERLAP'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 215
try:
    GV_NCATS_MAX = PORT_INT_MAX
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 217
try:
    GV_FIELD_MAX = PORT_INT_MAX
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 219
try:
    GV_CAT_MAX = PORT_INT_MAX
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 222
try:
    GV_ASCII_FORMAT_POINT = 0
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 224
try:
    GV_ASCII_FORMAT_STD = 1
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 226
try:
    GV_ASCII_FORMAT_WKT = 2
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 268
try:
    HEADSTR = 50
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 271
try:
    GV_PG_FID_COLUMN = 'fid'
except:
    pass

# C:\\msys64\\usr\\src\\grass841\\dist.x86_64-w64-mingw32\\include\\grass\\vect\\dig_defines.h: 273
try:
    GV_PG_GEOMETRY_COLUMN = 'geom'
except:
    pass

site_att = struct_site_att# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 46

bound_box = struct_bound_box# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 64

gvfile = struct_gvfile# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 94

field_info = struct_field_info# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 131

dblinks = struct_dblinks# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 161

Port_info = struct_Port_info# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 181

recycle = struct_recycle# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 266

Version_info = struct_Version_info# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 271

dig_head = struct_dig_head# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 287

Coor_info = struct_Coor_info# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 371

Format_info_offset = struct_Format_info_offset# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 388

line_pnts = struct_line_pnts# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1651

Format_info_cache = struct_Format_info_cache# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 450

Format_info_ogr = struct_Format_info_ogr# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 505

Format_info_pg = struct_Format_info_pg# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 590

Format_info = struct_Format_info# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 700

Cat_index = struct_Cat_index# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 718

P_node = struct_P_node# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1433

P_line = struct_P_line# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1553

P_area = struct_P_area# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1583

P_isle = struct_P_isle# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1623

Plus_head = struct_Plus_head# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 769

Graph_info = struct_Graph_info# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1204

Map_info = struct_Map_info# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1243

P_topo_l = struct_P_topo_l# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1478

P_topo_b = struct_P_topo_b# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1492

P_topo_c = struct_P_topo_c# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1514

P_topo_f = struct_P_topo_f# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1524

P_topo_k = struct_P_topo_k# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1543

line_cats = struct_line_cats# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1677

cat_list = struct_cat_list# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1697

boxlist = struct_boxlist# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1723

varray = struct_varray# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1751

spatial_index = struct_spatial_index# C:/msys64/usr/src/grass841/dist.x86_64-w64-mingw32/include/grass/vect/dig_structs.h: 1770

# No inserted files

# No prefix-stripping

