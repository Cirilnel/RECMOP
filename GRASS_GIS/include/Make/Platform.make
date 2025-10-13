#############################################################################
#
# MODULE:   	Grass Compilation
# AUTHOR(S):	Original author unknown - probably CERL
#		Markus Neteler - Germany/Italy - neteler@itc.it
#   	    	Justin Hickey - Thailand - jhickey@hpcc.nectec.or.th
#   	    	Huidae Cho - Korea - grass4u@gmail.com
#   	    	Eric G. Miller - egm2@jps.net
# PURPOSE:  	The source file for this Makefile is in src/CMD/head/head.in.
#		It is the top part of a file called make.rules which is used
#		for compiling all GRASS modules. This part of the file provides
#		make variables that are dependent on the results of the
#		configure script.
# COPYRIGHT:    (C) 2000 by the GRASS Development Team
#
#               This program is free software under the GNU General Public
#   	    	License (>=v2). Read the file COPYING that comes with GRASS
#   	    	for details.
#
#############################################################################

############################## Make Variables ###############################

CC                  = x86_64-w64-mingw32-gcc
CXX                 = x86_64-w64-mingw32-g++
LEX                 = flex
YACC                = bison -y
PERL                = /usr/bin/perl
AR                  = ar
RANLIB              = ranlib
MKDIR               = mkdir -p
CHMOD               = chmod
INSTALL             = /usr/bin/install -c
INSTALL_DATA        = ${INSTALL} -m 644

prefix              = /c/osgeo4w/apps/grass
exec_prefix         = ${prefix}
ARCH                = x86_64-w64-mingw32
UNIX_BIN            = /c/osgeo4w/bin
INST_DIR            = ${prefix}/grass84

GRASS_HOME          = /c/osgeo4w/apps/grass/grass84
RUN_GISBASE         = C:/msys64/c/osgeo4w/apps/grass/grass84

GRASS_VERSION_MAJOR = 8
GRASS_VERSION_MINOR = 4
GRASS_VERSION_RELEASE = 1
GRASS_VERSION_DATE  = 2025
GRASS_VERSION_GIT   = 45ca3179ab

STRIPFLAG           = 
LD_SEARCH_FLAGS     = 
LD_LIBRARY_PATH_VAR = PATH

#generate static (ST) or shared (SH)
GRASS_LIBRARY_TYPE  = shlib

#static libs:
STLIB_LD            = ${AR} cr
STLIB_PREFIX        = lib
STLIB_SUFFIX        = .a

#shared libs
SHLIB_PREFIX        = lib
SHLIB_LD            = x86_64-w64-mingw32-gcc -shared
SHLIB_LDX           = x86_64-w64-mingw32-g++ -shared
SHLIB_LDFLAGS       = 
SHLIB_CFLAGS        = 
SHLIB_SUFFIX        = .dll
EXE                 = .exe

DEFAULT_DATABASE    =
DEFAULT_LOCATION    =

CPPFLAGS            =   -I/c/osgeo4w/include
CFLAGS              = -g -O2 
CXXFLAGS            = -g -O2
INCLUDE_DIRS        =  -I/c/osgeo4w/include
LINK_FLAGS          =   -Wl,--export-dynamic,--enable-runtime-pseudo-reloc  -L/c/osgeo4w/lib

DLLIB               = 
XCFLAGS             = 
XLIBPATH            = 
XLIB                =  
XEXTRALIBS          = 
USE_X11             = 

MATHLIB             = 
ICONVLIB            = -liconv
INTLLIB             = -lintl
SOCKLIB             = 

#ZLIB:
ZLIB                =  -lz 
ZLIBINCPATH         = 
ZLIBLIBPATH         = 

#BZIP2:
BZIP2LIB            =  -lbz2 
BZIP2INCPATH        = 
BZIP2LIBPATH        = 

#ZSTD:
ZSTDLIB             =  -lzstd 
ZSTDINCPATH         = 
ZSTDLIBPATH         = 

DBMIEXTRALIB        = 

#readline
READLINEINCPATH     = 
READLINELIBPATH     = 
READLINELIB         = 
HISTORYLIB          = 

#PostgreSQL:
PQINCPATH           =  -I/c/osgeo4w/include
PQLIBPATH           =  -L/usr/src/grass841/mswindows/osgeo4w/lib
PQLIB               =  -lpq 
USE_POSTGRES        = 1

#MySQL:
MYSQLINCPATH        = 
MYSQLLIBPATH        = 
MYSQLLIB            = 
MYSQLDLIB           = 

#SQLite:
SQLITEINCPATH       =  -I/c/osgeo4w/include
SQLITELIBPATH       =  -L/usr/src/grass841/mswindows/osgeo4w/lib
SQLITELIB           =  -lsqlite3 

#ODBC:
ODBCINC             = 
ODBCLIB             =  -lodbc32 

#Image formats:
PNGINC              = -I/mingw64/include/libpng16
PNGLIB              = -L/mingw64/lib -lpng16 -lz
USE_PNG             = 1

TIFFINCPATH         = 
TIFFLIBPATH         = 
TIFFLIB             =  -ltiff 

#openGL files for NVIZ/r3.showdspf
OPENGLINC           = 
OPENGLLIB           =   -lopengl32 
OPENGLULIB          =   -lglu32 
OPENGL_X11          = 
OPENGL_AQUA         = 
OPENGL_WINDOWS      = 1
USE_OPENGL          = 1

#FFTW:
FFTWINC             = 
FFTWLIB             =  -lfftw3 

#LAPACK/BLAS stuff for gmath lib:
BLASLIB             = -lblas
BLASINC             = 
LAPACKLIB           =  -llapack 
LAPACKINC           =  -I/mingw64/include

#LIBSVM
LIBSVM_LIB          = 
LIBSVM_INC          = 
USE_LIBSVM          = 

#GDAL/OGR
GDALLIBS            = /c/osgeo4w/lib/gdal_i.lib
GDALCFLAGS          = -I/c/osgeo4w/include
USE_GDAL            = 1
USE_OGR             = 1

#NetCDF
NETCDFLIBS          = -L/C/OSGeo4W/lib -lnetcdf
NETCDFCFLAGS        = -I/C/OSGeo4W/include
USE_NETCDF          = 1

#LAS LiDAR through libLAS
LASLIBS             = /c/osgeo4w/lib/liblas_c.lib
LASCFLAGS           = 
LASINC              = -I/c/osgeo4w/include
USE_LIBLAS          = 1

#LAS LiDAR through PDAL
PDALLIBS             = 
PDALINC              = 
USE_PDAL             = 

#GEOS
GEOSLIBS            = /c/osgeo4w/lib/geos_c.lib -lgeos_c 
GEOSCFLAGS          = -I/c/osgeo4w/include
USE_GEOS            = 1

#FreeType:
FTINC               = -IC:/msys64/mingw64/bin/../include/freetype2 -IC:/msys64/mingw64/bin/../include -IC:/msys64/mingw64/bin/../include/libpng16 -IC:/msys64/mingw64/bin/../include/harfbuzz -IC:/msys64/mingw64/bin/../include/glib-2.0 -IC:/msys64/mingw64/bin/../lib/glib-2.0/include -I/mingw64/include/freetype2
FTLIB               = -LC:/msys64/mingw64/bin/../lib -lfreetype -lfreetype 

#PROJ.4:
PROJINC             =  -I/c/osgeo4w/include
PROJLIB             =  -L/c/osgeo4w/lib -lproj 
PROJSHARE           = /c/osgeo4w/share/proj

#OPENDWG:
OPENDWGINCPATH      = 
OPENDWGLIBPATH      = 
OPENDWGLIB          = 
USE_OPENDWG         = 

#cairo
CAIROINC                  = -IC:/msys64/mingw64/bin/../include -IC:/msys64/mingw64/bin/../include/cairo -IC:/msys64/mingw64/bin/../include/pixman-1 -IC:/msys64/mingw64/bin/../include/freetype2 -IC:/msys64/mingw64/bin/../include/libpng16 -IC:/msys64/mingw64/bin/../include/harfbuzz -IC:/msys64/mingw64/bin/../include/glib-2.0 -IC:/msys64/mingw64/bin/../lib/glib-2.0/include -I/c/osgeo4w/include
CAIROLIB                  = -LC:/msys64/mingw64/bin/../lib -lz -lcairo -lpng16 -lfontconfig -lfreetype -L/usr/src/grass841/mswindows/osgeo4w/lib -lcairo -lfontconfig 
USE_CAIRO                 = 1
CAIRO_HAS_XRENDER         = 
CAIRO_HAS_XRENDER_SURFACE = 

#Python
PYTHON              = python3

#regex
REGEXINCPATH        = 
REGEXLIBPATH        = 
REGEXLIB            =  -lregex 
USE_REGEX           = 1

#pthreads
PTHREADINCPATH      = 
PTHREADLIBPATH      = 
PTHREADLIB          = 
USE_PTHREAD         = 

#OpenMP
OPENMP_INCPATH      = 
OPENMP_LIBPATH      = 
OPENMP_LIB          =  -lgomp 
OPENMP_CFLAGS       = -fopenmp
USE_OPENMP          = 1

#OpenCL
OCLINCPATH          = 
OCLLIBPATH          = 
OCLLIB              = 
USE_OPENCL          = 

#i18N
HAVE_NLS            = 1

#Large File Support (LFS)
USE_LARGEFILES      = 1
LFS_CFLAGS          = -D_FILE_OFFSET_BITS=64

#BSD sockets
HAVE_SOCKET         = 

MINGW		    = yes
WINDRES		    = windres
MACOSX_APP	    = 
MACOSX_ARCHS        = 
MACOSX_SDK          = 

# Cross compilation
CROSS_COMPILING     = 
