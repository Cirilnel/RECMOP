#! C:\OSGeo4W_grass8\apps\Python312\python3.exe

import sys

from osgeo.gdal import deprecation_warn

# import osgeo_utils.gdal_pansharpen as a convenience to use as a script
from osgeo_utils.gdal_pansharpen import *  # noqa
from osgeo_utils.gdal_pansharpen import main

deprecation_warn("gdal_pansharpen")
sys.exit(main(sys.argv))
