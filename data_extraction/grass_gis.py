import os
import sys
import subprocess
import logging
from dotenv import load_dotenv
import rasterio
import geopandas as gpd
import tempfile
import shutil

# =======================
# CONFIGURAZIONE LOGGING
# =======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =======================
# COSTANTI STATICHE
# =======================
DEM_PATH = os.path.abspath(os.path.join('..', 'grass_gis', 'DEM.tif'))
DOMANDA_PATH = os.path.abspath(os.path.join('..', 'FABBRICATI_geometry_only', 'FABBRICATI_geom_only.shp'))
OUTPUT_DIR = os.path.abspath(os.path.join('..', 'grass_gis'))

# =======================
# CARICAMENTO .ENV
# =======================
load_dotenv(os.path.join('..', '.env'))
GRASS_BASE = os.getenv("GRASS_BASE")
GRASS_GISDB = os.getenv("GRASS_GISDB")
LOCATION = os.getenv("GRASS_LOCATION", "auto_location")
MAPSET = os.getenv("GRASS_MAPSET", "PERMANENT")

# =======================
# FUNZIONI
# =======================
def get_epsg_from_dem(dem_path):
    with rasterio.open(dem_path) as src:
        crs = src.crs
        epsg = crs.to_epsg()
        if epsg is None:
            raise ValueError("EPSG non rilevabile dal DEM.")
        logger.info(f"EPSG rilevato dal DEM: {epsg}")
        return epsg

def validate_or_convert_vector_crs(dem_path, vector_path):
    with rasterio.open(dem_path) as src:
        dem_epsg = src.crs.to_epsg()

    gdf = gpd.read_file(vector_path)
    vector_epsg = gdf.crs.to_epsg()

    if vector_epsg == dem_epsg:
        logger.info(f"CRS coerenti (EPSG:{dem_epsg}) tra DEM e shapefile.")
        return vector_path

    logger.warning(f"CRS incoerente: vettore in EPSG:{vector_epsg}, DEM in EPSG:{dem_epsg}. Eseguo riproiezione...")
    gdf_converted = gdf.to_crs(epsg=dem_epsg)

    tmpdir = tempfile.mkdtemp()
    temp_path = os.path.join(tmpdir, "vector_reprojected.shp")
    gdf_converted.to_file(temp_path)

    logger.info(f"Shapefile riproiettato salvato temporaneamente in: {temp_path}")

    # Funzione interna per cleanup dopo l'import GRASS
    def cleanup():
        try:
            shutil.rmtree(tmpdir)
            logger.info(f"Cartella temporanea rimossa: {tmpdir}")
        except Exception as e:
            logger.warning(f"Errore durante la rimozione del file temporaneo: {e}")

    # Restituisce sia il path riproiettato che la funzione di cleanup
    return temp_path, cleanup

def create_grass_location(grass_base, gisdb, location, epsg_code):
    location_path = os.path.join(gisdb, location)
    if not os.path.exists(location_path):
        logger.info(f"Creazione location GRASS: {location}")
        grass_bin = os.path.join(grass_base, 'grass84.bat')
        command = [grass_bin, '-c', f"EPSG:{epsg_code}", '-e', location_path]
        subprocess.run(command, check=True)
    else:
        logger.info(f"Location GRASS già esistente: {location_path}")

def init_grass_environment(grass_base, gisdb, location, mapset):
    os.environ['GISBASE'] = grass_base
    os.environ['PATH'] = os.pathsep.join([
        os.path.join(grass_base, 'bin'),
        os.path.join(grass_base, 'scripts'),
        os.environ.get('PATH', '')
    ])
    pythonpath = os.path.join(grass_base, 'etc', 'python')
    if pythonpath not in sys.path:
        sys.path.insert(0, pythonpath)
    os.environ['PYTHONPATH'] = pythonpath

    import grass.script.setup as gsetup
    gsetup.init(gisdb, location, mapset)
    import grass.script as gs
    logger.info("Ambiente GRASS inizializzato")
    return gs

def run_solar_radiation_analysis(gs, dem_path, domanda_path, output_dir):
    gs.run_command('r.import', input=dem_path, output='dem_campania', overwrite=True)
    logger.info("DEM importato")

    gs.run_command('v.import', input=domanda_path, output='fabbricati_domanda', overwrite=True)
    logger.info("Shapefile edifici importato")

    gs.run_command('g.region', vector='fabbricati_domanda', res=5)
    logger.info("Region impostata")

    days = [15, 45, 74, 105, 135, 166, 196, 227, 258, 288, 319, 349]
    raster_list = []

    for day in days:
        output_rast = f"irradianza_globale_{day}"
        gs.run_command('r.sun', elevation='dem_campania', glob_rad=output_rast, day=day, step=0.5, overwrite=True)
        logger.info(f"r.sun eseguito per giorno {day}")
        raster_list.append(output_rast)

    cumulative_output = "irradianza_media_annua"
    gs.run_command('r.series', input=raster_list, output=cumulative_output, method='average', overwrite=True)
    logger.info(f"Raster media annua creato: {cumulative_output}")

    output_raster_path = os.path.join(output_dir, 'irradianza_media_annua.tif')
    gs.run_command('r.out.gdal', input=cumulative_output, output=output_raster_path,
                   format='GTiff', createopt="COMPRESS=DEFLATE", overwrite=True)
    logger.info(f"Raster esportato in: {output_raster_path}")
    return output_raster_path

# =======================
# MAIN
# =======================
if __name__ == "__main__":
    epsg_code = get_epsg_from_dem(DEM_PATH)
    DOMANDA_PATH_UPDATED, cleanup_fn = validate_or_convert_vector_crs(DEM_PATH, DOMANDA_PATH)
    create_grass_location(GRASS_BASE, GRASS_GISDB, LOCATION, epsg_code)
    gs = init_grass_environment(GRASS_BASE, GRASS_GISDB, LOCATION, MAPSET)
    run_solar_radiation_analysis(gs, DEM_PATH, DOMANDA_PATH_UPDATED, OUTPUT_DIR)
    # Pulisci cartella temporanea
    if callable(cleanup_fn):
        cleanup_fn()
