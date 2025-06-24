import os
import sys
import subprocess
import logging
from dotenv import load_dotenv
import rasterio
import geopandas as gpd
import tempfile
import shutil
import calendar
import pandas as pd
from pvlib.clearsky import lookup_linke_turbidity
from rasterstats import zonal_stats
from data_extraction.calcola_area_poligoni import calcola_area

# CONFIGURAZIONE LOG
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Carica configurazioni da .env
load_dotenv(os.path.join('..', '..', '.env'))
GRASS_BASE = os.getenv("GRASS_BASE")
GRASS_GISDB = os.getenv("GRASS_GISDB")
LOCATION = os.getenv("GRASS_LOCATION", "auto_location")
MAPSET = os.getenv("GRASS_MAPSET", "PERMANENT")

# Directory di input/output fissi
FABBRICATI_BASE = os.path.abspath(os.path.join('..', '..', 'FABBRICATI'))
DSM_BASE = os.path.abspath(os.path.join('..', '..', 'input_dsm'))
OUTPUT_DIR = os.path.abspath(os.path.join('..', 'grass_gis'))
SHAPE_OUT_DIR = os.path.abspath(os.path.join('..', '..', 'Data_Collection', 'shapefiles'))

def get_epsg_from_dem(dem_path) -> int:
    with rasterio.open(dem_path) as src:
        epsg = src.crs.to_epsg()
        if epsg is None:
            raise ValueError("EPSG non rilevabile dal DEM.")
        logger.info(f"EPSG rilevato dal DEM: {epsg}")
        return epsg


def validate_or_convert_vector_crs(dem_path, vector_path) -> tuple:
    with rasterio.open(dem_path) as src:
        dem_epsg = src.crs.to_epsg()
    gdf = gpd.read_file(vector_path)
    if gdf.crs.to_epsg() == dem_epsg:
        logger.info("CRS coerente.")
        return vector_path, None
    logger.warning("CRS incoerente. Riproietto...")
    gdf_converted = gdf.to_crs(epsg=dem_epsg)
    tmpdir = tempfile.mkdtemp()
    temp_path = os.path.join(tmpdir, "vector_reprojected.shp")
    gdf_converted.to_file(temp_path)

    def cleanup() -> None:
        shutil.rmtree(tmpdir)
    return temp_path, cleanup


def get_centroid_coordinates(shapefile_path) -> tuple:
    gdf = gpd.read_file(shapefile_path).to_crs(epsg=4326)
    centroid = gdf.geometry.union_all().centroid
    return centroid.y, centroid.x


def get_linke_turbidity_by_month(lat, lon) -> dict:
    days = []
    for m in range(1, 13):
        dim = calendar.monthrange(2021, m)[1]
        mid_day = int(dim / 2)
        doy = sum(calendar.monthrange(2021, mm)[1] for mm in range(1, m)) + mid_day
        days.append(doy)
    times = pd.to_datetime(['2021-01-01'] * 12) + pd.to_timedelta([d-1 for d in days], unit='D')
    times = times.tz_localize('UTC')
    turb_series = lookup_linke_turbidity(times, lat, lon)
    linke_vals = turb_series.values
    logger.info(f"Linke turbidity per mesi 1–12: {linke_vals}")
    return {i+1: float(linke_vals[i]) for i in range(12)}


def create_grass_location(grass_base, gisdb, location, epsg_code) -> None:
    location_path = os.path.join(gisdb, location)
    if not os.path.exists(location_path):
        grass_bin = os.path.join(grass_base, 'grass84.bat')
        subprocess.run([grass_bin, '-c', f"EPSG:{epsg_code}", '-e', location_path], check=True)


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
    logger.info("GRASS GIS inizializzato")
    return gs


def solar_radiation_pipeline(provincia, comune) -> str:
    # Definisci percorsi input
    provincia = provincia.strip().lower()
    comune = comune.strip().lower()
    dem_path = os.path.join(DSM_BASE, f"DSM_{provincia}_{comune}.tif")
    shp_dir = os.path.join(FABBRICATI_BASE, f"fabbricati_{provincia}_{comune}")
    # Trova lo shapefile
    shp_files = [f for f in os.listdir(shp_dir) if f.lower().endswith('.shp')]
    if not shp_files:
        raise FileNotFoundError(f"Nessuno shapefile trovato in {shp_dir}")
    domanda_path = os.path.join(shp_dir, shp_files[0])

    # Allineamento CRS vettoriale
    domanda_path_updated, cleanup_fn = validate_or_convert_vector_crs(dem_path, domanda_path)
    # EPSG del DEM
    epsg_code = get_epsg_from_dem(dem_path)
    # Prepara GRASS
    create_grass_location(GRASS_BASE, GRASS_GISDB, LOCATION, epsg_code)
    gs = init_grass_environment(GRASS_BASE, GRASS_GISDB, LOCATION, MAPSET)

    # Centroid e turbidity
    lat, lon = get_centroid_coordinates(domanda_path_updated)
    linke_by_month = get_linke_turbidity_by_month(lat, lon)
    logger.info(f"Linke stimato per mese: {linke_by_month}")

    # Import DEM e calcoli base
    gs.run_command('r.import', input=dem_path, output='dem', overwrite=True)
    gs.run_command('r.slope.aspect', elevation='dem', slope='slope', aspect='aspect', overwrite=True)
    gs.run_command('v.import', input=domanda_path_updated, output='fabbricati', overwrite=True)
    gs.run_command('g.region', vector='fabbricati', res=2)

    # Giorni medi per mese
    days = []
    for m in range(1, 13):
        dim = calendar.monthrange(2021, m)[1]
        doy_mid = sum(calendar.monthrange(2021, mm)[1] for mm in range(1, m)) + int(dim/2)
        days.append(doy_mid)

    # Calcolo radiazione per ciascun giorno medio
    raster_list = []
    for day in days:
        month = int((day - 1) / 30.5) + 1
        linke_value = linke_by_month.get(month, 3.5)
        rast_name = f"irradianza_{day}"
        gs.run_command('r.sun', elevation='dem', slope='slope', aspect='aspect',
                       glob_rad=rast_name, day=day, step=0.5,
                       linke_value=linke_value, albedo_value=0.2,
                       overwrite=True)
        raster_list.append(rast_name)

    # Media annua e conversione
    avg_rast = "irradianza_media_annua"
    gs.run_command('r.series', input=raster_list, output=avg_rast, method='average', overwrite=True)
    kwh_rast = f"{avg_rast}_kwh"
    gs.mapcalc(f"{kwh_rast} = {avg_rast} * 0.277778", overwrite=True)

    # Esporta GeoTIFF
    output_path = os.path.join(OUTPUT_DIR, f"irradianza_annua_{provincia}_{comune}_kwh.tif")
    gs.run_command('r.out.gdal', input=kwh_rast, output=output_path,
                   format='GTiff', type='Float64',
                   createopt="COMPRESS=DEFLATE", overwrite=True, flags='m')
    logger.info(f"Raster finale esportato: {output_path}")

    # Cleanup temporanei
    if callable(cleanup_fn):
        cleanup_fn()

    return output_path

def calculate_building_irradiance(provincia, comune) -> gpd.GeoDataFrame:
    provincia = provincia.strip().lower()
    comune = comune.strip().lower()
    raster_path = os.path.join(OUTPUT_DIR, f"irradianza_annua_{provincia}_{comune}_kwh.tif")
    shp_dir = os.path.join(FABBRICATI_BASE, f"fabbricati_{provincia}_{comune}")
    shp_files = [f for f in os.listdir(shp_dir) if f.lower().endswith('.shp')]
    if not shp_files:
        raise FileNotFoundError(f"Nessuno shapefile trovato in {shp_dir}")
    shapefile_path = os.path.join(shp_dir, shp_files[0])

    # Allineamento CRS
    shp_updated, cleanup_fn = validate_or_convert_vector_crs(raster_path, shapefile_path)
    gdf = gpd.read_file(shp_updated)

    # Zonal statistics: mean irradiance
    stats = zonal_stats(gdf, raster_path, stats=['mean'])
    gdf['irradiance_kwh_mq'] = [st['mean'] for st in stats]

    # Filtra i poligoni senza valore di irraggiamento (None o 0)
    gdf = gdf[gdf['irradiance_kwh_mq'].notnull() & (gdf['irradiance_kwh_mq'] > 0)]

    # Area dei poligoni
    gdf = calcola_area(gdf, nome_colonna='area_mq')

    # Offerta energetica totale per fabbricato
    gdf['offerta_energetica_kwh'] = gdf['irradiance_kwh_mq'] * gdf['area_mq']

    # Directory di output shapefile
    out_dir = os.path.join(SHAPE_OUT_DIR, f"offerta_energetica_{provincia}_{comune}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"offerta_energetica_{provincia}_{comune}.shp")
    gdf.to_file(out_path)
    logger.info(f"Shapefile offerta energetica salvato in: {out_path}")

    if callable(cleanup_fn):
        cleanup_fn()

    return gdf

if __name__ == "__main__":
    # Esempio di utilizzo
    provincia_esempio = "Salerno"
    comune_esempio = "Padula"
    raster_out = solar_radiation_pipeline(provincia_esempio, comune_esempio)
    logger.info(f"Pipeline completata, raster in: {raster_out}")
    gdf_offerta = calculate_building_irradiance(provincia_esempio, comune_esempio)
    logger.info("Calcolo offerta energetica completato.")