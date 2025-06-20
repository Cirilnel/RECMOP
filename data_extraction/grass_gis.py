import os
import sys
import datetime

def init_grass(grass_base, gisdb, location, mapset):
    os.environ['GISBASE'] = grass_base
    os.environ['PATH'] = os.pathsep.join([
        os.path.join(grass_base, 'bin'),
        os.path.join(grass_base, 'scripts'),
        os.path.join(grass_base, 'extrabin'),
        os.path.join(grass_base, 'lib'),
        os.environ.get('PATH', '')
    ])
    pythonpath = os.path.join(grass_base, 'etc', 'python')
    if pythonpath not in sys.path:
        sys.path.insert(0, pythonpath)
    os.environ['PYTHONPATH'] = pythonpath

    import grass.script.setup as gsetup
    gsetup.init(gisdb, location, mapset)
    import grass.script as gs
    return gs

def run_grass_solar_analysis(grass_gisdb, dem_path, domanda_path, output_dir, grass_base, location_name="auto_location", mapset_name="PERMANENT"):

    gs = init_grass(grass_base, grass_gisdb, location_name, mapset_name)
    print("✅ Ambiente GRASS inizializzato")

    location_path = os.path.join(grass_gisdb, location_name)
    if not os.path.isdir(location_path):
        print(f"❗ La location '{location_name}' non esiste. Creala prima.")
        return

    # Import DEM
    gs.run_command('r.import', input=dem_path, output='dem_campania', overwrite=True)
    print("✅ DEM importato")

    # Import shapefile edifici
    gs.run_command('v.import', input=domanda_path, output='fabbricati_domanda', overwrite=True)
    print("✅ Shapefile edifici importato")

    # Set region su edifici
    gs.run_command('g.region', vector='fabbricati_domanda', res=5)
    print("✅ Region impostata")

    # Giorni centrali di ogni mese (come fa ArcGIS di default)
    days_of_year = [15, 45, 74, 105, 135, 166, 196, 227, 258, 288, 319, 349]
    irradianza_rasters = []

    for day in days_of_year:
        output_rast = f"irradianza_globale_{day}"
        gs.run_command('r.sun',
                       elevation='dem_campania',
                       glob_rad=output_rast,
                       day=day,
                       step=0.5,
                       overwrite=True)
        print(f"✅ r.sun eseguito per giorno {day}")
        irradianza_rasters.append(output_rast)

    # Calcola media annua
    cumulative_output = "irradianza_media_annua"
    gs.run_command('r.series',
                   input=irradianza_rasters,
                   output=cumulative_output,
                   method='average',
                   overwrite=True)
    print(f"✅ Raster media annua creato: {cumulative_output}")

    # Esporta risultato finale
    output_raster_path = os.path.join(output_dir, 'irradianza_media_annua.tif')
    gs.run_command('r.out.gdal',
                   input=cumulative_output,
                   output=output_raster_path,
                   format='GTiff',
                   createopt="COMPRESS=DEFLATE",
                   overwrite=True)
    print(f"✅ Raster media annua esportato in {output_raster_path}")

    return output_raster_path

if __name__ == "__main__":
    GRASS_GISDB = r"C:\Users\utente\Documents\grassdata"
    GRASS_BASE = r"C:\Programmi\GRASS GIS 8.4"
    LOCATION = "auto_location"
    MAPSET = "PERMANENT"

    DEM_PATH = os.path.abspath(os.path.join('..', 'grass_gis', 'DEM.tif'))
    DOMANDA_PATH = os.path.abspath(os.path.join('..', 'Data_Collection', 'shapefiles_merged', 'domanda_energetica', 'domanda_energetica.shp'))
    OUTPUT_DIR = os.path.abspath(os.path.join('..', 'grass_gis'))

    run_grass_solar_analysis(GRASS_GISDB, DEM_PATH, DOMANDA_PATH, OUTPUT_DIR, GRASS_BASE, LOCATION, MAPSET)
