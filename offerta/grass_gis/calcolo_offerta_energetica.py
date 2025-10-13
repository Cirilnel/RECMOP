import os
import shutil
import sys
import subprocess
import logging
import uuid
import rasterio
from dotenv import load_dotenv
from rasterio.enums import Resampling
import geopandas as gpd
import calendar
import pandas as pd
from pvlib.clearsky import lookup_linke_turbidity
from rasterstats import zonal_stats
from data_extraction.calcola_area_poligoni import calcola_area
from utils import safe_name, configure_logging_if_main, load_dot_env, raster_is_empty, get_base_path

# CONFIGURAZIONE LOG
logger = logging.getLogger(__name__)

# Directory base del progetto (una volta per tutte)
# Determina BASE_DIR corretto (sia da sorgente che da PyInstaller)
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Carica configurazioni da .env
load_dot_env()

# Dopo load_dotenv
GRASS_BASE = os.getenv('GRASS_BASE')
GRASS_GISDB = os.getenv('GRASS_GISDB')
GRASS_MAPSET = os.getenv('GRASS_MAPSET', 'PERMANENT')

if not GRASS_BASE or not GRASS_GISDB:
    raise EnvironmentError("GRASS_BASE o GRASS_GISDB non configurate correttamente. Controlla .env.")


# Directory di input/output fissi
FABBRICATI_BASE = os.path.join(BASE_DIR, 'FABBRICATI')
VINCOLI_BASE = os.path.join(BASE_DIR, 'VINCOLI')
DSM_BASE = os.path.join(BASE_DIR, 'input_dsm')
OUTPUT_DIR = os.path.join(BASE_DIR, 'offerta', 'grass_gis', 'irradiance_tif')
SHAPE_OUT_DIR = os.path.join(BASE_DIR, 'Data_Collection', 'shapefiles')
PANEL_DATA_PATH = os.path.join(BASE_DIR, 'offerta', 'panel', 'panels.csv')

def get_env_path(env_var_name):
    path_from_env = os.getenv(env_var_name)
    if getattr(sys, 'frozen', False):
        return os.path.join(sys._MEIPASS, path_from_env)
    else:
        return os.path.join(BASE_DIR, path_from_env)


def generate_temp_location(epsg, comune, provincia):
    unique = uuid.uuid4().hex[:8]
    return f'tmp_location_{epsg}_{safe_name(comune)}_{safe_name(provincia)}_{unique}'

def create_grass_location(grass_base, gisdb, location, epsg_code) -> None:
    import subprocess
    import os
    import sys

    loc_path = os.path.abspath(os.path.join(get_base_path(), gisdb, location))
    if not os.path.exists(loc_path):
        print(f'Creazione GRASS location {location} con EPSG:{epsg_code}')

        # Path compatibile con eseguibile PyInstaller
        if getattr(sys, 'frozen', False):
            grass_bin = os.path.join(sys._MEIPASS,'GRASS_GIS', 'grass84.bat')
        else:
            grass_bin = os.path.join(get_env_path("GRASS_BASE"), 'grass84.bat')

        print(f"GRASS_BIN: {grass_bin}")
        print(f"Location path: {loc_path}")

        cmd = [f'"{grass_bin}"', '-c', f'EPSG:{epsg_code}', '-e', loc_path]
        print(f'Eseguo comando: {" ".join(cmd)}')

        result = subprocess.run(" ".join(cmd), capture_output=True, text=True, shell=True)
        print(f'Output stdout:\n{result.stdout}')
        print(f'Output stderr:\n{result.stderr}')
        print(f'Return code: {result.returncode}')

        if result.returncode != 0:
            raise RuntimeError(f"Creazione location GRASS fallita: {result.stderr}")

        if not os.path.exists(loc_path):
            raise RuntimeError(f"La directory location non è stata creata: {loc_path}")


def remove_grass_location(grass_gisdb, location_name):
    import shutil
    loc_path = os.path.join(grass_gisdb, location_name)
    if os.path.exists(loc_path):
        shutil.rmtree(loc_path)
        print(f"Location GRASS temporanea rimossa: {loc_path}")

def init_grass_environment(grass_base, gisdb, location, mapset):
    """Inizializza le variabili d'ambiente di GRASS GIS e ritorna il modulo grass.script."""
    print('Imposto ambiente GRASS GIS')

    gisbase_path = get_env_path("GRASS_BASE")
    os.environ['GISBASE'] = gisbase_path
    os.environ['PATH'] = os.pathsep.join([
        os.path.join(gisbase_path, 'bin'),
        os.path.join(gisbase_path, 'scripts'),
        os.environ.get('PATH', '')
    ])
    pythonpath = os.path.join(gisbase_path, 'etc', 'python')
    if pythonpath not in sys.path:
        sys.path.insert(0, pythonpath)
    os.environ['PYTHONPATH'] = pythonpath

    # 💡 Costruisco path assoluto a gisdb
    gisdb_path = os.path.abspath(os.path.join(get_base_path(), gisdb))

    import grass.script.setup as gsetup
    gsetup.init(gisdb_path, location, mapset)
    import grass.script as gs
    print('GRASS GIS inizializzato')
    return gs

def reproject_if_needed(src_crs, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Riproietta il GeoDataFrame per matchare src_crs."""
    if gdf.crs.to_epsg() != src_crs.to_epsg():
        print('CRS non corrispondente: eseguo riproiezione vettoriale')
        return gdf.to_crs(src_crs)
    print('CRS corrispondente: nessuna riproiezione necessaria')
    return gdf

def get_epsg(dem_path: str) -> int:
    """Estrae il codice EPSG dal file DEM."""
    print(f'Leggo CRS da DEM: {dem_path}')
    with rasterio.open(dem_path) as src:
        epsg = src.crs.to_epsg()
    if epsg is None:
        print('Impossibile rilevare EPSG dal DEM')
        raise ValueError('EPSG non rilevabile dal DEM')
    print(f'EPSG rilevato dal DEM: {epsg}')
    return epsg

def get_centroid(shp_path: str) -> tuple:
    """Ritorna lat, lon del centroide in WGS84."""
    print(f'Calcolo centroide per: {shp_path}')
    gdf = gpd.read_file(shp_path)
    gdf = reproject_if_needed(gdf.crs, gdf).to_crs(epsg=4326)
    union_geom = gdf.geometry.union_all()
    centroid = union_geom.centroid
    return centroid.y, centroid.x

def get_linke_turbidity(lat: float, lon: float) -> dict:
    """Recupera i valori di turbidity Linke per ciascun mese."""
    print('Richiedo turbidity Linke')
    mid_days = []
    for m in range(1, 13):
        dim = calendar.monthrange(2021, m)[1]
        mid = sum(calendar.monthrange(2021, mm)[1] for mm in range(1, m)) + dim // 2
        mid_days.append(mid)
    times = pd.to_datetime(['2021-01-01'] * 12) + pd.to_timedelta([d - 1 for d in mid_days], 'D')
    turb = lookup_linke_turbidity(times.tz_localize('UTC'), lat, lon)
    vals = {i + 1: float(v) for i, v in enumerate(turb.values)}
    print(f'Turbidity Linke per mesi: {vals}')
    return vals

def resample_dsm_to_1x1(src_path, out_path):
    with rasterio.open(src_path) as src:
        res_x, res_y = src.res  # tuple (xres, yres)
        if max(res_x, res_y) <= 2:
            # No resampling needed
            print(f"Nessun resampling: il DSM '{os.path.basename(src_path)}' ha risoluzione {res_x}x{res_y} m <= 2 m.")
            return src_path  # return original DSM path
        # Calculate new shape
        scale_x = res_x / 1.0
        scale_y = res_y / 1.0
        new_width = int(src.width * scale_x)
        new_height = int(src.height * scale_y)
        print(f"Attenzione: il DSM '{os.path.basename(src_path)}' ha risoluzione {res_x}x{res_y} m: lo risampio a 1x1 m...")
        # Prepare destination dataset
        kwargs = src.meta.copy()
        kwargs.update({
            'height': new_height,
            'width': new_width,
            'transform': src.transform * src.transform.scale(
                (src.width / new_width),
                (src.height / new_height)
            )
        })
        with rasterio.open(out_path, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                data = src.read(
                    i,
                    out_shape=(new_height, new_width),
                    resampling=Resampling.bilinear  # or Resampling.cubic for smoother results
                )
                dst.write(data, i)
        return out_path

def needs_resampling_to_1x1(tif_path):
    with rasterio.open(tif_path) as src:
        res_x, res_y = src.res
    return max(res_x, res_y) > 2

def solar_radiation_pipeline(provincia: str, comune: str, location_tmp: str) -> str:
    """Genera il raster annuale di irradianza in kWh, resampling solo sull'output finale."""
    prov_safe = safe_name(provincia)
    com_safe = safe_name(comune)
    output_tif = os.path.join(OUTPUT_DIR, f'irradianza_annua_{prov_safe}_{com_safe}_kwh.tif')
    output_tif_1x1 = os.path.join(OUTPUT_DIR, f'irradianza_annua_{prov_safe}_{com_safe}_kwh_1x1.tif')
    dem = os.path.join(DSM_BASE, f'DSM_{prov_safe}_{com_safe}.tif')
    shapefolder = os.path.join(FABBRICATI_BASE, f'fabbricati_{prov_safe}_{com_safe}')

    print(f'Avvio pipeline solare per {provincia}/{comune}')
    shp_list = [f for f in os.listdir(shapefolder) if f.lower().endswith('.shp')]
    if not shp_list:
        raise FileNotFoundError(f'Nessuno shapefile in {shapefolder}')
    shp = os.path.join(shapefolder, shp_list[0])

    epsg = get_epsg(dem)
    create_grass_location(GRASS_BASE, GRASS_GISDB, location_tmp, epsg)
    gs = init_grass_environment(GRASS_BASE, GRASS_GISDB, location_tmp, GRASS_MAPSET)

    lat, lon = get_centroid(shp)
    turb_by_month = get_linke_turbidity(lat, lon)

    gs.run_command('r.import', input=dem, output='dem', overwrite=True)
    gs.run_command('r.slope.aspect', elevation='dem', slope='slope', aspect='aspect', overwrite=True)
    gs.run_command('v.import', input=shp, output='fabbricati', overwrite=True)
    gs.run_command('v.build', map='fabbricati')

    bbox_fab = gs.parse_command('v.info', map='fabbricati', flags='g')
    bbox_dem = gs.parse_command('r.info', map='dem', flags='g')

    n = min(float(bbox_fab['north']), float(bbox_dem['north']))
    s = max(float(bbox_fab['south']), float(bbox_dem['south']))
    e = min(float(bbox_fab['east']), float(bbox_dem['east']))
    w = max(float(bbox_fab['west']), float(bbox_dem['west']))

    gs.run_command('g.region', n=n, s=s, e=e, w=w, align='dem', res=2)

    rasters = []
    for m in range(1, 13):
        dim = calendar.monthrange(2021, m)[1]
        mid = sum(calendar.monthrange(2021, mm)[1] for mm in range(1, m)) + dim // 2
        linke = turb_by_month.get(m, 3.5)
        nome = f'irr_{mid}'
        gs.run_command('r.sun', elevation='dem', slope='slope', aspect='aspect',
                       glob_rad=nome, day=mid, step=0.5, linke_value=linke, albedo_value=0.2, overwrite=True)
        rasters.append(nome)

    gs.run_command('r.series', input=rasters, output='annua_avg', method='average', overwrite=True)
    gs.mapcalc('annua_kwh = annua_avg * 0.277778', overwrite=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Esporta GTiff a risoluzione nativa (es: 10x10)
    gs.run_command('r.out.gdal', input='annua_kwh', output=output_tif,
                   format='GTiff', type='Float64', createopt='COMPRESS=DEFLATE', overwrite=True)
    print(f'Raster di irradianza salvato: {output_tif}')

    if needs_resampling_to_1x1(output_tif):
        print(f"Eseguo resampling raster irradianza da {output_tif} a risoluzione 1x1m...")
        resample_dsm_to_1x1(output_tif, output_tif_1x1)
        # Elimina il vecchio file e rinomina quello nuovo
        try:
            os.remove(output_tif)
            print(f"File originale {output_tif} eliminato.")
        except Exception as e:
            print(f"Impossibile eliminare {output_tif}: {e}")
        try:
            os.rename(output_tif_1x1, output_tif)
            print(f"File risamplato rinominato come {output_tif}")
        except Exception as e:
            print(f"Impossibile rinominare {output_tif_1x1} in {output_tif}: {e}")
    else:
        print("Nessun resampling necessario sull’output finale.")

    return output_tif



def calculate_building_irradiance(provincia: str, comune: str, idx_panel: int, use_vincoli: bool = True) -> gpd.GeoDataFrame:
    """Calcola l'offerta energetica per ogni fabbricato e salva shapefile con struttura cartelle coerente."""
    prov_safe = safe_name(provincia)
    com_safe = safe_name(comune)

    raster = os.path.join(OUTPUT_DIR, f'irradianza_annua_{prov_safe}_{com_safe}_kwh.tif')
    shapefolder = os.path.join(FABBRICATI_BASE, f'fabbricati_{prov_safe}_{com_safe}')
    vincoli_folder = os.path.join(VINCOLI_BASE, f'vincoli_{prov_safe}_{com_safe}')

    logger.info(f'Calcolo offerta energetica per {provincia}/{comune}')

    # Controllo shapefile
    shp_list = [f for f in os.listdir(shapefolder) if f.lower().endswith('.shp')]
    if not shp_list:
        raise FileNotFoundError(f'Nessuno shapefile in {shapefolder}')
    shp_path = os.path.join(shapefolder, shp_list[0])

    gdf = gpd.read_file(shp_path)

    # --- Lettura vincoli (tollerante)
    mask_in_vincolo = pd.Series([False] * len(gdf), index=gdf.index)
    gdf_offerta = gdf.copy()

    if use_vincoli:
        if os.path.isdir(vincoli_folder):
            vincoli_list = [f for f in os.listdir(vincoli_folder) if f.lower().endswith('.shp')]
            if vincoli_list:
                vincoli_path = os.path.join(vincoli_folder, vincoli_list[0])
                gdf_vincoli = gpd.read_file(vincoli_path)

                # Allinea CRS tra fabbricati e vincoli
                if gdf.crs != gdf_vincoli.crs:
                    logger.info("Allineamento CRS vincoli a quello dei fabbricati")
                    gdf_vincoli = gdf_vincoli.to_crs(gdf.crs)

                # Filtra i fabbricati FUORI dai vincoli usando i centroidi
                centroids = gdf.geometry.centroid
                vincoli_union = gdf_vincoli.unary_union
                mask_in_vincolo = centroids.within(vincoli_union)
                gdf_offerta = gdf[~mask_in_vincolo].copy()
            else:
                logger.info("Nessuno shapefile vincoli trovato")
        else:
            logger.info("Cartella vincoli non trovata")
    else:
        logger.info("Vincoli ignorati su richiesta utente.")

    # --- Riproiezione per zonal stats
    with rasterio.open(raster) as src:
        raster_crs = src.crs

    if gdf_offerta.crs != raster_crs:
        gdf_offerta = reproject_if_needed(raster_crs, gdf_offerta)

    # --- Zonal stats SOLO su edifici FUORI dai vincoli
    stats = zonal_stats(gdf_offerta, raster, stats=['mean'], nodata=0)
    gdf_offerta['irr_kwh_m2'] = [s['mean'] for s in stats]
    gdf_offerta = gdf_offerta[gdf_offerta['irr_kwh_m2'] > 0]
    gdf_offerta = calcola_area(gdf_offerta, nome_colonna='area')

    # --- Carica i dati del pannello
    panel_path = PANEL_DATA_PATH if not getattr(sys, 'frozen', False) else os.path.join(sys._MEIPASS, PANEL_DATA_PATH)
    panel_df = pd.read_csv(panel_path, delimiter=',', decimal=',', na_values=['n.a.', 'N.A.', 'na', 'NA', '-', ''])

    for col in ['Potenza (Wp)', 'Efficienza (%)', 'Prezzo', 'Dimensione']:
        panel_df[col] = pd.to_numeric(panel_df[col], errors='coerce')
    panel_df.dropna(subset=['Potenza (Wp)', 'Efficienza (%)', 'Prezzo', 'Dimensione'], inplace=True)

    if idx_panel >= len(panel_df):
        raise IndexError(f"Indice pannello {idx_panel} fuori dai limiti ({len(panel_df)} disponibili).")

    specs = panel_df.iloc[idx_panel]

    # --- Calcolo offerta energetica
    gdf_offerta['Ptnz_Wp'] = specs['Potenza (Wp)']
    gdf_offerta['Eff_pct'] = specs['Efficienza (%)']
    gdf_offerta['Dim_m2'] = specs['Dimensione']
    gdf_offerta['Prz_uni'] = specs['Prezzo']
    gdf_offerta['num_PV'] = (gdf_offerta['area'] / gdf_offerta['Dim_m2']).astype(int).clip(lower=0)
    gdf_offerta['Prz_tot'] = gdf_offerta['num_PV'] * gdf_offerta['Prz_uni']
    gdf_offerta['Ptnz_tot'] = gdf_offerta['Ptnz_Wp'] * gdf_offerta['num_PV']
    gdf_offerta['Prod_kWh_y'] = gdf_offerta['irr_kwh_m2'] * (1 - gdf_offerta['Eff_pct'] / 100) * gdf_offerta['Ptnz_Wp'] * gdf_offerta['num_PV'] / 1000

    # --- Costruzione gdf finale
    float_cols = ['irr_kwh_m2', 'area', 'Ptnz_Wp', 'Eff_pct', 'Dim_m2', 'Prz_uni', 'Prz_tot', 'Ptnz_tot', 'Prod_kWh_y']
    int_cols = ['num_PV']

    for col in float_cols:
        if col not in gdf.columns:
            gdf[col] = 0.0
    for col in int_cols:
        if col not in gdf.columns:
            gdf[col] = 0

    gdf.loc[gdf_offerta.index, float_cols] = gdf_offerta[float_cols].astype(float)
    gdf.loc[gdf_offerta.index, int_cols] = gdf_offerta[int_cols].astype(int)

    # --- Salva risultato
    subdir = f'{prov_safe}_{com_safe}'
    dirname = f'offerta_energetica_{prov_safe}_{com_safe}'
    outdir = os.path.join(SHAPE_OUT_DIR, subdir, dirname)

    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir)

    outgpkg = os.path.join(outdir, f'{dirname}.gpkg')
    gdf.to_file(outgpkg)
    logger.info(f'Shapefile offerta energetica salvato: {outgpkg}')

    return gdf


def safe_building_irradiance(provincia: str, comune: str, idx_panel: int, pipeline_func=None, use_vincoli:bool=True):
    """
    Calcola l'offerta energetica per ogni fabbricato, rilanciando la pipeline se il risultato è vuoto.
    pipeline_func: funzione da chiamare per rigenerare i dati se necessario (es: solar_radiation_pipeline).
    Max 2 tentativi; se ancora vuoto solleva RuntimeError.
    """
    tentativi = 2
    prov_safe = safe_name(provincia)
    com_safe = safe_name(comune)
    raster = os.path.join(OUTPUT_DIR, f'irradianza_annua_{prov_safe}_{com_safe}_kwh.tif')
    for i in range(tentativi):
        if pipeline_func and i > 0:
            logger.info(f"Rilancio pipeline per {provincia}/{comune} (tentativo {i + 1})")
            pipeline_func(provincia, comune)
        # Primo controllo: raster contiene solo NaN?
        if not os.path.exists(raster) or raster_is_empty(raster):
            logger.warning(f"Il raster prodotto è vuoto (tentativo {i + 1}/{tentativi}).")
            continue  # rilancia la pipeline o tenta di nuovo
        gdf = calculate_building_irradiance(provincia, comune, idx_panel, use_vincoli=use_vincoli)
        # Secondo controllo: GeoDataFrame vuoto?
        if not gdf.empty:
            return gdf
        logger.warning(f"L'offerta energetica calcolata è vuota (tentativo {i + 1}/{tentativi}).")
        if pipeline_func is None:
            break  # Se non posso rilanciare la pipeline, non ha senso continuare
    logger.error(f"Offerta energetica vuota anche dopo {tentativi} tentativi per {provincia}/{comune}.")
    raise RuntimeError(
        f"Offerta energetica vuota anche dopo {tentativi} tentativi! Verificare input e pipeline per {provincia}/{comune}."
    )

def calcolo_offerta_energetica(provincia: str, comune: str, idx_panel: int, use_vincoli:bool=True):
    prov = safe_name(provincia)
    com = safe_name(comune)
    raster = os.path.join(OUTPUT_DIR, f'irradianza_annua_{prov}_{com}_kwh.tif')
    dem = os.path.join(DSM_BASE, f'DSM_{prov}_{com}.tif')
    # Prima controlla se esiste il DSM
    dem_exists = os.path.isfile(dem)
    raster_exists = os.path.isfile(raster)
    raster_empty = raster_is_empty(raster) if raster_exists else True

    epsg = get_epsg(dem) if dem_exists else None
    location_tmp = None

    try:
        # Caso 1: DSM esiste → se raster mancante/vuoto, rigeneralo
        if dem_exists:
            if (not raster_exists) or raster_empty:
                location_tmp = generate_temp_location(epsg, comune, provincia)
                logger.info(f'Creo location temporanea: {location_tmp}')
                solar_radiation_pipeline(provincia, comune, location_tmp)
        # Caso 2: DSM non esiste → NON tentare nessuna rigenerazione, usa solo quello che c'è
        gdf = safe_building_irradiance(provincia, comune, idx_panel,
                                       pipeline_func=lambda p, c: solar_radiation_pipeline(p, c, location_tmp),
                                       use_vincoli=use_vincoli)
        return gdf
    finally:
        if location_tmp:
            remove_grass_location(GRASS_GISDB, location_tmp)



def refresh_offerta_energetica(provincia: str, comune: str, idx_panel: int, use_vincoli:bool=True ):
    prov = safe_name(provincia)
    com = safe_name(comune)
    dem = os.path.join(DSM_BASE, f'DSM_{prov}_{com}.tif')
    epsg = get_epsg(dem)
    location_tmp = generate_temp_location(epsg, comune, provincia)
    try:
        logger.info(f"Refresh completo dell’offerta energetica per {provincia}/{comune} (location: {location_tmp})")
        solar_radiation_pipeline(provincia, comune, location_tmp)
        gdf = safe_building_irradiance(provincia, comune, idx_panel, pipeline_func=lambda p, c: solar_radiation_pipeline(p, c, location_tmp),use_vincoli=use_vincoli)
        remove_grass_location(GRASS_GISDB, location_tmp)
        return gdf
    except Exception as e:
        remove_grass_location(GRASS_GISDB, location_tmp)
        raise

if __name__ == '__main__':
    # Esempio di esecuzione
    # Abilita logging solo se eseguito standalone
    configure_logging_if_main(__name__)
    prov, com, idx = 'Salerno', 'Giffoni valle piana', 0
    _ = calcolo_offerta_energetica(prov, com, idx)
    logger.info('Processo completato')