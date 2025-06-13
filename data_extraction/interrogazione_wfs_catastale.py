import geopandas as gpd
import requests
import xml.etree.ElementTree as ET
import logging
import os

from calcola_area_poligoni import calcola_area

# ========================
# CONFIGURAZIONE LOGGING
# ========================
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# ========================
# COSTANTI GLOBALI
# ========================
BASE_DIR = os.path.abspath("..")
INPUT_SHP = os.path.join(BASE_DIR, "FABBRICATI_geometry_only", "FABBRICATI_geom_only.shp")
OUTPUT_DIR = os.path.join(BASE_DIR, "Data_Collection", "shapefiles_merged", "dati_catasto")
OUTPUT_SHP = os.path.join(OUTPUT_DIR, "dati_catasto.shp")

BASE_URL_WFS = 'https://wfs.cartografia.agenziaentrate.gov.it/inspire/wfs/owfs01.php'
SRS_NAME = 'urn:ogc:def:crs:EPSG::6706'
LANGUAGE = 'ita'
TYPENAME = 'CP:CadastralParcel'

# ========================
# FUNZIONE: Centroidi
# ========================
def genera_centroidi_da_gdf(gdf_poligoni: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    logger.info("Calcolo dei centroidi dai poligoni.")

    if 'id' not in gdf_poligoni.columns:
        raise ValueError("Il GeoDataFrame deve contenere una colonna 'id'")

    centroids = gdf_poligoni.geometry.centroid
    centroids_gdf = gpd.GeoDataFrame(gdf_poligoni[['id']], geometry=centroids, crs=gdf_poligoni.crs)
    centroids_gdf = centroids_gdf.to_crs(epsg=6706)
    return centroids_gdf

# ========================
# FUNZIONE: Query WFS
# ========================
def query_catasto_point(x, y):
    params = {
        'SERVICE': 'WFS',
        'VERSION': '2.0.0',
        'REQUEST': 'GetFeature',
        'TYPENAMES': TYPENAME,
        'SRSNAME': SRS_NAME,
        'BBOX': f'{y},{x},{y},{x}',
        'LANGUAGE': LANGUAGE
    }

    logger.info(f"Inviando richiesta WFS per il punto: ({x}, {y})")

    try:
        response = requests.get(BASE_URL_WFS, params=params)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Errore durante la richiesta WFS per ({x}, {y}): {e}")
        return None

    try:
        root = ET.fromstring(response.content)
        namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'gml': 'http://www.opengis.net/gml/3.2',
            'CP': 'http://mapserver.gis.umn.edu/mapserver'
        }

        features = root.findall('.//CP:CadastralParcel', namespaces)
        if features:
            feature = features[0]
            result = {
                'INSPIREID_LOCALID': feature.find('.//CP:INSPIREID_LOCALID', namespaces).text,
                'LABEL': feature.find('.//CP:LABEL', namespaces).text,
                'ADMINISTRATIVEUNIT': feature.find('.//CP:ADMINISTRATIVEUNIT', namespaces).text,
                'NATIONALCADASTRALREFERENCE': feature.find('.//CP:NATIONALCADASTRALREFERENCE', namespaces).text
            }
            logger.info(f"Dati catastali trovati per ({x}, {y}): {result}")
            return result
        else:
            logger.warning(f"Nessuna particella trovata per le coordinate ({x}, {y})")
            return None
    except ET.ParseError as e:
        logger.error(f"Errore nel parsing XML per ({x}, {y}): {e}")
        return None

# ========================
# FUNZIONE: Salva SHP
# ========================
def salva_shapefile_catastale(gdf: gpd.GeoDataFrame, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    logger.info(f"Salvataggio shapefile con dati catastali: {output_path}")
    gdf.to_file(output_path, driver='ESRI Shapefile')

# ========================
# FUNZIONE: Processa Shapefile
# ========================
def process_shapefile(input_shp: str) -> gpd.GeoDataFrame:
    logger.info("Lettura shapefile originale.")
    gdf_poligoni = gpd.read_file(input_shp)
    crs_originale = gdf_poligoni.crs  # Salva CRS originale

    if 'id' not in gdf_poligoni.columns:
        gdf_poligoni = gdf_poligoni.reset_index().rename(columns={'index': 'id'})

    gdf_centroidi = genera_centroidi_da_gdf(gdf_poligoni)

    for col in ['INSPIREID_LOCALID', 'LABEL', 'ADMINISTRATIVEUNIT', 'NATIONALCADASTRALREFERENCE']:
        gdf_centroidi[col] = None

    logger.info(f"Elaborazione di {len(gdf_centroidi)} centroidi...")
    for index, row in gdf_centroidi.iterrows():
        x, y = row.geometry.x, row.geometry.y
        result = query_catasto_point(x, y)
        if result:
            for key, value in result.items():
                gdf_centroidi.at[index, key] = value

    logger.info("Associazione dati catastali ai poligoni originali tramite 'id'.")
    gdf_risultato = gdf_poligoni.merge(gdf_centroidi.drop(columns='geometry'), on='id', how='left')
    gdf_risultato = gdf_risultato.drop(columns='id')

    # Reimposta CRS originale (se necessario)
    if gdf_risultato.crs != crs_originale:
        logger.info(f"Reimpostazione CRS originale: {crs_originale}")
        gdf_risultato = gdf_risultato.set_crs(crs_originale, allow_override=True)

    # Calcola l'area dei poligoni
    gdf_risultato = calcola_area(gdf_risultato)

    return gdf_risultato

# ========================
# MAIN
# ========================
def main():
    logger.info("Inizio elaborazione shapefile con dati catastali.")
    gdf_finale = process_shapefile(INPUT_SHP)
    salva_shapefile_catastale(gdf_finale, OUTPUT_SHP)
    logger.info("Elaborazione completata.")

if __name__ == "__main__":
    main()
