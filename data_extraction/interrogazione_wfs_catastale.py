import geopandas as gpd
import requests
import xml.etree.ElementTree as ET
import logging
from shapely.geometry import Point

# Definizione delle costanti
BASE_URL_WFS = 'https://wfs.cartografia.agenziaentrate.gov.it/inspire/wfs/owfs01.php'
SRS_NAME = 'urn:ogc:def:crs:EPSG::6706'
LANGUAGE = 'ita'
TYPENAME = 'CP:CadastralParcel'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# Configurazione del logger
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[logging.StreamHandler()])

# Funzione di query per il WFS
def query_catasto_point(x, y):
    """
    Interroga il WFS del Catasto per un punto specificato e restituisce l'INPSIRE ID e altre informazioni
    Args:
        x (float): Longitudine del punto (EPSG:6706)
        y (float): Latitudine del punto (EPSG:6706)
    """
    # Parametri per la query WFS
    params = {
        'SERVICE': 'WFS',
        'VERSION': '2.0.0',
        'REQUEST': 'GetFeature',
        'TYPENAMES': TYPENAME,
        'SRSNAME': SRS_NAME,  # EPSG:6706 per le coordinate
        'BBOX': f'{y},{x},{y},{x}',  # Bounding box per il punto
        'LANGUAGE': LANGUAGE
    }

    logging.info(f"Inviando richiesta WFS per il punto: ({x}, {y})")

    # Invia la richiesta al WFS
    try:
        response = requests.get(BASE_URL_WFS, params=params)
        response.raise_for_status()  # Verifica che la richiesta sia riuscita
        logging.info(f"Risposta ricevuta per il punto: ({x}, {y})")
    except requests.RequestException as e:
        logging.error(f"Errore durante la richiesta WFS per ({x}, {y}): {e}")
        return None

    # Parsing della risposta XML
    try:
        root = ET.fromstring(response.content)
        namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'gml': 'http://www.opengis.net/gml/3.2',
            'CP': 'http://mapserver.gis.umn.edu/mapserver'
        }

        # Trova le particelle catastali nel documento XML
        features = root.findall('.//CP:CadastralParcel', namespaces)

        if features:
            # Estrai le informazioni dalla prima particella
            feature = features[0]
            inspireid = feature.find('.//CP:INSPIREID_LOCALID', namespaces).text
            label = feature.find('.//CP:LABEL', namespaces).text
            admin_unit = feature.find('.//CP:ADMINISTRATIVEUNIT', namespaces).text
            national_ref = feature.find('.//CP:NATIONALCADASTRALREFERENCE', namespaces).text

            # Prepara il risultato
            result = {
                'INSPIREID_LOCALID': inspireid,
                'LABEL': label,
                'ADMINISTRATIVEUNIT': admin_unit,
                'NATIONALCADASTRALREFERENCE': national_ref
            }

            logging.info(f"Dati trovati per il punto ({x}, {y}): {result}")
            return result
        else:
            logging.warning(f"Nessuna particella trovata per le coordinate ({x}, {y})")
            return None
    except ET.ParseError as e:
        logging.error(f"Errore nel parsing della risposta XML per il punto ({x}, {y}): {e}")
        return None


def process_shapefile(input_shp, output_shp):
    """
    Elabora il file shapefile dei centroidi e aggiunge i dati WFS a ciascun punto.
    Args:
        input_shp (str): Il percorso del file shapefile di input (contenente i centroidi).
        output_shp (str): Il percorso del file shapefile di output.
    """
    # Carica il file shapefile
    logging.info(f"Caricando il file shapefile: {input_shp}")
    gdf = gpd.read_file(input_shp)

    # Aggiungi colonne per i dati del catasto
    gdf['INSPIREID_LOCALID'] = None
    gdf['LABEL'] = None
    gdf['ADMINISTRATIVEUNIT'] = None
    gdf['NATIONALCADASTRALREFERENCE'] = None

    # Cicla su ogni punto (centroide) nel file shapefile
    logging.info(f"Elaborando {len(gdf)} centroidi nel file shapefile.")
    for index, row in gdf.iterrows():
        point = row.geometry.centroid  # Ottieni il centroide
        x, y = point.x, point.y  # Estrai le coordinate (x, y)

        # Esegui la query per il punto
        result = query_catasto_point(x, y)

        if result:
            # Aggiungi i dati al dataframe
            gdf.at[index, 'INSPIREID_LOCALID'] = result['INSPIREID_LOCALID']
            gdf.at[index, 'LABEL'] = result['LABEL']
            gdf.at[index, 'ADMINISTRATIVEUNIT'] = result['ADMINISTRATIVEUNIT']
            gdf.at[index, 'NATIONALCADASTRALREFERENCE'] = result['NATIONALCADASTRALREFERENCE']

    # Salva il nuovo shapefile con i dati aggiunti
    logging.info(f"Salvando il nuovo shapefile con i dati catastali: {output_shp}")
    gdf.to_file(output_shp)


# Funzione di esempio di utilizzo
def main():
    input_shp = "../Data_Collection/shapefiles_merged/centroidi_fabbricati/centroidi_fabbricati.shp"  # File shapefile di input
    output_shp = "../Data_Collection/shapefiles_merged/dati_catasto/dati_catasto.shp"  # File shapefile di output

    logging.info("Inizio elaborazione del file shapefile.")
    process_shapefile(input_shp, output_shp)
    logging.info("Elaborazione completata.")


if __name__ == "__main__":
    main()
