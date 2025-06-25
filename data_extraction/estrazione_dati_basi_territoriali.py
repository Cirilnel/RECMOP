import os
import logging
import pandas as pd
from dbfread import DBF
import geopandas as gpd

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Costanti
BASE_INPUT_DIR = os.path.join("..", "Istat", "Regioni")
OUTPUT_DIR = os.path.join("..", "Data_Collection", "csv_tables-fase1")
CAMPI_ESTRATTI = ['COD_REG', 'COD_ISTAT', 'PRO_COM', 'SEZ2011', 'SEZ', 'COD_LOC', 'TIPO_LOC']


def trova_file_in_regione(regione: str, extension: str) -> str:
    """
    Trova un file con l'estensione specificata all'interno della cartella della regione.
    """
    cartella = os.path.join(BASE_INPUT_DIR, regione)
    files = [f for f in os.listdir(cartella) if f.lower().endswith(extension)]
    if not files:
        raise FileNotFoundError(f"Nessun file {extension} trovato nella cartella: {cartella}")
    if len(files) > 1:
        logger.warning(f"Trovati più file {extension} in {cartella}, verrà usato il primo: {files[0]}")
    return os.path.join(cartella, files[0])


def trova_dbf_in_regione(regione: str) -> str:
    return trova_file_in_regione(regione, ".dbf")


def estrai_dati_basi_territoriali(percorso_file: str) -> pd.DataFrame:
    table = DBF(percorso_file, load=True, ignorecase=True, recfactory=dict)
    records = [{campo: rec.get(campo) for campo in CAMPI_ESTRATTI} for rec in table]
    df = pd.DataFrame(records)
    for col in df.columns:
        try:
            df[col] = df[col].astype('int64')
        except (ValueError, TypeError):
            logger.warning(f"Colonna non convertita a intero: {col}")
    return df


def salva_dati_basi_territoriali(df: pd.DataFrame, cartella_output: str, nome_file: str,
                                 sep: str = ';', encoding: str = 'utf-8-sig') -> None:
    os.makedirs(cartella_output, exist_ok=True)
    output_path = os.path.join(cartella_output, nome_file)
    df.to_csv(output_path, index=False, sep=sep, encoding=encoding)
    logger.info(f"Dati salvati in: {output_path}")


def run_estrazione_basi_territoriali(regione: str) -> pd.DataFrame:
    input_path = trova_dbf_in_regione(regione)
    nome_file = f"basi_territoriali_{regione.lower()}.csv"
    df = estrai_dati_basi_territoriali(input_path)
    salva_dati_basi_territoriali(df, OUTPUT_DIR, nome_file)
    return df


def get_dati_basi_territoriali(regione: str) -> pd.DataFrame:
    nome_file = f"basi_territoriali_{regione.lower()}.csv"
    path_csv = os.path.join(OUTPUT_DIR, nome_file)
    if not os.path.exists(path_csv):
        logger.warning(f"File CSV non trovato per {regione}, avvio estrazione.")
        return run_estrazione_basi_territoriali(regione)
    df = pd.read_csv(path_csv, sep=';', encoding='utf-8-sig')
    logger.info(f"Dati caricati da: {path_csv}")
    return df


def trova_shp_in_regione(regione: str) -> str:
    return trova_file_in_regione(regione, ".shp")


def estrai_geometrie_sezioni(regione: str) -> gpd.GeoDataFrame:
    """
    Restituisce un GeoDataFrame con le geometrie delle sezioni censuarie
    e la colonna SEZ2011 per tutti i comuni nella regione.
    """
    shp_path = trova_shp_in_regione(regione)
    gdf = gpd.read_file(shp_path)

    # Identifica la colonna SEZ2011 e la geometria
    col_map = {c.upper(): c for c in gdf.columns}
    col_sez = col_map.get('SEZ2011')
    if not col_sez:
        raise KeyError("Colonna SEZ2011 mancante nel shapefile")

    geom_col = gdf.geometry.name

    # Seleziona solo geometria e SEZ2011 per tutta la regione
    gdf_sezioni = gdf[[geom_col, col_sez]].copy()

    # Rinomina la colonna geometria se necessario
    if geom_col != 'geometry':
        gdf_sezioni = gdf_sezioni.rename(columns={geom_col: 'geometry'}).set_geometry('geometry')

    logger.info(f"Estratte {len(gdf_sezioni)} sezioni in {regione}")
    return gdf_sezioni


if __name__ == '__main__':
    # Esempio di utilizzo
    df = get_dati_basi_territoriali("Campania")
