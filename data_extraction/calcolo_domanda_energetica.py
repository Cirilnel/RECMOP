# calcolo_domanda_energetica.py

import os
import pandas as pd
import geopandas as gpd
import logging

from join_data_normattiva_varcens_basiterr import join_data, salva_join_data
from siape_zc_range import estrai_dati_siape
from interrogazione_wfs_catastale import process_shapefile

# === CONFIGURAZIONE LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === Percorsi input ===
INPUT_PATH_BASI_TERRITORIALI = os.path.join('..', 'Istat', 'Regioni', 'Campania', 'R15_11_WGS84.dbf')
INPUT_PATH_VARIABILI_CENSUARIE = os.path.join("..", "Istat", "Variabili_Censuarie", "Sezioni_di_Censimento", "Campania.csv")
INPUT_SHP_FABBRICATI = os.path.join('..', 'FABBRICATI_geometry_only', 'FABBRICATI_geom_only.shp')
OUTPUT_PATH = os.path.join('..', 'Data_Collection', 'shapefiles_merged', 'domanda_energetica', 'domanda_energetica.shp')

def calcola_coefficiente_domanda(df_join: pd.DataFrame, df_siape: pd.DataFrame, comune: str, provincia: str) -> float:
    """
    Calcola il coefficiente medio pesato della domanda energetica EPgl_nren
    per un comune, tenendo conto del numero di edifici per classe d'età
    e del coefficiente corrispondente.
    """
    logger.info(f"Calcolo coefficiente domanda per {comune} ({provincia})...")

    df_comune = df_join[
        (df_join['COMUNE'].str.upper() == comune.upper()) &
        (df_join['PROVINCIA'].str.upper() == provincia.upper())
    ]

    logger.info(f"Numero di sezioni trovate per {comune}: {len(df_comune)}")

    if df_comune.empty:
        raise ValueError(f"Nessun dato trovato per il comune {comune} nella provincia {provincia}.")

    def somma_colonne(*colonne):
        return sum(
            df_comune[col].fillna(0).astype(int).sum() for col in colonne
        )

    b1 = somma_colonne('E8', 'E9')
    b2 = somma_colonne('E10', 'E11')
    b3 = somma_colonne('E12', 'E13')
    b4 = somma_colonne('E14', 'E15')
    b5 = somma_colonne('E16')

    totale_edifici = b1 + b2 + b3 + b4 + b5
    logger.info(f"Totale edifici: {totale_edifici} (b1={b1}, b2={b2}, b3={b3}, b4={b4}, b5={b5})")

    if totale_edifici == 0:
        raise ValueError(f"Totale edifici nullo per il comune {comune}.")

    zc = df_comune['ZONA_CLIMATICA'].dropna().unique()
    if len(zc) != 1:
        raise ValueError(f"Zona climatica ambigua o mancante per {comune}. Valori trovati: {zc}")
    zc = zc[0]
    logger.info(f"Zona climatica rilevata: {zc}")

    df_zc = df_siape[df_siape['zona_climatica'] == zc]
    if df_zc.empty:
        raise ValueError(f"Nessun dato SIAPE per la zona climatica {zc}")

    def get_coeff(df, periodo):
        val = df[df['periodo'] == periodo]['EPgl_nren']
        if val.empty or pd.isna(val.iloc[0]):
            raise ValueError(f"Valore EPgl_nren mancante per periodo {periodo} in zona {zc}")
        return float(val.iloc[0])

    epgl_nren_1 = get_coeff(df_zc, 'kE8E9')
    epgl_nren_2 = get_coeff(df_zc, 'kE10E11')
    epgl_nren_3 = get_coeff(df_zc, 'kE12E13')
    epgl_nren_4 = get_coeff(df_zc, 'kE14E15')
    epgl_nren_5 = get_coeff(df_zc, 'kE16')

    logger.info(f"Coefficiente EPgl_nren per periodo: "
                f"1={epgl_nren_1}, 2={epgl_nren_2}, 3={epgl_nren_3}, "
                f"4={epgl_nren_4}, 5={epgl_nren_5}")

    coefficiente_domanda = (
        (b1 * epgl_nren_1 + b2 * epgl_nren_2 + b3 * epgl_nren_3 +
         b4 * epgl_nren_4 + b5 * epgl_nren_5) / totale_edifici
    )

    logger.info(f"Coefficiente calcolato: {coefficiente_domanda:.2f}")
    return round(coefficiente_domanda, 2)


def calcola_domanda_energetica() -> gpd.GeoDataFrame:
    """
    Funzione principale: carica i dati, calcola il coefficiente di domanda
    energetica per un comune e lo stampa.
    """
    logger.info("Inizio calcolo domanda energetica...")

    df_join = join_data(INPUT_PATH_BASI_TERRITORIALI, INPUT_PATH_VARIABILI_CENSUARIE)
    logger.info("Dati unificati caricati correttamente.")

    df_siape = estrai_dati_siape()
    logger.info("Dati SIAPE caricati correttamente.")

    # Dovrei eseguire process_shapefile, ma per velocizzare uso il file shp già processato
    # df_fabbricati = process_shapefile(INPUT_SHP_FABBRICATI)
    try:
        gdf_fabbricati = gpd.read_file(
            os.path.join('..', 'Data_Collection', 'shapefiles_merged', 'dati_catasto', 'dati_catasto.shp')
        )
        logger.info("Shapefile fabbricati caricato con successo.")
    except Exception as e:
        logger.warning(f"Errore nel caricamento shapefile fabbricati: {e}")
        gdf_fabbricati = None

    coefficiente_domanda = calcola_coefficiente_domanda(df_join, df_siape, 'PADULA', 'SALERNO')
    logger.info(f"Il coefficiente di domanda energetica per il comune di Padula (SA) è: {coefficiente_domanda}")

    #aggiungi a gdf_fabbricati una colonna 'DOMANDA_ENERGETICA' data da area_mq moltiplicata per coefficiente_domanda
    if gdf_fabbricati is not None:
        gdf_fabbricati['DOMANDA_ENERGETICA'] = gdf_fabbricati['area_mq'] * coefficiente_domanda
        logger.info("Colonna DOMANDA_ENERGETICA aggiunta allo shapefile fabbricati.")

        # Salva il risultato in un nuovo file shapefile
        gdf_fabbricati.to_file(OUTPUT_PATH, driver='ESRI Shapefile')
        logger.info(f"Shapefile con domanda energetica salvato in {OUTPUT_PATH}")

    return gdf_fabbricati

if __name__ == '__main__':
    calcola_domanda_energetica()
