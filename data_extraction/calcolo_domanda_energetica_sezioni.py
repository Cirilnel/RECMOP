import os
import pandas as pd
import geopandas as gpd
import logging
import numpy as np

from join_data_normattiva_varcens_basiterr import get_join_data
from data_extraction_siape.siape_zc_range import get_dati_siape
from calcola_area_poligoni import calcola_area
from interrogazione_wfs_catastale import get_dati_catasto
from estrazione_dati_basi_territoriali import estrai_geometrie_sezioni

# === CONFIGURAZIONE LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# FUNZIONI AUSILIARIE
# =============================================================================

def get_regione_from_provincia(provincia: str) -> str:
    """
    Mappa il nome di una provincia alla sua regione italiana.
    """
    provincia = provincia.strip().upper()

    mappa_provincia_regione = {
        # Piemonte
        "TORINO": "PIEMONTE", "VERCELLI": "PIEMONTE", "BIELLA": "PIEMONTE", "CUNEO": "PIEMONTE",
        "ASTI": "PIEMONTE", "ALESSANDRIA": "PIEMONTE", "NOVARA": "PIEMONTE",
        # Valle d'Aosta
        "AOSTA": "VALLE D'AOSTA",
        # Lombardia
        "VARESE": "LOMBARDIA", "COMO": "LOMBARDIA", "SONDRIO": "LOMBARDIA", "MILANO": "LOMBARDIA",
        "BERGAMO": "LOMBARDIA", "BRESCIA": "LOMBARDIA", "PAVIA": "LOMBARDIA", "CREMONA": "LOMBARDIA",
        "MANTOVA": "LOMBARDIA",
        # Trentino-Alto Adige
        "BOLZANO": "TRENTINO-ALTO ADIGE", "TRENTO": "TRENTINO-ALTO ADIGE",
        # Veneto
        "VERONA": "VENETO", "VICENZA": "VENETO", "BELLUNO": "VENETO", "TREVISO": "VENETO",
        "VENEZIA": "VENETO", "PADOVA": "VENETO", "ROVIGO": "VENETO",
        # Friuli-Venezia Giulia
        "UDINE": "FRIULI-VENEZIA GIULIA", "GORIZIA": "FRIULI-VENEZIA GIULIA",
        "TRIESTE": "FRIULI-VENEZIA GIULIA", "PORDENONE": "FRIULI-VENEZIA GIULIA",
        # Liguria
        "IMPERIA": "LIGURIA", "SAVONA": "LIGURIA", "GENOVA": "LIGURIA", "LA SPEZIA": "LIGURIA",
        # Emilia-Romagna
        "PIACENZA": "EMILIA-ROMAGNA", "PARMA": "EMILIA-ROMAGNA", "REGGIO EMILIA": "EMILIA-ROMAGNA",
        "MODENA": "EMILIA-ROMAGNA", "BOLOGNA": "EMILIA-ROMAGNA", "FERRARA": "EMILIA-ROMAGNA",
        "RAVENNA": "EMILIA-ROMAGNA", "FORLÌ-CESENA": "EMILIA-ROMAGNA",
        # Toscana
        "MASSA-CARRARA": "TOSCANA", "LUCCA": "TOSCANA", "PISTOIA": "TOSCANA", "FIRENZE": "TOSCANA",
        "LIVORNO": "TOSCANA", "PISA": "TOSCANA", "AREZZO": "TOSCANA", "SIENA": "TOSCANA", "GROSSETO": "TOSCANA",
        # Umbria
        "PERUGIA": "UMBRIA", "TERNI": "UMBRIA",
        # Marche
        "PESARO E URBINO": "MARCHE", "ANCONA": "MARCHE", "MACERATA": "MARCHE", "ASCOLI PICENO": "MARCHE",
        # Lazio
        "VITERBO": "LAZIO", "RIETI": "LAZIO", "ROMA": "LAZIO", "LATINA": "LAZIO", "FROSINONE": "LAZIO",
        # Abruzzo
        "L'AQUILA": "ABRUZZO", "TERAMO": "ABRUZZO", "PESCARA": "ABRUZZO", "CHIETI": "ABRUZZO",
        # Molise
        "CAMPOBASSO": "MOLISE", "ISERNIA": "MOLISE",
        # Campania
        "CASERTA": "CAMPANIA", "BENEVENTO": "CAMPANIA", "NAPOLI": "CAMPANIA",
        "AVELLINO": "CAMPANIA", "SALERNO": "CAMPANIA",
        # Puglia
        "FOGGIA": "PUGLIA", "BARI": "PUGLIA", "TARANTO": "PUGLIA",
        "BRINDISI": "PUGLIA", "LECCE": "PUGLIA",
        # Basilicata
        "POTENZA": "BASILICATA", "MATERA": "BASILICATA",
        # Calabria
        "COSENZA": "CALABRIA", "CATANZARO": "CALABRIA", "REGGIO CALABRIA": "CALABRIA",
        # Sicilia
        "TRAPANI": "SICILIA", "PALERMO": "SICILIA", "MESSINA": "SICILIA", "AGRIGENTO": "SICILIA",
        "CALTANISSETTA": "SICILIA", "ENNA": "SICILIA", "CATANIA": "SICILIA", "RAGUSA": "SICILIA", "SIRACUSA": "SICILIA",
        # Sardegna
        "SASSARI": "SARDEGNA", "NUORO": "SARDEGNA", "CAGLIARI": "SARDEGNA", "ORISTANO": "SARDEGNA"
    }

    if provincia not in mappa_provincia_regione:
        raise ValueError(f"Provincia '{provincia}' non riconosciuta o non presente in mappa.")

    return mappa_provincia_regione[provincia]

def safe_name(nome: str) -> str:
    """Restituisce il nome in minuscolo e con spazi sostituiti da underscore."""
    return nome.strip().lower().replace(' ', '_')

# =============================================================================
# FUNZIONI DI CALCOLO
# =============================================================================

def calcola_coefficiente_domanda(
    df_join: pd.DataFrame,
    df_siape: pd.DataFrame,
    gdf_sezioni: gpd.GeoDataFrame,
    gdf_fabbricati: gpd.GeoDataFrame,
    comune: str,
    provincia: str
) -> gpd.GeoDataFrame:
    logger.info(f"Calcolo coefficiente domanda per ogni sezione di {comune} ({provincia})...")

    df_comune = df_join[
        (df_join['COMUNE'].str.upper() == comune.upper()) &
        (df_join['PROVINCIA'].str.upper() == provincia.upper())
    ].copy()

    if df_comune.empty:
        raise ValueError(f"Nessun dato trovato per il comune {comune} nella provincia {provincia}.")

    for col in ['E8', 'E9', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'E16']:
        df_comune[col] = df_comune[col].fillna(0).astype(int)

    df_comune['b1'] = df_comune['E8'] + df_comune['E9']
    df_comune['b2'] = df_comune['E10'] + df_comune['E11']
    df_comune['b3'] = df_comune['E12'] + df_comune['E13']
    df_comune['b4'] = df_comune['E14'] + df_comune['E15']
    df_comune['b5'] = df_comune['E16']
    df_comune['totale'] = (
        df_comune['b1'] + df_comune['b2'] + df_comune['b3'] + df_comune['b4'] + df_comune['b5']
    )

    sezioni_ids = df_comune['SEZ2011'].unique()
    gdf_filtered = gdf_sezioni[gdf_sezioni['SEZ2011'].isin(sezioni_ids)].copy()

    gdf_result = gdf_filtered.merge(
        df_comune[['SEZ2011', 'b1', 'b2', 'b3', 'b4', 'b5', 'totale']],
        on='SEZ2011',
        how='left'
    )

    zc = df_comune['ZONA_CLIMATICA'].dropna().unique()
    if len(zc) != 1:
        raise ValueError(f"Zona climatica ambigua o mancante per {comune}. Valori trovati: {zc}")
    zc = zc[0]

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

    # Calcolo del coefficiente per ogni sezione (media ponderata)
    gdf_result['coeff_sez'] = (
        gdf_result['b1'] * epgl_nren_1 +
        gdf_result['b2'] * epgl_nren_2 +
        gdf_result['b3'] * epgl_nren_3 +
        gdf_result['b4'] * epgl_nren_4 +
        gdf_result['b5'] * epgl_nren_5
    ) / gdf_result['totale']

    # Calcolo del centroide dei fabbricati
    gdf_fabbricati = gdf_fabbricati.copy()
    gdf_fabbricati['geometry_centroid'] = gdf_fabbricati.geometry.centroid

    if gdf_result.crs != gdf_fabbricati.crs:
        gdf_result = gdf_result.to_crs(gdf_fabbricati.crs)

    # Assegna coeff_sez a ogni edificio in base alla sezione in cui ricade il centroide
    fabb_centroidi = gpd.GeoDataFrame(gdf_fabbricati, geometry='geometry_centroid', crs=gdf_fabbricati.crs)
    joined = gpd.sjoin(fabb_centroidi, gdf_result[['SEZ2011', 'coeff_sez', 'geometry']], how='left', predicate='within')

    # Riporta il coefficiente nel gdf originale
    gdf_fabbricati['ceoff_nren'] = joined['coeff_sez'].values
    gdf_fabbricati['ceoff_nren'] = gdf_fabbricati['ceoff_nren'].astype(np.float32)
    gdf_fabbricati['ceoff_nren'] = gdf_fabbricati['ceoff_nren'].fillna(0.0)
    gdf_fabbricati = gdf_fabbricati.drop(columns=['geometry_centroid'])

    return gdf_fabbricati


def calcola_domanda_energetica(comune: str, provincia: str) -> gpd.GeoDataFrame:
    logger.info("Inizio calcolo domanda energetica...")

    comune = comune.strip().upper()
    provincia = provincia.strip().upper()
    regione = get_regione_from_provincia(provincia)

    # Carico dati unificati
    df_join = get_join_data(regione.lower())
    logger.info("Dati unificati caricati.")

    # ottengo geometrie delle sezioni
    gdf_sezioni = estrai_geometrie_sezioni(regione.lower())

    # Carico dati SIAPE
    df_siape = get_dati_siape()
    logger.info("Dati SIAPE caricati.")

    # Costruisco il percorso dinamico per lo shapefile dei fabbricati
    prov_safe = safe_name(provincia)
    comm_safe = safe_name(comune)
    shp_dir = os.path.join('..', 'FABBRICATI', f'fabbricati_{prov_safe}_{comm_safe}')
    if not os.path.isdir(shp_dir):
        raise FileNotFoundError(f"Directory shapefile non trovata: {shp_dir}")

    shp_files = [f for f in os.listdir(shp_dir) if f.lower().endswith('.shp')]
    if len(shp_files) != 1:
        raise ValueError(f"Atteso un unico file .shp in {shp_dir}, trovati: {shp_files}")
    shp_path = os.path.join(shp_dir, shp_files[0])

    try:
        gdf_fabbricati = gpd.read_file(shp_path)
        logger.info(f"Shapefile fabbricati caricato da {shp_path}.")
    except Exception as e:
        logger.warning(f"Errore caricamento shapefile fabbricati: {e}")
        gdf_fabbricati = None

    # Calcolo area edifici
    gdf_fabbricati = calcola_area(gdf_fabbricati, nome_colonna='area_mq')

    # Calcolo coefficiente domanda
    gdf_fabbricati = calcola_coefficiente_domanda(df_join, df_siape, gdf_sezioni, gdf_fabbricati, comune, provincia)
    logger.info(f"Coefficiente domanda per ogni sezione di {comune} ({provincia})")

    # Configuro directory e path output dinamicamente (struttura annidata)
    subdir = f"{prov_safe}_{comm_safe}"
    dirname = f"domanda_energetica_sezioni_{prov_safe}_{comm_safe}"
    out_dir = os.path.join('..', 'Data_Collection', 'shapefiles', subdir, dirname)
    os.makedirs(out_dir, exist_ok=True)
    out_shp = os.path.join(out_dir, f'{dirname}.shp')

    if gdf_fabbricati is not None:
        # Aggiungo colonna domanda energetica
        gdf_fabbricati['domanda_en'] = gdf_fabbricati['area_mq'] * gdf_fabbricati['ceoff_nren']
        logger.info("Colonna domanda energetica aggiunta.")

        # === 1. Salva le colonne personalizzate prima della chiamata ===
        extra_cols = ['ceoff_nren', 'domanda_en']
        gdf_custom = gdf_fabbricati[['FID'] + extra_cols].copy()

        # === 2. Chiamata alla funzione catastale (non modificata) ===
        gdf_fabbricati = get_dati_catasto(gdf_fabbricati, provincia, comune)

        # === 3. Reintegra le colonne ===
        gdf_fabbricati = gdf_fabbricati.merge(gdf_custom, on='FID', how='left', suffixes=('', '_dup'))
        # Elimina eventuale duplicato
        if 'domanda_en_dup' in gdf_fabbricati.columns:
            gdf_fabbricati = gdf_fabbricati.drop(columns=['domanda_en_dup'])

        # Salvo shapefile di output
        gdf_fabbricati.to_file(out_shp, driver='ESRI Shapefile')
        logger.info(f"Shapefile con domanda energetica salvato in {out_shp}")

    return gdf_fabbricati


if __name__ == '__main__':
    # Esempio di utilizzo
    gdf = calcola_domanda_energetica('PADULA', 'SALERNO')
