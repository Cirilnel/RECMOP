import os
import pandas as pd
import geopandas as gpd
import logging

from join_data_normattiva_varcens_basiterr import get_join_data
from data_extraction_siape.siape_zc_range import get_dati_siape
from calcola_area_poligoni import calcola_area
from interrogazione_wfs_catastale import get_dati_catasto

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

# =============================================================================
# FUNZIONI DI CALCOLO
# =============================================================================

def calcola_coefficiente_domanda(df_join: pd.DataFrame, df_siape: pd.DataFrame, comune: str, provincia: str) -> float:
    logger.info(f"Calcolo coefficiente domanda per {comune} ({provincia})...")

    df_comune = df_join[
        (df_join['COMUNE'].str.upper() == comune.upper()) &
        (df_join['PROVINCIA'].str.upper() == provincia.upper())
    ]

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
    if totale_edifici == 0:
        raise ValueError(f"Totale edifici nullo per il comune {comune}.")

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

    coefficiente_domanda = (
        (b1 * epgl_nren_1 + b2 * epgl_nren_2 + b3 * epgl_nren_3 +
         b4 * epgl_nren_4 + b5 * epgl_nren_5) / totale_edifici
    )

    return round(coefficiente_domanda, 2)


def calcola_domanda_energetica(comune: str, provincia: str) -> gpd.GeoDataFrame:
    logger.info("Inizio calcolo domanda energetica...")

    comune = comune.strip().upper()
    provincia = provincia.strip().upper()
    regione = get_regione_from_provincia(provincia)

    # Carico dati unificati
    df_join = get_join_data(regione.lower())
    logger.info("Dati unificati caricati.")

    # Carico dati SIAPE
    df_siape = get_dati_siape()
    logger.info("Dati SIAPE caricati.")

    # Costruisco il percorso dinamico per lo shapefile dei fabbricati
    prov_safe = provincia.lower().replace(' ', '_')
    comm_safe = comune.lower().replace(' ', '_')
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
    coeff_dom = calcola_coefficiente_domanda(df_join, df_siape, comune, provincia)
    logger.info(f"Coefficiente domanda per {comune} ({provincia}): {coeff_dom} kWh/mq/anno")

    # Configuro directory e path output dinamicamente
    out_dir = os.path.join(
        '..', 'Data_Collection', 'shapefiles',
        f'domanda_energetica_{prov_safe}_{comm_safe}'
    )
    os.makedirs(out_dir, exist_ok=True)
    out_shp = os.path.join(
        out_dir,
        f'domanda_energetica_{prov_safe}_{comm_safe}.shp'
    )

    if gdf_fabbricati is not None:
        # Aggiungo colonna domanda energetica
        gdf_fabbricati['domanda_en'] = gdf_fabbricati['area_mq'] * coeff_dom
        logger.info("Colonna domanda energetica aggiunta.")

        # Aggiungo informazioni catastali
        gdf_fabbricati = get_dati_catasto(gdf_fabbricati, provincia, comune)

        # Salvo shapefile di output
        gdf_fabbricati.to_file(out_shp, driver='ESRI Shapefile')
        logger.info(f"Shapefile con domanda energetica salvato in {out_shp}")

    return gdf_fabbricati

if __name__ == '__main__':
    # Esempio di utilizzo
    gdf = calcola_domanda_energetica('PADULA', 'SALERNO')
