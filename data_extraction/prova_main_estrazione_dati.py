import os
import logging

import pandas as pd
import geopandas as gpd
from rapidfuzz import process, fuzz

# =============================================================================
# COSTANTI
# =============================================================================

THRESHOLD = 50

PROV_DICT = {
    "BI":"Biella","TO":"Torino","VC":"Vercelli","NO":"Novara","CN":"Cuneo","AT":"Asti","AL":"Alessandria",
    "AO":"Aosta","VA":"Varese","CO":"Como","SO":"Sondrio","MI":"Milano","BG":"Bergamo","BS":"Brescia",
    "PV":"Pavia","CR":"Cremona","MN":"Mantova","BZ":"Bolzano","TN":"Trento","VR":"Verona","VI":"Vicenza",
    "BL":"Belluno","TV":"Treviso","VE":"Venezia","PD":"Padova","RO":"Rovigo","UD":"Udine","GO":"Gorizia",
    "TS":"Trieste","PN":"Pordenone","IM":"Imperia","SV":"Savona","GE":"Genova","SP":"La Spezia",
    "PC":"Piacenza","PR":"Parma","RE":"Reggio Emilia","MO":"Modena","BO":"Bologna","FE":"Ferrara",
    "RA":"Ravenna","FO":"Forlì-Cesena","MS":"Massa-Carrara","LU":"Lucca","PT":"Pistoia","FI":"Firenze",
    "LI":"Livorno","PI":"Pisa","AR":"Arezzo","SI":"Siena","GR":"Grosseto","PG":"Perugia","TR":"Terni",
    "PS":"Pesaro e Urbino","AN":"Ancona","MC":"Macerata","AP":"Ascoli Piceno","VT":"Viterbo",
    "RI":"Rieti","RM":"Roma","LT":"Latina","FR":"Frosinone","AQ":"L'Aquila","CS":"Cosenza",
    "CZ":"Catanzaro","RC":"Reggio Calabria","TP":"Trapani","PA":"Palermo","TE":"Teramo","PE":"Pescara",
    "CH":"Chieti","CB":"Campobasso","IS":"Isernia","CE":"Caserta","BN":"Benevento","NA":"Napoli",
    "AV":"Avellino","SA":"Salerno","FG":"Foggia","BA":"Bari","TA":"Taranto","BR":"Brindisi",
    "LE":"Lecce","PZ":"Potenza","MT":"Matera","MR":"Matera","ME":"Messina","AG":"Agrigento",
    "CL":"Caltanissetta","EN":"Enna","CT":"Catania","RG":"Ragusa","SR":"Siracusa","SS":"Sassari",
    "NU":"Nuoro","CA":"Cagliari","OR":"Oristano"
}

# Paths
OUTPUT_CSV = os.path.join('../Data_Collection/csv_tables-fase1', 'dati_fase1.csv')
SHAPEFILE_INPUT = "../Istat/Regioni/Campania/R15_11_WGS84.shp"
SHAPEFILE_OUTPUT = "../Data_Collection/shapefiles_merged/Campania/campania_merged.shp"

# =============================================================================
# SETUP LOGGING
# =============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# FUNZIONI ESTRAZIONE
# =============================================================================

from normattiva import run_estrazione_normattiva
from data_extraction.siape import run_estrazione_siape
from estrazione_dati_basi_territoriali import run_estrazione_basi_territoriali
from estrazione_dati_variabili_censuarie import run_estrazione_variabili_censuarie

# =============================================================================
# FUNZIONI DI ELABORAZIONE
# =============================================================================

def crea_dataframe_fase1() -> pd.DataFrame:
    # 1) Estrazione dati
    df_base = run_estrazione_basi_territoriali()
    df_cens = run_estrazione_variabili_censuarie()
    df_norm = run_estrazione_normattiva()
    df_siape = run_estrazione_siape()

    # 2) Merge base + censuarie
    df_merged = pd.merge(df_base, df_cens, on='SEZ2011', how='inner')
    df_merged['COMUNE'] = df_merged['COMUNE'].str.upper()
    df_norm['COMUNE']   = df_norm['COMUNE'].str.upper()

    # 3) Mappa sigla provincia → nome
    df_norm['PROVINCIA'] = df_norm['PROVINCIA'].map(PROV_DICT)
    logger.info("Province mappate in df_norm: %d di %d",
                df_norm['PROVINCIA'].notna().sum(), len(df_norm))

    # 4) Join diretto su PROVINCIA + COMUNE
    df_join = pd.merge(
        df_merged,
        df_norm,
        on=['PROVINCIA','COMUNE'],
        how='left',
        indicator=True
    )
    direct_matched = df_join['_merge']=='both'
    num_direct = direct_matched.sum()
    logger.info("Comuni con corrispondenza diretta: %d/%d",
                num_direct, len(df_merged))

    # 5) Identifico i non matchati
    df_unmatched = df_join.loc[~direct_matched, df_merged.columns]
    unmatched_list = df_unmatched['COMUNE'].unique().tolist()
    logger.info("Numero comuni da fuzzy match: %d", len(df_unmatched))
    logger.info("Elenco comuni non matchati direttamente: %s", unmatched_list)

    # 6) Preparo df_final con i matched
    df_final = df_join.loc[direct_matched].drop(columns=['_merge'])

    # 7) Fuzzy match limitato alla provincia
    for idx, row in df_unmatched.iterrows():
        prov = row['PROVINCIA']
        target = row['COMUNE']
        candidates = df_norm.loc[df_norm['PROVINCIA']==prov, 'COMUNE'].unique()
        if not len(candidates):
            logger.warning("Nessun candidato fuzzy per provincia %s (riga %s)", prov, idx)
            continue

        best, score, _ = process.extractOne(
            query=target,
            choices=candidates,
            scorer=fuzz.token_sort_ratio
        )

        if score >= THRESHOLD:
            norm_row = df_norm[
                (df_norm['PROVINCIA']==prov) & (df_norm['COMUNE']==best)
            ].iloc[0]
            merged_row = pd.concat([row, norm_row.drop(['PROVINCIA','COMUNE'])])
            df_final = pd.concat([df_final, merged_row.to_frame().T], ignore_index=True)
            logger.info("Fuzzy match: '%s'→'%s' (score %d)", target, best, score)
        else:
            logger.warning("Fuzzy LOW score per '%s' in %s: '%s' (%d)",
                           target, prov, best, score)
    logger.info("Comuni fuzzy matchati: %d/%d", len(df_final), len(df_merged))

    return df_final

def salva_dati_fase1(df: pd.DataFrame, path: str = OUTPUT_CSV) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, sep=';', encoding='utf-8-sig')
    logger.info("Salvato CSV fase1 in %s", path)


'''
def create_merged_shapefile() -> None:
    gdf = gpd.read_file(SHAPEFILE_INPUT)
    df_csv = pd.read_csv(OUTPUT_CSV, sep=';', encoding='utf-8-sig')

    logger.info("Colonne shapefile: %s", gdf.columns.tolist())
    logger.info("Colonne CSV fase1: %s", df_csv.columns.tolist())

    to_merge = [c for c in df_csv.columns if c not in gdf.columns or c=='SEZ2011']
    removed = [c for c in df_csv.columns if c not in to_merge]
    logger.info("Colonne unite: %s", to_merge)
    logger.info("Colonne scartate: %s", removed)

    df_csv = df_csv[to_merge]
    merged = gdf.merge(df_csv, how='left', on='SEZ2011')

    merged['LOC2011'] = merged['LOC2011'].astype(str)
    merged = merged.rename(columns={'ZONA_CLIMATICA':'ZONA_CLIM','GRADI_GIORNO':'GRADI_GIO'})

    os.makedirs(os.path.dirname(SHAPEFILE_OUTPUT), exist_ok=True)
    merged.to_file(SHAPEFILE_OUTPUT, encoding='utf-8')
    logger.info("Shapefile unito salvato in %s", SHAPEFILE_OUTPUT)
'''

def run_fase1() -> None:
    df_fase1 = crea_dataframe_fase1()
    salva_dati_fase1(df_fase1)
    #create_merged_shapefile()

if __name__ == "__main__":
    run_fase1()
