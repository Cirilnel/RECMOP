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
    "BI": "BIELLA", "TO": "TORINO", "VC": "VERCELLI", "NO": "NOVARA", "CN": "CUNEO", "AT": "ASTI", "AL": "ALESSANDRIA",
    "AO": "AOSTA", "VA": "VARESE", "CO": "COMO", "SO": "SONDRIO", "MI": "MILANO", "BG": "BERGAMO", "BS": "BRESCIA",
    "PV": "PAVIA", "CR": "CREMONA", "MN": "MANTOVA", "BZ": "BOLZANO", "TN": "TRENTO", "VR": "VERONA", "VI": "VICENZA",
    "BL": "BELLUNO", "TV": "TREVISO", "VE": "VENEZIA", "PD": "PADOVA", "RO": "ROVIGO", "UD": "UDINE", "GO": "GORIZIA",
    "TS": "TRIESTE", "PN": "PORDENONE", "IM": "IMPERIA", "SV": "SAVONA", "GE": "GENOVA", "SP": "LA SPEZIA",
    "PC": "PIACENZA", "PR": "PARMA", "RE": "REGGIO EMILIA", "MO": "MODENA", "BO": "BOLOGNA", "FE": "FERRARA",
    "RA": "RAVENNA", "FO": "FORLÌ-CESENA", "MS": "MASSA-CARRARA", "LU": "LUCCA", "PT": "PISTOIA", "FI": "FIRENZE",
    "LI": "LIVORNO", "PI": "PISA", "AR": "AREZZO", "SI": "SIENA", "GR": "GROSSETO", "PG": "PERUGIA", "TR": "TERNI",
    "PS": "PESARO E URBINO", "AN": "ANCONA", "MC": "MACERATA", "AP": "ASCOLI PICENO", "VT": "VITERBO",
    "RI": "RIETI", "RM": "ROMA", "LT": "LATINA", "FR": "FROSINONE", "AQ": "L'AQUILA", "CS": "COSENZA",
    "CZ": "CATANZARO", "RC": "REGGIO CALABRIA", "TP": "TRAPANI", "PA": "PALERMO", "TE": "TERAMO", "PE": "PESCARA",
    "CH": "CHIETI", "CB": "CAMPOBASSO", "IS": "ISERNIA", "CE": "CASERTA", "BN": "BENEVENTO", "NA": "NAPOLI",
    "AV": "AVELLINO", "SA": "SALERNO", "FG": "FOGGIA", "BA": "BARI", "TA": "TARANTO", "BR": "BRINDISI",
    "LE": "LECCE", "PZ": "POTENZA", "MT": "MATERA", "MR": "MATERA", "ME": "MESSINA", "AG": "AGRIGENTO",
    "CL": "CALTANISSETTA", "EN": "ENNA", "CT": "CATANIA", "RG": "RAGUSA", "SR": "SIRACUSA", "SS": "SASSARI",
    "NU": "NUORO", "CA": "CAGLIARI", "OR": "ORISTANO"
}


# Paths
OUTPUT_CSV = os.path.join('../Data_Collection/csv_tables-fase1', 'join_data.csv')

# =============================================================================
# SETUP LOGGING
# =============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# FUNZIONI ESTRAZIONE
# =============================================================================

from normattiva import estrai_dati_normattiva
from estrazione_dati_basi_territoriali import estrai_dati_basi_territoriali
from estrazione_dati_variabili_censuarie import estrai_dati_variabili_censuarie

# =============================================================================
# FUNZIONI DI ELABORAZIONE
# =============================================================================

def join_data(input_path_basi_territoriali: str, input_path_variabili_censuarie: str) -> pd.DataFrame:
    # 1) Estrazione dati
    df_base = estrai_dati_basi_territoriali(input_path_basi_territoriali)
    df_cens = estrai_dati_variabili_censuarie(input_path_variabili_censuarie)
    df_norm = estrai_dati_normattiva()

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

def salva_join_data(df: pd.DataFrame, path: str = OUTPUT_CSV) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, sep=';', encoding='utf-8-sig')
    logger.info("Salvato CSV fase1 in %s", path)

