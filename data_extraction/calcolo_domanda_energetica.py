import os
import shutil
import pandas as pd
import geopandas as gpd
import logging

from geopandas import GeoDataFrame

from data_extraction.estrazione_dati_basi_territoriali import get_geom_basi_territoriali
from data_extraction.join_data_normattiva_varcens_basiterr import get_join_data
from data_extraction.siape import get_dati_siape
from data_extraction.interrogazione_wfs_catastale import get_dati_catasto
from utils import safe_name, get_regione_from_provincia, calcola_area

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Definisci il percorso alla cartella CSV
CSV_DIR = os.path.join(BASE_DIR, "..", "Data_Collection", "csv_tables-fase1")

# Carica i CSV dal percorso corretto
CSV_DIR = os.path.join(BASE_DIR, "..", "Data_Collection", "csv_tables-fase1")

df_prelievi = pd.read_csv(
    os.path.join(CSV_DIR, "prelievi_mensili_per_provincia.csv"),
    sep=";",
    decimal=",",
    encoding="latin1"
)

# CORREGGI LA LETTURA DEL FILE coefficienti_consumi_orari_x_tipologia.csv
df_coeff_orari = pd.read_csv(
    os.path.join(CSV_DIR, "coefficienti_consumi_orari_x_tipologia.csv"),
    sep=";",
    decimal=",",
    encoding="latin1"
)

# CONVERTI LE PERCENTUALI NEL FILE coefficienti_consumi_orari_x_tipologia.csv
for stagione in ['Inverno', 'Media Stagione', 'Estate']:
    df_coeff_orari[stagione] = (
            df_coeff_orari[stagione]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float) / 100
    )

# La conversione per df_prelievi è già corretta, la mantieni
df_prelievi["Percentuale mensile dei prelievi 2023"] = (
        df_prelievi["Percentuale mensile dei prelievi 2023"]
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float) / 100
)

# Mappa dei mesi -> stagione
MESE_TO_STAGIONE = {
    "Gen": "Inverno", "Feb": "Inverno", "Dic": "Inverno",
    "Mar": "Media Stagione", "Apr": "Media Stagione", "Mag": "Media Stagione", "Ott": "Media Stagione", "Nov": "Media Stagione",
    "Giu": "Estate", "Lug": "Estate", "Ago": "Estate", "Set": "Estate"
}

# Giorni per mese (anno non bisestile)
GIORNI_PER_MESE = {
    "Gen": 31, "Feb": 28, "Mar": 31, "Apr": 30, "Mag": 31, "Giu": 30,
    "Lug": 31, "Ago": 31, "Set": 30, "Ott": 31, "Nov": 30, "Dic": 31
}

# Mappa mesi -> numeri
MESE_TO_NUMERO = {
    "Gen": 1, "Feb": 2, "Mar": 3, "Apr": 4, "Mag": 5, "Giu": 6,
    "Lug": 7, "Ago": 8, "Set": 9, "Ott": 10, "Nov": 11, "Dic": 12
}

# =============================================================================
# FUNZIONI DI UTILITY
# =============================================================================

def _filtra_df_comune(df_join, comune, provincia):
    df_join['_COMUNE_NORM'] = df_join['COMUNE'].astype(str).apply(safe_name)
    df_join['_PROVINCIA_NORM'] = df_join['PROVINCIA'].astype(str).apply(safe_name)
    df_comune = df_join[
        (df_join['_COMUNE_NORM'] == comune) &
        (df_join['_PROVINCIA_NORM'] == provincia)
        ]
    df_join.drop(columns=['_COMUNE_NORM', '_PROVINCIA_NORM'], inplace=True)
    if df_comune.empty:
        raise ValueError(f"Nessun dato trovato per il comune {comune} nella provincia {provincia}.")
    return df_comune

def _estrai_zona_climatica(df_comune, df_siape, comune):
    zc = df_comune['ZONA_CLIMATICA'].dropna().unique()
    if len(zc) != 1:
        raise ValueError(f"Zona climatica ambigua o mancante per {comune}. Valori trovati: {zc}")
    zc = zc[0]
    df_zc = df_siape[df_siape['zona_climatica'] == zc]
    if df_zc.empty:
        raise ValueError(f"Nessun dato SIAPE per la zona climatica {zc}")
    return zc, df_zc

def _trova_range(valore, ranges):
    for r in ranges:
        if r.startswith('<'):
            limite = float(r[1:])
            if valore < limite:
                return r
        elif r.startswith('>'):
            limite = float(r[1:])
            if valore > limite:
                return r
        elif '-' in r:
            min_, max_ = map(float, r.split('-'))
            if min_ <= valore < max_:
                return r
    return None

def _join_fabbricati_sezione(provincia: str, gdf_fabbricati: gpd.GeoDataFrame) -> pd.DataFrame:
    regione = get_regione_from_provincia(safe_name(provincia))
    gdf_sezioni = get_geom_basi_territoriali(regione)

    if gdf_fabbricati.crs != gdf_sezioni.crs:
        crs_orig = gdf_fabbricati.crs
        gdf_fabbricati = gdf_fabbricati.to_crs(gdf_sezioni.crs)
    else:
        crs_orig = None

    gdf_centroidi = gdf_fabbricati.copy()
    gdf_centroidi['geometry'] = gdf_centroidi.geometry.centroid

    joined = gpd.sjoin(
        gdf_centroidi[['ID_FAB', 'geometry']],
        gdf_sezioni[['SEZ2011', 'geometry']],
        how='inner',
        predicate='within'
    )
    result = joined[['ID_FAB', 'SEZ2011']].reset_index(drop=True)

    if crs_orig:
        gdf_fabbricati = gdf_fabbricati.to_crs(crs_orig)

    return result

# =============================================================================
# FUNZIONI DI CALCOLO COEFFICIENTI (INVARIATE)
# =============================================================================

def _calcola_coefficiente_domanda_zc_range(
        gdf_fabbricati: gpd.GeoDataFrame,
        df_join: pd.DataFrame,
        df_siape: pd.DataFrame,
        comune: str,
        provincia: str
) -> gpd.GeoDataFrame:
    logger.info(f"Calcolo coefficiente domanda per {comune} ({provincia})...")

    df_ID_FAB_sez = _join_fabbricati_sezione(provincia, gdf_fabbricati)
    df_comune = _filtra_df_comune(df_join, comune, provincia)
    zc, df_zc = _estrai_zona_climatica(df_comune, df_siape, comune)

    sezioni = df_comune['SEZ2011'].unique()
    lista = []
    for sez in sezioni:
        row = df_comune[df_comune['SEZ2011'] == sez]
        if row.empty:
            continue
        r = row.iloc[0]
        b1 = r.get('E8', 0) + r.get('E9', 0)
        b2 = r.get('E10', 0) + r.get('E11', 0)
        b3 = r.get('E12', 0) + r.get('E13', 0)
        b4 = r.get('E14', 0) + r.get('E15', 0)
        b5 = r.get('E16', 0)
        totale_edifici = b1 + b2 + b3 + b4 + b5
        if totale_edifici == 0:
            coeff_dom_sez = 0
        else:
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

            coeff_dom_sez = (
                    (b1 * epgl_nren_1 + b2 * epgl_nren_2 + b3 * epgl_nren_3 +
                     b4 * epgl_nren_4 + b5 * epgl_nren_5) / totale_edifici
            )
        lista.append({'SEZ2011': sez, 'coeff_dom_sez': round(coeff_dom_sez, 2)})

    df_coeff_sez = pd.DataFrame(lista)
    df_ID_FAB_sez = df_ID_FAB_sez.merge(df_coeff_sez, on='SEZ2011', how='left')

    gdf_fabbricati = gdf_fabbricati.copy()
    gdf_fabbricati = gdf_fabbricati.merge(df_ID_FAB_sez[['ID_FAB', 'coeff_dom_sez']], on='ID_FAB', how='left')
    gdf_fabbricati['coeff_dom'] = gdf_fabbricati['coeff_dom_sez'].fillna(0)
    gdf_fabbricati.drop(columns=['coeff_dom_sez'], inplace=True)

    return gdf_fabbricati

def _calcola_coefficiente_domanda_zc_suris_volris(
        gdf_fabbricati: gpd.GeoDataFrame,
        df_join: pd.DataFrame,
        df_siape: pd.DataFrame,
        comune: str,
        provincia: str
) -> gpd.GeoDataFrame:
    logger.info(f"Calcolo coefficiente domanda per {comune} ({provincia})...")

    df_comune = _filtra_df_comune(df_join, comune, provincia)
    zc, df_zc = _estrai_zona_climatica(df_comune, df_siape, comune)

    suris_ranges = list(df_zc['suris_range'].unique())
    volris_ranges = list(df_zc['volris_range'].unique())

    def trova_epgl_robusto(sur_range, vol_range):
        riga = df_zc[
            (df_zc['suris_range'] == sur_range) & (df_zc['volris_range'] == vol_range)
            ]
        if not riga.empty:
            coeff = riga.iloc[0]['EPgl_nren']
            if coeff != 0:
                return coeff
        riga = df_zc[df_zc['suris_range'] == sur_range]
        media1 = riga['EPgl_nren'].replace(0, pd.NA).dropna()
        if not media1.empty:
            coeff = media1.mean()
            if coeff != 0:
                return coeff
        riga = df_zc[df_zc['volris_range'] == vol_range]
        media2 = riga['EPgl_nren'].replace(0, pd.NA).dropna()
        if not media2.empty:
            coeff = media2.mean()
            if coeff != 0:
                return coeff
        media3 = df_zc['EPgl_nren'].replace(0, pd.NA).dropna()
        if not media3.empty:
            coeff = media3.mean()
            if coeff != 0:
                return coeff
        return 0

    coeff_dom_list = []
    for idx, row in gdf_fabbricati.iterrows():
        sup = row['sup_risc']
        vol = row['vol_risc']
        range_sur = _trova_range(sup, suris_ranges)
        range_vol = _trova_range(vol, volris_ranges)
        if range_sur is None or range_vol is None:
            logger.warning(f"Range non trovato per fabbricato ID_FAB={row.get('ID_FAB', idx)}: sup_risc={sup}, vol_risc={vol}")
            coeff_dom_list.append(None)
            continue
        coeff = trova_epgl_robusto(range_sur, range_vol)
        coeff_dom_list.append(coeff)

    gdf_fabbricati['coeff_dom'] = coeff_dom_list
    return gdf_fabbricati

def _calcola_coefficiente_domanda_zc_suris_volris_supdi(
        gdf_fabbricati: gpd.GeoDataFrame,
        df_join: pd.DataFrame,
        df_siape: pd.DataFrame,
        comune: str,
        provincia: str
) -> gpd.GeoDataFrame:
    logger.info(f"Calcolo coefficiente domanda con supdi per {comune} ({provincia})...")

    df_comune = _filtra_df_comune(df_join, comune, provincia)
    zc, df_zc = _estrai_zona_climatica(df_comune, df_siape, comune)

    suris_ranges = list(df_zc['suris_range'].unique())
    volris_ranges = list(df_zc['volris_range'].unique())
    supdi_ranges = list(df_zc['supdi_range'].unique())

    def trova_epgl_fallback(sur_range, vol_range, supdi_range):
        riga = df_zc[
            (df_zc['suris_range'] == sur_range) & (df_zc['volris_range'] == vol_range) & (df_zc['supdi_range'] == supdi_range)
            ]
        if not riga.empty:
            coeff = riga.iloc[0]['EPgl_nren']
            if coeff != 0:
                return coeff

        riga = df_zc[
            (df_zc['suris_range'] == sur_range) & (df_zc['volris_range'] == vol_range)
            ]
        media2 = riga['EPgl_nren'].replace(0, pd.NA).dropna()
        if not media2.empty:
            coeff = media2.mean()
            if coeff != 0:
                return coeff

        riga = df_zc[df_zc['suris_range'] == sur_range]
        media3 = riga['EPgl_nren'].replace(0, pd.NA).dropna()
        if not media3.empty:
            coeff = media3.mean()
            if coeff != 0:
                return coeff

        riga = df_zc[df_zc['volris_range'] == vol_range]
        media4 = riga['EPgl_nren'].replace(0, pd.NA).dropna()
        if not media4.empty:
            coeff = media4.mean()
            if coeff != 0:
                return coeff

        media5 = df_zc['EPgl_nren'].replace(0, pd.NA).dropna()
        if not media5.empty:
            coeff = media5.mean()
            if coeff != 0:
                return coeff

        return 0

    coeff_dom_list = []
    for idx, row in gdf_fabbricati.iterrows():
        sup = row['sup_risc']
        vol = row['vol_risc']
        supdi = row['sup_disp']
        range_sur = _trova_range(sup, suris_ranges)
        range_vol = _trova_range(vol, volris_ranges)
        range_supdi = _trova_range(supdi, supdi_ranges)
        if range_sur is None or range_vol is None or range_supdi is None:
            logger.warning(
                f"Range non trovato per fabbricato ID_FAB={row.get('ID_FAB', idx)}: "
                f"sup_risc={sup}, vol_risc={vol}, sup_disp={supdi}")
            coeff_dom_list.append(None)
            continue
        coeff = trova_epgl_fallback(range_sur, range_vol, range_supdi)
        coeff_dom_list.append(coeff)

    gdf_fabbricati['coeff_dom'] = coeff_dom_list
    return gdf_fabbricati

# =============================================================================
# NUOVE FUNZIONI PER TABELLA CON DOMANDA ORARIA MENSILE E TOTALE
# =============================================================================

def _calcola_domanda_oraria_mensile(gdf_fabbricati, df_prelievi, df_coeff_orari, anno=2023):
    """
    Calcola la domanda oraria mensile per ogni edificio.
    Restituisce un DataFrame con ID_Edificio, coeff_dom, D_an[anno]
    e colonne D_ms1, D_ms2, ..., D_or0_ms1, D_or1_ms1, ..., D_or23_ms12 (indicizzazione ore da 0 a 23).
    """
    risultati = []

    for idx, row in gdf_fabbricati.iterrows():
        domanda_annuale = float(row['domanda_en'])
        provincia = row['provincia']
        provincia_norm = safe_name(provincia)

        # Filtra prelievi per provincia
        prelievi_prov = df_prelievi[
            df_prelievi['Provincia'].apply(safe_name) == provincia_norm
            ]

        if prelievi_prov.empty:
            logger.warning(f"Nessun dato prelievi trovato per provincia: {provincia}")
            continue

        # Usa ID_FAB come ID_Edificio
        building_data = {
            'ID_Edificio': row['ID_FAB'],  # Questo è fondamentale - usa ID_FAB
            'coeff_dom': round(row['coeff_dom'], 4),
            f'D_an{anno}': round(domanda_annuale, 4)
        }

        # Calcola domanda mensile totale per ogni mese
        domanda_mensile_totale = 0
        for _, riga_mese in prelievi_prov.iterrows():
            mese = riga_mese['Mese']
            percentuale_mensile = float(riga_mese['Percentuale mensile dei prelievi 2023'])
            numero_mese = MESE_TO_NUMERO[mese]

            # Domanda mensile totale
            domanda_mensile = domanda_annuale * percentuale_mensile
            building_data[f'D_ms{numero_mese}'] = round(domanda_mensile, 4)
            domanda_mensile_totale += domanda_mensile

        # Verifica che la somma delle domande mensili sia uguale alla domanda annuale (con tolleranza)
        tolleranza = 0.01  # 1% di tolleranza
        if abs(domanda_mensile_totale - domanda_annuale) > tolleranza * domanda_annuale:
            logger.warning(f"Somma domande mensili ({domanda_mensile_totale}) diversa da domanda annuale ({domanda_annuale}) per edificio {row['ID_FAB']}")

        # Calcola domanda oraria mensile per ogni mese e ogni ora
        for _, riga_mese in prelievi_prov.iterrows():
            mese = riga_mese['Mese']
            percentuale_mensile = float(riga_mese['Percentuale mensile dei prelievi 2023'])
            numero_mese = MESE_TO_NUMERO[mese]
            giorni_mese = GIORNI_PER_MESE[mese]
            stagione = MESE_TO_STAGIONE[mese]

            # Domanda mensile totale
            domanda_mensile = domanda_annuale * percentuale_mensile

            # Domanda giornaliera media
            domanda_giornaliera = domanda_mensile / float(giorni_mese)

            # Coefficienti orari per la stagione
            try:
                coeff_stagione = (
                    df_coeff_orari[['ora', stagione]]
                    .set_index('ora')[stagione].astype(float)
                )
            except Exception as e:
                logger.error(f"Errore conversione coefficienti per {stagione}: {e}")
                continue

            # Calcola domanda per ogni ora (da 0 a 23)
            for ora in range(24):
                try:
                    coeff_ora = float(coeff_stagione.loc[ora])
                    domanda_oraria = domanda_giornaliera * coeff_ora

                    # Aggiungi colonna nel formato D_or{ora}_ms{numero_mese} (ora parte da 0)
                    colonna_ora = f'D_or{ora}_ms{numero_mese}'
                    building_data[colonna_ora] = round(domanda_oraria, 4)

                except Exception as e:
                    logger.warning(f"Errore calcolo ora {ora} per mese {mese}: {e}")
                    building_data[f'D_or{ora}_ms{numero_mese}'] = 0.0

        # Assicurati che tutte le colonne orarie siano presenti (anche se zero)
        for mese_num in range(1, 13):
            # Assicurati che la colonna D_ms{numero_mese} esista
            if f'D_ms{mese_num}' not in building_data:
                building_data[f'D_ms{mese_num}'] = 0.0

            for ora in range(24):
                colonna = f'D_or{ora}_ms{mese_num}'
                if colonna not in building_data:
                    building_data[colonna] = 0.0

        # Assicurati che tutte le colonne mensili siano presenti
        for mese_num in range(1, 13):
            if f'D_ms{mese_num}' not in building_data:
                building_data[f'D_ms{mese_num}'] = 0.0

        risultati.append(building_data)

    return pd.DataFrame(risultati)

def _salva_tabella_domanda_oraria_mensile(gdf_fabbricati, df_prelievi, df_coeff_orari, out_dir, prov_safe, com_safe, anno=2023):
    """
    Salva la tabella della domanda oraria mensile in formato CSV direttamente nella cartella out_dir.
    """
    # Calcola domanda oraria mensile
    df_domanda_oraria_mensile = _calcola_domanda_oraria_mensile(gdf_fabbricati, df_prelievi, df_coeff_orari, anno)

    if df_domanda_oraria_mensile.empty:
        logger.warning("Nessun dato di domanda oraria mensile da salvare")
        return None

    # Riordina le colonne per una migliore leggibilità - RIMUOVI domanda_en
    colonne_ordinate = ['ID_Edificio', 'coeff_dom', f'D_an{anno}']

    # Aggiungi colonne mensili in ordine
    for mese_num in range(1, 13):
        colonne_ordinate.append(f'D_ms{mese_num}')

    # Aggiungi colonne orarie in ordine (ora parte da 0)
    for mese_num in range(1, 13):
        for ora in range(24):
            colonne_ordinate.append(f'D_or{ora}_ms{mese_num}')

    # Mantieni solo le colonne che esistono nel DataFrame
    colonne_esistenti = [col for col in colonne_ordinate if col in df_domanda_oraria_mensile.columns]
    colonni_extra = [col for col in df_domanda_oraria_mensile.columns if col not in colonne_ordinate]

    df_domanda_oraria_mensile = df_domanda_oraria_mensile[colonne_esistenti + colonni_extra]

    # Salva CSV direttamente nella cartella out_dir
    csv_path = os.path.join(out_dir, f"domanda_oraria_mensile_{prov_safe}_{com_safe}.csv")
    df_domanda_oraria_mensile.to_csv(csv_path, index=False, sep=';', decimal=',')

    logger.info(f"Tabella domanda oraria mensile salvata in: {csv_path}")
    logger.info(f"Numero di edifici processati: {len(df_domanda_oraria_mensile)}")
    logger.info(f"Colonne generate: {len(df_domanda_oraria_mensile.columns)}")

    # Log delle prime colonne per verifica
    colonne_d_ms = [col for col in df_domanda_oraria_mensile.columns if col.startswith('D_ms')]
    colonne_d_or = [col for col in df_domanda_oraria_mensile.columns if col.startswith('D_or')]
    logger.info(f"Colonne D_ms generate: {len(colonne_d_ms)} - Esempio: {colonne_d_ms[:3]}...")
    logger.info(f"Colonne D_or generate: {len(colonne_d_or)} - Esempio: {colonne_d_or[:3]}...")
    logger.info(f"Colonna D_an{anno} presente: {f'D_an{anno}' in df_domanda_oraria_mensile.columns}")

    return csv_path

# =============================================================================
# IMPLEMENTAZIONE PRIVATA MODIFICATA
# =============================================================================

def _calcola_domanda_energetica_impl(
        comune: str,
        provincia: str,
        siape_key: str,
        coeff_func,
        coeff_moltiplicativo = 1.0,
        anno_riferimento = 2023
) -> GeoDataFrame | None:
    logger.info(f"Inizio calcolo domanda energetica...")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.normpath(os.path.join(BASE_DIR, ".."))

    prov_safe = safe_name(provincia)
    comm_safe = safe_name(comune)
    regione = get_regione_from_provincia(prov_safe)
    df_join = get_join_data(regione)
    df_siape = get_dati_siape(siape_key)

    shp_dir = os.path.normpath(os.path.join(PROJECT_ROOT, "FABBRICATI", f"fabbricati_{prov_safe}_{comm_safe}"))
    if not os.path.isdir(shp_dir):
        raise FileNotFoundError(f"Directory shapefile non trovata: {shp_dir}")

    shp_files = [f for f in os.listdir(shp_dir) if f.lower().endswith('.shp')]
    if len(shp_files) != 1:
        raise ValueError(f"Atteso un unico file .shp in {shp_dir}, trovati: {shp_files}")
    shp_path = os.path.join(shp_dir, shp_files[0])

    try:
        gdf_fabbricati = gpd.read_file(shp_path)
    except Exception as e:
        logger.warning(f"Errore caricamento shapefile fabbricati: {e}")
        return None

    gdf_fabbricati = calcola_area(gdf_fabbricati, nome_colonna='area')

    subdir = f"{prov_safe}_{comm_safe}"
    dirname = f"domanda_energetica_{prov_safe}_{comm_safe}"
    out_dir = os.path.normpath(os.path.join(PROJECT_ROOT, "Data_Collection", "shapefiles", subdir, dirname))

    # Pulisci e ricrea la directory di output
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)

    # Calcolo/assegnazione coefficiente
    gdf_fabbricati = coeff_func(gdf_fabbricati, df_join, df_siape, comm_safe, prov_safe)
    gdf_fabbricati['domanda_en'] = gdf_fabbricati['area'] * gdf_fabbricati['coeff_dom']

    # RIMOSSA LA CREAZIONE DELLA CARTELLA _zero E DEL FILE GPKG
    # Semplicemente filtra gli edifici con domanda_en > 0 senza creare cartelle separate
    if siape_key == "zc_range":
        n_totale = len(gdf_fabbricati)
        gdf_fabbricati = gdf_fabbricati[gdf_fabbricati['domanda_en'] > 0].copy()
        n_eliminati = n_totale - len(gdf_fabbricati)
        logger.info(f"Eliminati {n_eliminati} edifici su {n_totale} (domanda_en=0)")

    # Aggiunta delta_UHI se presente
    if 'delta_UHI' in gdf_fabbricati.columns:
        logger.info("Colonna 'delta_UHI' trovata: aggiunta alla domanda energetica.")
        gdf_fabbricati['domanda_en'] += gdf_fabbricati['delta_UHI']

    # Applica coefficiente moltiplicativo
    gdf_fabbricati['domanda_en'] *= coeff_moltiplicativo
    gdf_fabbricati['provincia'] = provincia

    # SALVA TABELLA DOMANDA ORARIA MENSILE (con colonne D_ms1, D_ms2, ..., D_an[anno], D_or0_ms1, ecc.)
    csv_path = _salva_tabella_domanda_oraria_mensile(
        gdf_fabbricati, df_prelievi, df_coeff_orari, out_dir, prov_safe, comm_safe, anno_riferimento
    )

    # RIMUOVI LA COLONNA domanda_en DAL GeoDataFrame PRIMA DI RESTITUIRLO
    # (ora manteniamo solo D_an[anno] nel CSV, quindi possiamo rimuovere domanda_en dal GeoDataFrame)
    if 'domanda_en' in gdf_fabbricati.columns:
        gdf_fabbricati = gdf_fabbricati.drop(columns=['domanda_en'])

    logger.info(f"Calcolo domanda energetica completato per {comune} ({provincia})")
    logger.info(f"File CSV generato: {csv_path}")

    return gdf_fabbricati

# =============================================================================
# FUNZIONE DI CALCOLO UNIFICATA (MODIFICATA)
# =============================================================================

def calcola_domanda_energetica(
        comune: str,
        provincia: str,
        fabbricati_tipo: str,
        coeff_moltiplicativo = 1.0,
        anno_riferimento = 2023
) -> gpd.GeoDataFrame:
    logger.info(f"Inizio calcolo domanda energetica [{fabbricati_tipo}]...")

    _COEFF_FUNCS = {
        "zc_range": _calcola_coefficiente_domanda_zc_range,
        "zc_suris_volris": _calcola_coefficiente_domanda_zc_suris_volris,
        "zc_suris_volris_supdi": _calcola_coefficiente_domanda_zc_suris_volris_supdi,
    }
    _SIAPE_KEYS = {
        "zc_range": "zc_range",
        "zc_suris_volris": "zc_suris_volris",
        "zc_suris_volris_supdi": "zc_suris_volris_supdi",
    }

    if fabbricati_tipo not in _COEFF_FUNCS:
        raise ValueError(f"Tipologia fabbricati non riconosciuta: {fabbricati_tipo}")

    coeff_func = _COEFF_FUNCS[fabbricati_tipo]
    siape_key = _SIAPE_KEYS[fabbricati_tipo]

    return _calcola_domanda_energetica_impl(
        comune, provincia, siape_key, coeff_func, coeff_moltiplicativo, anno_riferimento
    )