import os
import shutil
import geopandas as gpd
import pandas as pd
import logging
from utils import safe_name, configure_logging_if_main

# === CONFIGURAZIONE LOGGING ===
logger = logging.getLogger(__name__)

# === COSTANTI PATH ASSOLUTI ===
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SHAPE_IN_DIR = os.path.abspath(os.path.join(BASE_DIR, '..', 'Data_Collection', 'shapefiles'))
OUTPUT_MODEL_BUILDER = os.path.abspath(os.path.join(BASE_DIR, '..', 'model_builder_shapefiles'))

def carica_csv_con_geometria(provincia: str, comune: str, tipo: str) -> gpd.GeoDataFrame:
    """
    Carica i dati CSV e li unisce con le geometrie degli edifici.
    tipo: 'domanda' o 'offerta'
    """
    provincia_safe = safe_name(provincia)
    comune_safe = safe_name(comune)

    # Percorso ai CSV
    csv_dir = os.path.join(
        SHAPE_IN_DIR,
        f"{provincia_safe}_{comune_safe}",
        f"{tipo}_energetica_{provincia_safe}_{comune_safe}"
    )

    # Cerca file CSV
    csv_files = [f for f in os.listdir(csv_dir) if f.lower().endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError(f"Nessun file CSV trovato in {csv_dir}")

    csv_path = os.path.join(csv_dir, csv_files[0])
    logger.info(f"Caricamento CSV {tipo}: {csv_path}")

    # Carica CSV
    df = pd.read_csv(csv_path, sep=';', decimal=',')

    # Log degli ID presenti nel CSV
    id_col = 'ID_Edificio'
    logger.info(f"Primi 5 ID nel CSV {tipo}: {df[id_col].head().tolist() if id_col in df.columns else 'COLONNA NON TROVATA'}")
    logger.info(f"Colonne nel CSV {tipo}: {list(df.columns)}")

    # Carica geometrie degli edifici
    fabbricati_dir = os.path.join(
        os.path.abspath(os.path.join(BASE_DIR, '..', 'FABBRICATI')),
        f'fabbricati_{provincia_safe}_{comune_safe}'
    )

    shp_files = [f for f in os.listdir(fabbricati_dir) if f.lower().endswith('.shp')]
    if not shp_files:
        raise FileNotFoundError(f"Nessuno shapefile trovato in {fabbricati_dir}")

    shp_path = os.path.join(fabbricati_dir, shp_files[0])
    gdf_edifici = gpd.read_file(shp_path)

    # Log degli ID presenti nello shapefile
    logger.info(f"Primi 5 ID nello shapefile: {gdf_edifici['ID_FAB'].head().tolist() if 'ID_FAB' in gdf_edifici.columns else 'COLONNA ID_FAB NON TROVATA'}")
    logger.info(f"Colonne nello shapefile: {list(gdf_edifici.columns)}")

    # Rinomina ID_FAB in ID_Edificio per uniformità
    if 'ID_FAB' in gdf_edifici.columns:
        gdf_edifici = gdf_edifici.rename(columns={'ID_FAB': 'ID_Edificio'})
        logger.info("Rinominata colonna ID_FAB in ID_Edificio")

    # Unisci dati CSV con geometrie usando ID_Edificio
    logger.info(f"Unione su ID_Edificio...")
    gdf_result = gdf_edifici.merge(df, on='ID_Edificio', how='inner')

    logger.info(f"Uniti {len(gdf_result)} edifici per {tipo}")
    logger.info(f"ID dopo merge: {gdf_result['ID_Edificio'].head().tolist()}")
    logger.info(f"Colonne dopo merge: {list(gdf_result.columns)}")

    return gdf_result

def calcola_storage_mensile_annuale(gdf_domanda: gpd.GeoDataFrame, gdf_offerta: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Calcola lo storage mensile e annuale basato sulla differenza tra offerta e domanda oraria.
    Restituisce SOLO le colonne storage (Sto_ms*, Sto_or*_ms*, Sto_an*, ID_ene) + ID_Edificio e geometry.
    """
    logger.info("Unione domanda e offerta per calcolo storage...")

    # DEBUG: Verifica le colonne disponibili
    logger.info(f"Colonne in gdf_domanda: {list(gdf_domanda.columns)}")
    logger.info(f"Colonne in gdf_offerta: {list(gdf_offerta.columns)}")

    # Verifica che ID_Edificio esista in entrambi i GeoDataFrame
    if 'ID_Edificio' not in gdf_domanda.columns:
        logger.error("Colonna ID_Edificio non trovata in gdf_domanda!")
        # Prova a vedere se c'è ID_FAB e usalo
        if 'ID_FAB' in gdf_domanda.columns:
            logger.info("Trovata colonna ID_FAB in gdf_domanda, la rinomino in ID_Edificio")
            gdf_domanda = gdf_domanda.rename(columns={'ID_FAB': 'ID_Edificio'})
        else:
            raise KeyError("Né ID_Edificio né ID_FAB trovati in gdf_domanda")

    if 'ID_Edificio' not in gdf_offerta.columns:
        logger.error("Colonna ID_Edificio non trovata in gdf_offerta!")
        # Prova a vedere se c'è ID_FAB e usalo
        if 'ID_FAB' in gdf_offerta.columns:
            logger.info("Trovata colonna ID_FAB in gdf_offerta, la rinomino in ID_Edificio")
            gdf_offerta = gdf_offerta.rename(columns={'ID_FAB': 'ID_Edificio'})
        else:
            raise KeyError("Né ID_Edificio né ID_FAB trovati in gdf_offerta")

    # Prepara le colonne per il join - ora usa ID_Edificio
    colonne_base = ['ID_Edificio', 'geometry']

    # Colonne domanda (D_or*, D_ms*, D_an*)
    domanda_cols = colonne_base + [col for col in gdf_domanda.columns if col.startswith(('D_or', 'D_ms', 'D_an'))]

    # Colonne offerta (O_or*, O_ms*, O_an*)
    offerta_cols = ['ID_Edificio'] + [col for col in gdf_offerta.columns if col.startswith(('O_or', 'O_ms', 'O_an'))]

    # Filtra solo le colonne che esistono effettivamente
    domanda_cols = [col for col in domanda_cols if col in gdf_domanda.columns]
    offerta_cols = [col for col in offerta_cols if col in gdf_offerta.columns]

    logger.info(f"Colonne domanda per join: {domanda_cols}")
    logger.info(f"Colonne offerta per join: {offerta_cols}")

    # Esegui il join - usa solo geometry da domanda per evitare duplicati
    gdf_join = gdf_domanda[domanda_cols].merge(
        gdf_offerta[offerta_cols],
        on='ID_Edificio',
        how='inner'
    )

    logger.info(f"Join completato: {len(gdf_join)} edifici uniti")
    logger.info(f"Colonne dopo join: {list(gdf_join.columns)}")

    # Estrai l'anno di riferimento dalle colonne annuali
    anno_riferimento = None
    an_cols_domanda = [col for col in gdf_join.columns if col.startswith('D_an')]
    an_cols_offerta = [col for col in gdf_join.columns if col.startswith('O_an')]

    if an_cols_domanda:
        # Estrai anno dalla prima colonna annuale trovata (es: 'D_an2023' -> 2023)
        try:
            anno_riferimento = int(an_cols_domanda[0].replace('D_an', ''))
        except ValueError:
            pass

    if anno_riferimento is None and an_cols_offerta:
        try:
            anno_riferimento = int(an_cols_offerta[0].replace('O_an', ''))
        except ValueError:
            pass

    # Default se non riesci a estrarre l'anno
    if anno_riferimento is None:
        anno_riferimento = 2023
        logger.warning(f"Anno di riferimento non determinato, uso default: {anno_riferimento}")
    else:
        logger.info(f"Anno di riferimento determinato: {anno_riferimento}")

    # Dizionario giorni per mese (considerando anno non bisestile)
    giorni_per_mese = {
        1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
    }

    # Inizializza le nuove colonne storage
    storage_data = []

    for idx, row in gdf_join.iterrows():
        storage_row = {'ID_Edificio': row['ID_Edificio']}

        # Calcola storage orario (Sto_or0_ms1, Sto_or1_ms1, ...)
        storage_mensile = {}  # Dizionario per accumulare storage mensile
        for mese in range(1, 13):
            storage_mensile[mese] = 0.0  # Inizializza storage mensile

            for ora in range(24):
                domanda_oraria_col = f'D_or{ora}_ms{mese}'
                offerta_oraria_col = f'O_or{ora}_ms{mese}'

                if domanda_oraria_col in row.index and offerta_oraria_col in row.index:
                    domanda_oraria = row[domanda_oraria_col] if pd.notna(row[domanda_oraria_col]) else 0
                    offerta_oraria = row[offerta_oraria_col] if pd.notna(row[offerta_oraria_col]) else 0

                    # Se offerta > domanda, differenza, altrimenti 0
                    storage_orario = max(0, offerta_oraria - domanda_oraria)
                    storage_row[f'Sto_or{ora}_ms{mese}'] = round(storage_orario, 4)

                    # Accumula nello storage mensile (somma delle ore)
                    storage_mensile[mese] += storage_orario
                else:
                    storage_row[f'Sto_or{ora}_ms{mese}'] = 0.0

        # MODIFICA: Calcola storage mensile (Sto_ms1, Sto_ms2, ...) come somma degli storage orari * giorni del mese
        storage_annuale = 0.0
        for mese in range(1, 13):
            giorni_mese = giorni_per_mese[mese]
            # Storage mensile = somma degli storage orari * giorni del mese
            storage_mensile_calcolato = storage_mensile[mese] * giorni_mese
            storage_row[f'Sto_ms{mese}'] = round(storage_mensile_calcolato, 4)

            # Accumula per lo storage annuale
            storage_annuale += storage_mensile_calcolato

        # MODIFICA: Storage annuale (Sto_an[anno]) = somma di tutti i mesi dell'anno
        storage_row[f'Sto_an{anno_riferimento}'] = round(storage_annuale, 4)

        # Calcola ID_ene - 1 se storage annuale > 0, 0 altrimenti
        storage_row['ID_ene'] = 1 if storage_annuale > 0 else 0

        logger.debug(f"Edificio {row['ID_Edificio']}: Storage annuale={storage_annuale}, ID_ene={storage_row['ID_ene']}")

        storage_data.append(storage_row)

    # Crea DataFrame con i dati storage
    df_storage = pd.DataFrame(storage_data)

    # MODIFICA: Riordina le colonne nell'ordine: anno, mese, ora
    colonne_ordinate = []

    # Prima le colonne base
    colonne_base = ['ID_Edificio', 'ID_ene']

    # Poi le colonne annuali (Sto_an*)
    colonne_annuali = [col for col in df_storage.columns if col.startswith('Sto_an')]

    # Poi le colonne mensili (Sto_ms*)
    colonne_mensili = [col for col in df_storage.columns if col.startswith('Sto_ms') and not col.startswith('Sto_or')]

    # Infine le colonne orarie (Sto_or*_ms*)
    colonne_orarie = [col for col in df_storage.columns if col.startswith('Sto_or')]

    # Ordina le colonne orarie per mese e poi per ora
    colonne_orarie_ordinate = []
    for mese in range(1, 13):
        for ora in range(24):
            colonna = f'Sto_or{ora}_ms{mese}'
            if colonna in colonne_orarie:
                colonne_orarie_ordinate.append(colonna)

    # Combina tutte le colonne nell'ordine desiderato
    colonne_ordinate = colonne_base + colonne_annuali + colonne_mensili + colonne_orarie_ordinate

    # Mantieni solo le colonne che esistono effettivamente
    colonne_ordinate = [col for col in colonne_ordinate if col in df_storage.columns]

    # Riordina il DataFrame
    df_storage = df_storage[colonne_ordinate]

    logger.info(f"Calcolo storage completato per {len(df_storage)} edifici")
    logger.info(f"Colonne storage ordinate: {list(df_storage.columns)}")

    # Unisci SOLO le colonne storage al GeoDataFrame originale
    # Manteniamo solo ID_Edificio, geometry e le colonne storage
    gdf_result = gdf_join[['ID_Edificio', 'geometry']].merge(df_storage, on='ID_Edificio', how='left')

    # Controlla se ci sono valori di storage > 0
    sto_cols = [col for col in gdf_result.columns if col.startswith('Sto_ms')]
    if sto_cols:
        total_storage = gdf_result[sto_cols].sum().sum()
        logger.info(f"Storage mensile totale calcolato: {total_storage} kWh")

    # Verifica presenza colonna Sto_an
    sto_an_col = f'Sto_an{anno_riferimento}'
    if sto_an_col in gdf_result.columns:
        total_storage_annuale = gdf_result[sto_an_col].sum()
        logger.info(f"Storage annuale totale ({sto_an_col}): {total_storage_annuale} kWh")

        # Log statistiche ID_ene
        edifici_con_storage = gdf_result['ID_ene'].sum()
        logger.info(f"Edifici con storage attivo (ID_ene=1): {edifici_con_storage}/{len(gdf_result)}")

    # Approssima tutte le colonne numeriche a 4 cifre decimali
    for col in gdf_result.columns:
        if gdf_result[col].dtype in ['float64', 'float32']:
            gdf_result[col] = gdf_result[col].round(4)

    # Log delle colonne finali
    logger.info(f"Colonne finali nel risultato: {list(gdf_result.columns)}")

    return gdf_result

def crea_join_semplice_domanda_offerta(provincia: str, comune: str, gdf_domanda: gpd.GeoDataFrame, gdf_offerta: gpd.GeoDataFrame):
    """
    Crea un file GPKG con il semplice join di domanda e offerta per ID_Edificio
    senza i calcoli dei valori storage.
    Il file viene salvato nella cartella domanda-offerta_energetica_[provincia]_[comune]
    """
    provincia_safe = safe_name(provincia)
    comune_safe = safe_name(comune)
    logger.info(f"Creazione join semplice domanda-offerta per {provincia_safe} - {comune_safe}")

    # Prepara le colonne per il join
    colonne_base = ['ID_Edificio', 'geometry']

    # Colonne domanda (D_or*, D_ms*, D_an*)
    domanda_cols = colonne_base + [col for col in gdf_domanda.columns if col.startswith(('D_or', 'D_ms', 'D_an'))]

    # Colonne offerta (O_or*, O_ms*, O_an*)
    offerta_cols = ['ID_Edificio'] + [col for col in gdf_offerta.columns if col.startswith(('O_or', 'O_ms', 'O_an'))]

    # Filtra solo le colonne che esistono effettivamente
    domanda_cols = [col for col in domanda_cols if col in gdf_domanda.columns]
    offerta_cols = [col for col in offerta_cols if col in gdf_offerta.columns]

    logger.info(f"Colonne domanda per join semplice: {domanda_cols}")
    logger.info(f"Colonne offerta per join semplice: {offerta_cols}")

    # Esegui il join semplice
    gdf_join_semplice = gdf_domanda[domanda_cols].merge(
        gdf_offerta[offerta_cols],
        on='ID_Edificio',
        how='inner'
    )

    logger.info(f"Join semplice completato: {len(gdf_join_semplice)} edifici uniti")
    logger.info(f"Colonne dopo join semplice: {list(gdf_join_semplice.columns)}")

    # Approssima tutte le colonne numeriche a 4 cifre decimali
    for col in gdf_join_semplice.columns:
        if gdf_join_semplice[col].dtype in ['float64', 'float32']:
            gdf_join_semplice[col] = gdf_join_semplice[col].round(4)

    # Crea la cartella domanda-offerta_energetica_[provincia]_[comune]
    out_dir_domanda_offerta = os.path.join(
        SHAPE_IN_DIR,
        f"{provincia_safe}_{comune_safe}",
        f"domanda-offerta_energetica_{provincia_safe}_{comune_safe}"
    )
    os.makedirs(out_dir_domanda_offerta, exist_ok=True)

    # Salva come join_domanda_offerta.gpkg
    join_semplice_path = os.path.join(out_dir_domanda_offerta, "join_domanda_offerta.gpkg")

    logger.info(f"Salvataggio join semplice in {join_semplice_path}")
    gdf_join_semplice.to_file(join_semplice_path, encoding="utf-8")
    logger.info("Join semplice domanda-offerta completato e salvato como join_domanda_offerta.gpkg")

    return gdf_join_semplice

def join_domanda_offerta(provincia: str, comune: str, gdf_domanda: gpd.GeoDataFrame, gdf_offerta: gpd.GeoDataFrame):
    """
    Esegue il join tra domanda energetica e offerta energetica sui fabbricati (ID_Edificio)
    e calcola lo storage mensile e annuale.
    Salva SOLO il file bilancio.gpkg nella cartella bilancio_[provincia]_[comune]
    all'interno della cartella [provincia]_[comune] in SHAPE_IN_DIR.
    """
    provincia_safe = safe_name(provincia)
    comune_safe = safe_name(comune)
    logger.info(f"Avvio join domanda-offerta per {provincia_safe} - {comune_safe}")

    # Calcola storage
    logger.info("Calcolo storage mensile e annuale...")
    gdf_join = calcola_storage_mensile_annuale(gdf_domanda, gdf_offerta)

    # MODIFICA: La cartella bilancio va dentro [provincia]_[comune] in SHAPE_IN_DIR
    out_dir = os.path.join(
        SHAPE_IN_DIR,
        f"{provincia_safe}_{comune_safe}",
        f"bilancio_{provincia_safe}_{comune_safe}"
    )
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    # Salva come bilancio.gpkg
    join_path = os.path.join(out_dir, "bilancio.gpkg")

    logger.info(f"Salvataggio bilancio in {join_path}")
    gdf_join.to_file(join_path, encoding="utf-8")
    logger.info("Bilancio domanda-offerta completato e salvato come bilancio.gpkg")

def calcola_deficit_orario(gdf_join_semplice: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Calcola i valori di deficit orario Def_or[numero]_ms[numero] per gli edifici NEB.
    Def_or[numero]_ms[numero] = D_or[numero]_ms[numero] - O_or[numero]_ms[numero]
    """
    logger.info("Calcolo deficit orario per edifici NEB...")

    # Crea una copia per non modificare l'originale
    gdf_neb = gdf_join_semplice.copy()

    # Calcola deficit per ogni ora di ogni mese
    for mese in range(1, 13):
        for ora in range(24):
            domanda_col = f'D_or{ora}_ms{mese}'
            offerta_col = f'O_or{ora}_ms{mese}'
            deficit_col = f'Def_or{ora}_ms{mese}'

            if domanda_col in gdf_neb.columns and offerta_col in gdf_neb.columns:
                # Deficit = Domanda - Offerta (solo se positivo, altrimenti 0)
                gdf_neb[deficit_col] = gdf_neb.apply(
                    lambda row: max(0, row[domanda_col] - row[offerta_col]) if pd.notna(row[domanda_col]) and pd.notna(row[offerta_col]) else 0.0,
                    axis=1
                )
                # Approssima a 4 cifre decimali
                gdf_neb[deficit_col] = gdf_neb[deficit_col].round(4)
            else:
                # Se le colonne non esistono, imposta deficit a 0
                gdf_neb[deficit_col] = 0.0

    logger.info(f"Calcolate {12 * 24} colonne di deficit orario")
    return gdf_neb

def crea_peb_neb(provincia: str, comune: str):
    """
    Genera i dataset PEB e NEB:
    - PEB: edifici con Sto_an[anno] > 0, contiene SOLO ID_Edificio, geometry e Sto_or[numero]_ms[numero]
    - NEB: edifici con Sto_an[anno] = 0, contiene SOLO ID_Edificio, geometry e Def_or[numero]_ms[numero]
    """
    provincia_safe = safe_name(provincia)
    comune_safe = safe_name(comune)
    logger.info(f"Avvio generazione PEB e NEB per {provincia_safe} - {comune_safe}")

    try:
        # Carica dati domanda/offerta
        gdf_domanda = carica_csv_con_geometria(provincia, comune, 'domanda')
        gdf_offerta = carica_csv_con_geometria(provincia, comune, 'offerta')

        gdf_join_semplice = crea_join_semplice_domanda_offerta(provincia, comune, gdf_domanda, gdf_offerta)
        join_domanda_offerta(provincia, comune, gdf_domanda, gdf_offerta)

        join_dir = os.path.join(SHAPE_IN_DIR, f"{provincia_safe}_{comune_safe}", f"bilancio_{provincia_safe}_{comune_safe}")
        join_path = os.path.join(join_dir, "bilancio.gpkg")

        if not os.path.exists(join_path):
            logger.error(f"File bilancio non trovato: {join_path}")
            return

        gdf_join = gpd.read_file(join_path)

        # Colonna annuale di riferimento
        sto_an_cols = [col for col in gdf_join.columns if col.startswith('Sto_an')]
        if sto_an_cols:
            sto_an_col = sto_an_cols[0]
        else:
            sto_cols = [col for col in gdf_join.columns if col.startswith('Sto_ms')]
            gdf_join['diff'] = gdf_join[sto_cols].sum(axis=1) if sto_cols else 0.0
            sto_an_col = 'diff'

        # Arrotonda numerici
        for col in gdf_join.columns:
            if gdf_join[col].dtype in ['float64', 'float32']:
                gdf_join[col] = gdf_join[col].round(4)

        # === PEB ===
        gdf_peb = gdf_join[gdf_join[sto_an_col] > 0].copy()
        colonne_orarie_peb = [f'Sto_or{ora}_ms{mese}' for mese in range(1,13) for ora in range(24) if f'Sto_or{ora}_ms{mese}' in gdf_peb.columns]
        gdf_peb = gdf_peb[['geometry', 'ID_Edificio'] + colonne_orarie_peb]
        # Rinomina ID_Edificio in ID_P per PEB
        gdf_peb = gdf_peb.rename(columns={'ID_Edificio': 'ID_P'})
        gdf_peb = gpd.GeoDataFrame(gdf_peb, geometry='geometry', crs=gdf_domanda.crs)

        # === NEB ===
        gdf_neb_storage = gdf_join[gdf_join[sto_an_col] <= 0].copy()
        id_neb = gdf_neb_storage['ID_Edificio'].tolist()
        gdf_neb_base = gdf_join_semplice[gdf_join_semplice['ID_Edificio'].isin(id_neb)].copy()
        gdf_neb = calcola_deficit_orario(gdf_neb_base)
        colonne_orarie_neb = [f'Def_or{ora}_ms{mese}' for mese in range(1,13) for ora in range(24) if f'Def_or{ora}_ms{mese}' in gdf_neb.columns]
        gdf_neb = gdf_neb[['geometry', 'ID_Edificio'] + colonne_orarie_neb]
        # Rinomina ID_Edificio in ID_N per NEB
        gdf_neb = gdf_neb.rename(columns={'ID_Edificio': 'ID_N'})
        gdf_neb = gpd.GeoDataFrame(gdf_neb, geometry='geometry', crs=gdf_domanda.crs)

        # Arrotonda numerici
        for gdf in [gdf_peb, gdf_neb]:
            for col in gdf.columns:
                if gdf[col].dtype in ['float64', 'float32']:
                    gdf[col] = gdf[col].round(4)

        # === Salvataggio con percorsi corretti ===
        out_dir_peb = os.path.join(OUTPUT_MODEL_BUILDER, f"{provincia_safe}_{comune_safe}", "input", "peb")
        out_dir_neb = os.path.join(OUTPUT_MODEL_BUILDER, f"{provincia_safe}_{comune_safe}", "input", "neb")
        os.makedirs(out_dir_peb, exist_ok=True)
        os.makedirs(out_dir_neb, exist_ok=True)

        peb_path = os.path.join(out_dir_peb, f"PEB_{provincia_safe}_{comune_safe}.gpkg")
        neb_path = os.path.join(out_dir_neb, f"NEB_{provincia_safe}_{comune_safe}.gpkg")

        logger.info(f"Salvataggio PEB in {peb_path}")
        gdf_peb.to_file(peb_path, encoding="utf-8")

        logger.info(f"Salvataggio NEB in {neb_path}")
        gdf_neb.to_file(neb_path, encoding="utf-8")

        logger.info("Generazione PEB e NEB completata.")
        logger.info(f"PEB generati: {len(gdf_peb)} | NEB generati: {len(gdf_neb)}")

        return len(gdf_peb), len(gdf_neb)

    except Exception as e:
        logger.error(f"Errore durante la generazione PEB/NEB: {e}")
        raise

if __name__ == '__main__':
    configure_logging_if_main(__name__)
    # Esempio di utilizzo
    # crea_peb_neb("salerno", "padula")