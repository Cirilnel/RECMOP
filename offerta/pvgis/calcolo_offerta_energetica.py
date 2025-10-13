import os
import shutil
import logging
import pandas as pd
import geopandas as gpd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import safe_name, load_dot_env, calcola_area

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dot_env(os.path.join(BASE_DIR, '.env'))

FABBRICATI_BASE = os.path.join(BASE_DIR, 'FABBRICATI')
VINCOLI_BASE = os.path.join(BASE_DIR, 'VINCOLI')
SHAPE_OUT_DIR = os.path.join(BASE_DIR, 'Data_Collection', 'shapefiles')
PANEL_DATA_PATH = os.path.join(BASE_DIR, 'offerta', 'panel', 'panels.csv')

PVGIS_SERIES_URL = "https://re.jrc.ec.europa.eu/api/v5_3/seriescalc"


def _query_pvgis_hourly_full(lat, lon, tilt_deg=10, azimuth_deg=0, year=2023, peak_power_kw=None, loss_percent=0):
    """
    Restituisce DataFrame orario da PVGIS.
    Se peak_power_kw > 0 calcola produzione FV completa, altrimenti restituisce solo radiazione G(i).
    """
    params = {
        "lat": lat,
        "lon": lon,
        "startyear": int(year),
        "endyear": int(year),
        "outputformat": "json",
        "angle": tilt_deg,
        "aspect": azimuth_deg,
        "raddatabase": "PVGIS-SARAH3",
        "usehorizon": 1
    }

    if peak_power_kw is not None and peak_power_kw > 0:
        params.update({
            "pvcalculation": 1,
            "peakpower": float(peak_power_kw),
            "pvtechchoice": "crystSi",
            "mountingplace": "free",
            "loss": float(loss_percent)
        })

    try:
        r = requests.get(PVGIS_SERIES_URL, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        return pd.DataFrame(data.get("outputs", {}).get("hourly", []))
    except Exception as e:
        logger.warning(f"[PVGIS] seriescalc failed for ({lat},{lon}), peak_kW={peak_power_kw}: {e}")
        return pd.DataFrame()


def calculate_building_hourly_production(provincia: str, comune: str, idx_panel: int,
                                         use_vincoli: bool = True, max_workers: int = 4,
                                         pvgis_year: int = 2023) -> gpd.GeoDataFrame:
    """
    Calcola produzione oraria per ogni edificio e salva:
    - CSV con dati aggregati per ogni edifici (offerta_oraria_mensile_provincia_comune.csv)
    """
    prov_safe = safe_name(provincia)
    com_safe = safe_name(comune)

    shapefolder = os.path.join(FABBRICATI_BASE, f'fabbricati_{prov_safe}_{com_safe}')
    vincoli_folder = os.path.join(VINCOLI_BASE, f'vincoli_{prov_safe}_{com_safe}')

    shp_list = [f for f in os.listdir(shapefolder) if f.lower().endswith('.shp')]
    if not shp_list:
        raise FileNotFoundError(f"Nessuno shapefile trovato in {shapefolder}")
    shp = os.path.join(shapefolder, shp_list[0])
    gdf = gpd.read_file(shp)

    # Identifica gli edifici in vincoli PRIMA di qualsiasi calcolo
    edifici_in_vincoli = []
    if use_vincoli and os.path.isdir(vincoli_folder):
        vincoli_list = [f for f in os.listdir(vincoli_folder) if f.lower().endswith('.shp')]
        if vincoli_list:
            gdf_vincoli = gpd.read_file(os.path.join(vincoli_folder, vincoli_list[0]))
            if gdf.crs != gdf_vincoli.crs:
                gdf_vincoli = gdf_vincoli.to_crs(gdf.crs)
            centroids = gdf.geometry.centroid
            vincoli_union = gdf_vincoli.unary_union

            for idx, row in gdf.iterrows():
                building_id = row.get('ID_FAB', f"FAB_{idx}")
                if centroids.iloc[idx].within(vincoli_union):
                    edifici_in_vincoli.append(building_id)
                    logger.info(f"🔴 VINCOLI RILEVATO - Edificio {building_id}: saltato calcolo PVGIS")

            logger.info(f"Edifici in vincoli: {len(edifici_in_vincoli)}")

    # Crea un set per controllo rapido
    vincoli_set = set(edifici_in_vincoli)

    # Area edifici in m² - per TUTTI gli edifici
    gdf = calcola_area(gdf, nome_colonna='area')

    # Lat/Lon dei centroidi - per TUTTI gli edifici
    gdf = gdf.to_crs(epsg=4326)
    gdf['centroid'] = gdf.geometry.centroid
    gdf['lat'] = gdf.centroid.y
    gdf['lon'] = gdf.centroid.x

    # Dati pannello
    panel_df = pd.read_csv(PANEL_DATA_PATH, delimiter=';', decimal=',', na_values=['n.a.', 'N.A.', 'na', 'NA', '-', ''])
    for col in ['Potenza (Wp)', 'Efficienza (%)', 'Prezzo', 'Sup+30%']:
        panel_df[col] = pd.to_numeric(panel_df[col], errors='coerce')
    panel_df.dropna(subset=['Potenza (Wp)', 'Efficienza (%)', 'Prezzo', 'Sup+30%'], inplace=True)
    specs = panel_df.iloc[idx_panel]

    # Calcolo numero pannelli e potenza - per TUTTI gli edifici
    gdf['num_PV'] = (gdf['area'] / specs['Sup+30%']).astype(int).clip(lower=0)
    gdf['peak_kWp'] = gdf['num_PV'] * specs['Potenza (Wp)'] / 1000.0
    loss_percent = specs['Efficienza (%)']

    # Reset index per avere un ID univoco
    gdf = gdf.reset_index(drop=True)

    # Crea un mapping tra indice e ID_FAB
    id_mapping = {}
    for idx, row in gdf.iterrows():
        building_id = row.get('ID_FAB', f"FAB_{idx}")
        id_mapping[idx] = building_id

    # MODIFICA: Preparazione dati SOLO per edifici NON in vincoli e CON pannelli
    edifici_da_processare = []
    edifici_senza_pannelli = []

    for idx, row in gdf.iterrows():
        building_id = id_mapping[idx]

        # Controllo VINCOLI - se è in vincoli, salta completamente
        if building_id in vincoli_set:
            continue

        # Controllo PANNELLI - se non ha pannelli, salta
        if row['num_PV'] <= 0:
            edifici_senza_pannelli.append(building_id)
            logger.info(f"🟡 NO PANNELLI - Edificio {building_id}: nessun pannello installabile")
        else:
            edifici_da_processare.append((idx, building_id, row['lat'], row['lon'], row['peak_kWp']))
            logger.info(f"✅ PROCESS OFFERTA - Edificio {building_id}: {row['num_PV']} pannelli, {row['peak_kWp']:.2f} kWp")

    # Richieste PVGIS parallele - SOLO per edifici NON in vincoli E con pannelli
    results = {}
    if edifici_da_processare:
        logger.info(f"Avvio {len(edifici_da_processare)} richieste PVGIS per edifici senza vincoli e con pannelli")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(
                _query_pvgis_hourly_full,
                lat,
                lon,
                10,
                0,
                pvgis_year,
                peak_kWp,
                loss_percent
            ): (idx, building_id) for idx, building_id, lat, lon, peak_kWp in edifici_da_processare}

            for fut in as_completed(futures):
                idx, building_id = futures[fut]
                results[idx] = fut.result()
                logger.info(f"📡 PVGIS COMPLETATO - Edificio {building_id}")

    # Creazione dati per TUTTI gli edifici
    all_buildings_data = []

    # 1. Processa edifici con produzione reale (NON in vincoli, CON pannelli, PVGIS successo)
    all_records = []
    for idx, df_hourly in results.items():
        building_id = id_mapping[idx]

        if df_hourly.empty:
            logger.info(f"🔄 PVGIS FALLITO - Edificio {building_id}: nessun dato disponibile")
            # Verrà gestito come edificio senza produzione
            continue

        df_hourly = df_hourly.copy()

        try:
            df_hourly['datetime'] = pd.to_datetime(df_hourly['time'], format='%Y%m%d:%H%M', utc=True)
        except Exception:
            df_hourly['datetime'] = pd.to_datetime(df_hourly['time'], utc=True, errors='coerce')
        df_hourly = df_hourly.dropna(subset=['datetime'])
        if df_hourly.empty:
            logger.info(f"🔄 DATETIME ERRORE - Edificio {building_id}: problemi conversione")
            continue

        df_hourly['anno'] = df_hourly['datetime'].dt.year
        df_hourly['mese'] = df_hourly['datetime'].dt.month
        df_hourly['giorno'] = df_hourly['datetime'].dt.day
        df_hourly['ora'] = df_hourly['datetime'].dt.hour

        # Produzione in kWh
        if 'P' in df_hourly.columns:
            df_hourly['Produzione [kWh]'] = df_hourly['P']/1000  # kW → kWh

        df_hourly['building_id'] = building_id
        all_records.append(df_hourly[['building_id', 'anno', 'mese', 'giorno', 'ora', 'Produzione [kWh]']])

    # Processa i dati PVGIS per gli edifici con produzione reale
    if all_records:
        df_all = pd.concat(all_records, ignore_index=True)
        df_all['Produzione [kWh]'] = df_all['Produzione [kWh]'].round(4)

        for building_id, df_b in df_all.groupby('building_id'):
            # Pivot ore x mesi per calcoli interni
            pivot = df_b.pivot_table(
                index='ora',
                columns='mese',
                values='Produzione [kWh]',
                aggfunc='mean'
            ).fillna(0)

            # Riordino ore e mesi, riempiendo valori mancanti con 0
            pivot = pivot.reindex(index=range(24), fill_value=0).reindex(columns=range(1, 13), fill_value=0)

            # Calcolo totale mensile: somma delle ore per numero di giorni del mese
            giorni_per_mese = df_b.groupby('mese')['giorno'].nunique()
            totale_mensile = pivot.sum(axis=0) * giorni_per_mese
            totale_mensile = totale_mensile.round(4)

            # Calcolo totale annuo
            totale_annuo = totale_mensile.sum().round(4)
            anno_effettivo = df_b['anno'].iloc[0] if not df_b.empty else pvgis_year

            # Creo un dizionario con i dati dell'edificio
            building_data = {'ID_Edificio': building_id}
            building_data['ID_Pannello'] = idx_panel + 1

            # Metadati essenziali
            building_info = gdf[gdf['ID_FAB'] == building_id].iloc[0] if 'ID_FAB' in gdf.columns else None
            if building_info is not None:
                building_data['Num_Pannelli'] = building_info.get('num_PV', 0)
                building_data['Potenza_Picco'] = round(building_info.get('peak_kWp', 0), 4)

            # Totale annuale
            building_data[f'O_an{anno_effettivo}'] = round(totale_annuo, 4)

            # Totali mensili
            for mese in range(1, 13):
                building_data[f'O_ms{mese}'] = round(totale_mensile.get(mese, 0), 4)

            # Dati orari
            for mese in range(1, 13):
                for ora in range(24):
                    valore = pivot.loc[ora, mese] if ora in pivot.index and mese in pivot.columns else 0
                    building_data[f'O_or{ora}_ms{mese}'] = round(valore, 4)

            all_buildings_data.append(building_data)
            logger.info(f"✅ PRODUZIONE REALE - Edificio {building_id}: {totale_annuo:.2f} kWh/anno")

    # 2. MODIFICA: Crea dati per TUTTI gli altri edifici (in vincoli, senza pannelli, PVGIS fallito)
    for idx, row in gdf.iterrows():
        building_id = id_mapping[idx]

        # Se già processato con produzione reale, salta
        if any(b['ID_Edificio'] == building_id for b in all_buildings_data):
            continue

        # Crea record con valori a 0
        building_data = {
            'ID_Edificio': building_id,
            'ID_Pannello': idx_panel + 1,
            'Num_Pannelli': 0,
            'Potenza_Picco': 0.0,
            f'O_an{pvgis_year}': 0.0
        }

        # Tutti i mesi a 0
        for mese in range(1, 13):
            building_data[f'O_ms{mese}'] = 0.0

        # Tutte le ore a 0
        for mese in range(1, 13):
            for ora in range(24):
                building_data[f'O_or{ora}_ms{mese}'] = 0.0

        all_buildings_data.append(building_data)

        # Log appropriato in base al motivo
        if building_id in vincoli_set:
            logger.info(f"🔴 VINCOLI - Edificio {building_id}: valori impostati a 0")
        elif building_id in edifici_senza_pannelli:
            logger.info(f"🟡 NO PANNELLI - Edificio {building_id}: valori impostati a 0")
        else:
            logger.info(f"🔄 ALTRO - Edificio {building_id}: valori impostati a 0 (PVGIS fallito)")

    # Salvataggio directory
    subdir = f'{prov_safe}_{com_safe}'
    outdir = os.path.join(SHAPE_OUT_DIR, subdir, f'offerta_energetica_{subdir}')
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)

    # Creo DataFrame finale con TUTTI gli edifici
    if all_buildings_data:
        df_pivot_all = pd.DataFrame(all_buildings_data)

        # Riordino colonne
        colonne_ordinate = ['ID_Edificio', 'ID_Pannello', 'Num_Pannelli', 'Potenza_Picco']
        colonne_ordinate.append(f'O_an{pvgis_year}')
        colonne_ordinate.extend([f'O_ms{mese}' for mese in range(1, 13)])
        for mese in range(1, 13):
            colonne_ordinate.extend([f'O_or{ora}_ms{mese}' for ora in range(24)])

        # Seleziono solo le colonne esistenti
        colonne_esistenti = [col for col in colonne_ordinate if col in df_pivot_all.columns]
        df_pivot_all = df_pivot_all[colonne_esistenti]

        # Salvataggio del file
        csv_filename = f'offerta_oraria_mensile_{prov_safe}_{com_safe}.csv'
        csv_pivot_all = os.path.join(outdir, csv_filename)
        df_pivot_all.to_csv(csv_pivot_all, index=False, sep=';', decimal=',')
        logger.info(f"File offerta salvato: {csv_pivot_all}")
        logger.info(f"Totale edifici nel file: {len(all_buildings_data)}")
    else:
        # Creazione file CSV vuoto
        colonne_base = ['ID_Edificio', 'ID_Pannello', 'Num_Pannelli', 'Potenza_Picco']
        colonne_annuale = [f'O_an{pvgis_year}']
        colonne_mensili = [f'O_ms{mese}' for mese in range(1, 13)]
        colonne_orarie = []
        for mese in range(1, 13):
            colonne_orarie.extend([f'O_or{ora}_ms{mese}' for ora in range(24)])

        colonne_complete = colonne_base + colonne_annuale + colonne_mensili + colonne_orarie

        df_vuoto = pd.DataFrame(columns=colonne_complete)
        csv_filename = f'offerta_oraria_mensile_{prov_safe}_{com_safe}.csv'
        csv_pivot_all = os.path.join(outdir, csv_filename)
        df_vuoto.to_csv(csv_pivot_all, index=False, sep=';', decimal=',')
        logger.info(f"File CSV vuoto creato: {csv_pivot_all}")

    # RIEPILOGO FINALE
    logger.info("=== RIEPILOGO OFFERTA FOTOVOLTAICA ===")
    logger.info(f"Edifici totali: {len(gdf)}")
    logger.info(f"Edifici in vincoli: {len(edifici_in_vincoli)}")
    logger.info(f"Edifici senza pannelli: {len(edifici_senza_pannelli)}")
    logger.info(f"Edifici con produzione reale: {len(results)}")
    logger.info(f"Totale nel file CSV: {len(all_buildings_data)}")

    return gdf