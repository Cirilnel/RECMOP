#!/usr/bin/env python3
"""
Script: crea_report_finale.py

Descrizione:
    Genera un report finale dettagliato in formato GeoPackage per TUTTI gli edifici.
    Versione con logica di calcolo e unione dati corretta e robusta.
"""
import logging
import os
import re
import shutil
import numpy as np
import pandas as pd
import geopandas as gpd

from model_builder.interazione_peb_neb import save_if_not_empty
from utils import safe_name

# Configurazione del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Dizionario per i giorni di ogni mese
DAYS_IN_MONTH = {
    1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
    7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
}

def crea_report_finale(provincia: str, comune: str):
    prov_safe = safe_name(provincia)
    com_safe = safe_name(comune)
    prov_com = f"{prov_safe}_{com_safe}"

    logger.info(f"--- Avvio creazione Report Finale per TUTTI gli edifici di {prov_com} ---")

    try:
        # --- 1. Definizione Percorsi e Caricamento Dati ---
        logger.info("Caricamento file di input...")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_recmop_dir = os.path.abspath(os.path.join(script_dir, ".."))
        mb_shapefiles_dir = os.path.join(base_recmop_dir, "model_builder_shapefiles", prov_com)
        data_collection_dir = os.path.join(base_recmop_dir, "Data_Collection", "shapefiles", prov_com)

        dom_off_path = os.path.join(data_collection_dir, f"domanda-offerta_energetica_{prov_com}", "join_domanda_offerta.gpkg")
        ncer_finale_path = os.path.join(mb_shapefiles_dir, "outputs", f"ncer_finale_{prov_com}.gpkg")
        offerta_csv_path = os.path.join(data_collection_dir, f"offerta_energetica_{prov_com}", f"offerta_oraria_mensile_{prov_com}.csv")
        peb_path = os.path.join(mb_shapefiles_dir, "input", "peb", f"PEB_{prov_com}.gpkg")

        gdf_report = gpd.read_file(dom_off_path)
        df_pannelli = pd.read_csv(offerta_csv_path, sep=';', decimal=',')
        gdf_peb_orario = gpd.read_file(peb_path) if os.path.exists(peb_path) else gpd.GeoDataFrame()
        gdf_ncer = gpd.read_file(ncer_finale_path) if os.path.exists(ncer_finale_path) else gpd.GeoDataFrame()

        # --- 2. Preparazione Dati e Join Iniziali ---
        logger.info("Preparazione e unione dei dati di base...")

        gdf_report.rename(columns={'ID_Edificio': 'ID_Edificio_Orig'}, inplace=True)
        df_pannelli.columns = [c.replace(' ', '_') for c in df_pannelli.columns]
        gdf_report = gdf_report.merge(df_pannelli[['ID_Edificio', 'Num_Pannelli', 'Potenza_Picco']], left_on='ID_Edificio_Orig', right_on='ID_Edificio', how='left').drop(columns='ID_Edificio', errors='ignore')

        if not gdf_ncer.empty:
            gdf_ncer['id_cer_num'] = range(1, len(gdf_ncer) + 1)
            lista_membri = []
            for idx, row in gdf_ncer.iterrows():
                id_edifici = str(row['ID_Edificio']).split('_')
                for id_ed in id_edifici:
                    lista_membri.append({ 'ID_Edificio_Orig_str': str(id_ed), 'ID_Cer': row['ID_Edificio'], 'iterazione': row['iterazione'], 'id_cer': row['id_cer_num']})
            df_membri = pd.DataFrame(lista_membri)
            gdf_report['ID_Edificio_Orig_str'] = gdf_report['ID_Edificio_Orig'].astype(str)
            gdf_report = gdf_report.merge(df_membri, on='ID_Edificio_Orig_str', how='left').drop(columns='ID_Edificio_Orig_str')

        if 'ID_Cer' in gdf_report.columns:
            gdf_report['ID_Cer'].fillna('0', inplace=True)
        else:
            gdf_report['ID_Cer'] = '0'
        logger.info("Sostituiti i valori NULL in 'ID_Cer' con '0'.")

        # --- MODIFICA RICHIESTA: Sostituzione NULL con 0 per la colonna 'iterazione' ---
        if 'iterazione' in gdf_report.columns:
            gdf_report['iterazione'].fillna(0, inplace=True)
        else:
            gdf_report['iterazione'] = 0
        logger.info("Sostituiti i valori NULL in 'iterazione' con 0.")
        # --- FINE MODIFICA ---

        # --- 3. Calcolo Parametri Energetici Individuali ---
        o_cols = [c for c in gdf_report.columns if c.startswith('O_or')]
        logger.info("Calcolo AF individuale orario...")
        for o_col in o_cols:
            d_col = o_col.replace('O_or', 'D_or')
            gdf_report[o_col.replace('O_or', 'AF_or_ID')] = np.minimum(gdf_report[o_col], gdf_report[d_col])

        logger.info("Calcolo storage mensile e annuale individuale...")
        if not gdf_peb_orario.empty:
            sto_or_cols = [c for c in gdf_peb_orario.columns if c.startswith('Sto_or')]
            for month in range(1, 13):
                col_name = f'Sto_ms{month}'
                sto_or_month = [c for c in sto_or_cols if re.search(rf'_ms{month}\b', c)]
                if sto_or_month:
                    gdf_peb_orario[col_name] = gdf_peb_orario[sto_or_month].sum(axis=1) * DAYS_IN_MONTH[month]
            cols_to_merge = ['ID_P'] + [f'Sto_ms{m}' for m in range(1, 13) if f'Sto_ms{m}' in gdf_peb_orario.columns]
            gdf_report = gdf_report.merge(gdf_peb_orario[cols_to_merge].drop_duplicates(subset='ID_P'), left_on='ID_Edificio_Orig', right_on='ID_P', how='left').drop(columns='ID_P', errors='ignore')

        sto_ms_cols_final = [f'Sto_ms{m}' for m in range(1, 13)]
        for col in sto_ms_cols_final:
            if col not in gdf_report.columns:
                gdf_report[col] = 0
            else:
                gdf_report[col] = pd.to_numeric(gdf_report[col], errors='coerce').fillna(0)
        gdf_report['Sto_an'] = gdf_report[sto_ms_cols_final].sum(axis=1)

        # --- CALCOLO AD INDIVIDUALE (LOGICA ORIGINALE) ---
        logger.info("Calcolo AD individuale...")
        gdf_membri_cer_ad = gdf_report[gdf_report['ID_Cer'] != '0'].copy()
        if not gdf_membri_cer_ad.empty and not gdf_peb_orario.empty:
            d_minus_af_or_cols = []
            for o_col in o_cols:
                suffix = o_col.replace('O_or', '')
                d_col, af_id_col, d_minus_af_col = f'D_or{suffix}', f'AF_or_ID{suffix}', f'D_minus_AF{suffix}'
                gdf_membri_cer_ad[d_minus_af_col] = gdf_membri_cer_ad[d_col] - gdf_membri_cer_ad[af_id_col]
                gdf_membri_cer_ad[f'Total_{d_minus_af_col}'] = gdf_membri_cer_ad.groupby('ID_Cer')[d_minus_af_col].transform('sum')
                d_minus_af_or_cols.append(d_minus_af_col)

            storage_cer_map = {}
            sto_or_cols = [c for c in gdf_peb_orario.columns if c.startswith('Sto_or')]
            for id_cer_unico in gdf_membri_cer_ad['ID_Cer'].unique():
                membri_ids = str(id_cer_unico).split('_')
                storage_tot_cer_orario = gdf_peb_orario[gdf_peb_orario['ID_P'].astype(str).isin(membri_ids)][sto_or_cols].sum()
                storage_cer_map[id_cer_unico] = storage_tot_cer_orario
            df_storage_cer = pd.DataFrame.from_dict(storage_cer_map, orient='index').reset_index().rename(columns={'index': 'ID_Cer'})
            gdf_membri_cer_ad = gdf_membri_cer_ad.merge(df_storage_cer, on='ID_Cer', how='left', suffixes=('', '_cer_total'))

            ad_or_id_cols = []
            for o_col in o_cols:
                suffix = o_col.replace('O_or', '')
                ad_col = f"AD_or_ID{suffix}"
                X_i, Total_X = gdf_membri_cer_ad[f'D_minus_AF{suffix}'], gdf_membri_cer_ad[f'Total_D_minus_AF{suffix}']
                Storage_tot = gdf_membri_cer_ad.get(f'Sto_or{suffix}', 0)
                ratio = (X_i / Total_X).fillna(0)
                gdf_membri_cer_ad[ad_col] = np.maximum(0, np.minimum(X_i, ratio * Storage_tot))
                ad_or_id_cols.append(ad_col)

            if ad_or_id_cols:
                gdf_report = gdf_report.merge(gdf_membri_cer_ad[['ID_Edificio_Orig'] + ad_or_id_cols], on='ID_Edificio_Orig', how='left')

        logger.info("Aggregazione finale mensile e annuale per AF_ID e AD_ID...")
        for prefix in ['AF_ID', 'AD_ID']:
            an_col = prefix.replace('_ID', '_an_ID')
            ms_cols_list = []
            for month in range(1, 13):
                ms_col = prefix.replace('_ID', f'_ms{month}_ID')
                or_cols_month = [c for c in gdf_report.columns if re.search(rf'_ms{month}\b', c) and c.startswith(prefix.replace('_ID','_or_ID'))]
                if or_cols_month:
                    gdf_report[ms_col] = gdf_report[or_cols_month].sum(axis=1, skipna=True) * DAYS_IN_MONTH[month]
                    ms_cols_list.append(ms_col)
                else:
                    gdf_report[ms_col] = 0
            gdf_report[ms_cols_list] = gdf_report[ms_cols_list].fillna(0)
            gdf_report[an_col] = gdf_report[ms_cols_list].sum(axis=1, skipna=True) if ms_cols_list else 0

        logger.info("Calcolo di D_an e O_an come somma dei valori mensili...")
        d_ms_cols = [f'D_ms{m}' for m in range(1, 13)]
        o_ms_cols = [f'O_ms{m}' for m in range(1, 13)]
        gdf_report['D_an'] = gdf_report[d_ms_cols].sum(axis=1)
        gdf_report['O_an'] = gdf_report[o_ms_cols].sum(axis=1)
        logger.info("Calcolo di D_an e O_an completato.")

        # --- 4. Assemblaggio Finale e Calcoli a Livello di CER ---
        logger.info("Avvio assemblaggio finale e calcoli a livello di CER...")

        gdf_report['Em_evit_ID'] = (gdf_report['O_an'] * 0.268) / 1000
        logger.info("Calcolate le emissioni evitate individuali (Em_evit_ID).")

        gdf_membri_cer = gdf_report[gdf_report['ID_Cer'] != '0'].copy()
        if not gdf_membri_cer.empty:
            logger.info(f"Aggregazione dei dati per {gdf_membri_cer['ID_Cer'].nunique()} CER...")
            cols_to_sum = ['Em_evit_ID']
            for m in range(1, 13):
                cols_to_sum.extend([f'D_ms{m}', f'O_ms{m}', f'Sto_ms{m}', f'AD_ms{m}_ID'])

            # Assicurarsi che le colonne AD esistano prima di aggregarle
            existing_ad_cols = [c for c in cols_to_sum if c in gdf_membri_cer.columns]
            df_cer_aggregati = gdf_membri_cer.groupby('ID_Cer')[existing_ad_cols].sum()

            rename_dict = {'Em_evit_ID': 'Em_evit_CER'}
            for m in range(1, 13):
                rename_dict.update({f'D_ms{m}': f'D_ms{m}_CER', f'O_ms{m}': f'O_ms{m}_CER', f'Sto_ms{m}': f'Sto_ms{m}_CER', f'AD_ms{m}_ID': f'AD_ms{m}_CER'})
            df_cer_aggregati.rename(columns=rename_dict, inplace=True)
            gdf_report = gdf_report.merge(df_cer_aggregati, on='ID_Cer', how='left')

        logger.info("Calcolo dei totali annuali a livello di CER...")
        annual_prefixes_map = {'D_ms': 'D_an', 'O_ms': 'O_an', 'Sto_ms': 'Sto_an', 'AD_ms': 'AD_an'}
        for prefix, annual_prefix in annual_prefixes_map.items():
            monthly_cols_cer = [f'{prefix}{m}_CER' for m in range(1, 13)]
            annual_col_cer = f'{annual_prefix}_CER'
            existing_cols = [col for col in monthly_cols_cer if col in gdf_report.columns]
            if existing_cols:
                gdf_report[annual_col_cer] = gdf_report[existing_cols].sum(axis=1)

        gdf_report.rename(columns={'ID_Edificio_Orig': 'ID_Edificio'}, inplace=True)
        cer_cols = [col for col in gdf_report.columns if '_CER' in col]
        gdf_report[cer_cols] = gdf_report[cer_cols].fillna(0)

        final_order = [
            'ID_Edificio', 'ID_Cer', 'iterazione', 'Num_Pannelli', 'Potenza_Picco',
            'D_an', 'O_an', 'AF_an_ID', 'AD_an_ID', 'Sto_an', 'Em_evit_ID',
            'D_an_CER', 'O_an_CER', 'AD_an_CER', 'Sto_an_CER', 'Em_evit_CER'
        ]
        # Aggiunta di AF_an_CER all'ordine finale, se esiste
        if 'AF_an_CER' not in final_order: final_order.insert(14, 'AF_an_CER')

        monthly_groups = [('D_ms', ''), ('D_ms', '_CER'), ('O_ms', ''), ('O_ms', '_CER'), ('AF_ms', '_ID'),
                          ('AD_ms', '_ID'), ('AD_ms', '_CER'), ('Sto_ms', ''), ('Sto_ms', '_CER')]
        for prefix, suffix in monthly_groups:
            for m in range(1, 13):
                final_order.append(f'{prefix}{m}{suffix}')
        final_order.append('geometry')

        final_columns_to_keep = [col for col in final_order if col in gdf_report.columns]
        gdf_report_final = gdf_report[final_columns_to_keep]

        numeric_cols = gdf_report_final.select_dtypes(include=np.number).columns
        gdf_report_final[numeric_cols] = gdf_report_final[numeric_cols].round(4)

        # --- 5. Salvataggio ---
        report_dir = os.path.join(mb_shapefiles_dir, "report_finale")
        if os.path.exists(report_dir): shutil.rmtree(report_dir)
        os.makedirs(report_dir, exist_ok=True)
        output_path = os.path.join(report_dir, f"report_dettagliato_{prov_com}.gpkg")
        logger.info(f"Salvataggio del report finale in: {output_path}")
        save_if_not_empty(gdf_report_final, output_path)
        logger.info("--- Report Finale creato con successo! ---")

    except FileNotFoundError as e:
        logger.error(f"ERRORE: File di input non trovato. Dettagli: {e}")
    except KeyError as e:
        logger.error(f"ERRORE DI CHIAVE: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Si è verificato un errore imprevisto: {e}", exc_info=True)

if __name__ == '__main__':
    PROVINCIA = "salerno"
    COMUNE = "padula"
    crea_report_finale(provincia=PROVINCIA, comune=COMUNE)