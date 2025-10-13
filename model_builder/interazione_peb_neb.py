#!/usr/bin/env python3
"""
Script: interazione_peb_neb.py (versione con dissolve geometrie corretto)

Descrizione:
    Corregge la logica di dissolve per unire correttamente le geometrie di TUTTI
    i membri (PEB e NEB) che formano una NCER.
"""
import logging
import os
import sys
import shutil
import re
import numpy as np
import pandas as pd
import geopandas as gpd

# Configurazione del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Dizionario per i giorni di ogni mese
DAYS_IN_MONTH = {
    1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
    7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
}

def safe_name(name: str) -> str:
    """Normalizza un nome per l'uso nei percorsi di file."""
    return name.strip().lower().replace(" ", "_")

def save_if_not_empty(gdf: gpd.GeoDataFrame, path: str, driver: str = 'GPKG', **kwargs):
    """Salva il GeoDataFrame solo se non è vuoto."""
    if not gdf.empty:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        gdf.to_file(path, driver=driver, **kwargs)
        logger.info(f"Salvato file con {len(gdf)} feature in: {path}")
    else:
        logger.warning(f"GeoDataFrame vuoto, nessun file salvato per: {path}")

class InterazionePebNeb:
    """
    Classe che incapsula la logica di calcolo per una singola iterazione.
    """

    def _calculate_energetics(self, ncer_gdf: gpd.GeoDataFrame, dom_off_gdf: gpd.GeoDataFrame, ncer_storage: pd.DataFrame) -> gpd.GeoDataFrame:
        # ... (Questa funzione rimane invariata) ...
        if ncer_gdf.empty:
            return ncer_gdf

        logger.info("Avvio calcoli energetici per le NCER...")

        all_building_ids = set()
        for id_str in ncer_gdf['ID_Edificio'].unique():
            all_building_ids.update(str(id_str).split('_'))

        dom_off_relevant = dom_off_gdf[dom_off_gdf['ID_Edificio'].astype(str).isin(all_building_ids)].copy()

        id_to_ncer_map = {}
        for id_edificio_ncer in ncer_gdf['ID_Edificio'].unique():
            parts = str(id_edificio_ncer).split('_')
            for part in parts:
                id_to_ncer_map[part] = id_edificio_ncer

        dom_off_relevant['ID_Edificio_NCER'] = dom_off_relevant['ID_Edificio'].astype(str).map(id_to_ncer_map)

        o_cols = [c for c in dom_off_gdf.columns if c.startswith('O_or')]

        af_individual = dom_off_relevant.copy()
        for o_col in o_cols:
            d_col = o_col.replace('O_or', 'D_or')
            af_col = o_col.replace('O_or', 'AF_or')
            if d_col in af_individual.columns:
                af_individual[af_col] = np.minimum(af_individual[o_col], af_individual[d_col])

        af_or_cols = [c for c in af_individual.columns if c.startswith('AF_or')]
        ncer_af_profiles = af_individual.groupby('ID_Edificio_NCER')[af_or_cols].sum().reset_index()
        ncer_af_profiles = ncer_af_profiles.rename(columns={'ID_Edificio_NCER': 'ID_Edificio'})

        af_an_cols = []
        for month in range(1, 13):
            af_ms_col = f'AF_ms{month}'
            af_or_cols_month = [c for c in ncer_af_profiles.columns if re.search(rf'_ms{month}\b', c) and c.startswith('AF_or')]
            if af_or_cols_month:
                ncer_af_profiles[af_ms_col] = ncer_af_profiles[af_or_cols_month].sum(axis=1) * DAYS_IN_MONTH[month]
                af_an_cols.append(af_ms_col)
        ncer_af_profiles['AF_an'] = ncer_af_profiles[af_an_cols].sum(axis=1)

        logger.info("Calcolo Autoconsumo Differito (AD) con logica granulare...")
        d_minus_af_individual = dom_off_relevant.copy()
        for o_col in o_cols:
            suffix = o_col.replace('O_or', '')
            d_col = f'D_or{suffix}'

            af_individuale = np.minimum(d_minus_af_individual[o_col], d_minus_af_individual[d_col])
            d_minus_af_col = f"D_minus_AF{suffix}"
            d_minus_af_individual[d_minus_af_col] = d_minus_af_individual[d_col] - af_individuale

        d_minus_af_cols = [c for c in d_minus_af_individual.columns if c.startswith('D_minus_AF')]
        for col in d_minus_af_cols:
            total_col_name = f"Total_{col}"
            d_minus_af_individual[total_col_name] = d_minus_af_individual.groupby('ID_Edificio_NCER')[col].transform('sum')

        d_minus_af_individual = d_minus_af_individual.merge(ncer_storage, left_on='ID_Edificio_NCER', right_on='ID_Edificio', how='left', suffixes=('', '_storage'))

        for o_col in o_cols:
            suffix = o_col.replace('O_or', '')
            sto_col = f'Sto_or{suffix}'
            d_minus_af_col = f"D_minus_AF{suffix}"
            total_d_minus_af_col = f"Total_{d_minus_af_col}"
            ad_individual_col = f"AD_ind{suffix}"

            if d_minus_af_col in d_minus_af_individual.columns and sto_col in d_minus_af_individual.columns:
                X_i = d_minus_af_individual[d_minus_af_col]
                Total_X = d_minus_af_individual[total_d_minus_af_col]
                Storage_tot = d_minus_af_individual[sto_col]
                ratio = (X_i / Total_X).fillna(0)
                potential_ad = ratio * Storage_tot
                d_minus_af_individual[ad_individual_col] = np.maximum(0, np.minimum(X_i, potential_ad))

        ad_individual_cols = [c for c in d_minus_af_individual.columns if c.startswith('AD_ind')]
        ncer_ad_profiles = d_minus_af_individual.groupby('ID_Edificio_NCER')[ad_individual_cols].sum().reset_index()
        ncer_ad_profiles = ncer_ad_profiles.rename(columns={'ID_Edificio_NCER': 'ID_Edificio'})

        rename_dict = {col: col.replace('AD_ind', 'AD_or') for col in ad_individual_cols}
        ncer_ad_profiles = ncer_ad_profiles.rename(columns=rename_dict)

        ad_an_cols = []
        for month in range(1, 13):
            ad_ms_col = f'AD_ms{month}'
            ad_or_cols_month = [c for c in ncer_ad_profiles.columns if re.search(rf'_ms{month}\b', c) and c.startswith('AD_or')]
            if ad_or_cols_month:
                ncer_ad_profiles[ad_ms_col] = ncer_ad_profiles[ad_or_cols_month].sum(axis=1) * DAYS_IN_MONTH[month]
                ad_an_cols.append(ad_ms_col)
        ncer_ad_profiles['AD_an'] = ncer_ad_profiles[ad_an_cols].sum(axis=1)

        af_final_cols = [c for c in ncer_af_profiles.columns if c.startswith(('AF_ms', 'AF_an'))]
        ad_final_cols = [c for c in ncer_ad_profiles.columns if c.startswith(('AD_ms', 'AD_an'))]

        ncer_profiles = ncer_storage.merge(ncer_af_profiles[['ID_Edificio'] + af_final_cols], on='ID_Edificio', how='left')
        ncer_profiles = ncer_profiles.merge(ncer_ad_profiles[['ID_Edificio'] + ad_final_cols], on='ID_Edificio', how='left')

        logger.info("Calcolo dell'Indice...")
        ncer_profiles['Indice'] = (ncer_profiles['AD_an'] / ncer_profiles['Sto_an']).fillna(0)
        ncer_profiles['Indice'] = ncer_profiles['Indice'].replace([np.inf, -np.inf], 0)

        final_cols = [c for c in ncer_profiles.columns if c.startswith(('AF_ms', 'AD_ms', 'AF_an', 'AD_an', 'Sto_an')) or c == 'Indice']
        final_cols.append('ID_Edificio')

        ncer_gdf = ncer_gdf.merge(ncer_profiles[final_cols], on='ID_Edificio', how='left')
        return ncer_gdf

    def process_algorithm(self, gdf_positivo: gpd.GeoDataFrame, gdf_negativo: gpd.GeoDataFrame, gdf_dom_off: gpd.GeoDataFrame, indice_soglia: float) -> dict:
        try:
            if gdf_positivo.empty or gdf_negativo.empty:
                return {'NCER': gpd.GeoDataFrame(), 'PED2': gdf_positivo, 'NED2': gdf_negativo}

            o_cols = [c for c in gdf_dom_off.columns if c.startswith('O_or')]

            logger.info("Unione dei layer...")
            gdf_negativo['ID_N_unico'] = np.arange(len(gdf_negativo))
            gdf = gpd.sjoin_nearest(gdf_positivo.copy(), gdf_negativo.copy(), how='left', rsuffix='_neg')
            gdf = gdf.drop(columns=['index_neg'], errors='ignore')

            # ... (Logica di calcolo delta/count, sommatorie e filtri...)
            logger.info("Calcolo delta e count...")
            sto_cols_names = [c for c in gdf.columns if c.startswith('Sto_or')]
            new_delta_cols = {}
            for sto_col in sto_cols_names:
                suffix = sto_col.replace('Sto_', '')
                def_col = f'Def_{suffix}'
                if def_col in gdf.columns:
                    delta_col_name = f'delta_{suffix}'
                    cou_del_col_name = f'cou_del_{suffix}'
                    delta_values = gdf[sto_col] - gdf[def_col]
                    new_delta_cols[delta_col_name] = delta_values
                    new_delta_cols[cou_del_col_name] = np.where(delta_values > 0, 1, 0)
            gdf = gdf.assign(**new_delta_cols)

            logger.info("Calcolo sommatorie...")
            count_cols = [c for c in gdf.columns if c.startswith('cou_del_')]
            gdf['somma_count'] = gdf[count_cols].sum(axis=1) if count_cols else 0
            delta_cols = [c for c in gdf.columns if c.startswith('delta_')]
            gdf['somma_delta'] = gdf[delta_cols].sum(axis=1) if delta_cols else 0

            logger.info("Filtraggio per massimi...")
            gdf['max_somma_count'] = gdf.groupby('ID_N')['somma_count'].transform('max')
            gdf_filtrato_1 = gdf[gdf['somma_count'] == gdf['max_somma_count']].copy()
            gdf_filtrato_1['max_somma_delta'] = gdf_filtrato_1.groupby('ID_N')['somma_delta'].transform('max')
            configurazioni_potenziali = gdf_filtrato_1[gdf_filtrato_1['somma_delta'] == gdf_filtrato_1['max_somma_delta']].copy()

            ncer_final_gdf, ncer_successo, ncer_fallimento = gpd.GeoDataFrame(), gpd.GeoDataFrame(), gpd.GeoDataFrame()

            if not configurazioni_potenziali.empty:
                logger.info("Aggregazione dati per calcoli energetici...")
                id_peb_aggregati = configurazioni_potenziali.groupby('ID_N')['ID_P'].apply(
                    lambda ids: '_'.join(ids.astype(str).unique())
                ).reset_index(name='ID_P_agg')
                id_peb_aggregati['ID_Edificio'] = id_peb_aggregati['ID_N'].astype(str) + '_' + id_peb_aggregati['ID_P_agg']
                configurazioni_potenziali = configurazioni_potenziali.merge(id_peb_aggregati[['ID_N', 'ID_Edificio']], on='ID_N', how='left')

                peb_in_ncer = gdf_positivo[gdf_positivo['ID_P'].isin(configurazioni_potenziali['ID_P'].unique())].copy()
                peb_in_ncer = peb_in_ncer.merge(configurazioni_potenziali[['ID_P', 'ID_Edificio']], on='ID_P', how='left')
                ncer_storage = peb_in_ncer.groupby('ID_Edificio')[sto_cols_names].sum().reset_index()

                for month in range(1, 13):
                    sto_ms_col = f'Sto_ms{month}'
                    sto_or_cols_month = [c for c in ncer_storage.columns if re.search(rf'_ms{month}\b', c) and c.startswith('Sto_or')]
                    if sto_or_cols_month:
                        ncer_storage[sto_ms_col] = ncer_storage[sto_or_cols_month].sum(axis=1) * DAYS_IN_MONTH[month]

                sto_ms_cols = [c for c in ncer_storage.columns if c.startswith('Sto_ms')]
                ncer_storage['Sto_an'] = ncer_storage[sto_ms_cols].sum(axis=1)

                configurazioni_calcolate = self._calculate_energetics(configurazioni_potenziali, gdf_dom_off, ncer_storage)

                logger.info(f"Suddivisione configurazioni in base a Indice Soglia: {indice_soglia}")
                ncer_successo = configurazioni_calcolate[configurazioni_calcolate['Indice'] >= indice_soglia].copy()
                ncer_fallimento = configurazioni_calcolate[configurazioni_calcolate['Indice'] < indice_soglia].copy()

                # --- NUOVA LOGICA PER DISSOLVE CORRETTO ---
                if not ncer_successo.empty:
                    logger.info(f"{len(ncer_successo['ID_Edificio'].unique())} configurazioni hanno superato la soglia.")

                    # 1. Raccogli tutti i membri (PEB e NEB) delle NCER di successo
                    id_peb_successo = ncer_successo['ID_P'].unique()
                    id_neb_successo = ncer_successo['ID_N'].unique()

                    membri_peb = gdf_positivo[gdf_positivo['ID_P'].isin(id_peb_successo)][['ID_P', 'geometry']].rename(columns={'ID_P': 'ID_Membro'})
                    membri_neb = gdf_negativo[gdf_negativo['ID_N'].isin(id_neb_successo)][['ID_N', 'geometry']].rename(columns={'ID_N': 'ID_Membro'})

                    tutti_i_membri = pd.concat([membri_peb, membri_neb], ignore_index=True)

                    # 2. Mappa l'ID della NCER su ogni membro
                    map_id_ncer = ncer_successo.set_index('ID_P')['ID_Edificio'].to_dict()
                    map_id_ncer.update(ncer_successo.set_index('ID_N')['ID_Edificio'].to_dict())
                    tutti_i_membri['ID_Edificio'] = tutti_i_membri['ID_Membro'].map(map_id_ncer)

                    # 3. Dissolve le geometrie
                    geometrie_dissolte = tutti_i_membri.dissolve(by='ID_Edificio').reset_index()[['ID_Edificio', 'geometry']]

                    # 4. Prepara i dati energetici (una riga per NCER)
                    dati_energetici = ncer_successo.drop_duplicates(subset=['ID_Edificio']).drop(columns='geometry')
                    final_cols_to_keep = ['ID_Edificio'] + \
                                         [c for c in dati_energetici.columns if c.startswith(('AF_ms', 'AD_ms', 'AF_an', 'AD_an', 'Sto_an')) or c == 'Indice']

                    # 5. Unisci geometrie dissolte e dati energetici
                    ncer_final_gdf = geometrie_dissolte.merge(dati_energetici[final_cols_to_keep], on='ID_Edificio')

            id_positivi_usati_totale = configurazioni_potenziali['ID_P'].unique() if not configurazioni_potenziali.empty else []
            id_negativi_usati_totale = configurazioni_potenziali['ID_N_unico'].unique() if not configurazioni_potenziali.empty else []

            ped2_originali_rimanenti = gdf_positivo[~gdf_positivo['ID_P'].isin(id_positivi_usati_totale)]
            ned2_originali_rimanenti = gdf_negativo[~gdf_negativo['ID_N_unico'].isin(id_negativi_usati_totale)]

            nuovi_peb_falliti_list, nuovi_neb_falliti_list = [], []

            if not ncer_fallimento.empty:
                logger.info(f"{len(ncer_fallimento['ID_Edificio'].unique())} configurazioni non hanno superato la soglia e verranno ri-aggregate.")
                for id_config_fallita, group in ncer_fallimento.groupby('ID_Edificio'):
                    lista_id_edifici = str(id_config_fallita).split('_')
                    geometria_unita = group.geometry.unary_union

                    dati_edifici_config = gdf_dom_off[gdf_dom_off['ID_Edificio'].astype(str).isin(lista_id_edifici)]
                    profilo_aggregato = dati_edifici_config.sum(numeric_only=True)

                    sto_an_aggregato = 0
                    for month in range(1, 13):
                        eccesso_giornaliero_mese = 0
                        for hour in range(24):
                            o_col = f'O_or{hour}_ms{month}'
                            d_col = f'D_or{hour}_ms{month}'
                            eccesso_orario = max(0, profilo_aggregato.get(o_col, 0) - profilo_aggregato.get(d_col, 0))
                            eccesso_giornaliero_mese += eccesso_orario
                        sto_an_aggregato += eccesso_giornaliero_mese * DAYS_IN_MONTH[month]

                    if sto_an_aggregato > 0:
                        nuovo_peb = {'ID_P': id_config_fallita, 'geometry': geometria_unita}
                        for o_col in o_cols:
                            suffix = o_col.replace('O_or', '')
                            sto_col = f'Sto_or{suffix}'
                            o_agg = profilo_aggregato.get(f'O_or{suffix}', 0)
                            d_agg = profilo_aggregato.get(f'D_or{suffix}', 0)
                            nuovo_peb[sto_col] = max(0, o_agg - d_agg)
                        nuovi_peb_falliti_list.append(nuovo_peb)
                    else:
                        nuovo_neb = {'ID_N': id_config_fallita, 'geometry': geometria_unita}
                        for o_col in o_cols:
                            suffix = o_col.replace('O_or', '')
                            def_col = f'Def_or{suffix}'
                            o_agg = profilo_aggregato.get(f'O_or{suffix}', 0)
                            d_agg = profilo_aggregato.get(f'D_or{suffix}', 0)
                            nuovo_neb[def_col] = max(0, d_agg - o_agg)
                        nuovi_neb_falliti_list.append(nuovo_neb)

            if nuovi_peb_falliti_list:
                gdf_nuovi_peb = gpd.GeoDataFrame(nuovi_peb_falliti_list, crs=gdf_positivo.crs)
                ped2_gdf = gpd.GeoDataFrame(pd.concat([ped2_originali_rimanenti, gdf_nuovi_peb], ignore_index=True), crs=gdf_positivo.crs)
            else:
                ped2_gdf = ped2_originali_rimanenti

            if nuovi_neb_falliti_list:
                gdf_nuovi_neb = gpd.GeoDataFrame(nuovi_neb_falliti_list, crs=gdf_negativo.crs)
                ned2_gdf = gpd.GeoDataFrame(pd.concat([ned2_originali_rimanenti, gdf_nuovi_neb], ignore_index=True), crs=gdf_negativo.crs)
            else:
                ned2_gdf = ned2_originali_rimanenti

            if not ncer_final_gdf.empty:
                cols_to_round = [c for c in ncer_final_gdf.columns if c.startswith(('AF_', 'AD_', 'Sto_')) or c == 'Indice']
                ncer_final_gdf[cols_to_round] = ncer_final_gdf[cols_to_round].round(4)

            logger.info(f"Risultati iterazione: {len(ncer_final_gdf)} NCER create, {len(ped2_gdf)} PEB rimanenti, {len(ned2_gdf)} NEB rimanenti.")

            return {'NCER': ncer_final_gdf, 'PED2': ped2_gdf, 'NED2': ned2_gdf}

        except Exception as e:
            logger.error(f"Errore in 'process_algorithm': {e}", exc_info=True)
            return {'NCER': gpd.GeoDataFrame(), 'PED2': gpd.GeoDataFrame(), 'NED2': gpd.GeoDataFrame()}


def ciclo_interazione_peb_neb(provincia: str, comune: str, indice_soglia: float):
    # ... (Tutta la funzione ciclo_interazione_peb_neb rimane invariata) ...
    prov_norm = safe_name(provincia)
    com_norm = safe_name(comune)
    prov_com = f"{prov_norm}_{com_norm}"

    script_dir = os.path.dirname(os.path.abspath(__file__))

    BASE_DIR = os.path.abspath(os.path.join(script_dir, "..", "model_builder_shapefiles", prov_com))
    DATA_COLLECTION_DIR = os.path.abspath(os.path.join(script_dir, "..", "Data_Collection", "shapefiles", prov_com))

    input_pos_path = os.path.join(BASE_DIR, "input", "peb", f"PEB_{prov_com}.gpkg")
    input_neg_path = os.path.join(BASE_DIR, "input", "neb", f"NEB_{prov_com}.gpkg")
    dom_off_path = os.path.join(DATA_COLLECTION_DIR, f"domanda-offerta_energetica_{prov_com}", "join_domanda_offerta.gpkg")

    OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

    logger.info(f"Directory di base: {BASE_DIR}")
    logger.info(f"Dati Domanda/Offerta: {dom_off_path}")

    if os.path.exists(OUTPUTS_DIR):
        shutil.rmtree(OUTPUTS_DIR)
    os.makedirs(OUTPUTS_DIR, exist_ok=True)

    try:
        current_peb = gpd.read_file(input_pos_path)
        current_neb = gpd.read_file(input_neg_path)
        gdf_dom_off = gpd.read_file(dom_off_path)

        if 'ID_Edificio' not in gdf_dom_off.columns:
            logger.error(f"Colonna 'ID_Edificio' non trovata in {dom_off_path}. Verifica il nome della colonna ID.")
            return
    except Exception as e:
        logger.error(f"Impossibile caricare i file di input iniziali: {e}")
        return

    n_iter = 1
    lista_ncer = []

    while True:
        logger.info(f"\n{'='*20} INIZIO ITERAZIONE {n_iter} {'='*20}")

        if current_peb.empty or current_neb.empty:
            logger.info("Condizione di terminazione: non ci sono più PEB o NEB da accoppiare.")
            break

        output_iter_dir = os.path.join(OUTPUTS_DIR, f"output{n_iter}")
        os.makedirs(output_iter_dir, exist_ok=True)

        processor = InterazionePebNeb()
        results = processor.process_algorithm(current_peb, current_neb, gdf_dom_off, indice_soglia)

        ncer_iter = results['NCER']
        ped2_iter = results['PED2']
        ned2_iter = results['NED2']

        save_if_not_empty(ncer_iter, os.path.join(output_iter_dir, f"ncer_{prov_com}_{n_iter}.gpkg"))
        save_if_not_empty(ped2_iter, os.path.join(output_iter_dir, f"ped2_{prov_com}_{n_iter}.gpkg"))
        save_if_not_empty(ned2_iter, os.path.join(output_iter_dir, f"ned2_{prov_com}_{n_iter}.gpkg"))

        if ncer_iter.empty and len(ped2_iter) == len(current_peb) and len(ned2_iter) == len(current_neb):
            logger.info("Nessuna nuova NCER valida prodotta e nessuna nuova fusione creata. Ciclo interrotto.")
            break

        if not ncer_iter.empty:
            ncer_iter['iterazione'] = n_iter
            lista_ncer.append(ncer_iter)

        current_peb = ped2_iter
        current_neb = ned2_iter

        n_iter += 1

    if lista_ncer:
        logger.info("\nConsolidamento di tutti gli NCER...")
        ncer_finale_gdf = gpd.GeoDataFrame(pd.concat(lista_ncer, ignore_index=True), crs=lista_ncer[0].crs)
        ncer_finale_path = os.path.join(OUTPUTS_DIR, f"ncer_finale_{prov_com}.gpkg")
        save_if_not_empty(ncer_finale_gdf, ncer_finale_path)

    logger.info(f"\n{'='*20} CICLO COMPLETATO IN {n_iter - 1} ITERAZIONI {'='*20}")


if __name__ == '__main__':
    # ... (Parte main rimane invariata)
    pass