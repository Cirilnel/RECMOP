#!/usr/bin/env python3
"""
Script: interazione_peb_neb.py

Descrizione:
    Questo script riproduce in ambiente Python puro (utilizzando GeoPandas, Shapely, Pandas e Rtree)
    la logica del modello QGIS "INTERAZIONE PEB-NEB".
    Evita dipendenze QGIS e lavora con file vettoriali standard (shapefile, GeoPackage, ecc.).

Prerequisiti:
    - Python 3.7+
    - geopandas
    - pandas
    - rtree (per ottimizzare gli spatial join)
    - shapely

Uso:
    python interazione_peb_neb.py \
        --input_neg input_negativo.shp \
        --input_pos input_positivo.shp \
        --output_ned2 output_ned2.shp \
        --output_ped2 output_ped2.shp \
        --new_ned new_ned.shp \
        --new_ped new_ped.shp

Note sui campi d’ingresso:
    - Il layer "input_negativo" deve avere almeno:
        •   una colonna 'ID_N' (identificativo univoco negativo)
        •   una colonna 'Deficit' (valore numerico, può essere anche NaN)
    - Il layer "input_positivo" deve avere almeno:
        •   una colonna 'ID_P' (identificativo univoco positivo)
        •   una colonna 'Surplus' (valore numerico, può essere anche NaN)
    Se i vostri shapefile o GeoPackage utilizzano nomi diversi, rinominate le colonne
    prima di eseguire questo script o modificate opportunamente le stringhe 'ID_N', 'Deficit',
    'ID_P' e 'Surplus' nel codice.

Output prodotti:
    - output_ned2.shp : corrisponde a "OUTPUT NED2" nel modello QGIS
    - output_ped2.shp : corrisponde a "OUTPUT PED 2" nel modello QGIS
    - new_ned.shp     : corrisponde a "NEW NED" (poligoni 'aggregati' con DELTA < 0)
    - new_ped.shp     : corrisponde a "NEW PED" (poligoni 'aggregati' con DELTA ≥ 0)
"""

import argparse
import os
import sys

import geopandas as gpd
import pandas as pd


def check_required_columns(gdf: gpd.GeoDataFrame, required: list, layer_name: str):
    """
    Verifica che il GeoDataFrame contenga tutte le colonne elencate in 'required'.
    In caso contrario, stampa un messaggio di errore e termina.
    """
    missing = [fld for fld in required if fld not in gdf.columns]
    if missing:
        print(f"\nErrore: il layer '{layer_name}' non contiene le colonne richieste: {missing}\n"
              "Assicurarsi che i campi siano presenti o rinominare i campi nel file di input.")
        sys.exit(1)


def main(args):
    # 1. Caricamento dei layer di input
    neg = gpd.read_file(args.input_neg)
    pos = gpd.read_file(args.input_pos)

    # Controllo presenza campi obbligatori
    check_required_columns(neg, ['ID_N', 'Deficit'], 'input_negativo')
    check_required_columns(pos, ['ID_P', 'Surplus'], 'input_positivo')

    # 2. Assicurarsi che entrambi i layer abbiano lo stesso CRS
    if neg.crs != pos.crs:
        pos = pos.to_crs(neg.crs)

    # ------------------------------------------------------
    # STEP 2: "Unisci attributi dal vettore più vicino" (join by nearest)
    #   QGIS: native:joinbynearest (INPUT=pos, INPUT_2=neg)
    #
    # In GeoPandas si usa sjoin_nearest:
    #   - how="left": per ogni feature di 'pos', trova la più vicina in 'neg'
    #   - distance_col="distance": colonna (temporanea) con la distanza
    #
    # Attenzione: sjoin_nearest richiede rtree installato per performance ottimali.
    # ------------------------------------------------------
    pos_nearest = gpd.sjoin_nearest(
        pos,
        neg,
        how="left",
        distance_col="distance"
    )
    # I campi di 'neg' vengono aggiunti con suffisso "_right" se esistono duplicati.
    # Normalizziamo:
    if 'ID_N_right' in pos_nearest.columns:
        pos_nearest['ID_N'] = pos_nearest['ID_N_right']
        pos_nearest.drop(columns=['ID_N_right'], inplace=True)
    if 'Deficit_right' in pos_nearest.columns:
        pos_nearest['Deficit'] = pos_nearest['Deficit_right']
        pos_nearest.drop(columns=['Deficit_right'], inplace=True)

    # 3. Calcolo DELTA = Surplus + Deficit
    #    QGIS: native:fieldcalculator con FORMULA 'Surplus+Deficit'
    pos_nearest['DELTA'] = pos_nearest['Surplus'] + pos_nearest['Deficit']

    # 4. Rimozione delle colonne di servizio create dallo spatial join
    to_drop = []
    if 'index_right' in pos_nearest.columns:
        to_drop.append('index_right')
    if 'distance' in pos_nearest.columns:
        to_drop.append('distance')
    pos_nearest.drop(columns=to_drop, inplace=True)

    # 5. Group_stats_step4: calcolo del valore massimo di DELTA per ogni 'ID_N'
    #    QGIS: qgis:statisticsbycategories con CATEGORIES_FIELD_NAME = ['ID_N'], VALUES_FIELD_NAME = 'DELTA'
    delta_max = (
        pos_nearest
        .groupby('ID_N', dropna=False)['DELTA']
        .max()
        .reset_index()
        .rename(columns={'DELTA': 'max_delta'})
    )

    # 6. Unisci il campo 'max_delta' al pos_nearest originale
    pos_max = pos_nearest.merge(delta_max, on='ID_N', how='left')

    # 7. Calcolatore di campi_STEP5.1: delta2 = max_delta
    #    QGIS: fieldcalculator con FORMULA '"max"'
    pos_max['delta2'] = pos_max['max_delta']

    # 8. EstraiTramiteEspressione_step5.2: seleziona le righe per cui DELTA == delta2
    #    QGIS: native:extractbyexpression con '"DELTA" = "delta2"'
    selected = pos_max[pos_max['DELTA'] == pos_max['delta2']].copy()
    # Aggiungiamo 'Agr' = id di feature (qui usiamo l'indice del DataFrame come identificativo univoco)
    selected = selected.reset_index(drop=False).rename(columns={'index': 'Agr'})
    # 'Agr' servirà per raggruppare le coppie (poligono positivo + poligono negativo)
    # Ora 'selected' contiene:
    #   - tutte le colonne di pos_max (comprese: ID_P, Surplus, ID_N, Deficit, DELTA, max_delta, delta2, geometry_pos)
    #   - 'Agr' (intero) che identifica ciascuna coppia
    #   - geometry = poligono di 'pos' (originale)

    # 9. Calcolatore di campi_agr_Step6: 'Agr' è già assegnato in selected (già fatto)

    # 10. Creazione_Pasned2_step9.2: join attributes dal risultato 'selected' a 'neg'
    #     QGIS: native:joinattributestable con INPUT=neg, INPUT_2=selected, join su 'ID_N'
    neg_join1 = neg.merge(
        selected[['ID_N', 'ID_P', 'DELTA', 'Agr']],
        on='ID_N',
        how='left'
    )
    # 11. NEW_ned2: estrai i neg di neg_join1 con Agr == null (quelli non partecipanti)
    new_ned2 = neg_join1[neg_join1['Agr'].isna()].copy()

    # 12. Creazione_Pasped_step7.1: join attributes di 'selected' a 'pos', join su 'ID_P'
    #     QGIS: joinattributestable con DISCARD_NONMATCHING=True (quindi how='inner')
    pos_join1 = pos.merge(
        selected[['ID_P', 'ID_N', 'DELTA', 'Agr']],
        on='ID_P',
        how='inner'
    )

    # 13. Creazione_Pasned_step7.2: join attributes di 'selected' a 'neg', join su 'ID_N'
    neg_join2 = neg.merge(
        selected[['ID_N', 'ID_P', 'DELTA', 'Agr']],
        on='ID_N',
        how='inner'
    )

    # 14. Creazione_Pasped2_step9.1: join attributes di 'selected' a 'pos', join su 'ID_P'
    #     QGIS: joinattributestable con DISCARD_NONMATCHING=False (quindi how='left')
    pos_all_join = pos.merge(
        selected[['ID_P', 'ID_N', 'DELTA', 'Agr']],
        on='ID_P',
        how='left'
    )

    # 15. Fondi vettori_step7.3: unisci i layer 'pos_join1' e 'neg_join2'
    #     QGIS: native:mergevectorlayers
    #     In GeoPandas concatenazione di due GeoDataFrame (stesse colonne, medesimo CRS)
    #     Prima di concatenare, rinominiamo la colonna 'geometry' in modo da preservare:
    pos_part = selected[['ID_N', 'ID_P', 'DELTA', 'Agr', 'geometry']].copy()
    pos_part = gpd.GeoDataFrame(pos_part, geometry='geometry', crs=pos.crs)

    # Per neg_part, prendo la geometria originale di 'neg_join2'
    neg_part = neg_join2[['ID_N', 'ID_P', 'DELTA', 'Agr', 'geometry']].copy()
    neg_part = gpd.GeoDataFrame(neg_part, geometry='geometry', crs=neg.crs)

    # Concatenazione
    participants = gpd.GeoDataFrame(
        pd.concat([pos_part, neg_part], ignore_index=True),
        geometry='geometry',
        crs=neg.crs
    )

    # 16. Dissolvi_step7.4: dissolve dei 'participants' per campo 'Agr'
    #     QGIS: native:dissolve con FIELD=['Agr']
    dissolved = participants.dissolve(by='Agr', as_index=False)

    # 17. (In QGIS: EliminaCampo_step7.5 eliminava 'layer' e 'path', non creati qui – saltiamo)

    # 18. Creazione_newned_step8.1: estrai da 'dissolved' le feature con DELTA < 0
    new_ned = dissolved[dissolved['DELTA'] < 0].copy()

    # 19. NED2: unisci 'new_ned' e 'new_ned2'
    #     QGIS: mergevectorlayers
    ned2 = gpd.GeoDataFrame(
        pd.concat([new_ned, new_ned2], ignore_index=True),
        geometry='geometry',
        crs=neg.crs
    )

    # 20. Creazione_newped_step8.2: estrai da 'dissolved' le feature con DELTA >= 0
    new_ped = dissolved[dissolved['DELTA'] >= 0].copy()

    # 21. NEW_ped2 (precedentemente): 'pos_all_join' con Agr == null
    new_ped2 = pos_all_join[pos_all_join['Agr'].isna()].copy()

    # 22. PED2: unisci 'new_ped' e 'new_ped2'
    ped2 = gpd.GeoDataFrame(
        pd.concat([new_ped, new_ped2], ignore_index=True),
        geometry='geometry',
        crs=pos.crs
    )

    # ---------- ULTIMI PASSAGGI DI AGGIORNAMENTO CAMPI E PULIZIA ----------

    # ----- NEGATIVE OUTPUT (OutputNed2) -----
    # A) Calcolatore di campi step finale 1: Deficit2 = if Deficit is null, DELTA, else Deficit
    #    Creiamo colonna 'Deficit2'
    def _compute_deficit2(row):
        if pd.isna(row.get('Deficit')):
            return row.get('DELTA')
        else:
            return row.get('Deficit')

    ned2['Deficit2'] = ned2.apply(_compute_deficit2, axis=1)

    # B) Aggiorna id ned step finale 2: ID_N2 = ID_N + ',' + ID_P
    #    Attenzione: sia per le feature di 'new_ned' (aggregate) che per quelle di 'new_ned2' (Agr==NaN),
    #    potremmo avere ID_P o ID_N mancanti: conviene sostituire NaN con stringa vuota.
    ned2['ID_N'] = ned2['ID_N'].fillna('').astype(str)
    ned2['ID_P'] = ned2['ID_P'].fillna('').astype(str)
    ned2['ID_N2'] = ned2['ID_N'] + ',' + ned2['ID_P']

    # C) Elimina campi intermedi (corrisponde a EliminaCampoNedStepFinale3)
    drop_ned_fields = [
        'Deficit', 'Surplus', 'ID_P', 'DELTA', 'Agr',
        # 'layer', 'path' non esistono qui
        'max_delta', 'delta2',
        # Eventuali colonne di join non più utili
        'index',  # se presente (dato il reset_index su 'selected')
    ]
    for fld in drop_ned_fields:
        if fld in ned2.columns:
            ned2.drop(columns=[fld], inplace=True)

    # D) Aggiorna ned step finale 4: Deficit = Deficit2
    ned2['Deficit'] = ned2['Deficit2']

    # E) Aggiorna ned step finale 5: ID_N = ID_N2
    ned2['ID_N'] = ned2['ID_N2']

    # F) Elimina campi finali non necessari (Deficit2 e ID_N2) per ottenere OutputNed2
    for fld in ['Deficit2', 'ID_N2']:
        if fld in ned2.columns:
            ned2.drop(columns=[fld], inplace=True)

    # A questo punto 'ned2' ha:
    #   - colonna 'ID_N' (aggiornata)
    #   - eventuali altri campi originali di 'neg' che non abbiamo tolto
    #   - campo 'Deficit' (aggiornato secondo logica QGIS)
    #   - colonna 'geometry'

    # ----- POSITIVE OUTPUT (OutputPed2) -----
    # A) Aggiorna ped step finale 1: Surplus2 = if DELTA is null, Surplus, else DELTA
    def _compute_surplus2(row):
        if pd.isna(row.get('DELTA')):
            return row.get('Surplus')
        else:
            return row.get('DELTA')

    ped2['Surplus2'] = ped2.apply(_compute_surplus2, axis=1)

    # B) Aggiorna id ped step finale 2: ID_P2 = ID_P + ',' + ID_N
    ped2['ID_P'] = ped2['ID_P'].fillna('').astype(str)
    ped2['ID_N'] = ped2['ID_N'].fillna('').astype(str)
    ped2['ID_P2'] = ped2['ID_P'] + ',' + ped2['ID_N']

    # C) Elimina campi intermedi (corrisponde a EliminaCampoPedStepFinale3)
    drop_ped_fields = [
        'Deficit', 'Surplus', 'ID_N', 'DELTA', 'Agr',
        'max_delta', 'delta2',
        'index',  # se presente
    ]
    for fld in drop_ped_fields:
        if fld in ped2.columns:
            ped2.drop(columns=[fld], inplace=True)

    # D) Aggiorna ped step finale 4: Surplus = Surplus2
    ped2['Surplus'] = ped2['Surplus2']

    # E) Aggiorna ped step finale 5: ID_P = ID_P2
    ped2['ID_P'] = ped2['ID_P2']

    # F) Elimina campi finali non necessari (Surplus2 e ID_P2) per ottenere OutputPed2
    for fld in ['Surplus2', 'ID_P2']:
        if fld in ped2.columns:
            ped2.drop(columns=[fld], inplace=True)

    # A questo punto 'ped2' ha:
    #   - colonna 'ID_P' (aggiornata)
    #   - eventuali altri campi originali di 'pos' che non abbiamo tolto
    #   - campo 'Surplus' (aggiornato secondo logica QGIS)
    #   - colonna 'geometry'

    # --------------------------------------------------------------------
    # SALVATAGGIO DEI RISULTATI SU FILE VETTORIALI
    # --------------------------------------------------------------------
    # Creazione delle directory se non esistono
    out_files = [
        args.output_ned2,
        args.output_ped2,
        args.new_ned,
        args.new_ped
    ]
    for fpath in out_files:
        out_dir = os.path.dirname(os.path.abspath(fpath))
        if out_dir and not os.path.isdir(out_dir):
            os.makedirs(out_dir)

    # 1) OutputNed2
    ned2.to_file(args.output_ned2)
    print(f"✔ OutputNed2 salvato in: {args.output_ned2}")

    # 2) OutputPed2
    ped2.to_file(args.output_ped2)
    print(f"✔ OutputPed2 salvato in: {args.output_ped2}")

    # 3) NewNed: poligoni aggregati con DELTA < 0 (prima del merging coi non partecipanti)
    #    Possiamo salvare 'new_ned' (il dissolve parziale)
    new_ned.to_file(args.new_ned)
    print(f"✔ NewNed salvato in: {args.new_ned}")

    # 4) NewPed: poligoni aggregati con DELTA ≥ 0 (prima del merging coi non partecipanti)
    new_ped.to_file(args.new_ped)
    print(f"✔ NewPed salvato in: {args.new_ped}")

    print("\nProcessamento completato con successo.")

# ── Configurazione manuale dei percorsi ──
INPUT_NEG = "../Model_Builder_Args/NEB_mb.shp"
INPUT_POS = "../Model_Builder_Args/PEB_mb.shp"
OUTPUT_NED2 = "../Model_Builder_Args/output/OUTPUT_NED2.shp"
OUTPUT_PED2 = "../Model_Builder_Args/output/OUTPUT_PED2.shp"
NEW_NED = "../Model_Builder_Args/output/NEW_NED.shp"
NEW_PED = "../Model_Builder_Args/output/NEW_PED.shp"


if __name__ == "__main__":
    # 1) Definisco un “contenitore” per i parametri (simula argparse)
    class Args:
        pass

    args = Args()
    args.input_neg    = INPUT_NEG
    args.input_pos    = INPUT_POS
    args.output_ned2  = OUTPUT_NED2
    args.output_ped2  = OUTPUT_PED2
    args.new_ned      = NEW_NED
    args.new_ped      = NEW_PED

    # 2) Chiamo main con quei valori fissi
    main(args)
