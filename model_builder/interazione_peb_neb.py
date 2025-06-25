#!/usr/bin/env python3
"""
Script: interazione_peb_neb.py (refactored)

Descrizione:
    Riproduce la logica del modello QGIS "INTERAZIONE PEB-NEB"
    in GeoPandas, con modularità, logging, validazioni e tipizzazione.
"""
import argparse
import logging
import os
import sys
from typing import Tuple

import geopandas as gpd
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def safe_read_file(path: str, label: str) -> gpd.GeoDataFrame:
    """Carica un file shapefile/GeoPackage in modo sicuro con logging e gestione errori."""
    try:
        gdf = gpd.read_file(path)
        logger.info("Caricato '%s' (%d feature) da %s", label, len(gdf), path)
        return gdf
    except Exception as e:
        logger.error("Errore nel caricamento di '%s': %s", label, e)
        sys.exit(1)


def check_required_columns(gdf: gpd.GeoDataFrame, required: list, layer_name: str) -> None:
    """
    Verifica che il GeoDataFrame contenga tutte le colonne elencate in 'required'.
    In caso contrario, logga un errore ed esce.
    """
    missing = [fld for fld in required if fld not in gdf.columns]
    if missing:
        logger.error(
            "Layer '%s' manca le colonne: %s. Assicurarsi di rinominare o includere questi campi.",
            layer_name,
            missing
        )
        sys.exit(1)


def validate_and_clean_geometry(gdf: gpd.GeoDataFrame, id_field: str, layer_name: str) -> gpd.GeoDataFrame:
    """
    Rimuove geometrie vuote, None, non-polygonali e feature con ID null.
    Logga numero di feature rimosse.
    """
    original_count = len(gdf)
    mask_valid_geom = gdf.geometry.notnull() & ~gdf.geometry.is_empty
    gdf = gdf[mask_valid_geom]
    mask_poly = gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
    gdf = gdf[mask_poly]
    mask_id = gdf[id_field].notna()
    gdf = gdf[mask_id]
    removed = original_count - len(gdf)
    if removed > 0:
        logger.warning("Rimosse %d feature invalide da '%s'", removed, layer_name)
    return gdf


def load_and_validate_inputs(args: argparse.Namespace) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Carica i layer input e ne verifica colonne, CRS, geometrie e ID.
    Restituisce due GeoDataFrame (negativo, positivo).
    """
    neg = safe_read_file(args.input_neg, "input_negativo")
    pos = safe_read_file(args.input_pos, "input_positivo")
    check_required_columns(neg, ["ID_N", "deficit"], "input_negativo")
    check_required_columns(pos, ["ID_P", "surplus"], "input_positivo")
    if neg.crs != pos.crs:
        pos = pos.to_crs(neg.crs)
        logger.info("Riproiettato 'input_positivo' in CRS di 'input_negativo'")
    neg = validate_and_clean_geometry(neg, "ID_N", "input_negativo")
    pos = validate_and_clean_geometry(pos, "ID_P", "input_positivo")
    return neg, pos


def perform_spatial_join(
    pos: gpd.GeoDataFrame,
    neg: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Esegue uno spatial join per feature positiva con la più vicina negativa.
    Restituisce GeoDataFrame con campi uniti.
    """
    joined = gpd.sjoin_nearest(
        pos,
        neg,
        how="left",
        distance_col="distance"
    )
    for field in ["ID_N", "deficit"]:
        right = f"{field}_right"
        if right in joined:
            joined[field] = joined[right]
            joined.drop(columns=[right], inplace=True)
    joined.drop(columns=[c for c in ["index_right", "distance"] if c in joined.columns], inplace=True)
    return joined


def calculate_delta_fields(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Calcola il campo 'DELTA' = surplus + deficit.
    """
    gdf["DELTA"] = gdf["surplus"].fillna(0) + gdf["deficit"].fillna(0)
    return gdf


def generate_outputs(
    pos_delta: gpd.GeoDataFrame,
    neg: gpd.GeoDataFrame,
    pos: gpd.GeoDataFrame
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Crea i layer OutputNed2, OutputPed2, NewNed, NewPed.
    Restituisce quattro GeoDataFrame.
    """
    delta_max = (
        pos_delta.groupby("ID_N", dropna=False)["DELTA"]
        .max()
        .reset_index()
        .rename(columns={"DELTA": "max_delta"})
    )
    pos_max = pos_delta.merge(delta_max, on="ID_N", how="left")
    pos_max["delta2"] = pos_max["max_delta"]
    selected = pos_max[pos_max["DELTA"] == pos_max["delta2"]].copy()
    selected = selected.reset_index(drop=False).rename(columns={"index": "Agr"})
    neg_join1 = neg.merge(
        selected[["ID_N", "ID_P", "DELTA", "Agr"]],
        on="ID_N",
        how="left"
    )
    new_ned2 = neg_join1[neg_join1["Agr"].isna()].copy()
    pos_join1 = pos.merge(selected[["ID_P", "ID_N", "DELTA", "Agr"]], on="ID_P", how="inner")
    neg_join2 = neg.merge(selected[["ID_N", "ID_P", "DELTA", "Agr"]], on="ID_N", how="inner")
    pos_part = gpd.GeoDataFrame(
        selected[["ID_N", "ID_P", "DELTA", "Agr", "geometry"]],
        geometry="geometry",
        crs=pos.crs
    )
    neg_part = gpd.GeoDataFrame(
        neg_join2[["ID_N", "ID_P", "DELTA", "Agr", "geometry"]],
        geometry="geometry",
        crs=neg.crs
    )
    participants = gpd.GeoDataFrame(
        pd.concat([pos_part, neg_part], ignore_index=True),
        geometry="geometry",
        crs=neg.crs
    )
    dissolved = participants.dissolve(by="Agr", as_index=False)
    new_ned = dissolved[dissolved["DELTA"] < 0].copy()
    ned2 = gpd.GeoDataFrame(
        pd.concat([new_ned, new_ned2], ignore_index=True),
        geometry="geometry",
        crs=neg.crs
    )
    new_ped = dissolved[dissolved["DELTA"] >= 0].copy()
    pos_all_join = pos.merge(selected[["ID_P", "ID_N", "DELTA", "Agr"]], on="ID_P", how="left")
    new_ped2 = pos_all_join[pos_all_join["Agr"].isna()].copy()
    ped2 = gpd.GeoDataFrame(
        pd.concat([new_ped, new_ped2], ignore_index=True),
        geometry="geometry",
        crs=pos.crs
    )
    return ned2, ped2, new_ned, new_ped


def cleanup_attributes(
    ned2: gpd.GeoDataFrame,
    ped2: gpd.GeoDataFrame
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Rimuove campi intermedi e aggiorna ID e valori secondo logica QGIS."""

    def aggiorna(gdf, campo_val: str, campo_id: str, neg: bool = True):
        altro_id = f"ID_{'P' if campo_id == 'ID_N' else 'N'}"

        # Valore aggiornato: usa DELTA se disponibile, altrimenti il campo originale
        gdf[f"{campo_val}2"] = gdf.apply(
            lambda r: r["DELTA"] if pd.notna(r.get("DELTA")) else r.get(campo_val),
            axis=1
        )

        # ID concatenato (senza virgola finale se mancante)
        def build_id(r):
            id1 = str(r[campo_id]) if pd.notna(r.get(campo_id)) else ""
            id2 = str(r[altro_id]) if pd.notna(r.get(altro_id)) else ""
            return ",".join([x for x in [id1, id2] if x])

        gdf[f"{campo_id}2"] = gdf.apply(build_id, axis=1)

        # Pulizia colonne vecchie
        for fld in ["deficit", "surplus", "ID_P", "ID_N", "DELTA", "Agr", "max_delta", "delta2", "index"]:
            if fld in gdf.columns:
                gdf.drop(columns=fld, inplace=True)

        # Rinominazione finale
        gdf[campo_val] = gdf.pop(f"{campo_val}2")
        gdf[campo_id] = gdf.pop(f"{campo_id}2")
        return gdf

    ned2 = aggiorna(ned2, "deficit", "ID_N", neg=True)
    ped2 = aggiorna(ped2, "surplus", "ID_P", neg=False)
    return ned2, ped2



def save_outputs(
    args: argparse.Namespace,
    ned2: gpd.GeoDataFrame,
    ped2: gpd.GeoDataFrame,
    new_ned: gpd.GeoDataFrame,
    new_ped: gpd.GeoDataFrame
) -> None:
    """
    Crea directory di output e salva i file vettoriali.
    """
    for path in [args.output_ned2, args.output_ped2, args.new_ned, args.new_ped]:
        outdir = os.path.dirname(os.path.abspath(path))
        if outdir and not os.path.isdir(outdir):
            os.makedirs(outdir, exist_ok=True)
    ned2.to_file(args.output_ned2)
    logger.info("OutputNed2 salvato in: %s", args.output_ned2)
    ped2.to_file(args.output_ped2)
    logger.info("OutputPed2 salvato in: %s", args.output_ped2)
    new_ned.to_file(args.new_ned)
    logger.info("NewNed salvato in: %s", args.new_ned)
    new_ped.to_file(args.new_ped)
    logger.info("NewPed salvato in: %s", args.new_ped)


def processa_interazione_peb_neb(provincia: str, comune: str) -> None:
    """
    Esegue l'interazione PEB-NEB per una specifica coppia provincia-comune,
    costruendo i percorsi input/output sulla base della struttura dei file.
    Salva gli shapefile con nomi file in minuscolo.
    """
    def normalize(s: str) -> str:
        return s.lower().replace(" ", "_")

    prov_com = f"{normalize(provincia)}_{normalize(comune)}"
    BASE_DIR = os.path.join("..", "model_builder_shapefiles", prov_com)

    # Costruzione percorsi input
    input_neg_dir = os.path.join(BASE_DIR, "input", "neb")
    input_pos_dir = os.path.join(BASE_DIR, "input", "peb")
    input_neg = os.path.join(input_neg_dir, f"NEB_{provincia}_{comune}.shp")
    input_pos = os.path.join(input_pos_dir, f"PEB_{provincia}_{comune}.shp")

    # Costruzione percorsi output (uso nomi file minuscoli)
    prov_norm = normalize(provincia)
    com_norm = normalize(comune)
    output_ned = os.path.join(BASE_DIR, "output", "neb", f"outneb_{prov_norm}_{com_norm}.shp")
    output_ped = os.path.join(BASE_DIR, "output", "peb", f"outpeb_{prov_norm}_{com_norm}.shp")
    new_ned = os.path.join(BASE_DIR, "new", "neb", f"newneb_{prov_norm}_{com_norm}.shp")
    new_ped = os.path.join(BASE_DIR, "new", "peb", f"newpeb_{prov_norm}_{com_norm}.shp")

    # Costruzione oggetto args fittizio
    args = argparse.Namespace(
        input_neg=input_neg,
        input_pos=input_pos,
        output_ned2=output_ned,
        output_ped2=output_ped,
        new_ned=new_ned,
        new_ped=new_ped
    )

    # Esecuzione logica
    neg, pos = load_and_validate_inputs(args)
    pos_joined = perform_spatial_join(pos, neg)
    pos_delta = calculate_delta_fields(pos_joined)
    ned2, ped2, new_ned_gdf, new_ped_gdf = generate_outputs(pos_delta, neg, pos)
    ned2_clean, ped2_clean = cleanup_attributes(ned2, ped2)
    save_outputs(args, ned2_clean, ped2_clean, new_ned_gdf, new_ped_gdf)
    logger.info("Totale record OUTPUT_NED2: %d", len(ned2_clean))
    logger.info("Totale record OUTPUT_PED2: %d", len(ped2_clean))
    logger.info("Totale record NEW_NED: %d", len(new_ned_gdf))
    logger.info("Totale record NEW_PED: %d", len(new_ped_gdf))
    logger.info("Interazione completata per %s (%s)", comune, provincia)


if __name__ == "__main__":
    processa_interazione_peb_neb("Salerno", "Padula")
