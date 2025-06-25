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
from shapely.geometry.base import BaseGeometry
from shapely.geometry import Polygon, MultiPolygon

# Percorsi di default
INPUT_NEG = "../ESEMPIO_Model_Builder_Args/NEB_mb.shp"
INPUT_POS = "../ESEMPIO_Model_Builder_Args/PEB_mb.shp"
OUTPUT_NED2 = "../ESEMPIO_Model_Builder_Args/output/OUTPUT_NED2.shp"
OUTPUT_PED2 = "../ESEMPIO_Model_Builder_Args/output/OUTPUT_PED2.shp"
NEW_NED = "../ESEMPIO_Model_Builder_Args/output/NEW_NED.shp"
NEW_PED = "../ESEMPIO_Model_Builder_Args/output/NEW_PED.shp"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


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
    neg = gpd.read_file(args.input_neg)
    pos = gpd.read_file(args.input_pos)
    check_required_columns(neg, ["ID_N", "Deficit"], "input_negativo")
    check_required_columns(pos, ["ID_P", "Surplus"], "input_positivo")
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
    for field in ["ID_N", "Deficit"]:
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
    Calcola il campo 'DELTA' = Surplus + Deficit.
    """
    gdf["DELTA"] = gdf["Surplus"].fillna(0) + gdf["Deficit"].fillna(0)
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
    """
    Rimuove campi intermedi e aggiorna ID e valori secondo logica QGIS.
    """
    ned2["Deficit2"] = ned2.apply(
        lambda r: r["DELTA"] if pd.isna(r.get("Deficit")) else r.get("Deficit"),
        axis=1
    )
    ned2["ID_N2"] = (
        ned2["ID_N"].fillna("").astype(str)
        + ","
        + ned2["ID_P"].fillna("").astype(str)
    )
    for fld in ["Deficit", "Surplus", "ID_P", "DELTA", "Agr", "max_delta", "delta2", "index"]:
        if fld in ned2.columns:
            ned2.drop(columns=[fld], inplace=True)
    ned2["Deficit"] = ned2.pop("Deficit2")
    ned2["ID_N"] = ned2.pop("ID_N2")
    ped2["Surplus2"] = ped2.apply(
        lambda r: r["DELTA"] if not pd.isna(r.get("DELTA")) else r.get("Surplus"), axis=1
    )
    ped2["ID_P2"] = (
        ped2["ID_P"].fillna("").astype(str)
        + ","
        + ped2["ID_N"].fillna("").astype(str)
    )
    for fld in ["Deficit", "Surplus", "ID_N", "DELTA", "Agr", "max_delta", "delta2", "index"]:
        if fld in ped2.columns:
            ped2.drop(columns=[fld], inplace=True)
    ped2["Surplus"] = ped2.pop("Surplus2")
    ped2["ID_P"] = ped2.pop("ID_P2")
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
    logger.info("✔ OutputNed2 salvato in: %s", args.output_ned2)
    ped2.to_file(args.output_ped2)
    logger.info("✔ OutputPed2 salvato in: %s", args.output_ped2)
    new_ned.to_file(args.new_ned)
    logger.info("✔ NewNed salvato in: %s", args.new_ned)
    new_ped.to_file(args.new_ped)
    logger.info("✔ NewPed salvato in: %s", args.new_ped)


def main(args: argparse.Namespace) -> None:
    neg, pos = load_and_validate_inputs(args)
    pos_joined = perform_spatial_join(pos, neg)
    pos_delta = calculate_delta_fields(pos_joined)
    ned2, ped2, new_ned, new_ped = generate_outputs(pos_delta, neg, pos)
    ned2_clean, ped2_clean = cleanup_attributes(ned2, ped2)
    save_outputs(args, ned2_clean, ped2_clean, new_ned, new_ped)
    logger.info("Processamento completato con successo.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Interazione PEB-NEB: calcola output NED2, PED2, NEW_NED, NEW_PED"
    )
    parser.add_argument("--input_neg", default=INPUT_NEG, help="Percorso al layer negativo (shp/gpkg)")
    parser.add_argument("--input_pos", default=INPUT_POS, help="Percorso al layer positivo (shp/gpkg)")
    parser.add_argument("--output_ned2", default=OUTPUT_NED2, help="Percorso di output per OUTPUT_NED2")
    parser.add_argument("--output_ped2", default=OUTPUT_PED2, help="Percorso di output per OUTPUT_PED2")
    parser.add_argument("--new_ned", default=NEW_NED, help="Percorso di output per NEW_NED")
    parser.add_argument("--new_ped", default=NEW_PED, help="Percorso di output per NEW_PED")
    args = parser.parse_args()
    main(args)
