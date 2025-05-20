import os
import pandas as pd
import geopandas as gpd
import logging

# Import delle funzioni di estrazione dai moduli dedicati
from normattiva import run_estrazione_normattiva
from data_extraction.siape import run_estrazione_siape
from estrazione_dati_basi_territoriali import run_estrazione_basi_territoriali
from estrazione_dati_variabili_censuarie import run_estrazione_variabili_censuarie

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def crea_dataframe_fase1() -> pd.DataFrame:
    """
    Crea il DataFrame unendo le basi territoriali, le variabili censuarie,
    i dati estratti da Normattiva e quelli da SIAPE.

    Returns:
        pd.DataFrame: DataFrame risultante dalla fase 1 dell'elaborazione.
    """
    # Estrazione dati grezzi
    df_base = run_estrazione_basi_territoriali()
    df_cens = run_estrazione_variabili_censuarie()
    df_norm = run_estrazione_normattiva()
    df_siape = run_estrazione_siape()

    # Join tra basi territoriali e variabili censuarie
    df_merged = pd.merge(
        df_base,
        df_cens,
        on=['SEZ2011'],
        how='inner'
    )

    # Join case-insensitive su Comune con Normattiva
    df_merged['COMUNE'] = df_merged['COMUNE'].str.upper()
    df_norm['Comune'] = df_norm['Comune'].str.upper()

    # Rinomina colonne di Normattiva per allineamento
    df_norm = df_norm.rename(
        columns={
            'Zona': 'ZONA_CLIMATICA',
            'GradiGiorno': 'GRADI_GIORNO',
            'Altitudine': 'ALTITUDINE',
            'Comune': 'COMUNE'
        }
    )

    # Join con dati Normattiva su SEZ, COD_LOC, TIPO_LOC, PROVINCIA, COMUNE
    df_merged2 = pd.merge(
        df_merged,
        df_norm,
        on=['COMUNE'],
        how='inner'
    )

    # Preparazione dati SIAPE (pivot su zona climatica)
    df_siape = df_siape.reset_index().rename_axis(None)
    df_siape = df_siape.rename(columns={'zona_climatica': 'ZONA_CLIMATICA'})

    # Join finale con SIAPE su ZONA_CLIMATICA, GRADI_GIORNO, ALTITUDINE
    df_finale = pd.merge(
        df_merged2,
        df_siape,
        on=['ZONA_CLIMATICA'],
        how='left'
    )

    return df_finale


def salva_dati_fase1(
    df: pd.DataFrame,
    output_path: str = os.path.join('../Data_Collection/csv_tables-fase1', 'dati_fase1.csv')
) -> None:
    """
    Salva il DataFrame risultante dalla fase 1 in un file CSV.

    Args:
        df (pd.DataFrame): DataFrame da salvare.
        output_path (str): Percorso completo del file di output.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')

def create_merged_shapefile() -> None:

    # Leggi lo shapefile
    gdf = gpd.read_file("../Istat/Regioni/Campania/R15_11_WGS84.shp")

    # Leggi il CSV
    df_csv = pd.read_csv("../Data_Collection/csv_tables-fase1/dati_fase1.csv", sep=';',encoding='utf-8-sig')

    # Logging delle colonne del CSV
    logger.info(f"Colonne CSV: {df_csv.columns.tolist()}")
    logger.info(f"Colonne Shapefile: {gdf.columns.tolist()}")

    # Colonne già presenti nello shapefile
    colonne_shapefile = gdf.columns.tolist()

    # Rimuovi dal CSV solo quelle che esistono anche nello shapefile (eccetto la chiave di join)
    colonne_da_unire = [col for col in df_csv.columns if col not in colonne_shapefile or col == 'SEZ2011']

    # Logger delle colonne da unire e di quelle rimosse
    colonne_rimosse = [col for col in df_csv.columns if col not in colonne_da_unire]
    logger.info(f"Colonne rimosse dal CSV: {colonne_rimosse}")
    logger.info(f"Colonne da unire: {colonne_da_unire}")

    # Riduci il CSV prima del merge
    df_csv_filtrato = df_csv[colonne_da_unire]

    # Esegui il merge
    merged = gdf.merge(df_csv_filtrato, how='left', on='SEZ2011')

    # Converti 'LOC2011' in stringa per evitare valori troppo grandi
    merged['LOC2011'] = merged['LOC2011'].astype(str)

    # Rinomina le colonne più lunghe di 10 caratteri per evitare warning
    merged = merged.rename(columns={
        'ZONA_CLIMATICA': 'ZONA_CLIM',
        'GRADI_GIORNO': 'GRADI_GIO'
    })

    # Salva il risultato come nuovo shapefile
    merged.to_file("../Data_Collection/shapefiles_merged/Campania/campania_merged.shp", encoding='utf-8')

def run_fase1() -> None:
    """
    Funzione principale per eseguire la fase 1 dell'elaborazione dei dati.

    Returns:
        pd.DataFrame: DataFrame risultante dalla fase 1.
    """
    df_fase1 = crea_dataframe_fase1()
    salva_dati_fase1(df_fase1)
    create_merged_shapefile()

if __name__ == "__main__":
    run_fase1()