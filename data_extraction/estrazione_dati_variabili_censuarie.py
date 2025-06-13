import os
import logging
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Costanti
INPUT_PATH = os.path.join("..", "Istat", "Variabili_Censuarie", "Sezioni_di_Censimento", "Campania.csv")
OUTPUT_DIR = "../Data_Collection/csv_tables-fase1"
OUTPUT_FILENAME = "variabili_censuarie_R15_campania.csv"
COLONNE_RICHIESTE = ['SEZ2011', 'COMUNE', 'PROVINCIA', 'P1', 'E8', 'E9', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'E16', 'A44']


def estrai_dati_variabili_censuarie(percorso_file: str, sep: str = ';', encoding: str = 'latin-1') -> pd.DataFrame:
    """
    Estrae le colonne censuarie da un file CSV Istat.

    Args:
        percorso_file: Percorso al file CSV.
        sep: Separatore del file CSV.
        encoding: Codifica del file.

    Returns:
        DataFrame contenente solo le colonne desiderate (presenti).
    """
    df = pd.read_csv(percorso_file, sep=sep, encoding=encoding, dtype=str)
    df.columns = df.columns.str.strip()

    colonne_presenti = [col for col in COLONNE_RICHIESTE if col in df.columns]
    colonne_mancanti = [col for col in COLONNE_RICHIESTE if col not in df.columns]

    if colonne_mancanti:
        logger.warning(f"Colonne mancanti nel CSV: {colonne_mancanti}")

    df_result = df[colonne_presenti].copy()

    # Conversione di SEZ2011 a int64 se presente
    if 'SEZ2011' in df_result.columns:
        df_result['SEZ2011'] = df_result['SEZ2011'].astype('int64')

    # Conversione del nome del comune a maiuscolo
    if 'COMUNE' in df_result.columns:
        df_result['COMUNE'] = df_result['COMUNE'].str.upper()

    # Conversione del nome della provincia a maiuscolo
    if 'PROVINCIA' in df_result.columns:
        df_result['PROVINCIA'] = df_result['PROVINCIA'].str.upper()

    return df_result


def salva_dati_variabili_censuarie(df: pd.DataFrame, cartella_output: str, nome_file: str,
                                    sep: str = ';', encoding: str = 'utf-8-sig') -> None:
    """
    Salva un DataFrame in formato CSV.

    Args:
        df: DataFrame da salvare.
        cartella_output: Cartella di destinazione.
        nome_file: Nome del file CSV.
        sep: Separatore.
        encoding: Codifica del file.
    """
    os.makedirs(cartella_output, exist_ok=True)
    output_path = os.path.join(cartella_output, nome_file)
    df.to_csv(output_path, index=False, sep=sep, encoding=encoding)
    logger.info(f"Dati estratti e salvati in: {output_path}")


def run_estrazione_variabili_censuarie() -> pd.DataFrame:
    """
    Funzione principale per l'estrazione e il salvataggio dei dati censuari.
    """
    df_estratto = estrai_dati_variabili_censuarie(INPUT_PATH)
    salva_dati_variabili_censuarie(df_estratto, cartella_output=OUTPUT_DIR, nome_file=OUTPUT_FILENAME)
    return df_estratto


if __name__ == "__main__":
    run_estrazione_variabili_censuarie()
