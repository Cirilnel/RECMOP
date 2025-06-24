import os
import logging
import pandas as pd
from dbfread import DBF

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Costanti
BASE_INPUT_DIR = os.path.join("..", "Istat", "Regioni")
OUTPUT_DIR = "../Data_Collection/csv_tables-fase1"
CAMPI_ESTRATTI = ['COD_REG', 'COD_ISTAT', 'PRO_COM', 'SEZ2011', 'SEZ', 'COD_LOC', 'TIPO_LOC']


def trova_dbf_in_regione(regione: str) -> str:
    """
    Trova il file .dbf all'interno della cartella della regione specificata.

    Args:
        regione: Nome della regione (es. "Campania")

    Returns:
        Percorso completo al file .dbf

    Raises:
        FileNotFoundError: Se nessun file DBF viene trovato.
    """
    cartella = os.path.join(BASE_INPUT_DIR, regione)
    dbf_files = [f for f in os.listdir(cartella) if f.lower().endswith(".dbf")]

    if not dbf_files:
        raise FileNotFoundError(f"Nessun file DBF trovato nella cartella: {cartella}")
    if len(dbf_files) > 1:
        logger.warning(f"Trovati più file DBF in {cartella}, verrà usato il primo: {dbf_files[0]}")

    return os.path.join(cartella, dbf_files[0])


def estrai_dati_basi_territoriali(percorso_file: str) -> pd.DataFrame:
    """
    Estrae i campi territoriali specificati da un file DBF.
    """
    table = DBF(percorso_file, load=True, ignorecase=True, recfactory=dict)
    records = []

    for record in table:
        riga = {campo: record.get(campo) for campo in CAMPI_ESTRATTI}
        records.append(riga)

    df = pd.DataFrame(records)

    for col in df.columns:
        try:
            df[col] = df[col].astype("int64")
        except (ValueError, TypeError):
            logger.warning(f"Colonna non convertita a intero: {col}")

    return df


def salva_dati_basi_territoriali(df: pd.DataFrame, cartella_output: str, nome_file: str,
                                 sep: str = ';', encoding: str = 'utf-8-sig') -> None:
    """
    Salva un DataFrame in formato CSV.
    """
    os.makedirs(cartella_output, exist_ok=True)
    output_path = os.path.join(cartella_output, nome_file)
    df.to_csv(output_path, index=False, sep=sep, encoding=encoding)
    logger.info(f"Dati estratti e salvati in: {output_path}")


def run_estrazione_basi_territoriali(regione: str) -> pd.DataFrame:
    """
    Estrae e salva i dati delle basi territoriali per una data regione.

    Args:
        regione: Nome della regione (es. "Campania")

    Returns:
        DataFrame estratto.
    """
    input_path = trova_dbf_in_regione(regione)
    output_filename = f"basi_territoriali_{regione.lower()}.csv"

    df_estratto = estrai_dati_basi_territoriali(input_path)
    salva_dati_basi_territoriali(df_estratto, cartella_output=OUTPUT_DIR, nome_file=output_filename)
    return df_estratto


def get_dati_basi_territoriali(regione: str) -> pd.DataFrame:
    """
    Restituisce il DataFrame delle basi territoriali, estraendolo se non esiste il CSV.

    Args:
        regione: Nome della regione

    Returns:
        DataFrame
    """
    output_filename = f"basi_territoriali_{regione.lower()}.csv"
    path_csv = os.path.join(OUTPUT_DIR, output_filename)

    if not os.path.exists(path_csv):
        logger.warning(f"File CSV non trovato. Avvio estrazione per la regione: {regione}")
        return run_estrazione_basi_territoriali(regione)

    df = pd.read_csv(path_csv, sep=';', encoding='utf-8-sig')
    logger.info(f"Dati caricati da: {path_csv}")
    return df


if __name__ == '__main__':
    get_dati_basi_territoriali("Campania")
