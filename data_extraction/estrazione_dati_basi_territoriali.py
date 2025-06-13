import os
import logging
import pandas as pd
from dbfread import DBF

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Costanti
INPUT_PATH = os.path.join('..', 'Istat', 'Regioni', 'Campania', 'R15_11_WGS84.dbf')
OUTPUT_DIR = "../Data_Collection/csv_tables-fase1"
OUTPUT_FILENAME = "basi_territoriali_R15_Campania.csv"
CAMPI_ESTRATTI = ['COD_REG', 'COD_ISTAT', 'PRO_COM', 'SEZ2011', 'SEZ', 'COD_LOC', 'TIPO_LOC']


def estrai_dati_basi_territoriali(percorso_file: str) -> pd.DataFrame:
    """
    Estrae i campi territoriali specificati da un file DBF.

    Args:
        percorso_file: Percorso al file DBF.

    Returns:
        DataFrame con i campi selezionati convertiti in int64 se possibile.
    """
    table = DBF(percorso_file, load=True, ignorecase=True, recfactory=dict)
    records = []

    for record in table:
        riga = {campo: record.get(campo) for campo in CAMPI_ESTRATTI}
        records.append(riga)

    df = pd.DataFrame(records)

    # Conversione colonne a intero dove possibile
    for col in df.columns:
        try:
            df[col] = df[col].astype('int64')
        except (ValueError, TypeError):
            logger.warning(f"Colonna non convertita a intero: {col}")

    return df


def salva_dati_basi_territoriali(df: pd.DataFrame, cartella_output: str, nome_file: str,
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


def run_estrazione_basi_territoriali() -> pd.DataFrame:
    """
    Funzione principale per l'estrazione e il salvataggio dei dati delle basi territoriali.
    """
    df_estratto = estrai_dati_basi_territoriali(INPUT_PATH)
    salva_dati_basi_territoriali(df_estratto, cartella_output=OUTPUT_DIR, nome_file=OUTPUT_FILENAME)
    return df_estratto


if __name__ == '__main__':
    run_estrazione_basi_territoriali()
