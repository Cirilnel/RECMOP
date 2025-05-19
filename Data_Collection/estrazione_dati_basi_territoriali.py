import os
import logging
from typing import List, Dict
import pandas as pd
from dbfread import DBF

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Costanti
DBF_PATH = os.path.join('..', 'Istat', 'Regioni', 'Campania', 'R15_11_WGS84.dbf')
OUTPUT_DIR = "Table"
OUTPUT_FILENAME = "basi_territoriali_R15_Campania.csv"
CAMPI_ESTRATTI = ['COD_REG', 'COD_ISTAT', 'PRO_COM', 'SEZ2011', 'SEZ', 'COD_LOC', 'TIPO_LOC']


def estrai_dati_basi_territoriali(percorso_dbf: str) -> List[Dict[str, int]]:
    """
    Estrae dal file DBF i campi specificati.

    Args:
        percorso_dbf: Percorso al file .dbf.

    Returns:
        Lista di dizionari con i valori interi estratti.
    """
    table = DBF(percorso_dbf, load=True, ignorecase=True, recfactory=dict)
    records = []

    for record in table:
        riga = {campo: record.get(campo) for campo in CAMPI_ESTRATTI}
        records.append(riga)

    return records


def salva_dati_basi_territoriali(records: List[Dict[str, int]], output_csv_path: str) -> pd.DataFrame:
    """
    Salva i dati in formato CSV, forzando le colonne a interi.

    Args:
        records: Lista di dizionari estratti dal DBF.
        output_csv_path: Percorso del file CSV di output.
    """
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df = pd.DataFrame(records)
    #df = df.astype('int64')
    df.to_csv(output_csv_path, index=False, sep=';')
    logger.info(f"Dati salvati in: {output_csv_path}")

    return df


def run_estrazione_basi_territoriali() -> pd.DataFrame:
    """
    Funzione principale per estrarre e salvare i dati delle basi territoriali.
    """
    dati = estrai_dati_basi_territoriali(DBF_PATH)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
    df = salva_dati_basi_territoriali(dati, output_path)
    logger.info(f"Esportazione completata: {len(dati)} record scritti.")

    return df


if __name__ == '__main__':
    run_estrazione_basi_territoriali()
