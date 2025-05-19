from dbfread import DBF
import pandas as pd
import os

def estrai_dati_basi_territoriali(dbf_path):
    """
    Estrae dal file DBF di input le colonne specificate e le restituisce come lista di dizionari.

    Parametri:
        dbf_path (str): percorso al file .dbf da cui estrarre i dati.

    Ritorna:
        list of dict: ogni dizionario contiene le chiavi:
            - COD_REG
            - COD_ISTAT
            - PRO_COM
            - SEZ2011
            - SEZ
            - COD_LOC
            - TIPO_LOC
    """
    fields = ['COD_REG', 'COD_ISTAT', 'PRO_COM', 'SEZ2011', 'SEZ', 'COD_LOC', 'TIPO_LOC']
    table = DBF(dbf_path, load=True, ignorecase=True, recfactory=dict)
    records = []
    for record in table:
        row = {field: record.get(field) for field in fields}
        records.append(row)
    return records


def salva_csv(records, output_csv_path):
    """
    Scrive la lista di dizionari in un file CSV, forzando i valori interi senza decimali.

    Parametri:
        records (list of dict): dati estratti dalla funzione extract_columns.
        output_csv_path (str): percorso di destinazione del file CSV.
    """
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

    # Crea il DataFrame
    df = pd.DataFrame(records)

    # Forza tutte le colonne a intero (int64)
    df = df.astype({col: 'int64' for col in df.columns})

    # Esporta in CSV usando punto e virgola come separatore
    df.to_csv(output_csv_path, index=False, sep=';')

def get_dati_basi_territoriali():
    """
    Funzione principale per l'estrazione dei dati delle basi territoriali.
    """
    dbf_path = os.path.join('..', 'Istat', 'Regioni', 'Campania', 'R15_11_WGS84.dbf')
    output_csv_path = os.path.join('Table', 'basi_territoriali_R15_Campania.csv')

    dati = estrai_dati_basi_territoriali(dbf_path)
    salva_csv(dati, output_csv_path)

    print(f"Esportazione completata: {len(dati)} record scritti in {output_csv_path}")

if __name__ == '__main__':
    dbf_path = os.path.join('..', 'Istat', 'Regioni', 'Campania', 'R15_11_WGS84.dbf')
    output_csv_path = os.path.join('Table', 'basi_territoriali_R15_Campania.csv')

    dati = estrai_dati_basi_territoriali(dbf_path)
    salva_csv(dati, output_csv_path)

    print(f"Esportazione completata: {len(dati)} record scritti in {output_csv_path}")
