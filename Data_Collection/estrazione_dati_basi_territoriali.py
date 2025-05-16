from dbfread import DBF
import pandas as pd

def extract_columns(dbf_path):
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
    # Campi da estrarre
    fields = ['COD_REG', 'COD_ISTAT', 'PRO_COM', 'SEZ2011', 'SEZ', 'COD_LOC', 'TIPO_LOC']
    # Carica il DBF, ignorando il case delle colonne
    table = DBF(dbf_path, load=True, ignorecase=True, recfactory=dict)
    records = []
    for record in table:
        # Estrae solo i campi d'interesse
        row = {field: record.get(field) for field in fields}
        records.append(row)
    return records


def write_to_excel(records, output_excel_path='basi_territoriali_R15_Campania.xlsx'):
    """
    Scrive la lista di dizionari in un file Excel.

    Parametri:
        records (list of dict): dati estratti dalla funzione extract_columns.
        output_excel_path (str): percorso di destinazione del file Excel.
    """
    # Crea un DataFrame e lo esporta in Excel
    df = pd.DataFrame(records)
    df.to_excel(output_excel_path, index=False)


if __name__ == '__main__':
    # Esempio d'uso
    import os
    path = os.path.join('..','Istat', 'Regioni', 'Campania', 'R15_11_WGS84.dbf')
    dati = extract_columns(path)
    write_to_excel(dati)
    print(f"Esportazione completata: {len(dati)} record scritti in basi_territoriali_R15_Campania.xlsx")
