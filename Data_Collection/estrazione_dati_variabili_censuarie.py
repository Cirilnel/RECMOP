import pandas as pd
import os

def estra_dati_variabili_censuarie(file_input, colonne_da_estrarre, sep=';', encoding='latin-1'):
    # Leggi il CSV
    df = pd.read_csv(file_input, sep=sep, encoding=encoding, dtype=str)
    df.columns = df.columns.str.strip()  # Rimuove spazi negli header

    # Verifica la presenza delle colonne richieste
    colonne_mancanti = [col for col in colonne_da_estrarre if col not in df.columns]
    if colonne_mancanti:
        print(f"ATTENZIONE: Le seguenti colonne non sono presenti nel CSV: {colonne_mancanti}")

    # Seleziona solo le colonne presenti
    colonne_presenti = [col for col in colonne_da_estrarre if col in df.columns]
    return df[colonne_presenti]


def salva_csv(df, cartella_output, nome_file_output, sep=';', encoding='utf-8'):
    os.makedirs(cartella_output, exist_ok=True)
    output_path = os.path.join(cartella_output, nome_file_output)
    df.to_csv(output_path, index=False, sep=sep, encoding=encoding)
    print(f"Dati estratti e salvati in: {output_path}")

def get_dati_variabili_censuarie():
    """
    Funzione principale per l'estrazione dei dati delle variabili censuarie.
    """
    csv_path = os.path.join("..", "Istat", "Variabili_Censuarie", "Sezioni_di_Censimento", "Campania.csv")
    colonne = ['SEZ2011', 'COMUNE', 'P1', 'E8', 'E9', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'E16', 'A44']

    df_estratto = estra_dati_variabili_censuarie(csv_path, colonne)
    salva_csv(df_estratto, cartella_output="Table", nome_file_output="variabili_censuarie_R15_campania.csv")


if __name__ == "__main__":
    csv_path = os.path.join("..", "Istat", "Variabili_Censuarie", "Sezioni_di_Censimento", "Campania.csv")
    colonne = ['SEZ2011', 'COMUNE', 'P1', 'E8', 'E9', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'E16', 'A44']

    df_estratto = estra_dati_variabili_censuarie(csv_path, colonne)
    salva_csv(df_estratto, cartella_output="Table", nome_file_output="variabili_censuarie_R15_campania.csv")
