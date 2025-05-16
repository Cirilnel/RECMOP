import pandas as pd
import os

# Percorso del file sorgente
csv_path = os.path.join("..", "Istat", "Variabili_Censuarie", "Sezioni_di_Censimento", "Campania.csv")

# Percorso della cartella di output
output_dir = os.path.join("Table")
os.makedirs(output_dir, exist_ok=True)  # Crea la cartella se non esiste

# Percorso di output CSV dentro Table
output_csv_path = os.path.join(output_dir, "output_dati_campania.csv")

# Colonne da estrarre
colonne_da_estrarre = ['SEZ2011', 'COMUNE', 'P1', 'E8', 'E9', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'E16', 'A44']

# Leggi il CSV con encoding latin-1 e pulisci gli header
df = pd.read_csv(csv_path, sep=';', encoding='latin-1', dtype=str)
df.columns = df.columns.str.strip()  # Rimuove spazi bianchi da intestazioni

# Verifica se tutte le colonne richieste sono presenti
colonne_mancanti = [col for col in colonne_da_estrarre if col not in df.columns]
if colonne_mancanti:
    print(f"ATTENZIONE: Le seguenti colonne non sono presenti nel CSV: {colonne_mancanti}")

# Seleziona solo le colonne presenti
colonne_presenti = [col for col in colonne_da_estrarre if col in df.columns]
df_selezionato = df[colonne_presenti]

# Esporta in CSV con ; come separatore
df_selezionato.to_csv(output_csv_path, index=False, sep=';', encoding='utf-8')

print(f"Dati estratti e salvati in: {output_csv_path}")
