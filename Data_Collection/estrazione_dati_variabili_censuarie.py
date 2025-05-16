import pandas as pd
import os

# Percorso del file sorgente
csv_path = os.path.join("..", "Istat", "Variabili_Censuarie", "Sezioni_di_Censimento", "Campania.csv")

# Percorso di output
output_path = os.path.join("output_dati_campania.xlsx")

# Colonne da estrarre
colonne_da_estrarre = ['SEZ2011', 'COMUNE', 'P1', 'E8', 'E9', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'E16', 'A44']

# Leggi il CSV con encoding latin-1
df = pd.read_csv(csv_path, sep=';', encoding='latin-1', dtype=str)

# Seleziona solo le colonne richieste, verificando che ci siano tutte
colonne_presenti = [col for col in colonne_da_estrarre if col in df.columns]
df_selezionato = df[colonne_presenti]

# Esporta in Excel
df_selezionato.to_excel(output_path, index=False)

print(f"Dati estratti e salvati in: {output_path}")
