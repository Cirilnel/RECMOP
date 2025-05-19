from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import os

def estrai_dati_normattiva():
    atto_url = "https://www.normattiva.it/uri-res/N2Ls?urn:nir:presidente.repubblica:decreto:1993-08-26;412"
    base_articolo_url = (
        "https://www.normattiva.it/atto/caricaArticolo?"
        "art.versione=46&art.idGruppo=0&art.flagTipoArticolo=1&"
        "art.codiceRedazionale=093G0451&art.idArticolo=1&"
        "art.idSottoArticolo=1&art.idSottoArticolo1=10&"
        "art.dataPubblicazioneGazzetta=1993-10-14&art.progressivo="
    )
    numero_articoli = 4
    dati_finali = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print(f"Caricamento atto principale: {atto_url}")
        page.goto(atto_url)

        for progressivo in range(1, numero_articoli + 1):
            url = base_articolo_url + str(progressivo)
            print(f"\nScaricando dati da: {url}")

            page.goto(url)
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            testo = soup.get_text(separator="\n")

            if progressivo == 1:
                start_index = testo.find("pr z gr-g alt comune")
            else:
                start_index = testo.find("Testo in vigore dal")
                if start_index != -1:
                    start_index = testo.find("\n", start_index)

            if start_index != -1:
                dati_grezzi = testo[start_index:].strip().split("\n")

                dati_puliti = []
                for riga in dati_grezzi:
                    if progressivo == 1 and "pr z gr-g alt comune" in riga:
                        continue
                    if (riga.strip() == "" or
                        "parte" in riga.lower() or
                        "aggiornamenti" in riga.lower() or
                        "Testo in vigore" in riga or
                        "articolo precedente" in riga.lower() or
                        "articolo successivo" in riga.lower()):
                        continue
                    dati_puliti.append(riga.strip())

                dati_finali.extend(dati_puliti)
            else:
                print(f"Nessun dato trovato nella pagina {progressivo}")

        browser.close()

    return dati_finali


def salva_csv(dati, output_dir, nome_file_output):
    if not dati:
        print("Nessun dato da scrivere.")
        return

    os.makedirs(output_dir, exist_ok=True)
    records = []
    for riga in dati:
        colonne = riga.split()
        record = colonne[:4] + [' '.join(colonne[4:])]
        records.append(record)

    output_path = os.path.join(output_dir, nome_file_output)
    df = pd.DataFrame(records, columns=["Provincia", "Zona", "GradiGiorno", "Altitudine", "Comune"])
    df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
    print(f"Dati salvati in '{output_path}'")

def get_dati_normattiva():
    """
    Funzione principale per l'estrazione dei dati da Normattiva.
    """
    output_dir = "Table"
    nome_file_output = "dati_normattiva_playwright_finale.csv"

    dati = estrai_dati_normattiva()
    salva_csv(dati, output_dir, nome_file_output)
    print(f"Esportazione completata: {len(dati)} record scritti in {output_dir}/{nome_file_output}")

if __name__ == "__main__":
    output_dir = "Table"
    nome_file_output = "dati_normattiva_playwright_finale.csv"

    dati = estrai_dati_normattiva()
    salva_csv(dati, output_dir, nome_file_output)
