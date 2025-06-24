import os
import logging
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Costanti
ATTO_URL = "https://www.normattiva.it/uri-res/N2Ls?urn:nir:presidente.repubblica:decreto:1993-08-26;412"
BASE_ARTICOLO_URL = (
    "https://www.normattiva.it/atto/caricaArticolo?"
    "art.versione=46&art.idGruppo=0&art.flagTipoArticolo=1&"
    "art.codiceRedazionale=093G0451&art.idArticolo=1&"
    "art.idSottoArticolo=1&art.idSottoArticolo1=10&"
    "art.dataPubblicazioneGazzetta=1993-10-14&art.progressivo="
)
NUM_ARTICOLI = 4
OUTPUT_DIR = "../Data_Collection/csv_tables-fase1"
OUTPUT_FILENAME = "dati_normattiva.csv"


def estrai_dati_normattiva() -> pd.DataFrame:
    """
    Estrae i dati climatici dal decreto su Normattiva usando Playwright e li restituisce come DataFrame.
    """
    dati_finali = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        logger.info(f"Caricamento atto principale: {ATTO_URL}")
        page.goto(ATTO_URL)

        for progressivo in range(1, NUM_ARTICOLI + 1):
            url = BASE_ARTICOLO_URL + str(progressivo)
            logger.info(f"Scaricamento dati da: {url}")

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

                dati_puliti = [
                    riga.strip()
                    for riga in dati_grezzi
                    if riga.strip()
                    and "parte" not in riga.lower()
                    and "aggiornamenti" not in riga.lower()
                    and "Testo in vigore" not in riga
                    and "articolo precedente" not in riga.lower()
                    and "articolo successivo" not in riga.lower()
                    and not (progressivo == 1 and "pr z gr-g alt comune" in riga)
                ]

                dati_finali.extend(dati_puliti)
            else:
                logger.warning(f"Nessun dato trovato nella pagina {progressivo}")

        browser.close()

    # Costruzione del DataFrame dai dati finali
    records = []
    righe_scartate = 0

    for riga in dati_finali:
        riga_pulita = riga.strip()

        if riga_pulita.startswith("((") and riga_pulita.endswith("))"):
            riga_pulita = riga_pulita[2:-2].strip()

        colonne = riga_pulita.split()

        if len(colonne) >= 5 and len(colonne[0]) == 2:
            provincia = colonne[0]
            zona = colonne[1]
            gradi_giorno = colonne[2].replace("O", "0")
            altitudine = colonne[3].replace("O", "0")
            comune = ' '.join(colonne[4:])
            records.append([provincia, zona, gradi_giorno, altitudine, comune])
        else:
            righe_scartate += 1
            logger.debug(f"Riga scartata (formato non valido): {riga}")

    logger.info(f"Totale righe scartate: {righe_scartate}")

    df = pd.DataFrame(records, columns=["PROVINCIA", "ZONA_CLIMATICA", "GRADI_GIORNO", "ALTITUDINE", "COMUNE"])
    return df


def salva_dati_normattiva(df: pd.DataFrame, output_dir: str = OUTPUT_DIR, nome_file: str = OUTPUT_FILENAME) -> pd.DataFrame:
    """
    Salva i dati estratti da Normattiva in formato CSV.
    """
    if df.empty:
        logger.warning("Nessun dato da scrivere.")
        return df

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, nome_file)
    df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")

    logger.info(f"Dati salvati in: {output_path}")
    return df


def run_estrazione_normattiva() -> pd.DataFrame:
    """
    Funzione principale per estrarre e salvare i dati da Normattiva.
    """
    df = estrai_dati_normattiva()
    salva_dati_normattiva(df)
    logger.info(f"Esportazione completata: {len(df)} record scritti.")
    return df


def get_dati_normattiva() -> pd.DataFrame:
    """
    Ritorna il DataFrame dei dati Normattiva, estraendolo se il CSV non esiste.
    """
    path_csv = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

    if not os.path.exists(path_csv):
        logger.warning(f"File non trovato. Avvio estrazione: {path_csv}")
        return run_estrazione_normattiva()

    df = pd.read_csv(path_csv, sep=";", encoding="utf-8-sig", keep_default_na=False)
    logger.info(f"Dati caricati da: {path_csv}")
    return df


if __name__ == "__main__":
    get_dati_normattiva()
