from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd

# URL decreto principale
atto_url = "https://www.normattiva.it/uri-res/N2Ls?urn:nir:presidente.repubblica:decreto:1993-08-26;412"

# URL base articoli
base_articolo_url = "https://www.normattiva.it/atto/caricaArticolo?art.versione=46&art.idGruppo=0&art.flagTipoArticolo=1&art.codiceRedazionale=093G0451&art.idArticolo=1&art.idSottoArticolo=1&art.idSottoArticolo1=10&art.dataPubblicazioneGazzetta=1993-10-14&art.progressivo="

dati_finali = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    # 1️⃣ carico la pagina del decreto (crea la sessione server-side)
    print(f"📝 Caricamento atto principale: {atto_url}")
    page.goto(atto_url)

    # 2️⃣ ora posso accedere agli articoli
    for progressivo in range(1, 5):
        url = base_articolo_url + str(progressivo)
        print(f"\n👉 Scaricando dati da: {url}")

        page.goto(url)
        content = page.content()

        soup = BeautifulSoup(content, 'html.parser')
        testo = soup.get_text(separator="\n")
        print(f"📜 Primi 500 caratteri:\n{testo[:500]}")

        if progressivo == 1:
            start_index = testo.find("pr z gr-g alt comune")
            print(f"🔍 progressivo 1 — posizione 'pr z gr-g alt comune': {start_index}")
        else:
            start_index = testo.find("Testo in vigore dal")
            if start_index != -1:
                start_index = testo.find("\n", start_index)

        if start_index != -1:
            dati_grezzi = testo[start_index:].strip().split("\n")
            print(f"📦 Righe trovate ({len(dati_grezzi)}): {dati_grezzi[:10]}")

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
            print(f"❌ Nessun dato trovato nella pagina {progressivo}")

    browser.close()

# Scrittura su Excel
if dati_finali:
    records = []
    for riga in dati_finali:
        colonne = riga.split()
        record = colonne[:4] + [' '.join(colonne[4:])]
        records.append(record)

    df = pd.DataFrame(records, columns=["Provincia", "Zona", "GradiGiorno", "Altitudine", "Comune"])
    df.to_excel('dati_normattiva_playwright_finale.xlsx', index=False)
    print("\n📑 Dati salvati in 'dati_normattiva_playwright_finale.xlsx'")
else:
    print("\n⚠️ Nessun dato trovato.")
