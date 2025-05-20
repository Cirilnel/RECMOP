import os
import logging
from typing import List, Dict
import requests
import pandas as pd

# Impostazione logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Costanti
URL_SIAPE = "https://siape.enea.it/api/v1/aggr-data"
OUTPUT_DIR = "../Data_Collection/csv_tables-fase1"
OUTPUT_FILENAME = "epgl_nren_tabella_siape.csv"
ZONES = ['A', 'B', 'C', 'D', 'E', 'F']
PERIODS = [
    (-1000000000, 1944),
    (1944, 1972),
    (1972, 1991),
    (1991, 2005),
    (2005, 2015),
    (2015, 1000000000),
]
PERIOD_LABELS = {
    0: 'kE8E9',
    1: 'kE10E11',
    2: 'kE12E13',
    3: 'kE14E15',
    4: 'kE16',
    5: 'k2015',
}

HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-store, no-cache, must-revalidate',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://siape.enea.it',
    'Referer': 'https://siape.enea.it/caratteristiche-immobili',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/136.0.0.0 Safari/537.36'),
    'X-Requested-With': 'XMLHttpRequest',
}


def estrai_dati_siape() -> List[Dict[str, str]]:
    """
    Estrae i dati aggregati EPgl_nren da SIAPE per ogni zona climatica e periodo.

    Returns:
        List[Dict]: Lista di dizionari con i dati per ciascuna zona e periodo.
    """
    risultati = []

    for zona in ZONES:
        for inizio, fine in PERIODS:
            payload = {
                'group[]': 'claen',
                'where[destuso]': '0',
                'where[annoc][range][]': [str(inizio), str(fine)],
                'where[zoncli][]': zona,
                'nofilter': 'false',
            }
            try:
                response = requests.post(URL_SIAPE, headers=HEADERS, data=payload)
                response.raise_for_status()
                json_data = response.json()
                total = json_data.get('total', [])

                risultati.append({
                    'zona': zona,
                    'periodo': f"{inizio}-{fine}",
                    'EPgl_nren': total[1] if len(total) > 1 else None,
                    'EPgl_ren': total[2] if len(total) > 2 else None,
                    'CO2': total[3] if len(total) > 3 else None,
                })

                logger.info(f"[OK] Zona {zona}, anni {inizio}–{fine}: "
                            f"EPgl_nren={risultati[-1]['EPgl_nren']}, "
                            f"EPgl_ren={risultati[-1]['EPgl_ren']}, CO2={risultati[-1]['CO2']}")

            except Exception as e:
                logger.warning(f"[ERRORE] Zona {zona}, anni {inizio}–{fine}: {e}")

    return risultati


def salva_dati_siape(dati: List[Dict[str, str]], filename: str = OUTPUT_FILENAME, sep: str = ";") -> pd.DataFrame:
    """
    Salva i dati EPgl_nren in un file CSV pivotato per zona climatica e periodo.

    Args:
        filename: Nome del file di output.
        sep: Separatore per il CSV.
        dati: Lista di dizionari con i dati da salvare.
    """
    tabella = get_dataframe_siape(dati)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    percorso_output = os.path.join(OUTPUT_DIR, filename)
    tabella.to_csv(percorso_output, sep=sep, index=True, index_label='zona_climatica', encoding='utf-8')

    logger.info(f"Dati salvati in: {percorso_output}")

    return tabella

def get_dataframe_siape(dati: List[Dict[str, str]]) -> pd.DataFrame:
    valori = []
    contatori = {}

    for riga in dati:
        zona = riga['zona']
        indice = contatori.get(zona, 0)
        contatori[zona] = indice + 1

        valori.append({
            'zona_climatica': zona,
            'indice_periodo': indice,
            'EPgl_nren': riga['EPgl_nren']
        })

    df = pd.DataFrame(valori)
    tabella = df.pivot(index='zona_climatica', columns='indice_periodo', values='EPgl_nren')
    tabella = tabella.rename(columns=PERIOD_LABELS)
    tabella = tabella[[PERIOD_LABELS[i] for i in range(len(PERIOD_LABELS))]]

    return tabella

def run_estrazione_siape() -> pd.DataFrame:
    """
    Funzione principale per estrarre e salvare i dati SIAPE.
    """
    dati = estrai_dati_siape()
    df = salva_dati_siape(dati)
    logger.info(f"Esportazione completata: {len(dati)} record scritti.")
    return df


if __name__ == '__main__':
    run_estrazione_siape()
