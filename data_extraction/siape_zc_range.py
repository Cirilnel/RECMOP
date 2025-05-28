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
OUTPUT_FILENAME = "epgl_nren_ren_co2_tabella_siape_zc_range.csv"
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
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/136.0.0.0 Safari/537.36'
    ),
    'X-Requested-With': 'XMLHttpRequest',
}

def estrai_dati_siape() -> List[Dict[str, str]]:
    risultati = []

    for zona in ZONES:
        for idx, (inizio, fine) in enumerate(PERIODS):
            periodo_label = PERIOD_LABELS.get(idx, f"{inizio}-{fine}")

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
                    'periodo': periodo_label,
                    'EPgl_nren': total[1] if len(total) > 1 else None,
                    'EPgl_ren': total[2] if len(total) > 2 else None,
                    'CO2': total[3] if len(total) > 3 else None,
                })

                logger.info(
                    f"[OK] Zona {zona}, periodo {periodo_label}: "
                    f"EPgl_nren={total[1] if len(total) > 1 else 'N/D'}, "
                    f"EPgl_ren={total[2] if len(total) > 2 else 'N/D'}, "
                    f"CO2={total[3] if len(total) > 3 else 'N/D'}"
                )

            except Exception as e:
                logger.warning(
                    f"[ERRORE] Zona {zona}, periodo {periodo_label}: {e}"
                )

    return risultati


def get_dataframe_siape(dati: List[Dict[str, str]]) -> pd.DataFrame:
    df = pd.DataFrame([{
        'zona_climatica': r['zona'],
        'periodo': r['periodo'],
        'EPgl_nren': r['EPgl_nren'],
        'EPgl_ren': r['EPgl_ren'],
        'CO2': r['CO2'],
    } for r in dati])
    return df


def salva_dati_siape(dati: List[Dict[str, str]], filename: str = OUTPUT_FILENAME, sep: str = ";") -> pd.DataFrame:
    df = get_dataframe_siape(dati)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    percorso_output = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(percorso_output, sep=sep, index=False, encoding='utf-8')

    logger.info(f"Dati salvati in: {percorso_output}")
    return df


def run_estrazione_siape() -> pd.DataFrame:
    dati = estrai_dati_siape()
    df = salva_dati_siape(dati)
    logger.info(f"Esportazione completata: {len(dati)} record scritti.")
    return df


if __name__ == '__main__':
    run_estrazione_siape()
