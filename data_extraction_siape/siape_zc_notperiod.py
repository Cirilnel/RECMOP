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
OUTPUT_FILENAME = "epgl_nren_ren_co2_tabella_siape_zc_notperiod.csv"
ZONES = ['A', 'B', 'C', 'D', 'E', 'F']

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
    risultati = []

    SURIS_RANGES = [
        (-1000000000, 50), (50, 100), (100, 200),
        (200, 500), (500, 1000), (1000, 5000), (5000, 1000000000)
    ]
    VOLRIS_RANGES = [
        (-1000000000, 50), (50, 100), (100, 200),
        (200, 500), (500, 1000), (1000, 5000),
        (5000, 10000), (10000, 1000000000)
    ]

    for zona in ZONES:
        for suris_min, suris_max in SURIS_RANGES:
            for volris_min, volris_max in VOLRIS_RANGES:

                suris_str = format_range(suris_min, suris_max)
                volris_str = format_range(volris_min, volris_max)

                payload = {
                    'group[]': 'claen',
                    'where[destuso]': '0',
                    'where[zoncli][]': zona,
                    'where[suris][range][]': [str(suris_min), str(suris_max)],
                    'where[volris][range][]': [str(volris_min), str(volris_max)],
                    'nofilter': 'false',
                }

                try:
                    response = requests.post(URL_SIAPE, headers=HEADERS, data=payload)
                    response.raise_for_status()
                    json_data = response.json()
                    total = json_data.get('total', [])

                    risultati.append({
                        'zona': zona,
                        'suris': suris_str,
                        'volris': volris_str,
                        'EPgl_nren': total[1] if len(total) > 1 else None,
                        'EPgl_ren': total[2] if len(total) > 2 else None,
                        'CO2': total[3] if len(total) > 3 else None,
                    })

                    logger.info(f"[OK] Zona {zona}, "
                                f"SURIS {suris_str}, VOLRIS {volris_str}: "
                                f"EPgl_nren={total[1] if len(total) > 1 else 'N/D'}, "
                                f"EPgl_ren={total[2] if len(total) > 2 else 'N/D'}, "
                                f"CO2={total[3] if len(total) > 3 else 'N/D'}")

                except Exception as e:
                    logger.warning(f"[ERRORE] Zona {zona}, "
                                   f"SURIS {suris_str}, VOLRIS {volris_str}: {e}")

    return risultati


def format_range(min_val: int, max_val: int) -> str:
    if min_val == -1000000000:
        return f"<{max_val}"
    elif max_val == 1000000000:
        return f">{min_val}"
    else:
        return f"{min_val}-{max_val}"


def get_dataframe_siape(dati: List[Dict[str, str]]) -> pd.DataFrame:
    valori = []

    for riga in dati:
        valori.append({
            'zona_climatica': riga['zona'],
            'Superficie Utile Riscaldata': riga['suris'],
            'Volume Lordo Riscaldato': riga['volris'],
            'EPgl_nren': riga['EPgl_nren'],
            'EPgl_ren': riga['EPgl_ren'],
            'CO2': riga['CO2'],
        })

    df = pd.DataFrame(valori)
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
