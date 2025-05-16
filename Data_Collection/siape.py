import requests
import pandas as pd
import os

def fetch_totals_by_zone_and_period():
    url = 'https://siape.enea.it/api/v1/aggr-data'

    headers = {
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

    zones = ['A', 'B', 'C', 'D', 'E', 'F']
    periods = [
        (-1000000000, 1944),
        (1944, 1972),
        (1972, 1991),
        (1991, 2005),
        (2005, 2015),
        (2015, 1000000000),
    ]

    results = []

    for zone in zones:
        for start, end in periods:
            data = {
                'group[]': 'claen',
                'where[destuso]': '0',
                'where[annoc][range][]': [str(start), str(end)],
                'where[zoncli][]': zone,
                'nofilter': 'false',
            }
            try:
                resp = requests.post(url, headers=headers, data=data)
                resp.raise_for_status()
                js = resp.json()
                total = js.get('total', [])
                results.append({
                    'zone': zone,
                    'period_label': None,
                    'EPgl_nren': total[1] if len(total) > 1 else None,
                    'EPgl_ren': total[2] if len(total) > 2 else None,
                    'CO2': total[3] if len(total) > 3 else None,
                })

                print(f"[OK] Zone {zone}, anni {start}–{end}: EPgl_nren={results[-1]['EPgl_nren']}, "
                      f"EPgl_ren={results[-1]['EPgl_ren']}, CO2={results[-1]['CO2']}")

            except Exception as e:
                print(f"Error for zone {zone}, period {start}–{end}: {e}")
    return results

def save_epgl_nren_to_excel(results, filename="epgl_nren_tabella_siape.xlsx"):
    """
    Salva i dati EPgl_nren in un file Excel nella cartella 'Table'.
    """
    # Mappatura dell'ordine dei periodi alle intestazioni di colonna
    period_names = {
        0: 'kE8E9',    # (-1000000000, 1944)
        1: 'kE10E11',  # (1944, 1972)
        2: 'kE12E13',  # (1972, 1991)
        3: 'kE14E15',  # (1991, 2005)
        4: 'kE16',     # (2005, 2015)
        5: 'k2015',    # (2015, 1000000000)
    }

    # Aggiungiamo un indice "period_index" basato sulla posizione nella lista results
    enriched = []
    counts_by_zone = {}
    for row in results:
        zone = row['zone']
        counts_by_zone.setdefault(zone, 0)
        idx = counts_by_zone[zone]
        counts_by_zone[zone] += 1

        enriched.append({
            'zona_climatica': zone,
            'period_index': idx,
            'EPgl_nren': row['EPgl_nren']
        })

    # Costruiamo DataFrame e facciamo il pivot
    df = pd.DataFrame(enriched)
    pivot = df.pivot(index='zona_climatica',
                     columns='period_index',
                     values='EPgl_nren')

    # Rinominiamo le colonne
    pivot = pivot.rename(columns=period_names)

    # Assicuriamoci che tutte le colonne siano nell'ordine desiderato
    ordered_cols = [period_names[i] for i in range(len(period_names))]
    pivot = pivot[ordered_cols]

    # Creiamo la cartella Table se non esiste
    output_dir = "Table"
    os.makedirs(output_dir, exist_ok=True)

    # Percorso finale del file
    output_path = os.path.join(output_dir, filename)

    # Scriviamo in Excel
    pivot.to_excel(output_path, index=True, index_label='zona_climatica')

    print(f"📑 File salvato come {output_path}")

if __name__ == '__main__':
    data = fetch_totals_by_zone_and_period()
    save_epgl_nren_to_excel(data)
