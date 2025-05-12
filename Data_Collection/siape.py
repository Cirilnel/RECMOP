import requests
import pandas as pd

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


def save_multi_metric_pivot_to_excel(results, filename='epgl_metrics_pivot.xlsx'):
    """
    Crea un pivot table MultiIndex con zone come livello 0,
    metriche come livello 1, righe ordinati per periodi,
    e salva su Excel.
    """
    # Mappa periodi in etichette
    period_map = {
        0: 'Prima del 1945',
        1: '1945 - 1972',
        2: '1973 - 1991',
        3: '1992 - 2005',
        4: '2006 - 2015',
        5: 'Dopo il 2015'
    }
    # Ricostruiamo periodi ciclando
    periods = list(period_map.values())
    # Aggiungiamo period_label secondo l'ordine inserito in results
    labeled = []
    for i, record in enumerate(results):
        # period index = i mod len(periods)
        period_idx = i % len(periods)
        rec = record.copy()
        rec['period_label'] = period_map[period_idx]
        labeled.append(rec)
    df = pd.DataFrame(labeled)
    # Pivot table multiindice colonne
    pivot = df.pivot_table(
        index='period_label',
        columns='zone',
        values=['EPgl_nren', 'EPgl_ren', 'CO2']
    )
    # Swappa per avere zone come primo livello e metriche secondo
    pivot = pivot.swaplevel(0,1, axis=1)
    pivot.sort_index(axis=1, level=0, inplace=True)
    # Imposta nomi
    pivot.index.name = 'Periodo di costruzione'
    pivot.columns.names = ['Zona climatica', 'Metrica']
    # Ordine righe
    pivot = pivot.reindex(periods)
    # Salva
    pivot.to_excel(filename)
    print(f"File salvato: {filename}")

if __name__ == '__main__':
    data = fetch_totals_by_zone_and_period()
    save_multi_metric_pivot_to_excel(data)
