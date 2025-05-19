import requests
import pandas as pd
import os

def estrai_dati_siape():
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
            payload = {
                'group[]': 'claen',
                'where[destuso]': '0',
                'where[annoc][range][]': [str(start), str(end)],
                'where[zoncli][]': zone,
                'nofilter': 'false',
            }
            try:
                resp = requests.post(url, headers=headers, data=payload)
                resp.raise_for_status()
                js = resp.json()
                total = js.get('total', [])
                results.append({
                    'zone': zone,
                    'period_label': f"{start}-{end}",
                    'EPgl_nren': total[1] if len(total) > 1 else None,
                    'EPgl_ren': total[2] if len(total) > 2 else None,
                    'CO2': total[3] if len(total) > 3 else None,
                })
                print(f"[OK] Zone {zone}, anni {start}–{end}: EPgl_nren={results[-1]['EPgl_nren']}, "
                      f"EPgl_ren={results[-1]['EPgl_ren']}, CO2={results[-1]['CO2']}")
            except Exception as e:
                print(f"Error for zone {zone}, period {start}–{end}: {e}")
    return results


def salva_csv(results, filename="epgl_nren_tabella_siape.csv", sep=';'):
    """
    Salva i dati EPgl_nren in un file CSV nella cartella 'Table'.
    Usa come separatore il carattere specificato (default ';') per compatibilità con Excel locale.
    """
    period_names = {
        0: 'kE8E9',
        1: 'kE10E11',
        2: 'kE12E13',
        3: 'kE14E15',
        4: 'kE16',
        5: 'k2015',
    }

    enriched = []
    counts_by_zone = {}
    for row in results:
        zone = row['zone']
        idx = counts_by_zone.get(zone, 0)
        counts_by_zone[zone] = idx + 1
        enriched.append({
            'zona_climatica': zone,
            'period_index': idx,
            'EPgl_nren': row['EPgl_nren']
        })

    df = pd.DataFrame(enriched)
    pivot = df.pivot(index='zona_climatica', columns='period_index', values='EPgl_nren')
    pivot = pivot.rename(columns=period_names)
    ordered_cols = [period_names[i] for i in range(len(period_names))]
    pivot = pivot[ordered_cols]

    output_dir = "Table"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    # Scriviamo in CSV con header esplicito e label dell'indice
    pivot.to_csv(output_path,
                 sep=sep,
                 index=True,
                 index_label='zona_climatica',
                 header=True,
                 encoding='utf-8')

    print(f"File salvato come {output_path} separato da '{sep}'")

def get_dati_siape():
    """
    Funzione principale per l'estrazione dei dati EPgl_nren da SIAPE.
    """
    data = estrai_dati_siape()
    salva_csv(data)
    print(f"Esportazione completata: {len(data)} record scritti in epgl_nren_tabella_siape.csv")


if __name__ == '__main__':
    data = estrai_dati_siape()
    salva_csv(data)
