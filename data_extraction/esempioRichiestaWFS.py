import requests
from pyproj import Transformer

# URL del servizio WFS
url = "https://wfs.cartografia.agenziaentrate.gov.it/inspire/wfs/owfs01.php"

lon = 15.657233665710933
lat = 40.338808262979505

# Parametri della richiesta
params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "typenames": "CP:CadastralParcel",
    "outputFormat": "application/gml+xml; version=3.2",
     "cql_filter": f"CONTAINS(geom, POINT({lat} {lon}))"
}

# Effettua la richiesta
response = requests.get(url, params=params)

# Stampa il link della richiesta
print(f"Link della richiesta: {response.url}")

# Salva la risposta se ok
if response.status_code == 200:
    with open("particella_punto.gml", "wb") as f:
        f.write(response.content)
    print("Dati salvati in particella_punto.gml")
else:
    print(f"Errore: {response.status_code}")
