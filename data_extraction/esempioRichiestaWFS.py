import requests
from pyproj import Transformer

# Coordinate in EPSG:25833 (UTM zone 33N, metri)
x_utm, y_utm = 555823.2153689115, 4465569.97881136

# Crea trasformatore da EPSG:25833 a EPSG:6706
transformer = Transformer.from_crs("EPSG:25833", "EPSG:6706", always_xy=True)
lon, lat = transformer.transform(x_utm, y_utm)
print(f"Coordinate trasformate: lon={lon}, lat={lat}")

# URL del servizio WFS
url = "https://wfs.cartografia.agenziaentrate.gov.it/inspire/wfs/owfs01.php"

# Parametri della richiesta
params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "typenames": "CP:CadastralParcel",
    "outputFormat": "application/gml+xml; version=3.2",
    "cql_filter": f"CONTAINS(geom, POINT({lon} {lat}))"
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
