import geopandas as gpd
import logging

# Configura il logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Percorsi input/output
input_shp = '../FABBRICATI/FABBRICATI.shp'

# Carica lo shapefile
gdf = gpd.read_file(input_shp)

print("CRS dichiarato:", gdf.crs)
print("Primi valori:", gdf.geometry.iloc[1325])

# Aggiungi colonna 'id' se mancante
if 'id' not in gdf.columns:
    gdf = gdf.reset_index().rename(columns={'index': 'id'})

# Calcola i centroidi
centroids = gdf.centroid

# Crea un nuovo GeoDataFrame con id e geometria dei centroidi
centroids_gdf = gpd.GeoDataFrame(gdf[['id']], geometry=centroids, crs=gdf.crs)

print("CRS dichiarato:", centroids_gdf.crs)
print("centroide con id 771:", centroids_gdf[centroids_gdf['id'] == 771].geometry.iloc[0])

# Salva i centroidi in un nuovo shapefile
output_shp = '../Data_Collection/shapefiles_merged/centroidi_fabbricati/centroidi_fabbricati.shp'

centroids_gdf.to_file(output_shp, driver='ESRI Shapefile')