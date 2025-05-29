import geopandas as gpd

input_shp = '../FABBRICATI/FABBRICATI.shp'
output_shp = '../Data_Collection/shapefiles_merged/centroidi_fabbricati.shp'

gdf = gpd.read_file(input_shp)

if 'id' not in gdf.columns:
    gdf = gdf.reset_index().rename(columns={'index': 'id'})

# Calcola i centroidi
centroids = gdf.centroid

# Crea GeoDataFrame con id e centroidi
centroids_gdf = gpd.GeoDataFrame(gdf[['id']], geometry=centroids, crs=gdf.crs)

# Salva
centroids_gdf.to_file(output_shp)

print(f"Centroidi salvati in {output_shp}")
