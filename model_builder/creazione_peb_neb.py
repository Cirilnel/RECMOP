import os
import geopandas as gpd
import logging

# === CONFIGURAZIONE LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === COSTANTI PATH ===
SHAPE_IN_DIR = os.path.abspath(os.path.join('..', 'Data_Collection', 'shapefiles'))
OUTPUT_MODEL_BUILDER = os.path.abspath(os.path.join('..', 'model_builder_shapefiles'))

def safe_name(nome: str) -> str:
    return nome.strip().lower().replace(' ', '_')

def crea_peb_neb(provincia: str, comune: str):
    """
    Genera shapefile di PEB e NEB a partire da offerta_energetica e domanda_energetica.
    """
    provincia_safe = safe_name(provincia)
    comune_safe = safe_name(comune)
    logger.info(f"Avvio generazione PEB e NEB per {provincia_safe} - {comune_safe}")

    # PATH INPUT
    domanda_dir = os.path.join(SHAPE_IN_DIR, f"{provincia_safe}_{comune_safe}", f"domanda_energetica_{provincia_safe}_{comune_safe}")
    offerta_dir = os.path.join(SHAPE_IN_DIR, f"{provincia_safe}_{comune_safe}", f"offerta_energetica_{provincia_safe}_{comune_safe}")

    domanda_files = [f for f in os.listdir(domanda_dir) if f.lower().endswith('.shp')]
    offerta_files = [f for f in os.listdir(offerta_dir) if f.lower().endswith('.shp')]

    if not domanda_files or not offerta_files:
        logger.error("File shapefile non trovati nei percorsi specificati.")
        return

    domanda_shp = os.path.join(domanda_dir, domanda_files[0])
    offerta_shp = os.path.join(offerta_dir, offerta_files[0])

    # Lettura SHP
    logger.info("Caricamento shapefile di domanda e offerta...")
    gdf_domanda = gpd.read_file(domanda_shp)
    gdf_offerta = gpd.read_file(offerta_shp)

    # Join su FID (ID edificio)
    logger.info("Eseguo join tra domanda e offerta su FID...")
    gdf_join = gdf_domanda.merge(
        gdf_offerta[['FID', 'Prod_kWh_y']],
        on='FID',
        how='inner',
        suffixes=('_dom', '_off')
    )

    # Calcolo surplus/deficit
    gdf_join['diff'] = gdf_join['Prod_kWh_y'] - gdf_join['domanda_en']

    # Split tra PEB (surplus >= 0) e NEB (deficit < 0)
    gdf_peb = gdf_join[gdf_join['diff'] >= 0].copy()
    gdf_peb['ID_P'] = gdf_peb['FID']
    gdf_peb['surplus'] = gdf_peb['diff']
    gdf_peb = gdf_peb[['geometry', 'ID_P', 'surplus']]
    gdf_peb = gpd.GeoDataFrame(gdf_peb, geometry='geometry', crs=gdf_domanda.crs)

    gdf_neb = gdf_join[gdf_join['diff'] < 0].copy()
    gdf_neb['ID_N'] = gdf_neb['FID']
    gdf_neb['deficit'] = gdf_neb['diff']
    gdf_neb = gdf_neb[['geometry', 'ID_N', 'deficit']]
    gdf_neb = gpd.GeoDataFrame(gdf_neb, geometry='geometry', crs=gdf_domanda.crs)

    # Path output
    out_dir = os.path.join(OUTPUT_MODEL_BUILDER, f"{provincia_safe}_{comune_safe}", "input")
    peb_dir = os.path.join(out_dir, "peb")
    neb_dir = os.path.join(out_dir, "neb")
    os.makedirs(peb_dir, exist_ok=True)
    os.makedirs(neb_dir, exist_ok=True)

    peb_path = os.path.join(peb_dir, f"PEB_{provincia_safe}_{comune_safe}.shp")
    neb_path = os.path.join(neb_dir, f"NEB_{provincia_safe}_{comune_safe}.shp")

    # Salvataggio
    logger.info(f"Salvataggio PEB in {peb_path}")
    gdf_peb.to_file(peb_path, encoding="utf-8")

    logger.info(f"Salvataggio NEB in {neb_path}")
    gdf_neb.to_file(neb_path, encoding="utf-8")

    logger.info("Generazione shapefile PEB e NEB completata.")

# --- ESEMPIO USO ---
if __name__ == "__main__":
    crea_peb_neb("salerno", "padula")
