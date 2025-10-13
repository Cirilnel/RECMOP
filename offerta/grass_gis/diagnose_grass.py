import os
import sys
import logging
import rasterio
import subprocess
from pathlib import Path

def diagnose_grass_gis_error(dem_path, grass_base):
    """
    Diagnostica completa per l'errore di importazione GRASS GIS
    """
    print("=" * 60)
    print("DIAGNOSTICA ERRORE GRASS GIS")
    print("=" * 60)

    # 1. Verifica esistenza file DEM
    print(f"\n1. VERIFICA FILE DEM")
    print(f"   Path: {dem_path}")
    print(f"   Esiste: {os.path.exists(dem_path)}")

    if os.path.exists(dem_path):
        file_size = os.path.getsize(dem_path)
        print(f"   Dimensione: {file_size} bytes ({file_size/1024/1024:.2f} MB)")

        # Verifica se il file è corrotto o vuoto
        if file_size == 0:
            print("   ⚠️  ERRORE: File vuoto!")
            return False

        # Verifica leggibilità con rasterio
        try:
            with rasterio.open(dem_path) as src:
                print(f"   CRS: {src.crs}")
                print(f"   Dimensioni: {src.width} x {src.height}")
                print(f"   Bande: {src.count}")
                print(f"   Tipo dati: {src.dtypes[0]}")
                print(f"   Bounds: {src.bounds}")

                # Verifica se ci sono dati validi
                data = src.read(1, window=rasterio.windows.Window(0, 0, min(100, src.width), min(100, src.height)))
                valid_pixels = (~data.mask).sum() if hasattr(data, 'mask') else (data != src.nodata).sum()
                print(f"   Pixel validi nel campione: {valid_pixels}")

        except Exception as e:
            print(f"   ⚠️  ERRORE nella lettura con rasterio: {e}")
            return False
    else:
        print("   ⚠️  ERRORE: File non trovato!")
        return False

    # 2. Verifica installazione GRASS GIS
    print(f"\n2. VERIFICA GRASS GIS")
    print(f"   GRASS_BASE: {grass_base}")
    print(f"   Directory esiste: {os.path.exists(grass_base)}")

    if os.path.exists(grass_base):
        # Verifica eseguibili principali
        executables = ['grass84.bat', 'grass84.exe', 'grass.bat', 'grass.exe']
        grass_exe = None
        for exe in executables:
            exe_path = os.path.join(grass_base, exe)
            if os.path.exists(exe_path):
                grass_exe = exe_path
                print(f"   Eseguibile trovato: {exe}")
                break

        if not grass_exe:
            print("   ⚠️  ERRORE: Nessun eseguibile GRASS trovato!")
            return False

        # Verifica directory critiche
        critical_dirs = ['bin', 'etc/python', 'lib', 'share/proj']
        for dir_name in critical_dirs:
            dir_path = os.path.join(grass_base, dir_name)
            exists = os.path.exists(dir_path)
            print(f"   {dir_name}: {'✓' if exists else '✗'}")
            if not exists:
                print(f"   ⚠️  ERRORE: Directory mancante: {dir_path}")

    # 3. Verifica variabili d'ambiente
    print(f"\n3. VERIFICA VARIABILI D'AMBIENTE")
    env_vars = ['GISBASE', 'PATH', 'PYTHONPATH', 'GRASS_PROJSHARE']
    for var in env_vars:
        value = os.environ.get(var, 'NON IMPOSTATA')
        print(f"   {var}: {value[:100]}{'...' if len(str(value)) > 100 else ''}")

    # 4. Test GDAL
    print(f"\n4. VERIFICA GDAL")
    try:
        from osgeo import gdal
        print(f"   Versione GDAL: {gdal.__version__}")

        # Test apertura file
        dataset = gdal.Open(dem_path)
        if dataset:
            print(f"   ✓ GDAL può leggere il file")
            print(f"   Driver: {dataset.GetDriver().GetDescription()}")
            dataset = None
        else:
            print(f"   ✗ GDAL non può leggere il file")

    except ImportError:
        print("   ⚠️  GDAL non disponibile in Python")
    except Exception as e:
        print(f"   ⚠️  Errore GDAL: {e}")

    return True

def fix_grass_environment(grass_base):
    """
    Corregge le variabili d'ambiente per GRASS GIS
    """
    print(f"\n5. CORREZIONE AMBIENTE GRASS GIS")

    # Imposta GISBASE
    os.environ['GISBASE'] = grass_base
    print(f"   GISBASE impostato: {grass_base}")

    # Aggiorna PATH - versione corretta
    grass_paths = [
        os.path.join(grass_base, 'bin'),
        os.path.join(grass_base, 'scripts'),
        os.path.join(grass_base, 'extrabin'),
        os.path.join(grass_base, 'lib')  # Aggiunto lib per i DLL
    ]

    # Filtra solo i path esistenti
    existing_paths = [p for p in grass_paths if os.path.exists(p)]

    # Rimuovi vecchi path GRASS da PATH
    current_path = os.environ.get('PATH', '')
    path_parts = current_path.split(os.pathsep)
    clean_path_parts = [p for p in path_parts if 'GRASS GIS' not in p]

    # Aggiungi i nuovi path all'inizio
    new_path = os.pathsep.join(existing_paths + clean_path_parts)
    os.environ['PATH'] = new_path
    print(f"   PATH aggiornato con {len(existing_paths)} directory GRASS")

    # Imposta PYTHONPATH
    python_path = os.path.join(grass_base, 'etc', 'python')
    if os.path.exists(python_path):
        if python_path not in sys.path:
            sys.path.insert(0, python_path)
        os.environ['PYTHONPATH'] = python_path
        print(f"   PYTHONPATH impostato: {python_path}")

    # Imposta GRASS_PROJSHARE
    proj_path = os.path.join(grass_base, 'share', 'proj')
    if os.path.exists(proj_path):
        os.environ['GRASS_PROJSHARE'] = proj_path
        print(f"   GRASS_PROJSHARE impostato: {proj_path}")

    # Imposta altre variabili utili
    os.environ['GRASS_SKIP_MAPSET_OWNER_CHECK'] = '1'
    os.environ['GRASS_OVERWRITE'] = '1'

    print("   ✓ Ambiente GRASS GIS configurato")

def improved_solar_radiation_pipeline(provincia: str, comune: str, grass_base: str,
                                      grass_gisdb: str, grass_location: str, grass_mapset: str):
    """
    Versione migliorata della pipeline con gestione errori avanzata
    """
    from utils import safe_name

    prov_safe = safe_name(provincia)
    com_safe = safe_name(comune)

    # Path dei file
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    DSM_BASE = os.path.join(BASE_DIR, 'input_dsm')
    OUTPUT_DIR = os.path.join(BASE_DIR, 'offerta', 'grass_gis', 'irradiance_tif')
    FABBRICATI_BASE = os.path.join(BASE_DIR, 'FABBRICATI')

    dem = os.path.join(DSM_BASE, f'DSM_{prov_safe}_{com_safe}.tif')
    output_tif = os.path.join(OUTPUT_DIR, f'irradianza_annua_{prov_safe}_{com_safe}_kwh.tif')
    shapefolder = os.path.join(FABBRICATI_BASE, f'fabbricati_{prov_safe}_{com_safe}')

    print(f"\n6. ESECUZIONE PIPELINE MIGLIORATA")
    print(f"   DEM: {dem}")
    print(f"   Output: {output_tif}")

    # Diagnostica completa
    if not diagnose_grass_gis_error(dem, grass_base):
        print("   ⚠️  Diagnostica fallita, impossibile continuare")
        return None

    # Correggi ambiente
    fix_grass_environment(grass_base)

    try:
        # Inizializza GRASS
        print("   Inizializzazione GRASS GIS...")

        # Verifica se la location esiste, altrimenti creala
        loc_path = os.path.join(grass_gisdb, grass_location)
        if not os.path.exists(loc_path):
            print(f"   Creazione location: {grass_location}")

            # Ottieni EPSG dal DEM
            with rasterio.open(dem) as src:
                epsg = src.crs.to_epsg()

            # Crea location
            grass_exe = None
            for exe in ['grass84.bat', 'grass84.exe', 'grass.bat', 'grass.exe']:
                exe_path = os.path.join(grass_base, exe)
                if os.path.exists(exe_path):
                    grass_exe = exe_path
                    break

            if grass_exe:
                cmd = [grass_exe, '-c', f'EPSG:{epsg}', '-e', loc_path]
                print(f"   Comando: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    print(f"   ⚠️  Errore creazione location: {result.stderr}")
                    return None

        # Importa moduli GRASS
        import grass.script.setup as gsetup
        gsetup.init(grass_gisdb, grass_location, grass_mapset)
        import grass.script as gs

        print("   ✓ GRASS GIS inizializzato")

        # Prova import del DEM con opzioni aggiuntive
        print("   Importazione DEM...")

        # Prova prima con r.in.gdal (più basilare)
        try:
            gs.run_command('r.in.gdal',
                           input=dem,
                           output='dem_test',
                           overwrite=True,
                           flags='o')  # flag 'o' per override projection check
            print("   ✓ DEM importato con r.in.gdal")

            # Se funziona, usa r.import per il resto
            gs.run_command('r.import',
                           input=dem,
                           output='dem',
                           overwrite=True)
            print("   ✓ DEM importato con r.import")

        except Exception as e:
            print(f"   ⚠️  Errore importazione DEM: {e}")

            # Fallback: prova con gdalwarp per convertire il file
            print("   Tentativo conversione con gdalwarp...")
            temp_dem = dem.replace('.tif', '_temp.tif')

            try:
                from osgeo import gdal

                # Apri dataset originale
                src_ds = gdal.Open(dem)
                if src_ds is None:
                    raise Exception("Impossibile aprire DEM originale")

                # Converti in formato compatibile
                gdal.Translate(temp_dem, src_ds,
                               options=['-of', 'GTiff', '-co', 'COMPRESS=NONE'])

                # Prova importazione del file convertito
                gs.run_command('r.import',
                               input=temp_dem,
                               output='dem',
                               overwrite=True)

                print("   ✓ DEM importato dopo conversione")

                # Rimuovi file temporaneo
                if os.path.exists(temp_dem):
                    os.remove(temp_dem)

            except Exception as e2:
                print(f"   ⚠️  Errore anche con conversione: {e2}")
                return None

        # Continua con il resto della pipeline...
        print("   Calcolo slope e aspect...")
        gs.run_command('r.slope.aspect',
                       elevation='dem',
                       slope='slope',
                       aspect='aspect',
                       overwrite=True)

        print("   ✓ Pipeline completata con successo!")
        return output_tif

    except Exception as e:
        print(f"   ⚠️  Errore generale: {e}")
        import traceback
        traceback.print_exc()
        return None

# Esempio di utilizzo
if __name__ == "__main__":
    # Parametri dal tuo codice
    GRASS_BASE = r"C:\Program Files\GRASS GIS 8.4"
    GRASS_GISDB = r"C:\path\to\your\grassdata"  # Sostituisci con il tuo path
    GRASS_LOCATION = "auto_location"
    GRASS_MAPSET = "PERMANENT"

    # Test con i tuoi dati
    dem_path = r"C:\Users\utente\Documents\GitHub\RECMOP\input_dsm\DSM_salerno_padula.tif"

    # Esegui diagnostica
    diagnose_grass_gis_error(dem_path, GRASS_BASE)

    # Esegui pipeline migliorata
    # result = improved_solar_radiation_pipeline('Salerno', 'Padula',
    #                                           GRASS_BASE, GRASS_GISDB,
    #                                           GRASS_LOCATION, GRASS_MAPSET)