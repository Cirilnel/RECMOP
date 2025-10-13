from tabulate import tabulate
from utils import get_pannelli, get_regione_from_provincia, safe_name, \
    normalize_dsm_input, get_file_modification_date, configure_logging_globale, \
    normalize_fabbricati_input_auto, normalize_vincoli_input, load_dot_env
from data_extraction.calcolo_domanda_energetica import calcola_domanda_energetica
from model_builder.creazione_peb_neb import join_domanda_offerta, crea_peb_neb
from model_builder.interazione_peb_neb import ciclo_interazione_peb_neb
# --- NUOVA IMPORTAZIONE ---
from model_builder.crea_report_finale import crea_report_finale
from data_extraction.join_data_normattiva_varcens_basiterr import refresh_join_data
from data_extraction.siape import run_estrazione_siape
from offerta.pvgis.calcolo_offerta_energetica import calculate_building_hourly_production
import warnings
from pandas.errors import PerformanceWarning
warnings.filterwarnings("ignore", category=PerformanceWarning)
import os
import sys
import pandas as pd
import geopandas as gpd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
load_dot_env()


def mostra_pannelli(df: pd.DataFrame) -> None:
    colonne = ['Marca', 'Modello', 'Potenza(Wp)', 'Efficienza(%)', 'Tecnologia', 'Prezzo', 'Superficie', 'Sup+30%']
    df_vis = df[colonne].copy()
    df_vis.index += 1
    df_vis = df_vis.rename(columns={"Sup+30%": "Superficie + 30%"})
    print("\nSeleziona il pannello che preferisci:\n")
    print(tabulate(df_vis, headers="keys", tablefmt="grid", showindex=True))


def report_interazione_outputs(provincia: str, comune: str):
    """
    Mostra un report dettagliato dei file di output generati dal ciclo di interazione,
    tracciando il numero di PEB, NEB e NCER per ogni iterazione.
    """
    prov_safe = safe_name(provincia)
    com_safe = safe_name(comune)
    prov_com = f"{prov_safe}_{com_safe}"

    # Percorsi
    input_dir = os.path.join("model_builder_shapefiles", prov_com, "input")
    outputs_dir = os.path.join("model_builder_shapefiles", prov_com, "outputs")

    # --- Conteggio Iniziale ---
    peb_iniziali_path = os.path.join(input_dir, "peb", f"PEB_{prov_com}.gpkg")
    neb_iniziali_path = os.path.join(input_dir, "neb", f"NEB_{prov_com}.gpkg")

    try:
        peb_precedenti = len(gpd.read_file(peb_iniziali_path)) if os.path.exists(peb_iniziali_path) else 0
        neb_precedenti = len(gpd.read_file(neb_iniziali_path)) if os.path.exists(neb_iniziali_path) else 0
        print("\n--- STATO INIZIALE ---")
        print(f"PEB di partenza: {peb_precedenti}")
        print(f"NEB di partenza: {neb_precedenti}")
        print("----------------------\n")
    except Exception as e:
        logger.error(f"Errore nel leggere i file di input iniziali per il report: {e}")
        peb_precedenti, neb_precedenti = 0, 0

    # --- Conteggio per Iterazione ---
    iter_num = 1
    output_exists = False

    print("--- ANDAMENTO ITERAZIONI ---")
    while True:
        iter_dir = os.path.join(outputs_dir, f"output{iter_num}")
        if not os.path.isdir(iter_dir):
            break

        # I file PEB/NEB in output sono l'input per il ciclo successivo
        peb_path = os.path.join(iter_dir, f"ped2_{prov_com}_{iter_num}.gpkg")
        neb_path = os.path.join(iter_dir, f"ned2_{prov_com}_{iter_num}.gpkg")
        ncer_path = os.path.join(iter_dir, f"ncer_{prov_com}_{iter_num}.gpkg")

        try:
            peb_attuali = len(gpd.read_file(peb_path)) if os.path.exists(peb_path) else 0
            neb_attuali = len(gpd.read_file(neb_path)) if os.path.exists(neb_path) else 0
            ncer_prodotti = len(gpd.read_file(ncer_path)) if os.path.exists(ncer_path) else 0

            delta_peb = peb_attuali - peb_precedenti
            delta_neb = neb_attuali - neb_precedenti

            print(f"Iterazione {iter_num}: "
                  f"NCER prodotte: {ncer_prodotti} | "
                  f"PEB finali: {peb_attuali} ({delta_peb:+.0f}) | "
                  f"NEB finali: {neb_attuali} ({delta_neb:+.0f})")

            peb_precedenti = peb_attuali
            neb_precedenti = neb_attuali
            output_exists = True

        except Exception as e:
            logger.error(f"Errore durante la lettura dei file per l'iterazione {iter_num}: {e}")
            # Interrompe il ciclo se non riesce a leggere i file di un'iterazione
            break

        iter_num += 1

    if not output_exists:
        print("Nessun output di iterazione trovato.")
    print("--------------------------\n")

    # --- Conteggio Finale Consolidato ---
    ncer_finale_path = os.path.join(outputs_dir, f"ncer_finale_{prov_com}.gpkg")
    cer_totali = 0
    if os.path.exists(ncer_finale_path):
        try:
            cer_totali = len(gpd.read_file(ncer_finale_path))
        except Exception as e:
            logger.error(f"Errore lettura file NCER finale: {e}")

    print("--- TOTALI FINALI ---")
    print(f"Totale NCER (Comunità Energetiche) create: {cer_totali}")
    print(f"PEB rimasti alla fine del ciclo: {peb_precedenti}")
    print(f"NEB rimasti alla fine del ciclo: {neb_precedenti}")
    print("---------------------\n")


def main():
    # LOGGING
    while True:
        usa_log = input("Vuoi visualizzare i log delle operazioni? (SI/NO): ").strip().upper()
        if usa_log in ("SI", "NO"):
            configure_logging_globale(attivo=(usa_log == "SI"))
            break
        print("Risposta non valida. Scrivi 'SI' oppure 'NO'.")

    # INPUT COMUNE/PROVINCIA
    comune = input("Inserisci il nome del comune: ")
    provincia = input("Inserisci la provincia del comune: ")
    com_safe = safe_name(comune)
    prov_safe = safe_name(provincia)

    try:
        fabbricati_tipo = normalize_fabbricati_input_auto(os.path.abspath("FABBRICATI"), prov_safe, com_safe)
        normalize_dsm_input(
            os.path.abspath("input_dsm"),
            os.path.abspath(os.path.join("offerta", "grass_gis", "irradiance_tif")),
            prov_safe, com_safe
        )
        exist_vincoli = normalize_vincoli_input(os.path.abspath("VINCOLI"), prov_safe, com_safe)
    except Exception as e:
        print(f"Errore nella normalizzazione dei dati: {e}")
        sys.exit(1)

    regione = get_regione_from_provincia(prov_safe)

    # AGGIORNAMENTO SIAPE E ZONE CLIMATICHE
    siape_paths = {
        "zc_range": os.path.join("Data_Collection", "csv_tables-fase1", "epgl_nren_ren_co2_tabella_siape_zc_range.csv"),
        "zc_suris_volris": os.path.join("Data_Collection", "csv_tables-fase1", "epgl_nren_ren_co2_tabella_siape_zc_suris_volris.csv"),
        "zc_suris_volris_supdi": os.path.join("Data_Collection", "csv_tables-fase1", "epgl_nren_ren_co2_tabella_siape_zc_suris_volris_supdi.csv"),
    }
    siape_path = siape_paths.get(fabbricati_tipo)
    if siape_path is None:
        print(f"Tipologia fabbricati sconosciuta: {fabbricati_tipo}")
        sys.exit(1)

    ultima_mod_siape = get_file_modification_date(siape_path)
    print(f"Dati SIAPE - Ultimo aggiornamento: {ultima_mod_siape}")
    if ultima_mod_siape != "File non trovato":
        risposta_siape = input("Vuoi aggiornare i dati SIAPE? (SI/NO): ").strip().upper()
        if risposta_siape == "SI":
            run_estrazione_siape(fabbricati_tipo)

    zona_path = os.path.join("Data_Collection", "csv_tables-fase1", "dati_normattiva.csv")
    ultima_mod_zone = get_file_modification_date(zona_path)
    print(f"Zone climatiche - Ultimo aggiornamento: {ultima_mod_zone}")
    if ultima_mod_zone != "File non trovato":
        risposta_zone = input("Vuoi aggiornare la lista dei comuni da normattiva? (SI/NO): ").strip().upper()
        if risposta_zone == "SI":
            refresh_join_data(regione)

    # COEFFICIENTE DOMANDA
    while True:
        coeff_moltiplicativo = input("Specifica il coefficiente moltiplicativo per la domanda energetica (1 o invio per default): ").strip()
        if coeff_moltiplicativo == "":
            coeff_moltiplicativo = 1.0
            break
        try:
            coeff_moltiplicativo = float(coeff_moltiplicativo)
            if coeff_moltiplicativo > 0:
                break
        except ValueError:
            pass
        print("Valore non valido. Inserisci un numero > 0.")

    try:
        gdf_domanda = calcola_domanda_energetica(com_safe, prov_safe, fabbricati_tipo, coeff_moltiplicativo)
        print("Domanda energetica calcolata.")
    except Exception as e:
        print(f"Errore nel calcolo domanda: {e}")
        sys.exit(1)

    use_vincoli = False
    if exist_vincoli:
        risposta_vincoli = input("Vuoi considerare i vincoli? (SI/NO): ").strip().upper()
        use_vincoli = (risposta_vincoli == "SI")

    pannelli_df = get_pannelli()
    mostra_pannelli(pannelli_df)
    while True:
        try:
            indice_pannello = int(input("Seleziona il numero del pannello: ")) - 1
            if 0 <= indice_pannello < len(pannelli_df):
                break
        except ValueError:
            pass
        print("Input non valido.")
    pannello_selezionato = pannelli_df.iloc[indice_pannello]
    print("\nPannello selezionato:")
    print(tabulate(pannello_selezionato.to_frame().T, headers="keys", tablefmt="grid", showindex=False))

    # INPUT ANNO PER PVGIS
    max_pvgis_year = 2023
    while True:
        anno_input = input(f"Inserisci l'anno per il calcolo PVGIS (2005-{max_pvgis_year}, invio per {max_pvgis_year}): ").strip()
        if anno_input == "":
            pvgis_year = max_pvgis_year
            break
        try:
            pvgis_year = int(anno_input)
            if 2005 <= pvgis_year <= max_pvgis_year:
                break
            else:
                print(f"L'anno deve essere compreso tra 2005 e {max_pvgis_year}")
        except ValueError:
            print("Inserisci un anno valido (numero intero)")

    print(f"Utilizzerò l'anno {pvgis_year} per il calcolo PVGIS")

    try:
        gdf_offerta = calculate_building_hourly_production(
            provincia=prov_safe,
            comune=com_safe,
            idx_panel=indice_pannello,
            use_vincoli=use_vincoli,
            pvgis_year=pvgis_year
        )
        print(f"✅ Produzione oraria per l'anno {pvgis_year} calcolata e salvata.")
    except Exception as e:
        print(f"Errore nel calcolo produzione oraria PVGIS: {e}")
        sys.exit(1)

    # JOIN DOMANDA-OFFERTA
    try:
        gdf_join = join_domanda_offerta(prov_safe, com_safe, gdf_domanda, gdf_offerta)
        print("Join domanda-offerta completato con successo.")
    except Exception as e:
        print(f"Errore nel join domanda-offerta: {e}")
        sys.exit(1)

    # CREAZIONE PEB/NEB
    print("Creazione PEB/NEB in corso...")
    crea_peb_neb(prov_safe, com_safe)

    # INPUT INDICE SOGLIA
    while True:
        try:
            indice_soglia = float(input("Inserisci la soglia dell'indice per le NCER (0.0 - 1.0): "))
            if 0.0 <= indice_soglia <= 1.0:
                break
            else:
                print("Valore non valido. Inserisci un numero tra 0.0 e 1.0.")
        except ValueError:
            print("Valore non valido. Inserisci un numero decimale tra 0.0 e 1.0.")

    # ESECUZIONE CICLO INTERAZIONE
    print("\n=== Avvio ciclo interazione PEB-NEB ===")
    try:
        ciclo_interazione_peb_neb(
            provincia=provincia, # Passa il nome originale, non safe
            comune=comune,
            indice_soglia=indice_soglia
        )
    except Exception as e:
        print(f"Errore durante il ciclo interazione PEB-NEB: {e}")

    # --- NUOVA SEZIONE: CREAZIONE REPORT FINALE ---
    print("\n=== Creazione del Report Finale ===")
    try:
        crea_report_finale(
            provincia=provincia, # Passa il nome originale
            comune=comune
        )
    except Exception as e:
        print(f"Errore durante la creazione del report finale: {e}")
    # --- FINE NUOVA SEZIONE ---

    report_interazione_outputs(prov_safe, com_safe)
    print("Analisi completata. Risultati in 'Data_Collection' e 'model_builder_shapefiles'.")


if __name__ == "__main__":
    main()