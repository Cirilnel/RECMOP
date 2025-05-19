import os
import pandas as pd

# Import delle funzioni di estrazione dai moduli dedicati
from normattiva import run_estrazione_normattiva
from siape import run_estrazione_siape
from estrazione_dati_basi_territoriali import run_estrazione_basi_territoriali
from estrazione_dati_variabili_censuarie import run_estrazione_variabili_censuarie


def crea_dataframe_fase1() -> pd.DataFrame:
    """
    Crea il DataFrame unendo le basi territoriali, le variabili censuarie,
    i dati estratti da Normattiva e quelli da SIAPE.

    Returns:
        pd.DataFrame: DataFrame risultante dalla fase 1 dell'elaborazione.
    """
    # Estrazione dati grezzi
    df_base = run_estrazione_basi_territoriali()
    df_cens = run_estrazione_variabili_censuarie()
    df_norm = run_estrazione_normattiva()
    df_siape = run_estrazione_siape()

    # Uniformazione colonna per il join su Sezione 2011
    df_base = df_base.rename(columns={'SEZ2011': 'SEZ_2011'})
    df_cens = df_cens.rename(columns={'SEZ2011': 'SEZ_2011'})

    # Join tra basi territoriali e variabili censuarie
    df_merged = pd.merge(
        df_base,
        df_cens,
        on=['SEZ_2011'],
        how='inner'
    )

    # Join case-insensitive su Comune con Normattiva
    df_merged['COMUNE'] = df_merged['COMUNE'].str.upper()
    df_norm['Comune'] = df_norm['Comune'].str.upper()

    # Rinomina colonne di Normattiva per allineamento
    df_norm = df_norm.rename(
        columns={
            'Zona': 'ZONA_CLIMATICA',
            'GradiGiorno': 'GRADI_GIORNO',
            'Altitudine': 'ALTITUDINE',
            'Comune': 'COMUNE'
        }
    )

    # Join con dati Normattiva su SEZ, COD_LOC, TIPO_LOC, PROVINCIA, COMUNE
    df_merged2 = pd.merge(
        df_merged,
        df_norm,
        on=['COMUNE'],
        how='inner'
    )

    # Preparazione dati SIAPE (pivot su zona climatica)
    df_siape = df_siape.reset_index().rename_axis(None)
    df_siape = df_siape.rename(columns={'zona_climatica': 'ZONA_CLIMATICA'})

    # Join finale con SIAPE su ZONA_CLIMATICA, GRADI_GIORNO, ALTITUDINE
    df_finale = pd.merge(
        df_merged2,
        df_siape,
        on=['ZONA_CLIMATICA'],
        how='left'
    )

    return df_finale


def salva_dati_fase1(
    df: pd.DataFrame,
    output_path: str = os.path.join('Table', 'dati_fase1.csv')
) -> None:
    """
    Salva il DataFrame risultante dalla fase 1 in un file CSV.

    Args:
        df (pd.DataFrame): DataFrame da salvare.
        output_path (str): Percorso completo del file di output.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')

def run_fase1() -> None:
    """
    Funzione principale per eseguire la fase 1 dell'elaborazione dei dati.

    Returns:
        pd.DataFrame: DataFrame risultante dalla fase 1.
    """
    df_fase1 = crea_dataframe_fase1()
    salva_dati_fase1(df_fase1)

if __name__ == "__main__":
    run_fase1()