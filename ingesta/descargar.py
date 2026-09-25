import glob
import os
import sys

DESTINO = "/datos"
DATASET = "sobhanmoosavi/us-accidents"


def main():
    existentes = glob.glob(os.path.join(DESTINO, "US_Accidents*.csv"))
    if existentes:
        print(f"El dataset ya esta descargado: {existentes[0]}")
        return

    if not os.getenv("KAGGLE_USERNAME") or not os.getenv("KAGGLE_KEY"):
        print("No se encontraron KAGGLE_USERNAME y KAGGLE_KEY en el entorno")
        sys.exit(1)

    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    print(f"Descargando {DATASET} en {DESTINO}...")
    api.dataset_download_files(DATASET, path=DESTINO, unzip=True, quiet=False)

    descargados = glob.glob(os.path.join(DESTINO, "US_Accidents*.csv"))
    if not descargados:
        print("La descarga termino pero no aparece el csv")
        sys.exit(1)
    print(f"Descarga lista: {descargados[0]}")


if __name__ == "__main__":
    main()
