import glob
import os
import time
from datetime import datetime

import dask.dataframe as dd
import numpy as np
from dask.distributed import Client
from distributed.diagnostics import MemorySampler
from pymongo import MongoClient

from limpieza import LAT_MAX, LAT_MIN, LNG_MAX, LNG_MIN

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
BASE = os.getenv("MONGO_DB", "accidentes_db")
SCHEDULER = os.getenv("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
CELDA = float(os.getenv("TAMANO_CELDA", "0.1"))


def agregar_celdas(df):
    df = df.copy()
    df["celda_lat"] = np.floor(df["Start_Lat"] / CELDA).astype("int64")
    df["celda_lng"] = np.floor(df["Start_Lng"] / CELDA).astype("int64")
    return df


def main():
    ruta = glob.glob("/datos/US_Accidents*.csv")[0]
    cliente = Client(SCHEDULER)
    cliente.wait_for_workers(1, timeout=120)
    time.sleep(5)
    workers = len(cliente.scheduler_info()["workers"])
    print(f"Benchmark Dask con {workers} workers")

    muestreo = MemorySampler()
    inicio = time.time()
    with muestreo.sample("grilla", interval=0.5):
        puntos = dd.read_csv(
            ruta,
            usecols=["Start_Lat", "Start_Lng"],
            dtype={"Start_Lat": "float64", "Start_Lng": "float64"},
            blocksize="64MB",
        )
        puntos = puntos.dropna()
        puntos = puntos[
            puntos["Start_Lat"].between(LAT_MIN, LAT_MAX)
            & puntos["Start_Lng"].between(LNG_MIN, LNG_MAX)
        ]
        meta = puntos._meta.assign(celda_lat=np.int64(0), celda_lng=np.int64(0))
        puntos = puntos.map_partitions(agregar_celdas, meta=meta)
        conteo = puntos.groupby(["celda_lat", "celda_lng"]).size().compute()
    tiempo = time.time() - inicio

    memoria_mb = float(muestreo.to_pandas()["grilla"].max()) / (1024 * 1024)
    celdas = int(len(conteo))
    registros = int(conteo.sum())

    print(f"Tiempo: {tiempo:.2f} s")
    print(f"Memoria maxima del cluster: {memoria_mb:.1f} MB")
    print(f"Celdas: {celdas}  Registros: {registros}")

    mongo = MongoClient(MONGO_URI)
    mongo[BASE]["benchmark"].insert_one({
        "motor": "dask",
        "workers": workers,
        "tiempo_s": round(tiempo, 2),
        "memoria_mb": round(memoria_mb, 1),
        "celdas": celdas,
        "registros": registros,
        "fecha": datetime.utcnow(),
    })
    mongo.close()
    cliente.close()


if __name__ == "__main__":
    main()
