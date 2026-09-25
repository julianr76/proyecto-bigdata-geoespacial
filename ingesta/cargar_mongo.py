import glob
import os
import sys
import time
from datetime import datetime

import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from pymongo import ASCENDING, GEOSPHERE, MongoClient

from limpieza import COLUMNAS, TIPOS, a_documento, limpiar_particion

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
BASE = os.getenv("MONGO_DB", "accidentes_db")
COLECCION = "accidentes"
SCHEDULER = os.getenv("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
MAX_REGISTROS = int(os.getenv("MAX_REGISTROS", "1000000"))
TAMANO_LOTE = int(os.getenv("TAMANO_LOTE", "5000"))
FORZAR = os.getenv("FORZAR_CARGA", "0") == "1"


def buscar_csv():
    archivos = glob.glob("/datos/US_Accidents*.csv")
    if not archivos:
        print("No se encontro el csv en /datos, primero hay que correr descargar.py")
        sys.exit(1)
    return archivos[0]


def cargar_particion(df, uri, base, coleccion, lote):
    cliente = MongoClient(uri)
    destino = cliente[base][coleccion]
    pendientes = []
    insertados = 0
    for fila in df.to_dict("records"):
        pendientes.append(a_documento(fila))
        if len(pendientes) >= lote:
            destino.insert_many(pendientes, ordered=False)
            insertados += len(pendientes)
            pendientes = []
    if pendientes:
        destino.insert_many(pendientes, ordered=False)
        insertados += len(pendientes)
    cliente.close()
    return pd.DataFrame({"insertados": [insertados]})


def main():
    mongo = MongoClient(MONGO_URI)
    db = mongo[BASE]

    control = db["control_carga"].find_one({"_id": "accidentes"})
    if control and not FORZAR:
        print(f"Los datos ya estaban cargados ({control['registros']} registros), se omite la carga")
        return

    ruta = buscar_csv()
    cliente_dask = Client(SCHEDULER)
    cliente_dask.wait_for_workers(1, timeout=120)
    print(f"Conectado a Dask con {len(cliente_dask.scheduler_info()['workers'])} workers")

    inicio = time.time()
    crudo = dd.read_csv(ruta, usecols=COLUMNAS, dtype=TIPOS, blocksize="64MB")
    total_crudo = len(crudo)

    esquema = limpiar_particion(crudo._meta.copy())
    limpio = crudo.map_partitions(limpiar_particion, meta=esquema)
    total_limpio = len(limpio)
    print(f"Registros originales: {total_crudo}")
    print(f"Registros despues de limpiar: {total_limpio}")
    print(f"Descartados: {total_crudo - total_limpio}")

    if MAX_REGISTROS > 0 and total_limpio > MAX_REGISTROS:
        fraccion = MAX_REGISTROS / total_limpio
        limpio = limpio.sample(frac=fraccion, random_state=42)
        print(f"Se toma una muestra del {fraccion:.2%} para quedar cerca de {MAX_REGISTROS} registros")

    db[COLECCION].drop()
    db["control_carga"].delete_many({})

    resultado = limpio.map_partitions(
        cargar_particion,
        MONGO_URI,
        BASE,
        COLECCION,
        TAMANO_LOTE,
        meta={"insertados": "int64"},
    ).compute()
    total_insertados = int(resultado["insertados"].sum())

    print("Creando indices...")
    db[COLECCION].create_index([("location", GEOSPHERE)])
    db[COLECCION].create_index([("fecha", ASCENDING)])

    duracion = time.time() - inicio
    db["control_carga"].insert_one({
        "_id": "accidentes",
        "registros": total_insertados,
        "originales": total_crudo,
        "limpios": total_limpio,
        "segundos": round(duracion, 1),
        "fecha_carga": datetime.utcnow(),
    })
    print(f"Se insertaron {total_insertados} documentos en {duracion:.1f} segundos")

    cliente_dask.close()
    mongo.close()


if __name__ == "__main__":
    main()
