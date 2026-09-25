import glob
import json
import os
import time
import urllib.request
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
BASE = os.getenv("MONGO_DB", "accidentes_db")
CELDA = float(os.getenv("TAMANO_CELDA", "0.1"))
LAT_MIN, LAT_MAX = 24.0, 50.0
LNG_MIN, LNG_MAX = -125.0, -66.0


def contar_ejecutores(sc):
    return sc._jsc.sc().getExecutorMemoryStatus().size() - 1


def memoria_ejecutores_mb(sc):
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/executors"
    try:
        with urllib.request.urlopen(url, timeout=10) as respuesta:
            ejecutores = json.loads(respuesta.read().decode())
    except Exception as error:
        print(f"No se pudo leer la memoria desde la UI de Spark: {error}")
        return None
    total = 0
    for ejecutor in ejecutores:
        if ejecutor.get("id") == "driver":
            continue
        pico = ejecutor.get("peakMemoryMetrics") or {}
        total += pico.get("JVMHeapMemory", ejecutor.get("memoryUsed", 0))
    return total / (1024 * 1024)


def main():
    ruta = glob.glob("/datos/US_Accidents*.csv")[0]
    spark = (
        SparkSession.builder.appName("benchmark_grilla")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    for _ in range(60):
        if contar_ejecutores(sc) > 0:
            break
        time.sleep(1)
    time.sleep(5)
    workers = contar_ejecutores(sc)
    print(f"Benchmark Spark con {workers} workers")

    inicio = time.time()
    puntos = (
        spark.read.option("header", True).csv(ruta)
        .select(
            F.col("Start_Lat").cast("double").alias("lat"),
            F.col("Start_Lng").cast("double").alias("lng"),
        )
        .dropna()
        .filter(F.col("lat").between(LAT_MIN, LAT_MAX) & F.col("lng").between(LNG_MIN, LNG_MAX))
    )
    conteo = (
        puntos.groupBy(
            F.floor(F.col("lat") / CELDA).alias("celda_lat"),
            F.floor(F.col("lng") / CELDA).alias("celda_lng"),
        )
        .count()
        .collect()
    )
    tiempo = time.time() - inicio

    time.sleep(12)
    memoria_mb = memoria_ejecutores_mb(sc)
    celdas = len(conteo)
    registros = sum(fila["count"] for fila in conteo)

    print(f"Tiempo: {tiempo:.2f} s")
    print(f"Memoria maxima de los ejecutores: {memoria_mb} MB")
    print(f"Celdas: {celdas}  Registros: {registros}")

    esquema = StructType([
        StructField("motor", StringType()),
        StructField("workers", IntegerType()),
        StructField("tiempo_s", DoubleType()),
        StructField("memoria_mb", DoubleType()),
        StructField("celdas", IntegerType()),
        StructField("registros", LongType()),
        StructField("fecha", TimestampType()),
    ])
    fila = [(
        "spark",
        workers,
        round(tiempo, 2),
        round(memoria_mb, 1) if memoria_mb is not None else None,
        celdas,
        int(registros),
        datetime.utcnow(),
    )]
    (
        spark.createDataFrame(fila, esquema)
        .write.format("mongodb")
        .mode("append")
        .option("database", BASE)
        .option("collection", "benchmark")
        .save()
    )
    spark.stop()


if __name__ == "__main__":
    main()
