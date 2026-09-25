import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
BASE = os.getenv("MONGO_DB", "accidentes_db")
CELDA = float(os.getenv("TAMANO_CELDA", "0.1"))
DIAS = ["domingo", "lunes", "martes", "miercoles", "jueves", "viernes", "sabado"]


def crear_sesion():
    return (
        SparkSession.builder.appName("procesamiento_accidentes")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )


def leer(spark, coleccion):
    return (
        spark.read.format("mongodb")
        .option("database", BASE)
        .option("collection", coleccion)
        .load()
    )


def guardar(df, coleccion):
    (
        df.write.format("mongodb")
        .mode("overwrite")
        .option("database", BASE)
        .option("collection", coleccion)
        .save()
    )
    print(f"Guardado {coleccion}")


def calcular_grilla(accidentes):
    return (
        accidentes
        .withColumn("celda_lat", F.floor(F.col("lat") / CELDA))
        .withColumn("celda_lng", F.floor(F.col("lng") / CELDA))
        .groupBy("celda_lat", "celda_lng")
        .agg(
            F.count("*").alias("total"),
            F.round(F.avg("severidad"), 2).alias("severidad_promedio"),
        )
        .withColumn("centro_lat", F.round((F.col("celda_lat") + 0.5) * CELDA, 4))
        .withColumn("centro_lng", F.round((F.col("celda_lng") + 0.5) * CELDA, 4))
        .withColumn(
            "centro",
            F.struct(
                F.lit("Point").alias("type"),
                F.array(F.col("centro_lng"), F.col("centro_lat")).alias("coordinates"),
            ),
        )
        .withColumn("tamano_celda", F.lit(CELDA))
        .withColumn("_id", F.concat_ws("_", F.col("celda_lat"), F.col("celda_lng")))
    )


def calcular_zonas_calientes(grilla):
    umbral = grilla.approxQuantile("total", [0.99], 0.001)[0]
    print(f"Umbral para zona caliente (percentil 99): {umbral}")
    return (
        grilla.filter(F.col("total") >= umbral)
        .withColumn("umbral", F.lit(umbral))
        .orderBy(F.col("total").desc())
    )


def calcular_por_hora(accidentes):
    return (
        accidentes.groupBy(F.hour("fecha").alias("hora"))
        .agg(
            F.count("*").alias("total"),
            F.round(F.avg("severidad"), 2).alias("severidad_promedio"),
        )
        .withColumn("_id", F.col("hora"))
    )


def calcular_por_dia(accidentes):
    nombres = F.array(*[F.lit(d) for d in DIAS])
    return (
        accidentes.groupBy(F.dayofweek("fecha").alias("dia"))
        .agg(
            F.count("*").alias("total"),
            F.round(F.avg("severidad"), 2).alias("severidad_promedio"),
        )
        .withColumn("nombre", F.element_at(nombres, F.col("dia")))
        .withColumn("_id", F.col("dia"))
    )


def calcular_por_mes(accidentes):
    return (
        accidentes.groupBy(F.date_format("fecha", "yyyy-MM").alias("mes"))
        .agg(
            F.count("*").alias("total"),
            F.round(F.avg("severidad"), 2).alias("severidad_promedio"),
        )
        .withColumn("_id", F.col("mes"))
    )


def main():
    spark = crear_sesion()
    spark.sparkContext.setLogLevel("WARN")

    accidentes = leer(spark, "accidentes").select(
        F.col("location.coordinates").getItem(0).alias("lng"),
        F.col("location.coordinates").getItem(1).alias("lat"),
        F.col("fecha"),
        F.col("severidad"),
    )
    accidentes.cache()
    total = accidentes.count()
    print(f"Documentos leidos desde MongoDB: {total}")

    grilla = calcular_grilla(accidentes)
    grilla.cache()
    suma_grilla = grilla.agg(F.sum("total")).collect()[0][0]
    print(f"Celdas con datos: {grilla.count()}  Suma de conteos: {suma_grilla}")
    if suma_grilla != total:
        print("La suma de la grilla no coincide con el total de documentos")

    guardar(grilla, "resultados_grilla")
    guardar(calcular_zonas_calientes(grilla), "resultados_zonas_calientes")
    guardar(calcular_por_hora(accidentes), "resultados_hora")
    guardar(calcular_por_dia(accidentes), "resultados_dia")
    guardar(calcular_por_mes(accidentes), "resultados_mes")

    spark.stop()


if __name__ == "__main__":
    main()
