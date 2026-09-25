import os
from datetime import datetime

from flask import Flask, jsonify, request
from pymongo import MongoClient

import consultas

RESULTADOS = {
    "grilla": ("resultados_grilla", [("total", -1)]),
    "zonas-calientes": ("resultados_zonas_calientes", [("total", -1)]),
    "hora": ("resultados_hora", [("_id", 1)]),
    "dia": ("resultados_dia", [("_id", 1)]),
    "mes": ("resultados_mes", [("_id", 1)]),
}

app = Flask(__name__)
app.json.ensure_ascii = False
cliente_mongo = None


def obtener_db():
    global cliente_mongo
    if cliente_mongo is None:
        cliente_mongo = MongoClient(
            os.getenv("MONGO_URI", "mongodb://mongo:27017"),
            serverSelectionTimeoutMS=5000,
        )
    return cliente_mongo[os.getenv("MONGO_DB", "accidentes_db")]


def preparar(documento):
    salida = {}
    for clave, valor in documento.items():
        if isinstance(valor, datetime):
            salida[clave] = valor.isoformat()
        elif clave == "_id":
            salida[clave] = str(valor)
        else:
            salida[clave] = valor
    return salida


def respuesta(documentos):
    lista = [preparar(d) for d in documentos]
    return jsonify({"total": len(lista), "resultados": lista})


@app.errorhandler(consultas.ErrorParametros)
def error_parametros(error):
    return jsonify({"error": str(error)}), 400


@app.get("/salud")
def salud():
    try:
        obtener_db().command("ping")
        return jsonify({"estado": "ok"})
    except Exception as error:
        return jsonify({"estado": "sin conexion a mongo", "detalle": str(error)}), 503


@app.get("/accidentes/cercano")
def cercanos():
    lat = consultas.leer_numero(request.args.get("lat"), "lat")
    lng = consultas.leer_numero(request.args.get("lng"), "lng")
    radio = consultas.leer_numero(request.args.get("radio"), "radio")
    limite = consultas.leer_limite(request.args.get("limite"))
    filtro = consultas.filtro_cercanos(lat, lng, radio)
    documentos = obtener_db()["accidentes"].find(filtro, {"_id": 0}).limit(limite)
    return respuesta(documentos)


@app.post("/accidentes/poligono")
def dentro_poligono():
    geometria = consultas.validar_poligono(request.get_json(silent=True))
    limite = consultas.leer_limite(request.args.get("limite"))
    filtro = consultas.filtro_poligono(geometria)
    documentos = obtener_db()["accidentes"].find(filtro, {"_id": 0}).limit(limite)
    return respuesta(documentos)


@app.get("/accidentes/resumen-radio")
def resumen_radio():
    lat = consultas.leer_numero(request.args.get("lat"), "lat")
    lng = consultas.leer_numero(request.args.get("lng"), "lng")
    radio = consultas.leer_numero(request.args.get("radio"), "radio")
    pipeline = consultas.pipeline_resumen_radio(lat, lng, radio)
    documentos = obtener_db()["accidentes"].aggregate(pipeline)
    return respuesta(documentos)


@app.get("/resultados/<tipo>")
def resultados_spark(tipo):
    if tipo not in RESULTADOS:
        opciones = ", ".join(RESULTADOS.keys())
        return jsonify({"error": f"Tipo no valido, opciones: {opciones}"}), 404
    coleccion, orden = RESULTADOS[tipo]
    limite = consultas.leer_limite(request.args.get("limite"), defecto=500)
    documentos = obtener_db()[coleccion].find({}).sort(orden).limit(limite)
    return respuesta(documentos)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
