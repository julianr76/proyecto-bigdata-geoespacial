MAX_RADIO = 100000
MAX_LIMITE = 1000


class ErrorParametros(ValueError):
    pass


def leer_numero(valor, nombre):
    if valor is None or str(valor).strip() == "":
        raise ErrorParametros(f"Falta el parametro {nombre}")
    try:
        return float(valor)
    except (TypeError, ValueError):
        raise ErrorParametros(f"El parametro {nombre} debe ser numerico")


def validar_punto(lat, lng):
    if not -90 <= lat <= 90:
        raise ErrorParametros("La latitud debe estar entre -90 y 90")
    if not -180 <= lng <= 180:
        raise ErrorParametros("La longitud debe estar entre -180 y 180")


def validar_radio(radio):
    if radio <= 0 or radio > MAX_RADIO:
        raise ErrorParametros(f"El radio debe ser mayor a 0 y maximo {MAX_RADIO} metros")


def leer_limite(valor, defecto=100):
    if valor is None or str(valor).strip() == "":
        return defecto
    try:
        limite = int(valor)
    except (TypeError, ValueError):
        raise ErrorParametros("El limite debe ser un entero")
    if limite <= 0 or limite > MAX_LIMITE:
        raise ErrorParametros(f"El limite debe estar entre 1 y {MAX_LIMITE}")
    return limite


def punto_geojson(lat, lng):
    return {"type": "Point", "coordinates": [lng, lat]}


def filtro_cercanos(lat, lng, radio):
    validar_punto(lat, lng)
    validar_radio(radio)
    return {
        "location": {
            "$near": {
                "$geometry": punto_geojson(lat, lng),
                "$maxDistance": radio,
            }
        }
    }


def validar_poligono(datos):
    if not isinstance(datos, dict):
        raise ErrorParametros("Se esperaba un objeto GeoJSON en el cuerpo de la peticion")
    if datos.get("type") == "Feature":
        datos = datos.get("geometry") or {}
    if datos.get("type") != "Polygon":
        raise ErrorParametros("La geometria debe ser de tipo Polygon")
    anillos = datos.get("coordinates")
    if not isinstance(anillos, list) or len(anillos) == 0:
        raise ErrorParametros("El poligono no tiene coordenadas")
    for anillo in anillos:
        if not isinstance(anillo, list) or len(anillo) < 4:
            raise ErrorParametros("Cada anillo del poligono necesita al menos 4 puntos")
        for punto in anillo:
            if not isinstance(punto, list) or len(punto) != 2:
                raise ErrorParametros("Cada punto debe ser [longitud, latitud]")
            lng = leer_numero(punto[0], "longitud")
            lat = leer_numero(punto[1], "latitud")
            validar_punto(lat, lng)
        if anillo[0] != anillo[-1]:
            raise ErrorParametros("El poligono debe estar cerrado (primer punto igual al ultimo)")
    return {"type": "Polygon", "coordinates": anillos}


def filtro_poligono(geometria):
    return {"location": {"$geoWithin": {"$geometry": geometria}}}


def pipeline_resumen_radio(lat, lng, radio):
    validar_punto(lat, lng)
    validar_radio(radio)
    return [
        {
            "$geoNear": {
                "near": punto_geojson(lat, lng),
                "distanceField": "distancia",
                "maxDistance": radio,
                "spherical": True,
                "key": "location",
            }
        },
        {
            "$group": {
                "_id": "$severidad",
                "total": {"$sum": 1},
                "distancia_promedio": {"$avg": "$distancia"},
                "distancia_minima": {"$min": "$distancia"},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "severidad": "$_id",
                "total": 1,
                "distancia_promedio": {"$round": ["$distancia_promedio", 1]},
                "distancia_minima": {"$round": ["$distancia_minima", 1]},
            }
        },
    ]
