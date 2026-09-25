import pytest

import consultas


def test_filtro_cercanos_usa_near():
    filtro = consultas.filtro_cercanos(34.05, -118.24, 5000)
    near = filtro["location"]["$near"]
    assert near["$geometry"]["coordinates"] == [-118.24, 34.05]
    assert near["$maxDistance"] == 5000


def test_latitud_invalida():
    with pytest.raises(consultas.ErrorParametros):
        consultas.filtro_cercanos(120, -118.24, 5000)


def test_radio_invalido():
    with pytest.raises(consultas.ErrorParametros):
        consultas.filtro_cercanos(34.05, -118.24, -5)
    with pytest.raises(consultas.ErrorParametros):
        consultas.filtro_cercanos(34.05, -118.24, 500000)


def test_leer_numero():
    assert consultas.leer_numero("3.5", "x") == 3.5
    with pytest.raises(consultas.ErrorParametros):
        consultas.leer_numero("abc", "x")
    with pytest.raises(consultas.ErrorParametros):
        consultas.leer_numero(None, "x")


def test_limite():
    assert consultas.leer_limite(None) == 100
    assert consultas.leer_limite("20") == 20
    with pytest.raises(consultas.ErrorParametros):
        consultas.leer_limite("5000")


def test_poligono_valido():
    poligono = {"type": "Polygon", "coordinates": [[[-118, 34], [-117, 34], [-117, 35], [-118, 34]]]}
    geometria = consultas.validar_poligono(poligono)
    filtro = consultas.filtro_poligono(geometria)
    assert filtro["location"]["$geoWithin"]["$geometry"]["type"] == "Polygon"


def test_poligono_como_feature():
    feature = {
        "type": "Feature",
        "properties": {},
        "geometry": {"type": "Polygon", "coordinates": [[[-118, 34], [-117, 34], [-117, 35], [-118, 34]]]},
    }
    assert consultas.validar_poligono(feature)["type"] == "Polygon"


def test_poligono_sin_cerrar():
    poligono = {"type": "Polygon", "coordinates": [[[-118, 34], [-117, 34], [-117, 35], [-118, 35]]]}
    with pytest.raises(consultas.ErrorParametros):
        consultas.validar_poligono(poligono)


def test_poligono_tipo_incorrecto():
    with pytest.raises(consultas.ErrorParametros):
        consultas.validar_poligono({"type": "Point", "coordinates": [-118, 34]})


def test_pipeline_empieza_con_geonear():
    pipeline = consultas.pipeline_resumen_radio(34.05, -118.24, 10000)
    assert "$geoNear" in pipeline[0]
    assert pipeline[0]["$geoNear"]["maxDistance"] == 10000
