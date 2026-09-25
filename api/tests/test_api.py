from datetime import datetime

import pytest

import app as servidor


class CursorFalso:
    def __init__(self, documentos):
        self.documentos = documentos

    def sort(self, orden):
        return self

    def limit(self, cantidad):
        return self.documentos[:cantidad]


class ColeccionFalsa:
    def __init__(self, documentos):
        self.documentos = documentos
        self.ultimo_filtro = None
        self.ultimo_pipeline = None

    def find(self, filtro, proyeccion=None):
        self.ultimo_filtro = filtro
        return CursorFalso(self.documentos)

    def aggregate(self, pipeline):
        self.ultimo_pipeline = pipeline
        return iter([{"severidad": 2, "total": 3, "distancia_promedio": 120.5, "distancia_minima": 10.0}])


class BaseFalsa:
    def __init__(self):
        self.colecciones = {
            "accidentes": ColeccionFalsa([
                {
                    "id": "A-1",
                    "severidad": 2,
                    "fecha": datetime(2021, 3, 1, 8, 15),
                    "location": {"type": "Point", "coordinates": [-118.24, 34.05]},
                }
            ]),
            "resultados_hora": ColeccionFalsa([{"_id": 8, "hora": 8, "total": 50}]),
        }

    def __getitem__(self, nombre):
        return self.colecciones.setdefault(nombre, ColeccionFalsa([]))

    def command(self, nombre):
        return {"ok": 1}


@pytest.fixture
def cliente(monkeypatch):
    base = BaseFalsa()
    monkeypatch.setattr(servidor, "obtener_db", lambda: base)
    servidor.app.config["TESTING"] = True
    with servidor.app.test_client() as c:
        c.base = base
        yield c


def test_salud(cliente):
    r = cliente.get("/salud")
    assert r.status_code == 200
    assert r.get_json()["estado"] == "ok"


def test_cercanos(cliente):
    r = cliente.get("/accidentes/cercanos?lat=34.05&lng=-118.24&radio=3000")
    assert r.status_code == 200
    datos = r.get_json()
    assert datos["total"] == 1
    assert datos["resultados"][0]["fecha"] == "2021-03-01T08:15:00"
    assert "$near" in cliente.base["accidentes"].ultimo_filtro["location"]


def test_cercanos_sin_radio(cliente):
    r = cliente.get("/accidentes/cercanos?lat=34.05&lng=-118.24")
    assert r.status_code == 400


def test_poligono(cliente):
    poligono = {"type": "Polygon", "coordinates": [[[-118.3, 34.0], [-118.1, 34.0], [-118.1, 34.2], [-118.3, 34.0]]]}
    r = cliente.post("/accidentes/poligono", json=poligono)
    assert r.status_code == 200
    assert "$geoWithin" in cliente.base["accidentes"].ultimo_filtro["location"]


def test_poligono_sin_cuerpo(cliente):
    r = cliente.post("/accidentes/poligono")
    assert r.status_code == 400


def test_resumen_radio(cliente):
    r = cliente.get("/accidentes/resumen-radio?lat=34.05&lng=-118.24&radio=10000")
    assert r.status_code == 200
    assert "$geoNear" in cliente.base["accidentes"].ultimo_pipeline[0]


def test_resultados_spark(cliente):
    r = cliente.get("/resultados/hora")
    assert r.status_code == 200
    assert r.get_json()["resultados"][0]["total"] == 50


def test_resultados_tipo_invalido(cliente):
    r = cliente.get("/resultados/otra-cosa")
    assert r.status_code == 404
