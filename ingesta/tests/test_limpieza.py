import pandas as pd

from limpieza import a_documento, coordenadas_validas, limpiar_particion


def crear_datos():
    return pd.DataFrame({
        "ID": ["A-1", "A-2", "A-3", "A-4", "A-5", "A-6"],
        "Severity": [2.0, 3.0, 1.0, 4.0, 2.0, 2.0],
        "Start_Time": [
            "2021-03-01 08:15:00",
            "2021-03-01 09:00:00.000000000",
            "2021-03-02 10:00:00",
            "fecha mala",
            "2021-03-04 18:30:00",
            "2021-03-05 07:45:00",
        ],
        "Start_Lat": [34.05, 40.71, None, 29.76, 95.0, 10.0],
        "Start_Lng": [-118.24, -74.0, -87.6, -95.36, -80.0, -70.0],
        "City": ["Los Angeles", "New York", "Chicago", "Houston", "X", "Y"],
        "County": ["LA", "NY", "Cook", "Harris", "X", "Y"],
        "State": ["CA", "NY", "IL", "TX", "X", "Y"],
        "Weather_Condition": ["Clear", None, "Rain", "Fog", "Clear", "Clear"],
        "Temperature(F)": [70.0, None, 50.0, 80.0, 60.0, 60.0],
        "Visibility(mi)": [10.0, 5.0, None, 2.0, 10.0, 10.0],
    })


def test_descarta_nulos_y_fuera_de_rango():
    limpio = limpiar_particion(crear_datos())
    assert list(limpio["id"]) == ["A-1", "A-2"]


def test_fecha_con_decimales_se_convierte():
    limpio = limpiar_particion(crear_datos())
    assert limpio["fecha"].iloc[1] == pd.Timestamp("2021-03-01 09:00:00")


def test_rango_de_coordenadas():
    lat = pd.Series([34.0, 60.0, -95.0])
    lng = pd.Series([-100.0, -100.0, -100.0])
    assert list(coordenadas_validas(lat, lng)) == [True, False, False]


def test_documento_es_geojson_valido():
    limpio = limpiar_particion(crear_datos())
    documento = a_documento(limpio.to_dict("records")[0])
    assert documento["location"]["type"] == "Point"
    assert documento["location"]["coordinates"] == [-118.24, 34.05]
    assert documento["severidad"] == 2


def test_nulos_quedan_como_none():
    limpio = limpiar_particion(crear_datos())
    documento = a_documento(limpio.to_dict("records")[1])
    assert documento["clima"] is None
    assert documento["temperatura"] is None
