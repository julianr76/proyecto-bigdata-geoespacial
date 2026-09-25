import os
import sys
import time

import requests

URL = os.getenv("API_URL", "http://api-pruebas:5000")

POLIGONO_LA = {
    "type": "Polygon",
    "coordinates": [[
        [-118.35, 33.95],
        [-118.15, 33.95],
        [-118.15, 34.15],
        [-118.35, 34.15],
        [-118.35, 33.95],
    ]],
}


def esperar_api():
    for _ in range(30):
        try:
            r = requests.get(f"{URL}/salud", timeout=5)
            if r.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    print("La API no respondio a tiempo")
    sys.exit(1)


def revisar(nombre, respuesta, codigo_esperado):
    if respuesta.status_code != codigo_esperado:
        print(f"FALLO {nombre}: se esperaba {codigo_esperado} y llego {respuesta.status_code}")
        print(respuesta.text[:500])
        return False
    print(f"OK {nombre}")
    return True


def main():
    esperar_api()
    resultados = [
        revisar("salud", requests.get(f"{URL}/salud", timeout=10), 200),
        revisar(
            "cercanos",
            requests.get(f"{URL}/accidentes/cercanos", params={"lat": 34.05, "lng": -118.24, "radio": 5000, "limite": 5}, timeout=30),
            200,
        ),
        revisar(
            "cercanos con latitud invalida",
            requests.get(f"{URL}/accidentes/cercanos", params={"lat": 200, "lng": -118.24, "radio": 5000}, timeout=30),
            400,
        ),
        revisar(
            "poligono",
            requests.post(f"{URL}/accidentes/poligono", params={"limite": 5}, json=POLIGONO_LA, timeout=30),
            200,
        ),
        revisar(
            "poligono sin cerrar",
            requests.post(f"{URL}/accidentes/poligono", json={"type": "Polygon", "coordinates": [[[-118, 34], [-117, 34], [-117, 35], [-118, 35]]]}, timeout=30),
            400,
        ),
        revisar(
            "resumen por radio",
            requests.get(f"{URL}/accidentes/resumen-radio", params={"lat": 34.05, "lng": -118.24, "radio": 10000}, timeout=30),
            200,
        ),
        revisar("resultados spark", requests.get(f"{URL}/resultados/hora", timeout=30), 200),
    ]
    if not all(resultados):
        sys.exit(1)
    print("Todas las pruebas contra la API pasaron")


if __name__ == "__main__":
    main()
