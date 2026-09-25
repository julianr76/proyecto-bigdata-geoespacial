import pandas as pd

COLUMNAS = [
    "ID",
    "Severity",
    "Start_Time",
    "Start_Lat",
    "Start_Lng",
    "City",
    "County",
    "State",
    "Weather_Condition",
    "Temperature(F)",
    "Visibility(mi)",
]

TIPOS = {
    "ID": "object",
    "Severity": "float64",
    "Start_Time": "object",
    "Start_Lat": "float64",
    "Start_Lng": "float64",
    "City": "object",
    "County": "object",
    "State": "object",
    "Weather_Condition": "object",
    "Temperature(F)": "float64",
    "Visibility(mi)": "float64",
}

NOMBRES = {
    "ID": "id",
    "Severity": "severidad",
    "Start_Time": "fecha",
    "Start_Lat": "lat",
    "Start_Lng": "lng",
    "City": "ciudad",
    "County": "condado",
    "State": "estado",
    "Weather_Condition": "clima",
    "Temperature(F)": "temperatura",
    "Visibility(mi)": "visibilidad",
}

LAT_MIN, LAT_MAX = 24.0, 50.0
LNG_MIN, LNG_MAX = -125.0, -66.0


def coordenadas_validas(lat, lng):
    rango_ok = lat.between(-90, 90) & lng.between(-180, 180)
    en_eeuu = lat.between(LAT_MIN, LAT_MAX) & lng.between(LNG_MIN, LNG_MAX)
    return rango_ok & en_eeuu


def limpiar_particion(df):
    df = df.rename(columns=NOMBRES)
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce")
    df = df.dropna(subset=["lat", "lng"])
    df = df[coordenadas_validas(df["lat"], df["lng"])].copy()
    texto_fecha = df["fecha"].astype("string").str.slice(0, 19)
    df["fecha"] = pd.to_datetime(texto_fecha, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    df = df.dropna(subset=["fecha", "id"])
    return df


def valor_o_nulo(valor):
    if valor is None:
        return None
    try:
        if pd.isna(valor):
            return None
    except (TypeError, ValueError):
        pass
    return valor


def a_documento(fila):
    severidad = valor_o_nulo(fila.get("severidad"))
    temperatura = valor_o_nulo(fila.get("temperatura"))
    visibilidad = valor_o_nulo(fila.get("visibilidad"))
    return {
        "id": str(fila["id"]),
        "severidad": int(severidad) if severidad is not None else None,
        "fecha": pd.Timestamp(fila["fecha"]).to_pydatetime(),
        "ciudad": valor_o_nulo(fila.get("ciudad")),
        "condado": valor_o_nulo(fila.get("condado")),
        "estado": valor_o_nulo(fila.get("estado")),
        "clima": valor_o_nulo(fila.get("clima")),
        "temperatura": float(temperatura) if temperatura is not None else None,
        "visibilidad": float(visibilidad) if visibilidad is not None else None,
        "location": {
            "type": "Point",
            "coordinates": [float(fila["lng"]), float(fila["lat"])],
        },
    }
