"""
Prepara los documentos que se cargan en MongoDB Atlas (sección 3 de la memoria) a partir de ../datos:

  lecturas_recientes.json  las últimas 6 h de lecturas de cada sensor, en documentos de 15 minutos (patrón bucket),
                           con el dispositivo y la carga en curso copiados dentro del documento
  eventos_app.json         los eventos de la app de los últimos 7 días, con campos que dependen del tipo de evento
  provincias.json          las 50 provincias (para el $lookup)

Se aplican las mismas reglas de limpieza que en silver (sin duplicados, sin lecturas de error, sin sensores sin alta).
Uso:  python mongo/generar_mongo.py      y después Compass -> ADD DATA -> Import JSON en la base coldtrack
"""
import csv, json, os
from datetime import datetime, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
D = "../datos"
DESDE_LECT = datetime(2026, 11, 25, 18, 0, 0)
DESDE_EV = datetime(2026, 11, 19, 0, 0, 0)
RANGO = {"refrigerada": {"min": 2.0, "max": 8.0}, "congelada": {"min": -30.0, "max": -18.0}}


def leer(nombre):
    with open(f"{D}/operativa/{nombre}", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fecha(s):
    return {"$date": s.replace(" ", "T") + ("Z" if len(s) > 10 else "T00:00:00Z")}


prov = {p["cod_provincia"]: p for p in leer("provincias.csv")}
modelos = {m["id_modelo"]: m for m in leer("modelos_camion.csv")}
camiones = {c["id_camion"]: c for c in leer("camiones.csv")}
camaras = {c["id_camara"]: c for c in leer("camaras.csv")}
sensores = {s["id_sensor"] for s in leer("sensores.csv")}
envios = [e for e in leer("envios.csv") if e["fecha_entrega"] >= "2026-11-25"]
reservas = [r for r in leer("reservas.csv") if r["recurso"] == "camara" and r["estado"] != "cancelada" and r["fin"] >= "2026-11-25"]


def carga_en(tipo, ident, ts):
    s = ts.strftime("%Y-%m-%d %H:%M:%S")
    if tipo == "camion":
        e = next((e for e in envios if e["id_camion"] == ident and e["fecha_recogida"] <= s <= e["fecha_entrega"]), None)
        return None if e is None else {"referencia": e["id_envio"], "tipo_carga": e["tipo_carga"], "rango": RANGO[e["tipo_carga"]],
                                       "origen": e["cod_origen"], "destino": e["cod_destino"]}
    r = next((r for r in reservas if r["id_camara"] == ident and r["inicio"] <= s <= r["fin"]), None)
    return None if r is None else {"referencia": r["id_reserva"], "tipo_carga": r["tipo_carga"], "rango": RANGO[r["tipo_carga"]]}


# ---------------------------------------------------------------- lecturas en buckets de 15 minutos
buckets, vistos = {}, set()
with open(f"{D}/sensores/lecturas_2026-11-25.jsonl", encoding="utf-8") as f:
    for linea in f:
        l = json.loads(linea)
        try:
            ts = datetime.strptime(l["ts"], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
        t = l.get("temp_int_c", l.get("temp"))
        if ts < DESDE_LECT or l["sensor_id"] not in sensores or t is None or not -40 <= t <= 40 or (l["sensor_id"], ts) in vistos:
            continue
        vistos.add((l["sensor_id"], ts))
        ini = ts.replace(minute=ts.minute // 15 * 15, second=0)
        clave = (l["sensor_id"], ini)
        if clave not in buckets:
            tipo = l["tipo_dispositivo"]
            ident = l.get("id_camion") or l.get("id_camara")
            disp = {"tipo": tipo, "id": ident}
            if tipo == "camion":
                m = modelos[camiones[ident]["id_modelo"]]
                disp["modelo"] = f"{m['marca']} {m['modelo']}"
            else:
                disp["almacen"] = camaras[ident]["id_almacen"]
            buckets[clave] = {"_id": f"{l['sensor_id']}|{ini:%Y-%m-%dT%H:%M}", "sensor_id": l["sensor_id"], "dispositivo": disp,
                              "firmware": l["fw"], "inicio": ini, "fin": ini + timedelta(seconds=15 * 59),
                              "carga": carga_en(tipo, ident, ini + timedelta(minutes=7)), "lecturas": []}
        lec = {"ts": ts, "t": t, "h": l["humedad_pct"], "puerta": l["puerta"] == "abierta"}
        if l["tipo_dispositivo"] == "camion":
            lec.update({"v": l["velocidad_kmh"], "pos": [l["lon"], l["lat"]], "litros": l["combustible_l"], "motor": l["motor"]})
        else:
            lec["compresor"] = l["compresor"]
        buckets[clave]["lecturas"].append(lec)

docs = []
for b in buckets.values():
    temps = [x["t"] for x in b["lecturas"]]
    b["n"] = len(temps)
    b["resumen"] = {"t_min": min(temps), "t_max": max(temps), "t_media": round(sum(temps) / len(temps), 2),
                    "aperturas": sum(x["puerta"] for x in b["lecturas"])}
    b["inicio"], b["fin"] = {"$date": b["inicio"].isoformat() + "Z"}, {"$date": b["fin"].isoformat() + "Z"}
    for x in b["lecturas"]:
        x["ts"] = {"$date": x["ts"].isoformat() + "Z"}
    docs.append(b)
with open("lecturas_recientes.json", "w", encoding="utf-8") as f:
    json.dump(docs, f, ensure_ascii=False)
print(f"lecturas_recientes.json: {len(docs):,} documentos con {sum(d['n'] for d in docs):,} lecturas")

# ---------------------------------------------------------------- eventos de la app (últimos 7 días)
nombre_a_cod = {p["nombre"].lower(): c for c, p in prov.items()}
import unicodedata
quitar = lambda s: "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn").lower()
nombre_a_cod = {quitar(p["nombre"]): c for c, p in prov.items()}
evs = []
with open(f"{D}/logs/eventos_app.jsonl", encoding="utf-8") as f:
    for linea in f:
        e = json.loads(linea)
        ts = datetime.strptime(e["ts"], "%Y-%m-%dT%H:%M:%S")
        if ts < DESDE_EV:
            continue
        doc = {"ts": {"$date": e["ts"] + "Z"}, "evento": e["evento"], "usuario": e["id_usuario"], "cliente": e["id_cliente"],
               "sesion": e["id_sesion"], "app": {"canal": e["app"], "version": e["version"]}}
        o = e.get("origen") or nombre_a_cod.get(quitar(e.get("origen_nombre", "")))
        d = e.get("destino") or nombre_a_cod.get(quitar(e.get("destino_nombre", "")))
        if e["evento"] in ("busqueda_ruta", "cotizacion", "reserva_iniciada", "pago_completado"):
            if not (o and d):
                continue
            doc["ruta"] = {"origen": o, "destino": d}
        if e["evento"] == "cotizacion":
            doc["cotizacion"] = {"tipo_carga": e["tipo_carga"], "peso_kg": e["peso_kg"], "precio_eur": e["precio_ofrecido"]}
        if e["evento"] == "pago_completado":
            doc["pago"] = {"importe_eur": e["importe"]}
        if e["evento"] == "clic":
            doc["elemento"] = e["elemento"]
        evs.append(doc)
with open("eventos_app.json", "w", encoding="utf-8") as f:
    json.dump(evs, f, ensure_ascii=False)
print(f"eventos_app.json: {len(evs):,} documentos")

with open("provincias.json", "w", encoding="utf-8") as f:
    json.dump([{"_id": c, "nombre": p["nombre"], "comunidad": p["comunidad"], "pos": [float(p["lon"]), float(p["lat"])]}
               for c, p in prov.items()], f, ensure_ascii=False)
print("provincias.json: 50 documentos")
