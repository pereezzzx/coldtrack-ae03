"""
ColdTrack · generador de datos sintéticos del prototipo (AE03, grupo 6).

Genera, con semilla fija, una muestra a escala 1/20 del negocio supuesto:
  - 100 sensores (75 camiones y 25 cámaras), una lectura cada 15 s durante 24 h  -> ~576.000 lecturas (JSON por líneas)
  - 90 días de operativa exportada de PostgreSQL: envíos, reservas y facturas      -> CSV
  - 30 días de logs de la aplicación (cotizador, búsquedas, pagos)                 -> JSON por líneas
  - 50 zonas × 24 h de observaciones de temperatura exterior al estilo AEMET OpenData -> JSON
  - Maestros: provincias, clientes, modelos de camión, camiones, almacenes, cámaras, sensores, tarifas

Los datos traen suciedad a propósito (duplicados, lecturas de error, marcas de tiempo mal escritas,
sensores desconocidos, provincias escritas a mano en los logs) para que la capa silver tenga trabajo.

Uso:  python generar_datos.py            (escribe en ./datos, unos 160 MB)
"""
import csv, json, math, os, random, shutil, time
from datetime import datetime, timedelta, date

SEMILLA = 2026
R = random.Random(SEMILLA)
SALIDA = "datos"

DIA_SENSORES = date(2026, 11, 25)              # día de la muestra de sensores
FIN_OPERATIVA = date(2026, 11, 25)             # último día de la operativa exportada
DIAS_OPERATIVA = 90
DIAS_LOGS = 30
N_CAMIONES, N_CAMARAS, N_CLIENTES = 75, 25, 200
PASO = 15                                      # segundos entre lecturas

# ---------------------------------------------------------------- maestros
# código INE, nombre, comunidad, lat, lon de la capital, temperatura media de noviembre (°C), amplitud diaria
PROVINCIAS = [
    ("01", "Álava", "País Vasco", 42.85, -2.67, 8.5, 4.0), ("02", "Albacete", "Castilla-La Mancha", 38.99, -1.86, 9.5, 5.0),
    ("03", "Alicante", "Comunitat Valenciana", 38.35, -0.48, 15.5, 4.0), ("04", "Almería", "Andalucía", 36.84, -2.46, 16.0, 3.5),
    ("05", "Ávila", "Castilla y León", 40.66, -4.70, 6.5, 4.5), ("06", "Badajoz", "Extremadura", 38.88, -6.97, 12.0, 5.0),
    ("07", "Illes Balears", "Illes Balears", 39.57, 2.65, 15.5, 3.5), ("08", "Barcelona", "Cataluña", 41.39, 2.17, 13.5, 3.5),
    ("09", "Burgos", "Castilla y León", 42.34, -3.70, 6.5, 4.5), ("10", "Cáceres", "Extremadura", 39.47, -6.37, 11.5, 5.0),
    ("11", "Cádiz", "Andalucía", 36.53, -6.29, 16.0, 3.5), ("12", "Castellón", "Comunitat Valenciana", 39.99, -0.05, 14.5, 4.0),
    ("13", "Ciudad Real", "Castilla-La Mancha", 38.99, -3.93, 10.0, 5.0), ("14", "Córdoba", "Andalucía", 37.88, -4.78, 13.0, 5.0),
    ("15", "A Coruña", "Galicia", 43.36, -8.41, 12.0, 3.0), ("16", "Cuenca", "Castilla-La Mancha", 40.07, -2.13, 8.0, 5.0),
    ("17", "Girona", "Cataluña", 41.98, 2.82, 11.0, 4.5), ("18", "Granada", "Andalucía", 37.18, -3.60, 10.5, 5.5),
    ("19", "Guadalajara", "Castilla-La Mancha", 40.63, -3.17, 9.0, 5.0), ("20", "Gipuzkoa", "País Vasco", 43.32, -1.98, 11.0, 3.5),
    ("21", "Huelva", "Andalucía", 37.26, -6.95, 15.0, 4.5), ("22", "Huesca", "Aragón", 42.14, -0.41, 8.0, 4.5),
    ("23", "Jaén", "Andalucía", 37.77, -3.79, 12.0, 5.0), ("24", "León", "Castilla y León", 42.60, -5.57, 7.0, 4.5),
    ("25", "Lleida", "Cataluña", 41.62, 0.62, 9.0, 4.5), ("26", "La Rioja", "La Rioja", 42.47, -2.45, 9.0, 4.5),
    ("27", "Lugo", "Galicia", 43.01, -7.56, 8.5, 4.0), ("28", "Madrid", "Comunidad de Madrid", 40.42, -3.70, 10.0, 5.0),
    ("29", "Málaga", "Andalucía", 36.72, -4.42, 16.5, 4.0), ("30", "Murcia", "Región de Murcia", 37.99, -1.13, 14.5, 5.0),
    ("31", "Navarra", "Navarra", 42.81, -1.65, 8.0, 4.5), ("32", "Ourense", "Galicia", 42.34, -7.86, 10.0, 5.0),
    ("33", "Asturias", "Principado de Asturias", 43.36, -5.85, 10.5, 3.5), ("34", "Palencia", "Castilla y León", 42.01, -4.53, 7.0, 4.5),
    ("35", "Las Palmas", "Canarias", 28.12, -15.43, 23.0, 3.5), ("36", "Pontevedra", "Galicia", 42.43, -8.65, 12.0, 3.5),
    ("37", "Salamanca", "Castilla y León", 40.97, -5.66, 7.5, 5.0), ("38", "Santa Cruz de Tenerife", "Canarias", 28.47, -16.25, 22.5, 3.5),
    ("39", "Cantabria", "Cantabria", 43.46, -3.80, 12.0, 3.0), ("40", "Segovia", "Castilla y León", 40.95, -4.12, 7.0, 4.5),
    ("41", "Sevilla", "Andalucía", 37.39, -5.98, 14.5, 5.5), ("42", "Soria", "Castilla y León", 41.76, -2.46, 6.0, 4.5),
    ("43", "Tarragona", "Cataluña", 41.12, 1.25, 13.5, 4.0), ("44", "Teruel", "Aragón", 40.34, -1.11, 7.0, 5.0),
    ("45", "Toledo", "Castilla-La Mancha", 39.86, -4.02, 11.0, 5.0), ("46", "Valencia", "Comunitat Valenciana", 39.47, -0.38, 15.0, 4.0),
    ("47", "Valladolid", "Castilla y León", 41.65, -4.72, 7.5, 4.5), ("48", "Bizkaia", "País Vasco", 43.26, -2.93, 12.0, 3.5),
    ("49", "Zamora", "Castilla y León", 41.50, -5.75, 8.0, 4.5), ("50", "Zaragoza", "Aragón", 41.65, -0.89, 10.0, 4.5),
]
PROV = {p[0]: p for p in PROVINCIAS}
ISLAS = {"07": {"07"}, "35": {"35"}, "38": {"38"}}
# peso de cada provincia como origen/destino (actividad logística, supuesto)
PESO_PROV = {c: 1.0 for c in PROV}
PESO_PROV.update({"28": 9, "08": 8, "46": 5, "41": 4, "29": 3.5, "30": 3.5, "03": 3, "50": 3, "48": 3, "15": 2.5,
                  "47": 2, "36": 2, "33": 2, "11": 2, "14": 1.8, "18": 1.8, "04": 2.5, "45": 1.5, "35": 1.5, "38": 1.5, "07": 1.5})
# rutas que se cotizan mucho y se contratan poco (supuesto del caso: precio alto o falta de camiones)
RUTAS_FRIAS = {("28", "15"): 5.0, ("08", "41"): 4.5, ("46", "33"): 4.0, ("50", "29"): 4.0, ("28", "04"): 3.5, ("08", "36"): 3.0}

MODELOS = [  # id, marca, modelo, capacidad_kg, L/h en marcha, L/h del equipo de frío por °C de diferencia
    ("M1", "Volvo", "FH 460 Frigo", 24000, 27.0, 0.22),
    ("M2", "Scania", "R 450 Reefer", 24000, 26.0, 0.25),
    ("M3", "Mercedes-Benz", "Actros 1845 Frio", 22000, 28.5, 0.19),
    ("M4", "Iveco", "S-Way 490 TK", 23000, 30.0, 0.30),
]
ALMACENES = [("A01", "Plataforma Madrid Sur", "28"), ("A02", "Plataforma Barcelona Zona Franca", "08"),
             ("A03", "Plataforma Valencia Puerto", "46"), ("A04", "Plataforma Sevilla", "41"),
             ("A05", "Plataforma Zaragoza PLAZA", "50"), ("A06", "Plataforma Bilbao", "48"),
             ("A07", "Plataforma A Coruña", "15"), ("A08", "Plataforma Murcia", "30"),
             ("A09", "Plataforma Málaga", "29"), ("A10", "Plataforma Gran Canaria", "35")]
TARIFAS = [  # id, tipo_carga, base €, €/km, €/kg, recargo urgente
    ("T-REF", "refrigerada", 60.0, 1.32, 0.012, 0.25),
    ("T-CON", "congelada", 75.0, 1.48, 0.015, 0.25),
]
RANGO = {"refrigerada": (2.0, 8.0, 4.5), "congelada": (-30.0, -18.0, -20.5)}


def km_entre(a, b):
    """Distancia por carretera aproximada: gran círculo × 1,25 (local: 35 km)."""
    if a == b:
        return 35.0
    la1, lo1, la2, lo2 = map(math.radians, (PROV[a][3], PROV[a][4], PROV[b][3], PROV[b][4]))
    d = 2 * 6371 * math.asin(math.sqrt(math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2))
    return round(d * 1.25, 1)


def temp_ext(cod, momento):
    """Temperatura exterior de una zona a una hora (media + ciclo diario)."""
    _, _, _, _, _, med, amp = PROV[cod]
    h = momento.hour + momento.minute / 60
    return med + amp * math.cos(2 * math.pi * (h - 15) / 24)


def destino_para(origen, rr):
    if origen in ISLAS:
        return origen
    cands = [c for c in PROV if c not in ISLAS]
    return rr.choices(cands, weights=[PESO_PROV[c] * (0.25 if c == origen else 1.0) for c in cands])[0]


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    t0 = time.time()
    shutil.rmtree(SALIDA, ignore_errors=True)
    for sub in ("operativa", "sensores", "logs", "aemet"):
        os.makedirs(f"{SALIDA}/{sub}", exist_ok=True)

    def escribir_csv(nombre, cabecera, filas):
        with open(f"{SALIDA}/operativa/{nombre}", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cabecera)
            w.writerows(filas)
        return len(filas)

    resumen = {}
    resumen["provincias.csv"] = escribir_csv("provincias.csv", ["cod_provincia", "nombre", "comunidad", "lat", "lon"],
                                             [p[:5] for p in PROVINCIAS])
    resumen["modelos_camion.csv"] = escribir_csv("modelos_camion.csv", ["id_modelo", "marca", "modelo", "capacidad_kg"],
                                                 [m[:4] for m in MODELOS])
    resumen["almacenes.csv"] = escribir_csv("almacenes.csv", ["id_almacen", "nombre", "cod_provincia"], ALMACENES)
    resumen["tarifas.csv"] = escribir_csv("tarifas.csv", ["id_tarifa", "tipo_carga", "base_eur", "eur_km", "eur_kg", "recargo_urgente"], TARIFAS)

    # clientes
    sectores = ["Distribución alimentaria", "Farmacéutica", "Productos del mar", "Lácteos", "Frutas y hortalizas", "Cárnicas", "Restauración"]
    formas = ["S.L.", "S.A.", "S.Coop."]
    nombres = ["Frescos", "Nórdica", "Ibérica", "Atlántica", "Levante", "Sur", "Cantábrica", "Mediterránea", "Central", "Galaica"]
    clientes = []
    for i in range(1, N_CLIENTES + 1):
        cod = R.choices(list(PROV), weights=[PESO_PROV[c] for c in PROV])[0]
        nom = f"{R.choice(nombres)} {R.choice(sectores).split()[0]} {i:03d} {R.choice(formas)}"
        alta = date(2023, 1, 1) + timedelta(days=R.randint(0, 1000))
        clientes.append((f"CLI-{i:04d}", f"B{R.randint(10000000, 99999999)}", nom, f"compras{i:03d}@cliente{i:03d}.example",
                         cod, R.choice(sectores), alta.isoformat()))
    resumen["clientes.csv"] = escribir_csv("clientes.csv", ["id_cliente", "cif", "razon_social", "email", "cod_provincia", "sector", "fecha_alta"], clientes)

    # flota: camiones, cámaras y sensores
    camiones, base_camion = [], {}
    for i in range(1, N_CAMIONES + 1):
        m = MODELOS[(i - 1) % len(MODELOS)] if i <= 60 else R.choice(MODELOS)
        base = R.choices([c for c in PROV], weights=[PESO_PROV[c] for c in PROV])[0]
        if i in (71, 72, 73):          # tres camiones en Canarias y Baleares
            base = {71: "35", 72: "38", 73: "07"}[i]
        camiones.append((f"CAM-{i:03d}", f"{R.randint(1000, 9999)} {''.join(R.choice('BCDFGHJKLMNPRSTVWXYZ') for _ in range(3))}",
                         m[0], base, (date(2021, 1, 1) + timedelta(days=R.randint(0, 1300))).isoformat()))
        base_camion[f"CAM-{i:03d}"] = base
    resumen["camiones.csv"] = escribir_csv("camiones.csv", ["id_camion", "matricula", "id_modelo", "cod_provincia_base", "fecha_alta"], camiones)
    modelo_de = {c[0]: c[2] for c in camiones}
    camaras = []
    for i in range(1, N_CAMARAS + 1):
        alm = ALMACENES[(i - 1) % len(ALMACENES)]
        camaras.append((f"CMR-{i:03d}", alm[0], R.choice([24, 32, 48, 64]), "congelada" if i % 3 == 0 else "refrigerada"))
    resumen["camaras.csv"] = escribir_csv("camaras.csv", ["id_camara", "id_almacen", "capacidad_palets", "tipo_frio"], camaras)
    sensores = [(f"S-{c[0]}", "camion", c[0], "", "2.3" if j % 3 else "2.1") for j, c in enumerate(camiones)] + \
               [(f"S-{c[0]}", "camara", "", c[0], "4.0") for c in camaras]
    resumen["sensores.csv"] = escribir_csv("sensores.csv", ["id_sensor", "tipo_dispositivo", "id_camion", "id_camara", "firmware"], sensores)

    # ------------------------------------------------------------ operativa: 90 días de viajes por camión
    tarifa = {t[1]: t for t in TARIFAS}
    reservas, envios, facturas = [], [], []
    viajes_dia_muestra = {}             # camión -> [(salida, llegada, origen, destino, tipo_carga, id_envio)]
    n_res = n_env = n_fac = 0
    inicio_op = FIN_OPERATIVA - timedelta(days=DIAS_OPERATIVA - 1)
    posicion = dict(base_camion)
    for d in range(DIAS_OPERATIVA):
        dia = inicio_op + timedelta(days=d)
        for cam in sorted(base_camion):
            t = datetime.combine(dia, datetime.min.time()) + timedelta(hours=R.uniform(3.5, 6.5))
            n_viajes = R.choices([1, 2, 3], weights=[0.05, 0.35, 0.60])[0]
            if posicion[cam] in ISLAS:
                n_viajes = 7                       # en las islas, repartos cortos durante todo el día
            for _ in range(n_viajes):
                orig = posicion[cam]
                dest = destino_para(orig, R)
                km = km_entre(orig, dest)
                dur = timedelta(hours=km / R.uniform(62, 74) + R.uniform(0.3, 0.7))
                if (t + dur).date() != dia or (t + dur).hour >= 23:
                    break
                cli = R.choice(clientes)[0]
                tipo = "congelada" if R.random() < 0.35 else "refrigerada"
                peso = R.randint(800, 21000)
                urg = R.random() < 0.15
                tb = tarifa[tipo]
                franja = 1.10 if 7 <= t.hour < 10 else 1.0      # tarifa dinámica: hora punta de recogida
                importe = round((tb[2] + km * tb[3] + peso * tb[4]) * (1 + tb[5] * urg) * franja * R.uniform(0.95, 1.05), 2)
                n_res += 1; n_env += 1
                id_res, id_env = f"R-{n_res:06d}", f"E-{n_env:06d}"
                f_res = t - timedelta(days=R.randint(1, 10), hours=R.randint(0, 12))
                estado = "completada" if dia < FIN_OPERATIVA else "confirmada"
                reservas.append((id_res, cli, f_res.isoformat(sep=" ", timespec="seconds"), "camion", cam, "",
                                 t.isoformat(sep=" ", timespec="seconds"), (t + dur).isoformat(sep=" ", timespec="seconds"), tipo, estado))
                envios.append((id_env, id_res, cli, orig, dest, tipo, peso, "true" if urg else "false", km,
                               t.isoformat(sep=" ", timespec="seconds"), (t + dur).isoformat(sep=" ", timespec="seconds"), cam, tb[0], importe))
                if estado == "completada" or R.random() < 0.5:
                    n_fac += 1
                    f_fac = (t + dur + timedelta(days=R.randint(0, 3))).date()
                    facturas.append((f"F-{n_fac:06d}", id_res, cli, f_fac.isoformat(), round(importe * 1.21, 2),
                                     "pagada" if R.random() < 0.82 else "pendiente"))
                if dia == DIA_SENSORES:
                    viajes_dia_muestra.setdefault(cam, []).append((t, t + dur, orig, dest, tipo, id_env))
                posicion[cam] = dest
                t = t + dur + timedelta(hours=R.uniform(0.5, 1.25))
    # reservas canceladas (sin envío) y reservas de cámaras
    for _ in range(int(n_env * 0.08)):
        n_res += 1
        dia = inicio_op + timedelta(days=R.randint(0, DIAS_OPERATIVA - 1))
        ini = datetime.combine(dia, datetime.min.time()) + timedelta(hours=R.uniform(5, 18))
        reservas.append((f"R-{n_res:06d}", R.choice(clientes)[0], (ini - timedelta(days=R.randint(2, 15))).isoformat(sep=" ", timespec="seconds"),
                         "camion", R.choice(camiones)[0], "", ini.isoformat(sep=" ", timespec="seconds"),
                         (ini + timedelta(hours=R.uniform(2, 9))).isoformat(sep=" ", timespec="seconds"),
                         R.choice(["refrigerada", "congelada"]), "cancelada"))
    carga_camara = {}                  # cámara -> (inicio, fin, tipo) de la reserva que cubre el día de la muestra
    for cmr in camaras:
        t = datetime.combine(inicio_op, datetime.min.time()) + timedelta(hours=R.uniform(0, 72))
        while t.date() <= FIN_OPERATIVA:
            dur = timedelta(days=R.uniform(3, 14))
            n_res += 1
            id_res = f"R-{n_res:06d}"
            cli = R.choice(clientes)[0]
            fin = t + dur
            if cmr[0] in ("CMR-007", "CMR-018") and t.date() <= DIA_SENSORES <= fin.date():
                if t < datetime.combine(DIA_SENSORES, datetime.min.time()) - timedelta(hours=12):
                    fin = datetime.combine(DIA_SENSORES, datetime.min.time()) - timedelta(hours=3)   # dos cámaras vacías ese día
                else:
                    t = datetime.combine(DIA_SENSORES, datetime.min.time()) + timedelta(days=2)   # su siguiente reserva empieza después
                    fin = t + dur
            cancel = R.random() < 0.06
            estado = "cancelada" if cancel else ("completada" if fin.date() < FIN_OPERATIVA else "confirmada")
            reservas.append((id_res, cli, (t - timedelta(days=R.randint(1, 20))).isoformat(sep=" ", timespec="seconds"), "camara", "", cmr[0],
                             t.isoformat(sep=" ", timespec="seconds"), fin.isoformat(sep=" ", timespec="seconds"), cmr[3], estado))
            if not cancel:
                n_fac += 1
                importe = round((fin - t).total_seconds() / 86400 * cmr[2] * (9.5 if cmr[3] == "congelada" else 7.0) * 1.21, 2)
                facturas.append((f"F-{n_fac:06d}", id_res, cli, (fin.date() + timedelta(days=1)).isoformat(), importe,
                                 "pagada" if R.random() < 0.85 else "pendiente"))
                if t <= datetime.combine(DIA_SENSORES, datetime.max.time()) and fin >= datetime.combine(DIA_SENSORES, datetime.min.time()):
                    carga_camara[cmr[0]] = (t, fin, cmr[3])
            t = fin + timedelta(hours=R.uniform(6, 60))
            if cmr[0] in ("CMR-007", "CMR-018") and t.date() == DIA_SENSORES:
                t = datetime.combine(DIA_SENSORES, datetime.min.time()) + timedelta(days=1, hours=8)
    reservas.sort(key=lambda r: r[0])
    resumen["reservas.csv"] = escribir_csv("reservas.csv", ["id_reserva", "id_cliente", "fecha_reserva", "recurso", "id_camion", "id_camara",
                                                            "inicio", "fin", "tipo_carga", "estado"], reservas)
    resumen["envios.csv"] = escribir_csv("envios.csv", ["id_envio", "id_reserva", "id_cliente", "cod_origen", "cod_destino", "tipo_carga",
                                                        "peso_kg", "urgente", "km", "fecha_recogida", "fecha_entrega", "id_camion",
                                                        "id_tarifa", "importe_eur"], envios)
    resumen["facturas.csv"] = escribir_csv("facturas.csv", ["id_factura", "id_reserva", "id_cliente", "fecha_emision", "importe_eur", "estado"], facturas)

    # ------------------------------------------------------------ AEMET: 50 zonas × 24 h
    obs = []
    for cod in PROV:
        for h in range(24):
            m = datetime.combine(DIA_SENSORES, datetime.min.time()) + timedelta(hours=h)
            obs.append({"idema": f"Z{cod}", "ubi": PROV[cod][1].upper(), "lat": PROV[cod][3], "lon": PROV[cod][4],
                        "fint": m.strftime("%Y-%m-%dT%H:%M:%S"), "ta": round(temp_ext(cod, m) + R.gauss(0, 0.6), 1),
                        "hr": round(min(99, max(35, 75 - 2 * (temp_ext(cod, m) - 10) + R.gauss(0, 6))), 0)})
    with open(f"{SALIDA}/aemet/observaciones_{DIA_SENSORES.isoformat()}.json", "w", encoding="utf-8") as f:
        json.dump(obs, f, ensure_ascii=False)
    resumen["aemet"] = len(obs)

    # ------------------------------------------------------------ sensores: 100 dispositivos × 5.760 lecturas
    inicio = datetime.combine(DIA_SENSORES, datetime.min.time())
    n_lect = 0
    fin_dia = inicio + timedelta(seconds=PASO * 5759)
    ultimos5 = fin_dia - timedelta(minutes=5)
    # anomalías inyectadas: (dispositivo, momento de inicio, tipo)
    anom_puerta = {"CAM-012": inicio + timedelta(hours=11, minutes=20), "CAM-047": ultimos5 + timedelta(seconds=60)}
    anom_subida = {"CAM-033": inicio + timedelta(hours=16, minutes=5), "CAM-005": ultimos5 + timedelta(seconds=90),
                   "CAM-061": ultimos5 + timedelta(seconds=150)}
    anom_camara_noche = {"CMR-004": inicio + timedelta(hours=2, minutes=40), "CMR-011": ultimos5 + timedelta(seconds=30),
                         "CMR-018": ultimos5 + timedelta(seconds=45)}
    # que los camiones de las anomalías de los últimos 5 minutos estén viajando a esa hora
    for cam in ("CAM-047", "CAM-005", "CAM-061"):
        vj = viajes_dia_muestra.setdefault(cam, [])
        if not any(s <= ultimos5 <= l for s, l, *_ in vj):
            orig = vj[-1][3] if vj else base_camion[cam]
            dest = destino_para(orig, R)
            sal = max(fin_dia - timedelta(hours=1, minutes=10), (vj[-1][1] + timedelta(minutes=45)) if vj else fin_dia - timedelta(hours=2))
            n_env += 1
            id_env = f"E-{n_env:06d}"
            vj.append((sal, fin_dia + timedelta(hours=3), orig, dest, "refrigerada", id_env))
            n_res += 1
            id_res = f"R-{n_res:06d}"
            km = km_entre(orig, dest)
            imp = round((60 + km * 1.32 + 9000 * 0.012), 2)
            cli = R.choice(clientes)[0]
            reservas.append((id_res, cli, (sal - timedelta(days=2)).isoformat(sep=" ", timespec="seconds"), "camion", cam, "",
                             sal.isoformat(sep=" ", timespec="seconds"), (fin_dia + timedelta(hours=3)).isoformat(sep=" ", timespec="seconds"),
                             "refrigerada", "confirmada"))
            envios.append((id_env, id_res, cli, orig, dest, "refrigerada", 9000, "false", km, sal.isoformat(sep=" ", timespec="seconds"),
                           (fin_dia + timedelta(hours=3)).isoformat(sep=" ", timespec="seconds"), cam, "T-REF", imp))
    # se reescriben envíos y reservas por si se añadió alguno
    resumen["reservas.csv"] = escribir_csv("reservas.csv", ["id_reserva", "id_cliente", "fecha_reserva", "recurso", "id_camion", "id_camara",
                                                            "inicio", "fin", "tipo_carga", "estado"], sorted(reservas, key=lambda r: r[0]))
    resumen["envios.csv"] = escribir_csv("envios.csv", ["id_envio", "id_reserva", "id_cliente", "cod_origen", "cod_destino", "tipo_carga",
                                                        "peso_kg", "urgente", "km", "fecha_recogida", "fecha_entrega", "id_camion",
                                                        "id_tarifa", "importe_eur"], envios)

    def zona_mas_cercana(lat, lon):
        return min(PROV, key=lambda c: (PROV[c][3] - lat) ** 2 + ((PROV[c][4] - lon) * math.cos(math.radians(lat))) ** 2)

    ruta = f"{SALIDA}/sensores/lecturas_{DIA_SENSORES.isoformat()}.jsonl"
    with open(ruta, "w", encoding="utf-8") as f:
        def emitir(doc):
            nonlocal n_lect
            linea = json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
            f.write(linea + "\n")
            n_lect += 1
            r = R.random()
            if r < 0.004:                                   # reintento del dispositivo: la misma lectura dos veces
                f.write(linea + "\n"); n_lect += 1

        for cam, _, mod, _, _ in camiones:
            sensor = f"S-{cam}"
            fw = next(s[4] for s in sensores if s[0] == sensor)
            mdl = next(m for m in MODELOS if m[0] == mod)
            viajes = sorted(viajes_dia_muestra.get(cam, []))
            temp = 12.0
            lat, lon = PROV[base_camion[cam]][3], PROV[base_camion[cam]][4]
            for k in range(5760):
                ts = inicio + timedelta(seconds=PASO * k)
                activo = next((v for v in viajes if v[0] - timedelta(minutes=20) <= ts <= v[1] + timedelta(minutes=15)), None)
                en_ruta = next((v for v in viajes if v[0] <= ts <= v[1]), None)
                if en_ruta:
                    s, l, o, d, tipo, _ = en_ruta
                    frac = (ts - s) / (l - s)
                    lat = PROV[o][3] + (PROV[d][3] - PROV[o][3]) * frac + R.gauss(0, 0.002)
                    lon = PROV[o][4] + (PROV[d][4] - PROV[o][4]) * frac + R.gauss(0, 0.002)
                    vel = max(0.0, R.gauss(78, 9)) if 0.02 < frac < 0.98 else R.uniform(5, 30)
                elif activo:
                    vel = 0.0
                else:
                    vel = 0.0
                text = temp_ext(zona_mas_cercana(lat, lon), ts) + R.gauss(0, 0.3)
                if activo:
                    lo_, hi_, obj = RANGO[activo[4]]
                    temp += (obj - temp) * 0.08 + R.gauss(0, 0.08)
                    puerta = "abierta" if (not en_ruta and (abs((ts - activo[0]).total_seconds()) < 180 or abs((ts - activo[1]).total_seconds()) < 180)) else "cerrada"
                    if puerta == "abierta":
                        temp += 0.12
                    motor = "en_marcha" if vel > 0 else "ralenti"
                    lh = (mdl[4] if vel > 0 else 2.0) + mdl[5] * max(0.0, text - temp) * 1.0
                else:
                    temp += (text - temp) * 0.004 + R.gauss(0, 0.05)
                    puerta, motor, lh = "cerrada", "apagado", 0.0
                # anomalías
                if cam in anom_puerta and anom_puerta[cam] <= ts < anom_puerta[cam] + timedelta(minutes=2):
                    puerta = "abierta"; temp += 0.25
                if cam in anom_subida and anom_subida[cam] <= ts < anom_subida[cam] + timedelta(seconds=PASO * 3):
                    temp += R.uniform(2.3, 3.2)
                litros = round(lh * PASO / 3600 * R.uniform(0.93, 1.07), 4)
                doc = {"sensor_id": sensor, "tipo_dispositivo": "camion", "id_camion": cam,
                       "ts": ts.strftime("%Y-%m-%dT%H:%M:%S"), "humedad_pct": round(min(98, max(40, R.gauss(85, 4))), 1),
                       "puerta": puerta, "motor": motor, "velocidad_kmh": round(vel, 1), "lat": round(lat, 5), "lon": round(lon, 5),
                       "combustible_l": litros, "fw": fw}
                t_val = round(temp, 2)
                if R.random() < 0.0012:                     # lectura de error del sensor
                    t_val = R.choice([-999.0, 999.0, None])
                if fw == "2.1":
                    doc["temp"] = t_val                       # el firmware antiguo usa otro nombre de campo
                else:
                    doc["temp_int_c"] = t_val
                if R.random() < 0.0002:
                    doc["ts"] = ts.strftime("%d/%m/%Y %H:%M:%S")  # marca de tiempo en otro formato
                emitir(doc)

        for cmr, alm, pal, tipo_frio in camaras:
            sensor = f"S-{cmr}"
            lo_, hi_, obj = RANGO[tipo_frio]
            temp = obj
            reserva = carga_camara.get(cmr)
            for k in range(5760):
                ts = inicio + timedelta(seconds=PASO * k)
                llena = reserva is not None and reserva[0] <= ts <= reserva[1]
                obj_t = obj if llena else obj + 2
                temp += (obj_t - temp) * 0.05 + R.gauss(0, 0.06)
                turno = 6 <= ts.hour < 22
                puerta = "abierta" if (turno and R.random() < 0.006) else "cerrada"
                if cmr in anom_camara_noche and anom_camara_noche[cmr] <= ts < anom_camara_noche[cmr] + timedelta(minutes=2):
                    puerta = "abierta"
                if puerta == "abierta":
                    temp += 0.15
                t_val = round(temp, 2)
                if R.random() < 0.0012:
                    t_val = R.choice([-999.0, 999.0, None])
                doc = {"sensor_id": sensor, "tipo_dispositivo": "camara", "id_camara": cmr, "ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                       "temp_int_c": t_val, "humedad_pct": round(min(98, max(40, R.gauss(80, 5))), 1), "puerta": puerta,
                       "compresor": "on" if temp > obj_t else "off", "fw": "4.0"}
                emitir(doc)
        # sensor que no está dado de alta en el maestro (instalado sin registrar)
        for k in range(0, 240):
            ts = inicio + timedelta(hours=10, seconds=PASO * k)
            emitir({"sensor_id": "S-CAM-099", "tipo_dispositivo": "camion", "id_camion": "CAM-099", "ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                    "temp_int_c": round(R.gauss(5, 0.3), 2), "humedad_pct": 80.0, "puerta": "cerrada", "motor": "en_marcha",
                    "velocidad_kmh": 70.0, "lat": 40.0, "lon": -3.7, "combustible_l": 0.11, "fw": "2.3"})
    resumen["lecturas"] = n_lect

    # ------------------------------------------------------------ logs de la aplicación: 30 días, ~10.000 eventos/día
    rutas_env = {}
    for e in envios:
        if e[9][:10] >= (FIN_OPERATIVA - timedelta(days=DIAS_LOGS - 1)).isoformat():
            rutas_env[(e[3], e[4])] = rutas_env.get((e[3], e[4]), 0) + 1
    rutas = list(rutas_env) + [r for r in RUTAS_FRIAS if r not in rutas_env]
    pesos = [rutas_env.get(r, 1) * RUTAS_FRIAS.get(r, 1.0) for r in rutas]
    usuarios = [(f"U-{i:05d}", c[0]) for i, c in enumerate(clientes * 25, 1)]   # 5.000 usuarios
    n_log = 0
    with open(f"{SALIDA}/logs/eventos_app.jsonl", "w", encoding="utf-8") as f:
        for d in range(DIAS_LOGS):
            dia = FIN_OPERATIVA - timedelta(days=DIAS_LOGS - 1 - d)
            n_sesiones = R.randint(1200, 1350)
            for s in range(n_sesiones):
                usr, cli = R.choice(usuarios)
                app = R.choices(["web", "android", "ios"], weights=[0.55, 0.3, 0.15])[0]
                version = R.choices(["3.4.1", "3.3.0", "3.2.5"], weights=[0.7, 0.2, 0.1])[0]
                t = datetime.combine(dia, datetime.min.time()) + timedelta(seconds=R.randint(6 * 3600, 22 * 3600))
                ses = f"{dia.strftime('%m%d')}-{s:05d}"
                eventos = [("login", {})]
                for _ in range(R.randint(1, 3)):
                    o, dd = R.choices(rutas, weights=pesos)[0]
                    eventos.append(("busqueda_ruta", {"o": o, "d": dd}))
                    if R.random() < 0.8:
                        tipo = "congelada" if R.random() < 0.35 else "refrigerada"
                        peso = R.randint(800, 21000)
                        tb = tarifa[tipo]
                        precio = round((tb[2] + km_entre(o, dd) * tb[3] + peso * tb[4]) * R.uniform(1.0, 1.25), 2)
                        eventos.append(("cotizacion", {"o": o, "d": dd, "tipo_carga": tipo, "peso_kg": peso, "precio_ofrecido": precio}))
                        if R.random() < 0.06 / RUTAS_FRIAS.get((o, dd), 1.0) ** 1.5:
                            eventos.append(("reserva_iniciada", {"o": o, "d": dd}))
                            eventos.append(("pago_completado", {"o": o, "d": dd, "importe": precio}))
                    for _ in range(R.randint(0, 2)):
                        eventos.append(("clic", {"elemento": R.choice(["mapa", "tarifas", "ayuda", "seguimiento", "facturas"])}))
                eventos.append(("logout", {}))
                for ev, datos in eventos:
                    t += timedelta(seconds=R.randint(3, 90))
                    doc = {"ts": t.strftime("%Y-%m-%dT%H:%M:%S"), "evento": ev, "id_usuario": usr, "id_cliente": cli,
                           "id_sesion": ses, "app": app, "version": version}
                    if "o" in datos:
                        if version == "3.2.5":           # la versión antigua envía el nombre escrito, no el código
                            nom_o, nom_d = PROV[datos["o"]][1], PROV[datos["d"]][1]
                            if R.random() < 0.3:
                                nom_o = nom_o.lower()
                            if R.random() < 0.02:
                                nom_d = nom_d[:3] + "."           # abreviatura que no se puede resolver
                            doc["origen_nombre"], doc["destino_nombre"] = nom_o, nom_d
                        else:
                            doc["origen"], doc["destino"] = datos["o"], datos["d"]
                    for kk in ("tipo_carga", "peso_kg", "precio_ofrecido", "importe", "elemento"):
                        if kk in datos:
                            doc[kk] = datos[kk]
                    f.write(json.dumps(doc, ensure_ascii=False, separators=(",", ":")) + "\n")
                    n_log += 1
    resumen["eventos_app"] = n_log

    print(f"Datos generados en ./{SALIDA} con semilla {SEMILLA} en {time.time() - t0:.1f} s")
    total = 0
    for k, v in resumen.items():
        print(f"  {k:22} {v:>9,} filas")
        total += v
    print(f"  {'TOTAL':22} {total:>9,} filas")
    for raiz, _, ficheros in sorted(os.walk(SALIDA)):
        for fi in sorted(ficheros):
            print(f"  {os.path.join(raiz, fi).replace(os.sep, '/'):55} {os.path.getsize(os.path.join(raiz, fi)) / 1e6:8.1f} MB")


if __name__ == "__main__":
    main()
