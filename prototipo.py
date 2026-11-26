"""
ColdTrack · prototipo del lago de datos (AE03, grupo 6).

Construye el lago desde cero con los datos de ./datos (generar_datos.py):
  bronze  -> copia sin tocar de cada fichero, en una carpeta con su fecha de carga
  silver  -> esquema declarado, reglas de calidad medidas, rechazos apartados con su motivo;
             lecturas y eventos en Delta Lake, operativa y maestros en Parquet
  gold    -> una tabla por pregunta de negocio
y responde a las cinco preguntas. Todo lo que imprime se guarda también en salida_prototipo.txt.

Uso:  python prototipo.py
"""
import decimal, duckdb, glob, json, os, shutil, sys, time
from deltalake import write_deltalake, DeltaTable

os.chdir(os.path.dirname(os.path.abspath(__file__)))   # se ejecuta siempre desde la carpeta del proyecto
DATOS, LAGO = "datos", "lago"
FECHA_CARGA = "2026-11-26"            # día en que llega la exportación (bronze se organiza por esta fecha)
DIA_SENSORES = "2026-11-25"
FIN_OPERATIVA = "2026-11-25"
CLIENTE_P1 = "CLI-0042"


class Tee:
    def __init__(self, *flujos): self.flujos = flujos
    def write(self, s):
        for f in self.flujos: f.write(s); f.flush()
    def flush(self):
        for f in self.flujos: f.flush()
    def fileno(self): return self.flujos[-1].fileno()


_log = open("salida_prototipo.txt", "w", encoding="utf-8")
_stdout_original = sys.stdout
sys.stdout = Tee(_stdout_original, _log)
sys.stderr = Tee(*[f for f in (sys.stderr, _log) if f is not None])   # los errores también quedan en el fichero
import faulthandler
faulthandler.enable(_log)
con = duckdb.connect()
T0 = time.time()
TIEMPOS = []


def titulo(t):
    print("\n" + "=" * 100 + f"\n{t}\n" + "=" * 100)


def tabla(sql, max_filas=20):
    """Ejecuta una consulta e imprime el resultado como tabla de texto (con el número total de filas)."""
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    filas = cur.fetchall()
    fmt = lambda v: fmt(float(v)) if isinstance(v, decimal.Decimal) else f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if isinstance(v, float) else ("" if v is None else str(v))
    vis = [[fmt(v) for v in f] for f in filas[:max_filas]]
    anchos = [max([len(c)] + [len(r[i]) for r in vis]) for i, c in enumerate(cols)]
    linea = "+-" + "-+-".join("-" * a for a in anchos) + "-+"
    print(linea)
    print("| " + " | ".join(c.ljust(a) for c, a in zip(cols, anchos)) + " |")
    print(linea)
    for r in vis:
        print("| " + " | ".join(v.rjust(a) if v.replace(",", "").replace(".", "").replace("-", "").isdigit() else v.ljust(a)
                               for v, a in zip(r, anchos)) + " |")
    print(linea)
    print(f"({len(filas)} fila{'s' if len(filas) != 1 else ''}{', se muestran ' + str(max_filas) if len(filas) > max_filas else ''})")
    return filas


def medir(nombre):
    class _M:
        def __enter__(s): s.t = time.time(); return s
        def __exit__(s, *a):
            TIEMPOS.append((nombre, time.time() - s.t)); print(f"[{nombre}: {time.time() - s.t:.2f} s]")
    return _M()


def a_arrow(rel):
    for m in ("to_arrow_table", "fetch_arrow_table", "arrow"):
        if hasattr(rel, m):
            r = getattr(rel, m)()
            return r.read_all() if hasattr(r, "read_all") else r
    raise RuntimeError("versión de duckdb sin exportación a Arrow")


def tam(ruta):
    return sum(os.path.getsize(f) for f in glob.glob(ruta + "/**/*", recursive=True) if os.path.isfile(f)) if os.path.isdir(ruta) else os.path.getsize(ruta)


DELTA_EXT = False
try:
    con.execute("INSTALL delta"); con.execute("LOAD delta")
    DELTA_EXT = True
except Exception:
    pass


def vista_delta(nombre, ruta):
    """Expone una tabla Delta como vista: con la extensión delta de DuckDB o, si no está, a través de deltalake."""
    if DELTA_EXT:
        con.execute(f"CREATE OR REPLACE VIEW {nombre} AS SELECT * FROM delta_scan('{ruta}')")
    else:
        con.register(nombre, DeltaTable(ruta).to_pyarrow_dataset())


# =====================================================================================================
titulo("0 · Entorno y lago vacío")
import deltalake
print("python", sys.version.split()[0], "| duckdb", duckdb.__version__, "| deltalake", deltalake.__version__,
      "| extensión delta de DuckDB:", "sí" if DELTA_EXT else "no (se lee con deltalake)")
if not os.path.isdir(DATOS):
    sys.exit("No existe ./datos: ejecuta antes  python generar_datos.py")
shutil.rmtree(LAGO, ignore_errors=True)
print("lago/ borrado: se construye desde cero")

# =====================================================================================================
titulo("1 · BRONZE: cada fichero tal como llega, con su fecha de carga (no se toca)")
with medir("bronze"):
    fuentes = {"operativa": "operativa/*.csv", "sensores": "sensores/*.jsonl", "logs_app": "logs/*.jsonl", "aemet": "aemet/*.json"}
    for fuente, patron in fuentes.items():
        destino = f"{LAGO}/bronze/{fuente}/fecha_carga={FECHA_CARGA}"
        os.makedirs(destino, exist_ok=True)
        for f in sorted(glob.glob(f"{DATOS}/{patron}")):
            shutil.copy(f, destino)
    BR = {k: f"{LAGO}/bronze/{k}/fecha_carga={FECHA_CARGA}" for k in fuentes}
    con.execute(f"""CREATE TABLE inventario_bronze AS
        SELECT 'sensores' AS fuente, 'lecturas_{DIA_SENSORES}.jsonl' AS fichero, count(*) AS filas FROM read_json('{BR['sensores']}/*.jsonl', format='newline_delimited', columns={{sensor_id:'VARCHAR'}})
        UNION ALL SELECT 'logs_app', 'eventos_app.jsonl', count(*) FROM read_json('{BR['logs_app']}/*.jsonl', format='newline_delimited', columns={{evento:'VARCHAR'}})
        UNION ALL SELECT 'aemet', 'observaciones_{DIA_SENSORES}.json', count(*) FROM read_json('{BR['aemet']}/*.json', columns={{idema:'VARCHAR'}})""")
    for f in sorted(glob.glob(f"{BR['operativa']}/*.csv")):
        n = con.execute(f"SELECT count(*) FROM read_csv('{f}', all_varchar=true)").fetchone()[0]
        con.execute(f"INSERT INTO inventario_bronze VALUES ('operativa', '{os.path.basename(f)}', {n})")
    con.execute("CREATE TABLE tam_bronze(fichero VARCHAR, mb DOUBLE)")
    for f in glob.glob(f"{LAGO}/bronze/**/*.*", recursive=True):
        con.execute(f"INSERT INTO tam_bronze VALUES ('{os.path.basename(f)}', {os.path.getsize(f) / 1e6})")
    tabla("""SELECT i.fuente, i.fichero, i.filas, round(t.mb, 2) AS mb FROM inventario_bronze i JOIN tam_bronze t USING (fichero)
             ORDER BY i.fuente, i.fichero""", 30)
    tabla("SELECT sum(filas) AS filas_en_bronze, round(sum(mb), 1) AS mb FROM inventario_bronze JOIN tam_bronze USING (fichero)")

# =====================================================================================================
titulo("2 · SILVER: esquema declarado, reglas de calidad y rechazos con su motivo")
for _d in ("silver/rechazos", "silver/operativa", "gold"):
    os.makedirs(f"{LAGO}/{_d}", exist_ok=True)
CALIDAD = []          # (tabla, regla, filas_afectadas, acción)

with medir("silver · maestros y operativa"):
    TIPOS = {
        "provincias": {"cod_provincia": "VARCHAR", "nombre": "VARCHAR", "comunidad": "VARCHAR", "lat": "DOUBLE", "lon": "DOUBLE"},
        "modelos_camion": {"id_modelo": "VARCHAR", "marca": "VARCHAR", "modelo": "VARCHAR", "capacidad_kg": "INTEGER"},
        "camiones": {"id_camion": "VARCHAR", "matricula": "VARCHAR", "id_modelo": "VARCHAR", "cod_provincia_base": "VARCHAR", "fecha_alta": "DATE"},
        "almacenes": {"id_almacen": "VARCHAR", "nombre": "VARCHAR", "cod_provincia": "VARCHAR"},
        "camaras": {"id_camara": "VARCHAR", "id_almacen": "VARCHAR", "capacidad_palets": "INTEGER", "tipo_frio": "VARCHAR"},
        "sensores": {"id_sensor": "VARCHAR", "tipo_dispositivo": "VARCHAR", "id_camion": "VARCHAR", "id_camara": "VARCHAR", "firmware": "VARCHAR"},
        "tarifas": {"id_tarifa": "VARCHAR", "tipo_carga": "VARCHAR", "base_eur": "DECIMAL(8,2)", "eur_km": "DECIMAL(6,3)", "eur_kg": "DECIMAL(6,4)", "recargo_urgente": "DECIMAL(4,2)"},
        "clientes": {"id_cliente": "VARCHAR", "cif": "VARCHAR", "razon_social": "VARCHAR", "email": "VARCHAR", "cod_provincia": "VARCHAR", "sector": "VARCHAR", "fecha_alta": "DATE"},
        "envios": {"id_envio": "VARCHAR", "id_reserva": "VARCHAR", "id_cliente": "VARCHAR", "cod_origen": "VARCHAR", "cod_destino": "VARCHAR",
                   "tipo_carga": "VARCHAR", "peso_kg": "INTEGER", "urgente": "BOOLEAN", "km": "DOUBLE", "fecha_recogida": "TIMESTAMP",
                   "fecha_entrega": "TIMESTAMP", "id_camion": "VARCHAR", "id_tarifa": "VARCHAR", "importe_eur": "DECIMAL(10,2)"},
        "reservas": {"id_reserva": "VARCHAR", "id_cliente": "VARCHAR", "fecha_reserva": "TIMESTAMP", "recurso": "VARCHAR", "id_camion": "VARCHAR",
                     "id_camara": "VARCHAR", "inicio": "TIMESTAMP", "fin": "TIMESTAMP", "tipo_carga": "VARCHAR", "estado": "VARCHAR"},
        "facturas": {"id_factura": "VARCHAR", "id_reserva": "VARCHAR", "id_cliente": "VARCHAR", "fecha_emision": "DATE", "importe_eur": "DECIMAL(10,2)", "estado": "VARCHAR"},
    }
    for t, cols in TIPOS.items():
        tipos = ", ".join(f"'{c}': '{ty}'" for c, ty in cols.items())
        con.execute(f"CREATE TABLE s_{t} AS SELECT * FROM read_csv('{BR['operativa']}/{t}.csv', header=true, columns={{{tipos}}}, nullstr='')")
    # reglas de la operativa: la exportación viene de una base ACID, pero se comprueba que los códigos casan
    reglas_op = [
        ("envios", "envío cuya reserva no existe", "SELECT count(*) FROM s_envios e LEFT JOIN s_reservas r USING (id_reserva) WHERE r.id_reserva IS NULL"),
        ("envios", "fecha de entrega anterior a la de recogida", "SELECT count(*) FROM s_envios WHERE fecha_entrega < fecha_recogida"),
        ("envios", "provincia de origen o destino desconocida", "SELECT count(*) FROM s_envios WHERE cod_origen NOT IN (SELECT cod_provincia FROM s_provincias) OR cod_destino NOT IN (SELECT cod_provincia FROM s_provincias)"),
        ("facturas", "factura de una reserva cancelada", "SELECT count(*) FROM s_facturas f JOIN s_reservas r USING (id_reserva) WHERE r.estado = 'cancelada'"),
        ("reservas", "id_reserva repetido", "SELECT count(*) - count(DISTINCT id_reserva) FROM s_reservas"),
    ]
    for t, regla, q in reglas_op:
        CALIDAD.append((t, regla, con.execute(q).fetchone()[0], "comprobación (0 = la operativa es coherente)"))
    for t in TIPOS:
        con.execute(f"COPY s_{t} TO '{LAGO}/silver/operativa/{t}.parquet' (FORMAT parquet)")

with medir("silver · lecturas de sensores"):
    con.execute(f"""CREATE TABLE lect_bronze AS SELECT * FROM read_json('{BR['sensores']}/*.jsonl', format='newline_delimited',
        columns={{sensor_id:'VARCHAR', tipo_dispositivo:'VARCHAR', id_camion:'VARCHAR', id_camara:'VARCHAR', ts:'VARCHAR',
                 temp_int_c:'DOUBLE', temp:'DOUBLE', humedad_pct:'DOUBLE', puerta:'VARCHAR', motor:'VARCHAR', compresor:'VARCHAR',
                 velocidad_kmh:'DOUBLE', lat:'DOUBLE', lon:'DOUBLE', combustible_l:'DOUBLE', fw:'VARCHAR'}})""")
    n_bronze = con.execute("SELECT count(*) FROM lect_bronze").fetchone()[0]
    con.execute("""CREATE TABLE lect_tipada AS
        SELECT *, try_strptime(ts, '%Y-%m-%dT%H:%M:%S') AS ts_ok, coalesce(temp_int_c, temp) AS t_int,
               row_number() OVER (PARTITION BY sensor_id, ts ORDER BY sensor_id) AS copia
        FROM lect_bronze""")
    con.execute("""CREATE TABLE lect_rechazos AS
        SELECT *, CASE WHEN ts_ok IS NULL THEN 'R1 marca de tiempo con otro formato'
                       WHEN sensor_id NOT IN (SELECT id_sensor FROM s_sensores) THEN 'R2 sensor no registrado en el maestro'
                       WHEN t_int IS NULL OR t_int NOT BETWEEN -40 AND 40 THEN 'R3 temperatura nula o fuera del rango físico (-40..40 °C)'
                  END AS motivo
        FROM lect_tipada WHERE copia = 1 AND (ts_ok IS NULL OR sensor_id NOT IN (SELECT id_sensor FROM s_sensores)
                                              OR t_int IS NULL OR t_int NOT BETWEEN -40 AND 40)""")
    duplicados = con.execute("SELECT count(*) FROM lect_tipada WHERE copia > 1").fetchone()[0]
    con.execute(f"""CREATE TABLE s_lecturas AS
        SELECT sensor_id, tipo_dispositivo, coalesce(id_camion, id_camara) AS id_dispositivo, ts_ok AS ts, CAST(ts_ok AS DATE) AS fecha,
               round(t_int, 2) AS temp_int_c, humedad_pct, puerta = 'abierta' AS puerta_abierta, motor, compresor,
               velocidad_kmh, lat, lon, combustible_l, fw, DATE '{FECHA_CARGA}' AS fecha_carga
        FROM lect_tipada
        WHERE copia = 1 AND ts_ok IS NOT NULL AND sensor_id IN (SELECT id_sensor FROM s_sensores) AND t_int BETWEEN -40 AND 40
        ORDER BY sensor_id, ts""")
    CALIDAD.append(("lecturas", "R0 lectura duplicada (mismo sensor y misma marca de tiempo)", duplicados, "se conserva una"))
    for motivo, n in con.execute("SELECT motivo, count(*) FROM lect_rechazos GROUP BY 1 ORDER BY 1").fetchall():
        CALIDAD.append(("lecturas", motivo, n, "a rechazos"))
    con.execute(f"COPY (SELECT * EXCLUDE (ts_ok, copia) FROM lect_rechazos) TO '{LAGO}/silver/rechazos/lecturas.parquet' (FORMAT parquet)")
    write_deltalake(f"{LAGO}/silver/lecturas", a_arrow(con.sql("SELECT * FROM s_lecturas")), mode="overwrite",
                    partition_by=["fecha", "tipo_dispositivo"])
    vista_delta("silver_lecturas", f"{LAGO}/silver/lecturas")
    n_silver = con.execute("SELECT count(*) FROM silver_lecturas").fetchone()[0]
    print(f"lecturas: {n_bronze:,} en bronze -> {n_silver:,} en silver ({n_bronze - n_silver:,} fuera: duplicados y rechazos)")
    json_mb = os.path.getsize(glob.glob(f"{BR['sensores']}/*.jsonl")[0]) / 1e6
    delta_mb = tam(f"{LAGO}/silver/lecturas") / 1e6
    print(f"tamaño de un día de lecturas: JSON {json_mb:.1f} MB -> Delta/Parquet {delta_mb:.1f} MB (x{json_mb / delta_mb:.1f} menos)")
    print(f"a escala del supuesto (3,45 GB/día de JSON): ~{3.45 / (json_mb / delta_mb):.2f} GB/día en Parquet, "
          f"~{3.45 / (json_mb / delta_mb) * 365 / 1000:.2f} TB/año")
    RATIO_PARQUET = json_mb / delta_mb

with medir("silver · eventos de la aplicación"):
    con.execute(f"""CREATE TABLE ev_bronze AS SELECT * FROM read_json('{BR['logs_app']}/*.jsonl', format='newline_delimited',
        columns={{ts:'VARCHAR', evento:'VARCHAR', id_usuario:'VARCHAR', id_cliente:'VARCHAR', id_sesion:'VARCHAR', app:'VARCHAR',
                 version:'VARCHAR', origen:'VARCHAR', destino:'VARCHAR', origen_nombre:'VARCHAR', destino_nombre:'VARCHAR',
                 tipo_carga:'VARCHAR', peso_kg:'INTEGER', precio_ofrecido:'DOUBLE', importe:'DOUBLE', elemento:'VARCHAR'}})""")
    con.execute("""CREATE TABLE ev_resuelto AS
        SELECT e.*, strptime(e.ts, '%Y-%m-%dT%H:%M:%S') AS ts_ok,
               coalesce(e.origen, po.cod_provincia) AS cod_origen, coalesce(e.destino, pd.cod_provincia) AS cod_destino,
               e.evento IN ('busqueda_ruta', 'cotizacion', 'reserva_iniciada', 'pago_completado') AS es_de_ruta
        FROM ev_bronze e
        LEFT JOIN s_provincias po ON lower(strip_accents(po.nombre)) = lower(strip_accents(e.origen_nombre))
        LEFT JOIN s_provincias pd ON lower(strip_accents(pd.nombre)) = lower(strip_accents(e.destino_nombre))""")
    normalizados = con.execute("SELECT count(*) FROM ev_resuelto WHERE origen_nombre IS NOT NULL AND cod_origen IS NOT NULL AND cod_destino IS NOT NULL").fetchone()[0]
    con.execute("""CREATE TABLE ev_rechazos AS SELECT *, 'R4 evento de ruta con una provincia que no se puede resolver' AS motivo
               FROM ev_resuelto WHERE es_de_ruta AND (cod_origen IS NULL OR cod_destino IS NULL)""")
    CALIDAD.append(("eventos_app", "R5 provincia escrita por la app 3.2.5 (nombre, con o sin tildes) traducida a su código", normalizados, "se corrige"))
    CALIDAD.append(("eventos_app", "R4 evento de ruta con una provincia que no se puede resolver", con.execute("SELECT count(*) FROM ev_rechazos").fetchone()[0], "a rechazos"))
    con.execute(f"COPY ev_rechazos TO '{LAGO}/silver/rechazos/eventos_app.parquet' (FORMAT parquet)")
    rel = con.sql("""SELECT ts_ok AS ts, CAST(ts_ok AS DATE) AS fecha, evento, id_usuario, id_cliente, id_sesion, app, version,
                            cod_origen, cod_destino, tipo_carga, peso_kg, precio_ofrecido, importe, elemento
                     FROM ev_resuelto WHERE NOT (es_de_ruta AND (cod_origen IS NULL OR cod_destino IS NULL)) ORDER BY ts""")
    write_deltalake(f"{LAGO}/silver/eventos_app", a_arrow(rel), mode="overwrite", partition_by=["fecha"])
    vista_delta("silver_eventos", f"{LAGO}/silver/eventos_app")
    print(f"eventos: {con.execute('SELECT count(*) FROM ev_bronze').fetchone()[0]:,} en bronze -> {con.execute('SELECT count(*) FROM silver_eventos').fetchone()[0]:,} en silver")

with medir("silver · temperatura exterior (AEMET)"):
    con.execute(f"""CREATE TABLE s_aemet AS
        SELECT substr(idema, 2) AS cod_zona, ubi AS zona, lat, lon, strptime(fint, '%Y-%m-%dT%H:%M:%S') AS hora, ta AS temp_ext_c, hr AS humedad_ext
        FROM read_json('{BR['aemet']}/*.json', columns={{idema:'VARCHAR', ubi:'VARCHAR', lat:'DOUBLE', lon:'DOUBLE', fint:'VARCHAR', ta:'DOUBLE', hr:'DOUBLE'}})""")
    CALIDAD.append(("aemet", "observación sin temperatura", con.execute("SELECT count(*) FROM s_aemet WHERE temp_ext_c IS NULL").fetchone()[0], "comprobación"))
    con.execute(f"COPY s_aemet TO '{LAGO}/silver/aemet.parquet' (FORMAT parquet)")

titulo("2b · Informe de calidad de silver")
con.execute("CREATE TABLE calidad(tabla VARCHAR, regla VARCHAR, filas BIGINT, accion VARCHAR)")
con.executemany("INSERT INTO calidad VALUES (?, ?, ?, ?)", CALIDAD)
tabla("SELECT tabla, regla, filas, accion FROM calidad ORDER BY tabla, regla", 30)
con.execute(f"COPY calidad TO '{LAGO}/silver/rechazos/informe_calidad.csv' (FORMAT csv, HEADER)")

# =====================================================================================================
titulo("3 · GOLD: una tabla por pregunta")
os.makedirs(f"{LAGO}/gold", exist_ok=True)
for t in TIPOS:
    con.execute(f"CREATE OR REPLACE VIEW silver_{t} AS SELECT * FROM '{LAGO}/silver/operativa/{t}.parquet'")

with medir("gold · P2 tarifa media por tipo de carga"):
    con.execute(f"""COPY (
        SELECT tipo_carga, count(*) AS envios, round(avg(importe_eur), 2) AS tarifa_media_eur,
               round(median(importe_eur), 2) AS tarifa_mediana_eur, round(avg(importe_eur / km), 3) AS eur_por_km_medio,
               DATE '{FIN_OPERATIVA}' - INTERVAL 89 DAY AS desde, DATE '{FIN_OPERATIVA}' AS hasta
        FROM silver_envios
        WHERE fecha_recogida >= DATE '{FIN_OPERATIVA}' - INTERVAL 89 DAY AND fecha_recogida < DATE '{FIN_OPERATIVA}' + INTERVAL 1 DAY
        GROUP BY tipo_carga ORDER BY tipo_carga) TO '{LAGO}/gold/p2_tarifa_media_tipo_carga.parquet' (FORMAT parquet)""")

with medir("gold · P3 consumo por modelo y temperatura exterior"):
    # cada lectura de camión se asigna a la estación AEMET más cercana a su posición y a la observación de su hora
    con.execute(f"""COPY (
        WITH lect AS (
            SELECT l.id_dispositivo AS id_camion, l.ts, l.lat, l.lon, l.combustible_l
            FROM silver_lecturas l WHERE l.tipo_dispositivo = 'camion' AND l.motor = 'en_marcha'),
        estaciones AS (SELECT DISTINCT cod_zona, lat, lon FROM s_aemet),
        zona AS (
            SELECT l.*, arg_min(e.cod_zona, (e.lat - l.lat) ^ 2 + ((e.lon - l.lon) * cos(radians(l.lat))) ^ 2) AS cod_zona
            FROM lect l CROSS JOIN estaciones e GROUP BY ALL),
        con_temp AS (
            SELECT z.*, a.temp_ext_c,
                   CASE WHEN a.temp_ext_c < 10 THEN '1 · < 10 °C' WHEN a.temp_ext_c <= 25 THEN '2 · 10-25 °C' ELSE '3 · > 25 °C' END AS tramo
            FROM zona z JOIN s_aemet a ON a.cod_zona = z.cod_zona AND a.hora = date_trunc('hour', z.ts))
        SELECT m.marca || ' ' || m.modelo AS modelo, c.tramo, count(*) AS lecturas, round(count(*) * 15 / 3600.0, 1) AS horas,
               round(sum(c.combustible_l), 1) AS litros, round(sum(c.combustible_l) / (count(*) * 15 / 3600.0), 2) AS litros_hora,
               round(avg(c.temp_ext_c), 1) AS temp_ext_media
        FROM con_temp c JOIN silver_camiones k ON k.id_camion = c.id_camion JOIN silver_modelos_camion m USING (id_modelo)
        GROUP BY ALL ORDER BY modelo, tramo) TO '{LAGO}/gold/p3_consumo_modelo_tramo.parquet' (FORMAT parquet)""")

with medir("gold · P4 conversión por ruta"):
    con.execute(f"""COPY (
        WITH cot AS (
            SELECT cod_origen, cod_destino, count(*) AS cotizaciones FROM silver_eventos
            WHERE evento = 'cotizacion' AND ts >= DATE '{FIN_OPERATIVA}' - INTERVAL 29 DAY GROUP BY ALL),
        env AS (
            SELECT cod_origen, cod_destino, count(*) AS envios FROM silver_envios
            WHERE fecha_recogida >= DATE '{FIN_OPERATIVA}' - INTERVAL 29 DAY AND fecha_recogida < DATE '{FIN_OPERATIVA}' + INTERVAL 1 DAY
            GROUP BY ALL)
        SELECT po.nombre || ' - ' || pd.nombre AS ruta, c.cod_origen, c.cod_destino, c.cotizaciones, coalesce(e.envios, 0) AS envios,
               round(100.0 * coalesce(e.envios, 0) / c.cotizaciones, 2) AS conversion_pct
        FROM cot c LEFT JOIN env e USING (cod_origen, cod_destino)
        JOIN silver_provincias po ON po.cod_provincia = c.cod_origen JOIN silver_provincias pd ON pd.cod_provincia = c.cod_destino
        ORDER BY c.cotizaciones DESC) TO '{LAGO}/gold/p4_conversion_ruta.parquet' (FORMAT parquet)""")

with medir("gold · P5 alertas de carga"):
    con.execute(f"""COPY (
        WITH l AS (
            SELECT sensor_id, tipo_dispositivo, id_dispositivo, ts, temp_int_c, puerta_abierta, velocidad_kmh,
                   temp_int_c - lag(temp_int_c) OVER (PARTITION BY sensor_id ORDER BY ts) AS salto_c
            FROM silver_lecturas),
        carga AS (                       -- envío en curso (camión) o reserva en vigor (cámara) en el instante de la lectura
            SELECT l.*, coalesce(e.id_envio, r.id_reserva) AS referencia, coalesce(e.tipo_carga, r.tipo_carga) AS tipo_carga
            FROM l
            LEFT JOIN silver_envios e ON l.tipo_dispositivo = 'camion' AND e.id_camion = l.id_dispositivo
                                      AND l.ts BETWEEN e.fecha_recogida AND e.fecha_entrega
            LEFT JOIN silver_reservas r ON l.tipo_dispositivo = 'camara' AND r.id_camara = l.id_dispositivo AND r.estado <> 'cancelada'
                                      AND l.ts BETWEEN r.inicio AND r.fin)
        SELECT ts, tipo_dispositivo, id_dispositivo, referencia, tipo_carga, temp_int_c, round(salto_c, 2) AS salto_c, velocidad_kmh,
               CASE WHEN salto_c >= 2 AND ((tipo_carga = 'refrigerada' AND temp_int_c > 8) OR (tipo_carga = 'congelada' AND temp_int_c > -18))
                         THEN 'subida brusca de temperatura'
                    WHEN puerta_abierta AND tipo_dispositivo = 'camion' AND velocidad_kmh > 0 THEN 'puerta abierta en marcha'
                    WHEN puerta_abierta AND tipo_dispositivo = 'camara' AND (hour(ts) >= 22 OR hour(ts) < 6) THEN 'puerta abierta fuera de turno'
               END AS alerta,
               referencia IS NOT NULL AS carga_activa
        FROM carga
        WHERE (salto_c >= 2 AND ((tipo_carga = 'refrigerada' AND temp_int_c > 8) OR (tipo_carga = 'congelada' AND temp_int_c > -18)))
           OR (puerta_abierta AND tipo_dispositivo = 'camion' AND velocidad_kmh > 0)
           OR (puerta_abierta AND tipo_dispositivo = 'camara' AND (hour(ts) >= 22 OR hour(ts) < 6))
        ORDER BY ts) TO '{LAGO}/gold/p5_alertas_carga.parquet' (FORMAT parquet)""")

for f in sorted(glob.glob(f"{LAGO}/gold/*.parquet")):
    ruta = f.replace(os.sep, "/")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{ruta}')").fetchone()[0]
    print(f"{ruta:55} {n:>6} filas  {os.path.getsize(f) / 1e3:7.1f} KB")

# catálogo: nombres de tabla en lugar de rutas
if os.path.exists(f"{LAGO}/catalogo.duckdb"):
    os.remove(f"{LAGO}/catalogo.duckdb")
con.execute(f"ATTACH '{LAGO}/catalogo.duckdb' AS coldtrack")
for t in TIPOS:
    con.execute(f"CREATE VIEW coldtrack.silver_{t} AS SELECT * FROM '{LAGO}/silver/operativa/{t}.parquet'")
con.execute(f"CREATE VIEW coldtrack.silver_aemet AS SELECT * FROM '{LAGO}/silver/aemet.parquet'")
if DELTA_EXT:
    con.execute(f"CREATE VIEW coldtrack.silver_lecturas AS SELECT * FROM delta_scan('{LAGO}/silver/lecturas')")
    con.execute(f"CREATE VIEW coldtrack.silver_eventos_app AS SELECT * FROM delta_scan('{LAGO}/silver/eventos_app')")
for f in sorted(glob.glob(f"{LAGO}/gold/*.parquet")):
    con.execute(f"CREATE VIEW coldtrack.gold_{os.path.basename(f)[:-8]} AS SELECT * FROM '{f.replace(os.sep, '/')}'")
print(f"\ncatálogo {LAGO}/catalogo.duckdb:", ", ".join(r[0] for r in con.execute(
    "SELECT view_name FROM duckdb_views() WHERE database_name = 'coldtrack' ORDER BY 1").fetchall()))
con.execute("DETACH coldtrack")

# =====================================================================================================
titulo(f"P1 · Detalle: envíos, reservas canceladas y facturas de {CLIENTE_P1} en los últimos tres meses")
with medir("P1"):
    desde = f"DATE '{FIN_OPERATIVA}' - INTERVAL 3 MONTH"
    tabla(f"""SELECT 'envíos realizados' AS concepto, count(*) AS numero, sum(importe_eur) AS importe_eur FROM silver_envios
              WHERE id_cliente = '{CLIENTE_P1}' AND fecha_recogida >= {desde}
              UNION ALL SELECT 'reservas canceladas', count(*), NULL FROM silver_reservas
              WHERE id_cliente = '{CLIENTE_P1}' AND estado = 'cancelada' AND inicio >= {desde}
              UNION ALL SELECT 'facturas (' || estado || ')', count(*), sum(importe_eur) FROM silver_facturas
              WHERE id_cliente = '{CLIENTE_P1}' AND fecha_emision >= {desde} GROUP BY estado
              ORDER BY concepto""")
    tabla(f"""SELECT e.id_envio, e.fecha_recogida, po.nombre AS origen, pd.nombre AS destino, e.tipo_carga, e.importe_eur, f.id_factura, f.estado
              FROM silver_envios e JOIN silver_provincias po ON po.cod_provincia = e.cod_origen
              JOIN silver_provincias pd ON pd.cod_provincia = e.cod_destino
              LEFT JOIN silver_facturas f USING (id_reserva)
              WHERE e.id_cliente = '{CLIENTE_P1}' AND e.fecha_recogida >= {desde}
              ORDER BY e.fecha_recogida DESC""", 8)

titulo("P2 · Resumen: tarifa media por envío según el tipo de carga (último trimestre)")
with medir("P2"):
    tabla(f"SELECT * FROM '{LAGO}/gold/p2_tarifa_media_tipo_carga.parquet' ORDER BY tipo_carga")

titulo("P3 · Resumen: litros por hora de cada modelo de camión según la temperatura exterior")
with medir("P3"):
    tabla(f"SELECT * FROM '{LAGO}/gold/p3_consumo_modelo_tramo.parquet' ORDER BY modelo, tramo")

titulo("P4 · Cruce: las 10 rutas más cotizadas con menor conversión (último mes)")
with medir("P4"):
    tabla(f"""WITH top AS (SELECT * FROM '{LAGO}/gold/p4_conversion_ruta.parquet' ORDER BY cotizaciones DESC LIMIT 50)
              SELECT ruta, cotizaciones, envios, conversion_pct FROM top ORDER BY conversion_pct, cotizaciones DESC LIMIT 10""")
    tabla(f"""SELECT count(*) AS rutas, sum(cotizaciones) AS cotizaciones, sum(envios) AS envios,
                     round(100.0 * sum(envios) / sum(cotizaciones), 2) AS conversion_global_pct
              FROM '{LAGO}/gold/p4_conversion_ruta.parquet'""")

titulo("P5 · Segundos: camiones y cámaras con carga activa y una alerta en los últimos 5 minutos")
fin_muestra = con.execute("SELECT max(ts) FROM silver_lecturas").fetchone()[0]
print("instante de la consulta (última lectura de la muestra):", fin_muestra)
t = time.time()
tabla(f"""SELECT id_dispositivo, tipo_dispositivo AS tipo, referencia AS carga, tipo_carga, alerta, strftime(min(ts), '%H:%M:%S') AS desde,
                 strftime(max(ts), '%H:%M:%S') AS hasta, count(*) AS lecturas, max(temp_int_c) AS temp_max_c, max(salto_c) AS salto_max_c
          FROM '{LAGO}/gold/p5_alertas_carga.parquet'
          WHERE carga_activa AND ts > TIMESTAMP '{fin_muestra}' - INTERVAL 5 MINUTE
          GROUP BY ALL ORDER BY desde, id_dispositivo""")
print(f"consulta sobre gold: {(time.time() - t) * 1000:.0f} ms")
tabla(f"""SELECT alerta, count(*) FILTER (WHERE carga_activa) AS con_carga, count(*) FILTER (WHERE NOT carga_activa) AS sin_carga_descartadas
          FROM '{LAGO}/gold/p5_alertas_carga.parquet' GROUP BY alerta ORDER BY alerta""")
t = time.time()
n = con.execute(f"""SELECT count(*) FROM silver_lecturas WHERE ts > TIMESTAMP '{fin_muestra}' - INTERVAL 5 MINUTE""").fetchone()[0]
print(f"lecturas de los últimos 5 minutos en silver: {n} (100 sensores, una lectura cada 15 s, menos los rechazos), leídas en {(time.time() - t) * 1000:.0f} ms")

# =====================================================================================================
titulo("Resumen de la ejecución")
for nombre, s in TIEMPOS:
    print(f"  {nombre:50} {s:6.2f} s")
print(f"  {'TOTAL':50} {time.time() - T0:6.2f} s")
print(f"\n  {'carpeta del lago':42} {'ficheros':>8} {'MB':>9}")
for capa in ("bronze", "silver", "gold"):
    for sub in sorted(os.listdir(f"{LAGO}/{capa}")):
        ruta = f"{LAGO}/{capa}/{sub}"
        n = sum(1 for f in glob.glob(ruta + "/**/*", recursive=True) if os.path.isfile(f) and "_delta_log" not in f) if os.path.isdir(ruta) else 1
        print(f"  {capa + '/' + sub:42} {n:8} {tam(ruta) / 1e6:9.1f}")
print("\nSalida guardada en salida_prototipo.txt")
sys.stdout = _stdout_original
_log.close()
