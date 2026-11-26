"""
Convierte los CSV de ../datos/operativa en sql/02_carga_neon.sql (INSERT por lotes, en el orden de las claves ajenas).
El fichero generado no se sube al repositorio: se vuelve a crear con  python sql/generar_carga_neon.py
"""
import csv, os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
ORIGEN = "../datos/operativa"
LOTE = 500
# tabla destino, fichero CSV, columnas que se cargan (las que no están se derivan de otra tabla y no se guardan)
TABLAS = [
    ("provincia", "provincias.csv", ["cod_provincia", "nombre", "comunidad", "lat", "lon"]),
    ("cliente", "clientes.csv", ["id_cliente", "cif", "razon_social", "email", "cod_provincia", "sector", "fecha_alta"]),
    ("modelo_camion", "modelos_camion.csv", ["id_modelo", "marca", "modelo", "capacidad_kg"]),
    ("camion", "camiones.csv", ["id_camion", "matricula", "id_modelo", "cod_provincia_base", "fecha_alta"]),
    ("almacen", "almacenes.csv", ["id_almacen", "nombre", "cod_provincia"]),
    ("camara", "camaras.csv", ["id_camara", "id_almacen", "capacidad_palets", "tipo_frio"]),
    ("sensor", "sensores.csv", ["id_sensor", "tipo_dispositivo", "id_camion", "id_camara", "firmware"]),
    ("tarifa", "tarifas.csv", ["id_tarifa", "tipo_carga", "base_eur", "eur_km", "eur_kg", "recargo_urgente"]),
    ("reserva", "reservas.csv", ["id_reserva", "id_cliente", "fecha_reserva", "recurso", "id_camion", "id_camara", "inicio", "fin", "tipo_carga", "estado"]),
    ("envio", "envios.csv", ["id_envio", "id_reserva", "cod_origen", "cod_destino", "peso_kg", "urgente", "km", "fecha_recogida",
                             "fecha_entrega", "id_tarifa", "importe_eur"]),
    ("factura", "facturas.csv", ["id_factura", "id_reserva", "fecha_emision", "importe_eur", "estado"]),
]


def literal(v):
    if v == "":
        return "NULL"
    return "'" + v.replace("'", "''") + "'"


with open("02_carga_neon.sql", "w", encoding="utf-8") as out:
    out.write("-- Carga generada desde ../datos/operativa (semilla 2026). Se ejecuta después de 01_ddl_neon.sql.\n")
    out.write("SET search_path TO pablo_perez;\nBEGIN;\n")
    for tabla, fichero, cols in TABLAS:
        with open(os.path.join(ORIGEN, fichero), encoding="utf-8") as f:
            filas = list(csv.DictReader(f))
        for i in range(0, len(filas), LOTE):
            valores = ",\n".join("(" + ", ".join(literal(r[c]) for c in cols) + ")" for r in filas[i:i + LOTE])
            out.write(f"INSERT INTO {tabla} ({', '.join(cols)}) VALUES\n{valores};\n")
        print(f"{tabla:15} {len(filas):>7,} filas")
    out.write("COMMIT;\n")
    out.write("""SELECT 'provincia' AS tabla, count(*) AS filas FROM provincia UNION ALL SELECT 'cliente', count(*) FROM cliente
UNION ALL SELECT 'camion', count(*) FROM camion UNION ALL SELECT 'camara', count(*) FROM camara
UNION ALL SELECT 'sensor', count(*) FROM sensor UNION ALL SELECT 'reserva', count(*) FROM reserva
UNION ALL SELECT 'envio', count(*) FROM envio UNION ALL SELECT 'factura', count(*) FROM factura ORDER BY tabla;\n""")
print("escrito 02_carga_neon.sql", round(os.path.getsize("02_carga_neon.sql") / 1e6, 1), "MB")
