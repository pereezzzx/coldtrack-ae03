# ColdTrack · plataforma de datos de la cadena de frío

Proyecto final (AE03) de **Base de Datos y Big Data** (UIE, curso 2026-27) · Grupo 6.

ColdTrack es una empresa (supuesta) de transporte y almacenamiento refrigerado con 1.500 camiones y 500 cámaras
monitorizados por sensores IoT. Este repositorio contiene el **prototipo** del lago de datos (bronze → silver → gold)
que responde a las cinco preguntas de negocio de la memoria, y los scripts del modelo relacional (PostgreSQL en Neon)
y del modelo de documentos (MongoDB Atlas).

## Requisitos

- Python 3.10 o superior (probado con 3.13)
- Paquetes: `duckdb`, `deltalake` y `pyarrow`

```bash
pip install -r requirements.txt
```

## Ejecución desde un lago vacío

```bash
python generar_datos.py     # ~20 s: crea ./datos (~210 MB) con semilla 2026
python prototipo.py         # ~3 s: borra ./lago, lo construye y responde a P1-P5
```

En Windows, `EJECUTAR.bat` hace todo con un doble clic (instala los paquetes, genera los datos si no existen y ejecuta el prototipo).

`prototipo.py` imprime cada paso con sus filas y tiempos y deja la misma salida en `salida_prototipo.txt`.
Los datos y el lago no se suben al repositorio: se regeneran con los dos comandos anteriores y salen siempre iguales.

## Qué hay en cada fichero

| Fichero | Qué hace |
|---|---|
| `generar_datos.py` | Genera los datos sintéticos con semilla fija: 578.000 lecturas de sensores (100 dispositivos × 24 h), 90 días de operativa (envíos, reservas, facturas), 300.000 eventos de la app y la temperatura exterior de 50 zonas. Incluye suciedad a propósito |
| `prototipo.py` | Bronze (copia sin tocar con fecha de carga), silver (esquema declarado, 6 reglas de calidad, rechazos con motivo; Delta Lake y Parquet), gold (una tabla por pregunta), catálogo DuckDB y respuesta a P1-P5 |
| `sql/01_ddl_neon.sql` | Modelo relacional en 3FN para PostgreSQL (Neon): claves, CHECK, UNIQUE y un disparador que impide reservas solapadas del mismo camión o cámara. Se crea en el esquema personal de cada alumno (`SET search_path TO pablo_perez;`, cámbialo por el tuyo) |
| `sql/generar_carga_neon.py` | Convierte la operativa generada en `sql/02_carga_neon.sql` (INSERT por lotes) |
| `sql/03_consultas_neon.sql` | Las dos consultas de la sección 2.4 (P1 y P2) y la prueba de la regla de no solapamiento |
| `mongo/generar_mongo.py` | Prepara los documentos para Atlas: lecturas recientes en buckets de 15 min y eventos de la app |
| `mongo/coldtrack_mongo.js` | Validador `$jsonSchema`, dos pipelines de agregación (P5 y P4) e índice con `explain`. Se ejecuta por bloques en mongosh tras importar los JSON de `mongo/` en la base `coldtrack` |
| `EJECUTAR.bat` / `ejecutar_todo.py` | Atajos para ejecutar todo en Windows o desde IDLE |

## Estructura del lago

```
lago/
  bronze/<fuente>/fecha_carga=2026-11-26/     ficheros tal como llegan (CSV, JSON por líneas, JSON)
  silver/lecturas/        Delta Lake, particionada por fecha y tipo de dispositivo
  silver/eventos_app/     Delta Lake, particionada por fecha
  silver/operativa/       Parquet (envíos, reservas, facturas, maestros)
  silver/rechazos/        filas apartadas con su motivo e informe de calidad
  gold/                   p2_tarifa_media_tipo_carga, p3_consumo_modelo_tramo, p4_conversion_ruta, p5_alertas_carga
  catalogo.duckdb         vistas con nombre sobre silver y gold
```

## Las cinco preguntas

| | Pregunta | Tipo | Fuentes |
|---|---|---|---|
| P1 | ¿Qué envíos, reservas canceladas y facturas ha tenido un cliente concreto en los últimos tres meses? | Detalle | Operativa |
| P2 | ¿Cuál es la tarifa media por envío según el tipo de carga en el último trimestre? | Resumen | Operativa |
| P3 | Para cada modelo de camión, ¿cuántos litros por hora consume según la temperatura exterior (<10, 10-25, >25 °C)? | Resumen | Sensores + AEMET + maestro |
| P4 | ¿Qué 10 rutas se cotizan mucho y tienen menor conversión (envíos / cotizaciones) en el último mes? | Cruce | Logs + operativa |
| P5 | ¿Qué camiones o cámaras con carga activa han tenido en los últimos 5 minutos una subida brusca de temperatura o una apertura no autorizada? | Segundos | Sensores + operativa |

## Equipo

Adriana González Nespereira · Hugo Deaño Sobrino · Pablo Pérez Domínguez · Emma Suárez Galiñanes
