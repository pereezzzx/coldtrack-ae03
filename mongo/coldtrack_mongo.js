// =====================================================================================
// ColdTrack · MongoDB Atlas (sección 3 de la memoria) · AE03 grupo 6
// Se ejecuta por bloques en la consola mongosh de Compass, conectada al clúster, después de importar
// lecturas_recientes.json, eventos_app.json y provincias.json en la base coldtrack.
// =====================================================================================
use("coldtrack");
const T = ISODate("2026-11-25T23:59:45Z");            // instante de la consulta: última lectura de la muestra
const HACE_5_MIN = new Date(T.getTime() - 5 * 60 * 1000);

// ---------------------------------------------------------------- 0 · comprobación
printjson({ base: db.getName(), colecciones: db.getCollectionNames().sort(),
            lecturas_recientes: db.lecturas_recientes.countDocuments(), eventos_app: db.eventos_app.countDocuments(),
            provincias: db.provincias.countDocuments() });

// ---------------------------------------------------------------- 1 · validador de la colección principal
db.runCommand({
  collMod: "lecturas_recientes",
  validationLevel: "strict",
  validationAction: "error",
  validator: { $jsonSchema: {
    bsonType: "object",
    required: ["sensor_id", "dispositivo", "inicio", "fin", "n", "lecturas"],
    properties: {
      sensor_id:   { bsonType: "string", pattern: "^S-(CAM|CMR)-[0-9]{3}$" },
      dispositivo: { bsonType: "object", required: ["tipo", "id"],
                     properties: { tipo: { enum: ["camion", "camara"] }, id: { bsonType: "string" } } },
      inicio:      { bsonType: "date" },
      fin:         { bsonType: "date" },
      n:           { bsonType: ["int", "long"], minimum: 1, maximum: 60 },
      carga:       { bsonType: ["object", "null"], required: ["referencia", "tipo_carga", "rango"],
                     properties: { tipo_carga: { enum: ["refrigerada", "congelada"] } } },
      lecturas:    { bsonType: "array", minItems: 1, maxItems: 60,
                     items: { bsonType: "object", required: ["ts", "t", "puerta"],
                              properties: { ts: { bsonType: "date" },
                                            t: { bsonType: ["double", "int"], minimum: -40, maximum: 40 },
                                            puerta: { bsonType: "bool" } } } }
    } } }
});

// un documento con una lectura de error del sensor (999 °C) y sin campo puerta: el validador lo rechaza
try {
  db.lecturas_recientes.insertOne({ sensor_id: "S-CAM-001", dispositivo: { tipo: "camion", id: "CAM-001" },
    inicio: ISODate("2026-11-26T00:00:00Z"), fin: ISODate("2026-11-26T00:14:45Z"), n: 1, carga: null,
    lecturas: [ { ts: ISODate("2026-11-26T00:00:00Z"), t: 999.0 } ] });
} catch (e) {
  print(e.name + ": " + e.message);
  e.errInfo.details.schemaRulesNotSatisfied.forEach(r =>
    (r.propertiesNotSatisfied || []).forEach(p => print(" - " + p.propertyName + ": " + JSON.stringify(p.details).slice(0, 160))));
}

// ---------------------------------------------------------------- 2 · pipeline 1 (P5): alertas con carga activa en los últimos 5 minutos
db.lecturas_recientes.aggregate([
  { $match: { fin: { $gt: HACE_5_MIN }, carga: { $ne: null } } },          // solo los buckets que tocan la ventana y llevan carga
  { $unwind: "$lecturas" },                                                  // una fila por lectura
  { $match: { "lecturas.ts": { $gte: new Date(HACE_5_MIN.getTime() - 15000) } } },
  { $setWindowFields: { partitionBy: "$sensor_id", sortBy: { "lecturas.ts": 1 },
                        output: { t_anterior: { $shift: { output: "$lecturas.t", by: -1 } } } } },
  { $match: { "lecturas.ts": { $gt: HACE_5_MIN } } },
  { $set: { salto: { $subtract: ["$lecturas.t", "$t_anterior"] } } },
  { $set: { alerta: { $switch: { branches: [
      { case: { $and: [ { $gte: ["$salto", 2] }, { $gt: ["$lecturas.t", "$carga.rango.max"] } ] },
        then: "subida brusca de temperatura" },
      { case: { $and: [ "$lecturas.puerta", { $eq: ["$dispositivo.tipo", "camion"] }, { $gt: ["$lecturas.v", 0] } ] },
        then: "puerta abierta en marcha" },
      { case: { $and: [ "$lecturas.puerta", { $eq: ["$dispositivo.tipo", "camara"] },
                        { $or: [ { $gte: [ { $hour: "$lecturas.ts" }, 22 ] }, { $lt: [ { $hour: "$lecturas.ts" }, 6 ] } ] } ] },
        then: "puerta abierta fuera de turno" } ], default: null } } } },
  { $match: { alerta: { $ne: null } } },
  { $group: { _id: { dispositivo: "$dispositivo.id", alerta: "$alerta" },           // una fila por dispositivo y alerta
              tipo: { $first: "$dispositivo.tipo" }, carga: { $first: "$carga.referencia" }, tipo_carga: { $first: "$carga.tipo_carga" },
              desde: { $min: "$lecturas.ts" }, hasta: { $max: "$lecturas.ts" }, lecturas: { $sum: 1 },
              t_max: { $max: "$lecturas.t" }, salto_max: { $max: "$salto" } } },
  { $project: { _id: 0, dispositivo: "$_id.dispositivo", tipo: 1, carga: 1, tipo_carga: 1, alerta: "$_id.alerta",
                desde: { $dateToString: { date: "$desde", format: "%H:%M:%S" } }, hasta: { $dateToString: { date: "$hasta", format: "%H:%M:%S" } },
                lecturas: 1, t_max: 1, salto_max: { $round: ["$salto_max", 2] } } },
  { $sort: { desde: 1, dispositivo: 1 } }
]).forEach(d => print([d.dispositivo, d.tipo, d.carga, d.tipo_carga, d.alerta, d.desde + "-" + d.hasta,
                       d.lecturas + " lect.", "tmax " + d.t_max, "salto " + d.salto_max].join(" | ")));

// ---------------------------------------------------------------- 3 · pipeline 2 (P4 en la app): embudo cotización -> pago por ruta, última semana
db.eventos_app.aggregate([
  { $match: { evento: { $in: ["cotizacion", "pago_completado"] }, ts: { $gte: ISODate("2026-11-19T00:00:00Z") } } },
  { $group: { _id: { o: "$ruta.origen", d: "$ruta.destino" },
              cotizaciones: { $sum: { $cond: [ { $eq: ["$evento", "cotizacion"] }, 1, 0 ] } },
              pagos:        { $sum: { $cond: [ { $eq: ["$evento", "pago_completado"] }, 1, 0 ] } },
              precio_medio: { $avg: "$cotizacion.precio_eur" } } },
  { $match: { cotizaciones: { $gte: 150 } } },                               // solo las rutas que se consultan mucho
  { $set: { conversion_pct: { $round: [ { $multiply: [ { $divide: ["$pagos", "$cotizaciones"] }, 100 ] }, 2 ] } } },
  { $sort: { conversion_pct: 1, cotizaciones: -1 } },
  { $limit: 10 },
  { $lookup: { from: "provincias", localField: "_id.o", foreignField: "_id", as: "po" } },
  { $lookup: { from: "provincias", localField: "_id.d", foreignField: "_id", as: "pd" } },
  { $project: { _id: 0, ruta: { $concat: [ { $first: "$po.nombre" }, " - ", { $first: "$pd.nombre" } ] },
                cotizaciones: 1, pagos: 1, conversion_pct: 1, precio_medio: { $round: ["$precio_medio", 2] } } }
]).forEach(d => print([d.ruta, d.cotizaciones + " cotizaciones", d.pagos + " pagos", d.conversion_pct + " %", d.precio_medio + " EUR"].join(" | ")));

// ---------------------------------------------------------------- 4 · índice para la consulta de P5, con explain antes y después
function resumen(e) {
  const s = e.executionStats; let p = e.queryPlanner.winningPlan.queryPlan || e.queryPlanner.winningPlan, etapas = [];
  while (p) { etapas.push(p.stage + (p.indexName ? " (" + p.indexName + ")" : "")); p = p.inputStage; }
  return { plan: etapas.join(" <- "), nReturned: s.nReturned, totalKeysExamined: s.totalKeysExamined,
           totalDocsExamined: s.totalDocsExamined, ms: s.executionTimeMillis };
}
const filtroP5 = { fin: { $gt: HACE_5_MIN }, carga: { $ne: null } };
db.lecturas_recientes.dropIndexes();
print("ANTES:");
printjson(resumen(db.lecturas_recientes.find(filtroP5).explain("executionStats")));
print("createIndex -> " + db.lecturas_recientes.createIndex({ fin: -1 }, { name: "fin_desc" }));
print("DESPUÉS:");
printjson(resumen(db.lecturas_recientes.find(filtroP5).explain("executionStats")));
