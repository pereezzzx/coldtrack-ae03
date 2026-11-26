-- =====================================================================================
-- ColdTrack · consultas de la sección 2.4 sobre PostgreSQL (Neon)
-- La fecha de referencia es el último día de la operativa cargada (25/11/2026); en producción, CURRENT_DATE.
-- =====================================================================================
SET search_path TO pablo_perez;

-- Consulta 1 · P1 (detalle): envíos, reservas canceladas y facturas de un cliente en los últimos tres meses
WITH periodo AS (
    SELECT 'CLI-0042'::varchar AS cliente, DATE '2026-11-25' - INTERVAL '3 months' AS desde
),
envios AS (
    SELECT 'envío' AS tipo, e.id_envio AS referencia, e.fecha_recogida AS fecha,
           po.nombre || ' → ' || pd.nombre AS detalle, e.importe_eur, coalesce(f.estado, 'sin factura') AS estado
    FROM periodo p
    INNER JOIN reserva r   ON r.id_cliente = p.cliente
    INNER JOIN envio e     ON e.id_reserva = r.id_reserva AND e.fecha_recogida >= p.desde
    INNER JOIN provincia po ON po.cod_provincia = e.cod_origen
    INNER JOIN provincia pd ON pd.cod_provincia = e.cod_destino
    LEFT JOIN factura f    ON f.id_reserva = r.id_reserva
),
canceladas AS (
    SELECT 'reserva cancelada', r.id_reserva, r.inicio, r.recurso || ' ' || coalesce(r.id_camion, r.id_camara), NULL::numeric, r.estado
    FROM periodo p INNER JOIN reserva r ON r.id_cliente = p.cliente AND r.estado = 'cancelada' AND r.inicio >= p.desde
),
facturas AS (
    SELECT 'factura', f.id_factura, f.fecha_emision::timestamp, 'reserva ' || f.id_reserva, f.importe_eur, f.estado
    FROM periodo p INNER JOIN reserva r ON r.id_cliente = p.cliente
    INNER JOIN factura f ON f.id_reserva = r.id_reserva AND f.fecha_emision >= p.desde
)
SELECT * FROM envios UNION ALL SELECT * FROM canceladas UNION ALL SELECT * FROM facturas
ORDER BY fecha DESC, tipo;

-- Consulta 2 · P2 (resumen): tarifa media por envío según el tipo de carga en el último trimestre,
-- con su peso sobre el total y la diferencia frente a la media global (funciones de ventana)
WITH trimestre AS (
    SELECT r.tipo_carga, e.importe_eur, e.km, e.urgente
    FROM envio e INNER JOIN reserva r ON r.id_reserva = e.id_reserva
    WHERE e.fecha_recogida >= DATE '2026-11-25' - INTERVAL '89 days'
      AND e.fecha_recogida <  DATE '2026-11-26'
)
SELECT tipo_carga,
       count(*)                                                        AS envios,
       round(avg(importe_eur), 2)                                      AS tarifa_media_eur,
       round(avg(importe_eur) FILTER (WHERE urgente), 2)               AS media_urgentes_eur,
       round(avg(importe_eur / km), 3)                                 AS eur_por_km,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)              AS pct_envios,
       round(avg(importe_eur) - sum(sum(importe_eur)) OVER () / sum(count(*)) OVER (), 2) AS dif_vs_media_global_eur
FROM trimestre
GROUP BY tipo_carga
ORDER BY tarifa_media_eur DESC;

-- Comprobación de la regla de no solapamiento (disparador reserva_sin_solape): asignar CAM-001 a una
-- reserva que se solapa con la última suya debe fallar con SQLSTATE 23P01 (exclusion_violation)
INSERT INTO reserva VALUES ('R-PRUEBA', 'CLI-0001', '2026-11-20 08:00', 'camion', 'CAM-001', NULL,
       (SELECT inicio FROM reserva WHERE id_camion = 'CAM-001' AND estado <> 'cancelada' ORDER BY inicio DESC LIMIT 1),
       (SELECT inicio FROM reserva WHERE id_camion = 'CAM-001' AND estado <> 'cancelada' ORDER BY inicio DESC LIMIT 1) + INTERVAL '2 hours',
       'refrigerada', 'confirmada');
