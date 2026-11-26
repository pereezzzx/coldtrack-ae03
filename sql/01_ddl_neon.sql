-- =====================================================================================
-- ColdTrack · modelo operativo relacional (PostgreSQL en Neon) · AE03 grupo 6
-- Tercera forma normal. Se ejecuta entero (Alt+X en DBeaver); borra y recrea las tablas.
-- =====================================================================================
-- En el Neon de la asignatura cada alumno solo puede crear objetos en su esquema personal
-- (no hay permiso para crear esquemas ni extensiones). Cambia pablo_perez por el tuyo.
SET search_path TO pablo_perez;
DROP TABLE IF EXISTS factura, envio, reserva, tarifa, sensor, camara, almacen, camion, modelo_camion, cliente, provincia CASCADE;
DROP FUNCTION IF EXISTS reserva_sin_solape() CASCADE;

CREATE TABLE provincia (
    cod_provincia   CHAR(2)      PRIMARY KEY CHECK (cod_provincia ~ '^[0-9]{2}$'),   -- código INE
    nombre          VARCHAR(40)  NOT NULL UNIQUE,
    comunidad       VARCHAR(40)  NOT NULL,
    lat             NUMERIC(7,4) NOT NULL CHECK (lat BETWEEN 27 AND 44),
    lon             NUMERIC(7,4) NOT NULL CHECK (lon BETWEEN -19 AND 5)
);

CREATE TABLE cliente (
    id_cliente      VARCHAR(10)  PRIMARY KEY,
    cif             CHAR(9)      NOT NULL UNIQUE,
    razon_social    VARCHAR(80)  NOT NULL,
    email           VARCHAR(80)  NOT NULL UNIQUE CHECK (email LIKE '%_@_%'),
    cod_provincia   CHAR(2)      NOT NULL REFERENCES provincia,
    sector          VARCHAR(40)  NOT NULL,
    fecha_alta      DATE         NOT NULL
);

CREATE TABLE modelo_camion (
    id_modelo       VARCHAR(4)   PRIMARY KEY,
    marca           VARCHAR(30)  NOT NULL,
    modelo          VARCHAR(40)  NOT NULL,
    capacidad_kg    INTEGER      NOT NULL CHECK (capacidad_kg > 0),
    UNIQUE (marca, modelo)
);

CREATE TABLE camion (
    id_camion           VARCHAR(8)  PRIMARY KEY,
    matricula           VARCHAR(8)  NOT NULL UNIQUE,
    id_modelo           VARCHAR(4)  NOT NULL REFERENCES modelo_camion,
    cod_provincia_base  CHAR(2)     NOT NULL REFERENCES provincia,
    fecha_alta          DATE        NOT NULL
);

CREATE TABLE almacen (
    id_almacen      VARCHAR(4)   PRIMARY KEY,
    nombre          VARCHAR(60)  NOT NULL UNIQUE,
    cod_provincia   CHAR(2)      NOT NULL REFERENCES provincia
);

CREATE TABLE camara (
    id_camara         VARCHAR(8)  PRIMARY KEY,
    id_almacen        VARCHAR(4)  NOT NULL REFERENCES almacen,
    capacidad_palets  SMALLINT    NOT NULL CHECK (capacidad_palets > 0),
    tipo_frio         VARCHAR(12) NOT NULL CHECK (tipo_frio IN ('refrigerada', 'congelada'))
);

CREATE TABLE sensor (
    id_sensor         VARCHAR(10) PRIMARY KEY,
    tipo_dispositivo  VARCHAR(6)  NOT NULL CHECK (tipo_dispositivo IN ('camion', 'camara')),
    id_camion         VARCHAR(8)  UNIQUE REFERENCES camion,
    id_camara         VARCHAR(8)  UNIQUE REFERENCES camara,
    firmware          VARCHAR(8)  NOT NULL,
    -- un sensor va en un camión o en una cámara, nunca en los dos ni en ninguno
    CHECK ((tipo_dispositivo = 'camion' AND id_camion IS NOT NULL AND id_camara IS NULL)
        OR (tipo_dispositivo = 'camara' AND id_camara IS NOT NULL AND id_camion IS NULL))
);

CREATE TABLE tarifa (
    id_tarifa        VARCHAR(6)   PRIMARY KEY,
    tipo_carga       VARCHAR(12)  NOT NULL UNIQUE CHECK (tipo_carga IN ('refrigerada', 'congelada')),
    base_eur         NUMERIC(8,2) NOT NULL CHECK (base_eur >= 0),
    eur_km           NUMERIC(6,3) NOT NULL CHECK (eur_km > 0),
    eur_kg           NUMERIC(6,4) NOT NULL CHECK (eur_kg >= 0),
    recargo_urgente  NUMERIC(4,2) NOT NULL CHECK (recargo_urgente BETWEEN 0 AND 1)
);

CREATE TABLE reserva (
    id_reserva     VARCHAR(10) PRIMARY KEY,
    id_cliente     VARCHAR(10) NOT NULL REFERENCES cliente,
    fecha_reserva  TIMESTAMP   NOT NULL,
    recurso        VARCHAR(6)  NOT NULL CHECK (recurso IN ('camion', 'camara')),
    id_camion      VARCHAR(8)  REFERENCES camion,
    id_camara      VARCHAR(8)  REFERENCES camara,
    inicio         TIMESTAMP   NOT NULL,
    fin            TIMESTAMP   NOT NULL,
    tipo_carga     VARCHAR(12) NOT NULL CHECK (tipo_carga IN ('refrigerada', 'congelada')),
    estado         VARCHAR(10) NOT NULL DEFAULT 'confirmada' CHECK (estado IN ('confirmada', 'completada', 'cancelada')),
    CHECK (fin > inicio),
    CHECK (fecha_reserva <= inicio),
    CHECK ((recurso = 'camion' AND id_camion IS NOT NULL AND id_camara IS NULL)
        OR (recurso = 'camara' AND id_camara IS NOT NULL AND id_camion IS NULL))
);
CREATE INDEX ix_reserva_camion ON reserva (id_camion, inicio);
CREATE INDEX ix_reserva_camara ON reserva (id_camara, inicio);

-- El mismo camión o la misma cámara no se pueden asignar a dos reservas activas que se solapan en el tiempo.
-- Lo natural sería EXCLUDE USING gist (id_camion WITH =, tsrange(inicio, fin) WITH &&), pero necesita la
-- extensión btree_gist y en el Neon de clase no podemos instalar extensiones: la regla se hace con un disparador.
CREATE FUNCTION reserva_sin_solape() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.estado <> 'cancelada' AND EXISTS (
        SELECT 1 FROM reserva r
        WHERE r.id_reserva <> NEW.id_reserva
          AND r.estado <> 'cancelada'
          AND (r.id_camion = NEW.id_camion OR r.id_camara = NEW.id_camara)
          AND tsrange(r.inicio, r.fin) && tsrange(NEW.inicio, NEW.fin)) THEN
        RAISE EXCEPTION 'el recurso % ya tiene una reserva que se solapa con [%, %)',
            coalesce(NEW.id_camion, NEW.id_camara), NEW.inicio, NEW.fin
            USING ERRCODE = 'exclusion_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER trg_reserva_sin_solape BEFORE INSERT OR UPDATE ON reserva
    FOR EACH ROW EXECUTE FUNCTION reserva_sin_solape();

CREATE TABLE envio (
    id_envio        VARCHAR(10)   PRIMARY KEY,
    id_reserva      VARCHAR(10)   NOT NULL UNIQUE REFERENCES reserva,      -- cliente, camión y tipo de carga se leen de la reserva
    cod_origen      CHAR(2)       NOT NULL REFERENCES provincia,
    cod_destino     CHAR(2)       NOT NULL REFERENCES provincia,
    peso_kg         INTEGER       NOT NULL CHECK (peso_kg BETWEEN 1 AND 26000),
    urgente         BOOLEAN       NOT NULL DEFAULT false,
    km              NUMERIC(7,1)  NOT NULL CHECK (km > 0),
    fecha_recogida  TIMESTAMP     NOT NULL,
    fecha_entrega   TIMESTAMP     NOT NULL,
    id_tarifa       VARCHAR(6)    NOT NULL REFERENCES tarifa,
    importe_eur     NUMERIC(10,2) NOT NULL CHECK (importe_eur > 0),
    CHECK (fecha_entrega > fecha_recogida)
);

CREATE TABLE factura (
    id_factura     VARCHAR(10)   PRIMARY KEY,
    id_reserva     VARCHAR(10)   NOT NULL UNIQUE REFERENCES reserva,      -- una factura por reserva
    fecha_emision  DATE          NOT NULL,
    importe_eur    NUMERIC(10,2) NOT NULL CHECK (importe_eur > 0),
    estado         VARCHAR(10)   NOT NULL CHECK (estado IN ('pagada', 'pendiente'))
);

-- índices para las consultas de la sección 2.4 (las claves ajenas no se indexan solas en PostgreSQL)
CREATE INDEX ix_reserva_cliente ON reserva (id_cliente, inicio);
CREATE INDEX ix_envio_recogida  ON envio (fecha_recogida);
CREATE INDEX ix_factura_fecha   ON factura (fecha_emision);
