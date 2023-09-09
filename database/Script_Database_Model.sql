-------------------------------------------------------------
-------------------------------------------------------------
---------------------- CREATE DATABASE ----------------------
-------------------------------------------------------------
-------------------------------------------------------------

--- CREATE DATABASE
CREATE DATABASE UBER_ANALISIS
GO


-------------------------------------------------------------
-------------------------------------------------------------
----------------------- CREATE TABLES -----------------------
-------------------------------------------------------------
-------------------------------------------------------------

--- USE DATABASE
USE UBER_ANALISIS
GO

--- FINAL TABLE
CREATE SEQUENCE SEQ_PAGO_VIAJE  
AS INTEGER   
START WITH 1   
INCREMENT BY 1   
GO

CREATE TABLE FACT_PAGO_VIAJE (
  ID_VIAJE INTEGER DEFAULT (NEXT VALUE FOR SEQ_PAGO_VIAJE),
  ID_PROVEEDOR INTEGER,
  ID_FCH INTEGER,
  ID_TARIFA INTEGER,
  ID_LOCACION_RECOGIDA INTEGER,
  ID_LOCACION_DESCENSO INTEGER,
  ID_TIPO_PAGO INTEGER,
  NUM_PASAJEROS INTEGER,
  DISTANCIA_VIAJE FLOAT,
  FLG_TIENDA_AVANCE VARCHAR(5),
  MONTO_TARIFA FLOAT,
  EXTRA FLOAT,
  IMP_MTA FLOAT,
  MONTO_PROPINA FLOAT,
  MONTO_PEAJE FLOAT,
  RECARGO_MEJORA FLOAT,
  MONTO_TOTAL FLOAT,
  FCH_CARGA DATE
)
GO

CREATE NONCLUSTERED INDEX IDX_VIAJE
ON FACT_PAGO_VIAJE (ID_VIAJE)
GO

CREATE NONCLUSTERED INDEX IDX_PROVEEDOR
ON FACT_PAGO_VIAJE (ID_PROVEEDOR)
GO

CREATE NONCLUSTERED INDEX IDX_FCH
ON FACT_PAGO_VIAJE (ID_FCH)
GO

CREATE NONCLUSTERED INDEX IDX_TARIFA
ON FACT_PAGO_VIAJE (ID_TARIFA)
GO

CREATE NONCLUSTERED INDEX IDX_LOCACION_RECOGIDA
ON FACT_PAGO_VIAJE (ID_LOCACION_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_LOCACION_DESCENSO
ON FACT_PAGO_VIAJE (ID_LOCACION_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_TIPO_PAGO
ON FACT_PAGO_VIAJE (ID_TIPO_PAGO)
GO

--- IMAGE TABLE
CREATE TABLE IM_FACT_PAGO_VIAJE (
  ID_PROVEEDOR INTEGER,
  ID_FCH INTEGER,
  ID_TARIFA INTEGER,
  ID_LOCACION_RECOGIDA INTEGER,
  ID_LOCACION_DESCENSO INTEGER,
  ID_TIPO_PAGO INTEGER,
  NUM_PASAJEROS INTEGER,
  DISTANCIA_VIAJE FLOAT,
  FLG_TIENDA_AVANCE VARCHAR(5),
  MONTO_TARIFA FLOAT,
  EXTRA FLOAT,
  IMP_MTA FLOAT,
  MONTO_PROPINA FLOAT,
  MONTO_PEAJE FLOAT,
  RECARGO_MEJORA FLOAT,
  MONTO_TOTAL FLOAT
)
GO

CREATE NONCLUSTERED INDEX IDX_IM_PROVEEDOR
ON IM_FACT_PAGO_VIAJE (ID_PROVEEDOR)
GO

CREATE NONCLUSTERED INDEX IDX_IM_FCH
ON IM_FACT_PAGO_VIAJE (ID_FCH)
GO

CREATE NONCLUSTERED INDEX IDX_IM_TARIFA
ON IM_FACT_PAGO_VIAJE (ID_TARIFA)
GO

CREATE NONCLUSTERED INDEX IDX_IM_LOCACION_RECOGIDA
ON IM_FACT_PAGO_VIAJE (ID_LOCACION_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_IM_LOCACION_DESCENSO
ON IM_FACT_PAGO_VIAJE (ID_LOCACION_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_IM_TIPO_PAGO
ON IM_FACT_PAGO_VIAJE (ID_TIPO_PAGO)
GO

--- TEMP TABLE
CREATE TABLE TMP_FACT_PAGO_VIAJE (
  ID_PROVEEDOR INTEGER,
  FCH_HRA_RECOGIDA VARCHAR(30),
  FCH_HRA_DESCENSO VARCHAR(30),
  ID_TARIFA INTEGER,
  LATITUD_RECOGIDA FLOAT,
  LONGITUD_RECOGIDA FLOAT,
  LATITUD_DESCENSO FLOAT,
  LONGITUD_DESCENSO FLOAT,
  ID_TIPO_PAGO INTEGER,
  NUM_PASAJEROS INTEGER,
  DISTANCIA_VIAJE FLOAT,
  FLG_TIENDA_AVANCE VARCHAR(5),
  MONTO_TARIFA FLOAT,
  EXTRA FLOAT,
  IMP_MTA FLOAT,
  MONTO_PROPINA FLOAT,
  MONTO_PEAJE FLOAT,
  RECARGO_MEJORA FLOAT,
  MONTO_TOTAL FLOAT
)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_PROVEEDOR
ON TMP_FACT_PAGO_VIAJE (ID_PROVEEDOR)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_FCH_RECOGIDA
ON TMP_FACT_PAGO_VIAJE (FCH_HRA_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_FCH_DESCENSO
ON TMP_FACT_PAGO_VIAJE (FCH_HRA_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_TARIFA
ON TMP_FACT_PAGO_VIAJE (ID_TARIFA)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_LATITUD_RECOGIDA
ON TMP_FACT_PAGO_VIAJE (LATITUD_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_LONGITUD_RECOGIDA
ON TMP_FACT_PAGO_VIAJE (LONGITUD_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_LATITUD_DESCENSO
ON TMP_FACT_PAGO_VIAJE (LATITUD_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_LONGITUD_DESCENSO
ON TMP_FACT_PAGO_VIAJE (LONGITUD_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_TMP_TIPO_PAGO
ON TMP_FACT_PAGO_VIAJE (ID_TIPO_PAGO)
GO

--- FINAL TABLE
CREATE SEQUENCE SEQ_FCH  
AS INTEGER   
START WITH 1   
INCREMENT BY 1   
GO

CREATE TABLE DIM_FCH (
  ID_FCH INTEGER DEFAULT (NEXT VALUE FOR SEQ_FCH),
  FCH_HRA_RECOGIDA VARCHAR(30),
  FCH_RECOGIDA DATE,
  ANIO_RECOGIDA INTEGER,
  MES_RECOGIDA INTEGER,
  DIA_RECOGIDA INTEGER,
  DIA_SEMANA_RECOGIDA INTEGER,
  HRA_HRA_RECOGIDA VARCHAR(30),
  HRA_RECOGIDA INTEGER,
  MIN_RECOGIDA INTEGER,
  FCH_HRA_DESCENSO VARCHAR(30),
  FCH_DESCENSO DATE,
  ANIO_DESCENSO INTEGER,
  MES_DESCENSO INTEGER,
  DIA_DESCENSO INTEGER,
  DIA_SEMANA_DESCENSO INTEGER,
  HRA_HRA_DESCENSO VARCHAR(30),
  HRA_DESCENSO INTEGER,
  MIN_DESCENSO INTEGER,
  DURACION_VIAJE VARCHAR(30),
  FCH_CARGA DATE
)
GO

CREATE NONCLUSTERED INDEX IDX_FCH
ON DIM_FCH (ID_FCH)
GO

CREATE NONCLUSTERED INDEX IDX_FCH_HRA_RECOGIDA
ON DIM_FCH (FCH_HRA_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_FCH_RECOGIDA
ON DIM_FCH (FCH_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_FCH_HRA_DESCENSO
ON DIM_FCH (FCH_HRA_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_FCH_DESCENSO
ON DIM_FCH (FCH_DESCENSO)
GO

--- IMAGE TABLE
CREATE TABLE IM_DIM_FCH (
  FCH_HRA_RECOGIDA VARCHAR(30),
  FCH_HRA_DESCENSO VARCHAR(30)
)
GO

CREATE NONCLUSTERED INDEX IDX_IM_FCH_HRA_RECOGIDA
ON IM_DIM_FCH (FCH_HRA_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_IM_FCH_HRA_DESCENSO
ON IM_DIM_FCH (FCH_HRA_DESCENSO)
GO


--- FINAL TABLE
CREATE SEQUENCE SEQ_LOCACION_RECOGIDA  
AS INTEGER   
START WITH 1   
INCREMENT BY 1   
GO

CREATE TABLE CAT_LOCACION_RECOGIDA (
  ID_LOCACION_RECOGIDA INTEGER DEFAULT (NEXT VALUE FOR SEQ_LOCACION_RECOGIDA),
  LATITUD_RECOGIDA FLOAT,
  LONGITUD_RECOGIDA FLOAT,
  FCH_CARGA DATE
)
GO

CREATE NONCLUSTERED INDEX IDX_ID_LOCACION_RECOGIDA
ON CAT_LOCACION_RECOGIDA (ID_LOCACION_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_LATITUD_RECOGIDA
ON CAT_LOCACION_RECOGIDA (LATITUD_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_LONGITUD_RECOGIDA
ON CAT_LOCACION_RECOGIDA (LONGITUD_RECOGIDA)
GO


--- IMAGE TABLE
CREATE TABLE IM_CAT_LOCACION_RECOGIDA (
  LATITUD_RECOGIDA FLOAT,
  LONGITUD_RECOGIDA FLOAT
)
GO

CREATE NONCLUSTERED INDEX IDX_IM_LATITUD_RECOGIDA
ON IM_CAT_LOCACION_RECOGIDA (LATITUD_RECOGIDA)
GO

CREATE NONCLUSTERED INDEX IDX_IM_LONGITUD_RECOGIDA
ON IM_CAT_LOCACION_RECOGIDA (LONGITUD_RECOGIDA)
GO

--- FINAL TABLE
CREATE SEQUENCE SEQ_LOCACION_DESCENSO  
AS INTEGER   
START WITH 1   
INCREMENT BY 1   
GO

CREATE TABLE CAT_LOCACION_DESCENSO (
  ID_LOCACION_DESCENSO INTEGER DEFAULT (NEXT VALUE FOR SEQ_LOCACION_DESCENSO),
  LATITUD_DESCENSO FLOAT,
  LONGITUD_DESCENSO FLOAT,
  FCH_CARGA DATE
)
GO

CREATE NONCLUSTERED INDEX IDX_ID_LOCACION_DESCENSO
ON CAT_LOCACION_DESCENSO (ID_LOCACION_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_LATITUD_DESCENSO
ON CAT_LOCACION_DESCENSO (LATITUD_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_LONGITUD_DESCENSO
ON CAT_LOCACION_DESCENSO (LONGITUD_DESCENSO)
GO

--- IMAGE TABLE
CREATE TABLE IM_CAT_LOCACION_DESCENSO (
  LATITUD_DESCENSO FLOAT,
  LONGITUD_DESCENSO FLOAT
)
GO

CREATE NONCLUSTERED INDEX IDX_IM_LATITUD_DESCENSO
ON IM_CAT_LOCACION_DESCENSO (LATITUD_DESCENSO)
GO

CREATE NONCLUSTERED INDEX IDX_IM_LONGITUD_DESCENSO
ON IM_CAT_LOCACION_DESCENSO (LONGITUD_DESCENSO)
GO

--- FINAL TABLE
CREATE TABLE CAT_PROVEEDOR (
  ID_PROVEEDOR INTEGER,
  PROVEEDOR VARCHAR(50),
  FCH_CARGA DATE
)
GO

CREATE NONCLUSTERED INDEX IDX_ID_PROVEEDOR
ON CAT_PROVEEDOR (ID_PROVEEDOR)
GO

--- IMAGE TABLE
CREATE TABLE IM_CAT_PROVEEDOR (
  ID_PROVEEDOR INTEGER,
  PROVEEDOR VARCHAR(50)
)
GO

CREATE NONCLUSTERED INDEX IDX_IM_ID_PROVEEDOR
ON IM_CAT_PROVEEDOR (ID_PROVEEDOR)
GO

--- FINAL TABLE
CREATE TABLE CAT_TARIFA (
  ID_TARIFA INTEGER,
  TARIFA VARCHAR(50),
  FCH_CARGA DATE
)
GO

CREATE NONCLUSTERED INDEX IDX_ID_TARIFA
ON CAT_TARIFA (ID_TARIFA)
GO

--- IMAGE TABLE
CREATE TABLE IM_CAT_TARIFA (
  ID_TARIFA INTEGER,
  TARIFA VARCHAR(50)
)
GO

CREATE NONCLUSTERED INDEX IDX_IM_ID_TARIFA
ON IM_CAT_TARIFA (ID_TARIFA)
GO

--- FINAL TABLE
CREATE TABLE CAT_TIPO_PAGO (
  ID_TIPO_PAGO INTEGER,
  TIPO_PAGO VARCHAR(50),
  FCH_CARGA DATE
)
GO

CREATE NONCLUSTERED INDEX IDX_ID_TIPO_PAGO
ON CAT_TIPO_PAGO (ID_TIPO_PAGO)
GO

--- IMAGE TABLE
CREATE TABLE IM_CAT_TIPO_PAGO (
  ID_TIPO_PAGO INTEGER,
  TIPO_PAGO VARCHAR(50)
)
GO

CREATE NONCLUSTERED INDEX IDX_IM_ID_TIPO_PAGO
ON IM_CAT_TIPO_PAGO (ID_TIPO_PAGO)
GO

--- FINAL TABLE
CREATE TABLE DIM_LOG_CARGA (
  FCH_LOG VARCHAR(30),
  ID_PROCESO INTEGER,
  NOM_ARCHIVO VARCHAR(100),
  REGISTROS_NOM_ARCHIVO INTEGER,
  NOM_TABLA VARCHAR(100),
  REGISTROS_INSERTADOS_TABLA INTEGER,
  COMENTARIO VARCHAR(150)
)
GO

CREATE NONCLUSTERED INDEX IDX_FCH_LOG
ON DIM_LOG_CARGA (FCH_LOG)
GO

-------------------------------------------------------------
-------------------------------------------------------------
------------------------ CREATE VIEWS -----------------------
-------------------------------------------------------------
-------------------------------------------------------------

CREATE VIEW VCAT_LOCACION_DESCENSO
AS 
SELECT *
FROM   dbo.CAT_LOCACION_DESCENSO
GO

CREATE VIEW VCAT_LOCACION_RECOGIDA
AS 
SELECT *
FROM   dbo.CAT_LOCACION_RECOGIDA
GO

CREATE VIEW VCAT_PROVEEDOR
AS 
SELECT *
FROM   dbo.CAT_PROVEEDOR
GO

CREATE VIEW VCAT_TARIFA
AS 
SELECT *
FROM   dbo.CAT_TARIFA
GO

CREATE VIEW VCAT_TIPO_PAGO
AS 
SELECT *
FROM   dbo.CAT_TIPO_PAGO
GO

CREATE VIEW VDIM_FCH
AS 
SELECT *
FROM   dbo.DIM_FCH
GO

CREATE VIEW VFACT_PAGO_VIAJE
AS 
SELECT *
FROM   dbo.FACT_PAGO_VIAJE
GO

CREATE VIEW VDIM_LOG_CARGA
AS 
SELECT *
FROM   dbo.DIM_LOG_CARGA
GO


-------------------------------------------------------------
-------------------------------------------------------------
---------------------- RESET SEQUENCES ----------------------
-------------------------------------------------------------
-------------------------------------------------------------

ALTER SEQUENCE SEQ_FCH
RESTART WITH 1
GO

ALTER SEQUENCE SEQ_LOCACION_DESCENSO
RESTART WITH 1
GO

ALTER SEQUENCE SEQ_LOCACION_RECOGIDA
RESTART WITH 1
GO

ALTER SEQUENCE SEQ_PAGO_VIAJE
RESTART WITH 1
GO


-------------------------------------------------------------
-------------------------------------------------------------
---------------------- TRUNCATE TABLES ----------------------
-------------------------------------------------------------
-------------------------------------------------------------

TRUNCATE TABLE CAT_LOCACION_DESCENSO
GO

TRUNCATE TABLE CAT_LOCACION_RECOGIDA
GO

TRUNCATE TABLE CAT_PROVEEDOR
GO

TRUNCATE TABLE CAT_TARIFA
GO

TRUNCATE TABLE CAT_TIPO_PAGO
GO

TRUNCATE TABLE DIM_FCH
GO

TRUNCATE TABLE FACT_PAGO_VIAJE
GO

TRUNCATE TABLE DIM_LOG_CARGA
GO


-------------------------------------------------------------
-------------------------------------------------------------
----------------------- SELECT VIEWS ------------------------
-------------------------------------------------------------
-------------------------------------------------------------

SELECT * 
FROM VCAT_TARIFA;

SELECT * 
FROM VCAT_TIPO_PAGO;

SELECT * 
FROM VCAT_PROVEEDOR;

SELECT * 
FROM VCAT_LOCACION_RECOGIDA;

SELECT * 
FROM VCAT_LOCACION_DESCENSO;

SELECT * 
FROM VDIM_FCH;

SELECT * 
FROM VFACT_PAGO_VIAJE;

SELECT *
FROM VDIM_LOG_CARGA;
