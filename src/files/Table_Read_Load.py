"""
    Create csv files in the ouput path with database tables
"""

# The library is imported
from files.ETL_Param import *


# Validation CAT_TARIFA
table_tarifa = f"""SELECT 
       IM.ID_TARIFA,
	   IM.TARIFA
FROM   {tb_im_tarifa} AS IM
LEFT JOIN {tb_tarifa} AS FN
ON  IM.ID_TARIFA = FN.ID_TARIFA
WHERE FN.ID_TARIFA IS NULL"""


# Validation CAT_TIPO_PAGO
table_tipo_pago = f"""SELECT 
       IM.ID_TIPO_PAGO,
	   IM.TIPO_PAGO
FROM   {tb_im_tipo_pago} AS IM
LEFT JOIN {tb_tipo_pago} AS FN
ON  IM.ID_TIPO_PAGO = FN.ID_TIPO_PAGO
WHERE FN.ID_TIPO_PAGO IS NULL"""


# Validation CAT_PROVEEDOR
table_proveedor = f"""SELECT 
       IM.ID_PROVEEDOR,
	   IM.PROVEEDOR
FROM   {tb_im_proveedor} AS IM
LEFT JOIN {tb_proveedor} AS FN
ON  IM.ID_PROVEEDOR = FN.ID_PROVEEDOR
WHERE FN.ID_PROVEEDOR IS NULL"""


# Validation CAT_LOCACION_DESCENSO
table_locacion_descenso = f"""SELECT DISTINCT
       NULL AS ID_LOCACION_DESCENSO,
	   IM.LATITUD_DESCENSO,
	   IM.LONGITUD_DESCENSO
FROM   {tb_im_locacion_descenso} AS IM
LEFT JOIN {tb_locacion_descenso} AS FN
ON  IM.LATITUD_DESCENSO  = FN.LATITUD_DESCENSO
AND IM.LONGITUD_DESCENSO = FN.LONGITUD_DESCENSO
WHERE FN.LATITUD_DESCENSO IS NULL"""


# Validation CAT_LOCACION_RECOGIDA
table_locacion_recogida = f"""SELECT DISTINCT
       NULL AS ID_LOCACION_RECOGIDA,
	   IM.LATITUD_RECOGIDA,
	   IM.LONGITUD_RECOGIDA
FROM   {tb_im_locacion_recogida} AS IM
LEFT JOIN {tb_locacion_recogida} AS FN
ON  IM.LATITUD_RECOGIDA  = FN.LATITUD_RECOGIDA
AND IM.LONGITUD_RECOGIDA = FN.LONGITUD_RECOGIDA
WHERE FN.LATITUD_RECOGIDA IS NULL"""


# Validation DIM_FCH
table_fch = f"""SELECT DISTINCT
	   NULL AS ID_FCH,
	   IM.FCH_HRA_RECOGIDA,
	   CAST(IM.FCH_HRA_RECOGIDA AS DATE) AS FCH_RECOGIDA,
	   DATEPART(YEAR, IM.FCH_HRA_RECOGIDA) AS ANIO_RECOGIDA,
	   DATEPART(MONTH, IM.FCH_HRA_RECOGIDA) AS MES_RECOGIDA,
	   DATEPART(DAY, IM.FCH_HRA_RECOGIDA) AS DIA_RECOGIDA,
	   DATEPART(WEEKDAY, IM.FCH_HRA_RECOGIDA) AS DIA_SEMANA_RECOGIDA,
	   SUBSTRING(IM.FCH_HRA_RECOGIDA, 12, 19) AS HRA_HRA_RECOGIDA,
	   DATEPART(HOUR, IM.FCH_HRA_RECOGIDA) AS HRA_RECOGIDA,
	   DATEPART(MINUTE, IM.FCH_HRA_RECOGIDA) AS MIN_RECOGIDA,
	   IM.FCH_HRA_DESCENSO,
	   CAST(IM.FCH_HRA_DESCENSO AS DATE) AS FCH_DESCENSO,
	   DATEPART(YEAR, IM.FCH_HRA_DESCENSO) AS ANIO_DESCENSO,
	   DATEPART(MONTH, IM.FCH_HRA_DESCENSO) AS MES_DESCENSO,
	   DATEPART(DAY, IM.FCH_HRA_DESCENSO) AS DIA_DESCENSO,
	   DATEPART(WEEKDAY, IM.FCH_HRA_DESCENSO) AS DIA_SEMANA_DESCENSO,
	   SUBSTRING(IM.FCH_HRA_DESCENSO, 12, 19) AS HRA_HRA_DESCENSO,
	   DATEPART(HOUR, IM.FCH_HRA_DESCENSO) AS HRA_DESCENSO,
	   DATEPART(MINUTE, IM.FCH_HRA_DESCENSO) AS MIN_DESCENSO,
	   CASE WHEN DATEDIFF (HOUR, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) % 60 < 10
                THEN '0' + CAST(DATEDIFF (HOUR, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) AS VARCHAR)
			ELSE CAST(DATEDIFF (HOUR, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) % 60 AS VARCHAR)
	   END + ':' +
	   CASE WHEN DATEDIFF (MINUTE, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) % 60 < 10
                THEN '0' + CAST(DATEDIFF (MINUTE, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) AS VARCHAR)
			ELSE CAST(DATEDIFF (MINUTE, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) % 60 AS VARCHAR)
	   END + ':'  +
	   CASE WHEN LEN(CAST(DATEDIFF (SECOND, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) % 60 AS VARCHAR)) < 2
	            THEN '0' + (CAST(DATEDIFF (SECOND, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) % 60 AS VARCHAR))
			ELSE (CAST(DATEDIFF (SECOND, IM.FCH_HRA_RECOGIDA, IM.FCH_HRA_DESCENSO) % 60 AS VARCHAR))
	   END AS DURACION_VIAJE
FROM   {tb_im_fch} AS IM
LEFT JOIN {tb_fch} AS FN
ON  IM.FCH_HRA_RECOGIDA = FN.FCH_HRA_RECOGIDA
AND IM.FCH_HRA_DESCENSO = FN.FCH_HRA_DESCENSO
WHERE FN.FCH_HRA_RECOGIDA IS NULL"""


# Validation FACT_PAGO_VIAJE
table_im_pago_viaje = f"""SELECT 
       PGO.ID_PROVEEDOR,
       FCH.ID_FCH,
	   PGO.ID_TARIFA,
	   REC.ID_LOCACION_RECOGIDA,
	   DES.ID_LOCACION_DESCENSO,
	   PGO.ID_TIPO_PAGO,
	   PGO.NUM_PASAJEROS,
	   PGO.DISTANCIA_VIAJE,
	   PGO.FLG_TIENDA_AVANCE,
	   PGO.MONTO_TARIFA,
	   PGO.EXTRA,
	   PGO.IMP_MTA,
	   PGO.MONTO_PROPINA,
	   PGO.MONTO_PEAJE,
	   PGO.RECARGO_MEJORA,
	   PGO.MONTO_TOTAL
FROM   {tb_tmp_pago_viaje} AS PGO
INNER JOIN {tb_fch} AS FCH
ON  PGO.FCH_HRA_RECOGIDA = FCH.FCH_HRA_RECOGIDA
AND PGO.FCH_HRA_DESCENSO = FCH.FCH_HRA_DESCENSO
INNER JOIN {tb_locacion_recogida} AS REC
ON  PGO.LATITUD_RECOGIDA  = REC.LATITUD_RECOGIDA
AND PGO.LONGITUD_RECOGIDA = REC.LONGITUD_RECOGIDA
INNER JOIN {tb_locacion_descenso} AS DES
ON  PGO.LATITUD_DESCENSO  = DES.LATITUD_DESCENSO
AND PGO.LONGITUD_DESCENSO = DES.LONGITUD_DESCENSO"""


# Validation final FACT_PAGO_VIAJE
table_pago_viaje = f"""SELECT
       NULL AS ID_VIAJE,
       IM.ID_PROVEEDOR,
       IM.ID_FCH,
       IM.ID_TARIFA,
       IM.ID_LOCACION_RECOGIDA,
       IM.ID_LOCACION_DESCENSO,
       IM.ID_TIPO_PAGO,
       IM.NUM_PASAJEROS,
       IM.DISTANCIA_VIAJE,
       IM.FLG_TIENDA_AVANCE,
       IM.MONTO_TARIFA,
       IM.EXTRA,
       IM.IMP_MTA,
       IM.MONTO_PROPINA,
       IM.MONTO_PEAJE,
       IM.RECARGO_MEJORA,
       IM.MONTO_TOTAL
FROM   {tb_im_pago_viaje} IM
LEFT JOIN {tb_pago_viaje} FN
ON  IM.ID_PROVEEDOR         = FN.ID_PROVEEDOR
AND IM.ID_FCH               = FN.ID_FCH
AND IM.ID_TARIFA            = FN.ID_TARIFA
AND IM.ID_LOCACION_RECOGIDA = FN.ID_LOCACION_RECOGIDA
AND IM.ID_LOCACION_DESCENSO = FN.ID_LOCACION_DESCENSO
AND IM.ID_TIPO_PAGO         = FN.ID_TIPO_PAGO
AND IM.MONTO_TOTAL          = FN.MONTO_TOTAL
WHERE FN.ID_PROVEEDOR IS NULL"""
