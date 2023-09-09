"""
    File with ETL parameters
"""

# The libraries are imported
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import DateType
from datetime import datetime
from decouple import config


# URL parameter
URL = "jdbc:sqlserver://" + str(config('SQL_SERVER')) + ";databaseName=" + str(config('SQL_DB')) + ";"


# Path parameters
input_path = "../input/"
output_path = "../output/"
temp_path = "../temp/"
tempo_path = "../temp/_temporary/"
proces_path = "../processed/"
logs_path = "../logs/"
bads_path = "../bads/"


# Date parameter
current_datetime = datetime.today()
format_date = current_datetime.strftime("%Y%m%d")
format_log_date = current_datetime.strftime("%Y%m%d%H%M%S")


# Files names
file_json_name = "uber_data.json"
file_csv_name = "uber_data_" + format_date + ".csv"
file_tarifa_name = "cat_tarifa_" + format_date + ".csv"
file_tipo_pago_name = "cat_tipo_pago_" + format_date + ".csv"
file_proveedor_name = "cat_proveedor_" + format_date + ".csv"
file_locacion_descenso_name = "cat_locacion_descenso_" + format_date + ".csv"
file_locacion_recogida_name = "cat_locacion_recogida_" + format_date + ".csv"
file_fecha_name = "dim_fecha_" + format_date + ".csv"
file_pago_viaje_name = "fact_pago_viaje_" + format_date + ".csv"
file_final_json_name = "uber_data_" + format_date + ".json"
file_final_tarifa_name = "cat_final_tarifa_" + format_date + ".csv"
file_final_tipo_pago_name = "cat_final_tipo_pago_" + format_date + ".csv"
file_final_proveedor_name = "cat_final_proveedor_" + format_date + ".csv"
file_final_loc_descenso_name = "cat_final_loc_descenso_" + format_date + ".csv"
file_final_loc_recogida_name = "cat_final_loc_recogida_" + format_date + ".csv"
file_final_fch_name = "dim_final_fecha_" + format_date + ".csv"
file_image_pago_viaje_name =  "fact_image_pago_viaje_" + format_date + ".csv"
file_final_pago_viaje_name =  "fact_final_pago_viaje_" + format_date + ".csv"
file_log_carga_name = "DIM_LOG_CARGA_" + format_date + ".csv"


# Path and files names
path_json_name = input_path + file_json_name
path_csv_name = output_path + file_csv_name
path_tarifa_name = output_path + file_tarifa_name
path_tipo_pago_name = output_path + file_tipo_pago_name
path_proveedor_name = output_path + file_proveedor_name
path_locacion_descenso = output_path + file_locacion_descenso_name
path_locacion_recogida = output_path + file_locacion_recogida_name
path_fecha = output_path + file_fecha_name
path_pago_viaje = output_path + file_pago_viaje_name
path_final_tarifa = output_path + file_final_tarifa_name
path_final_tipo_pago = output_path + file_final_tipo_pago_name
path_final_proveedor = output_path + file_final_proveedor_name
path_final_loc_descenso = output_path + file_final_loc_descenso_name
path_final_loc_recogida = output_path + file_final_loc_recogida_name
path_final_fch = output_path + file_final_fch_name
path_image_pago_viaje = output_path + file_image_pago_viaje_name
path_final_pago_viaje = output_path + file_final_pago_viaje_name
path_log_carga = logs_path + file_log_carga_name


# Logs files
extract_log = "Extract_Process_" + format_log_date + ".log"
transform_log = "Transform_Process_" + format_log_date + ".log"
load_log = "Load_Process_" + format_log_date + ".log"


# Paths and logs file names
path_extract_log_name = logs_path + extract_log
path_transform_log_name = logs_path + transform_log
path_load_log_name = logs_path + load_log


# Bad file
file_bad_pago_viaje_name =  "fact_final_pago_viaje_" + format_date + ".bad"


# Path and bad file name
path_bad_pago_viaje = bads_path + file_bad_pago_viaje_name


# Tables mode load
append = "append"
overwrite = "overwrite"


# Tables names
tb_locacion_descenso = "UBER_ANALISIS.dbo.CAT_LOCACION_DESCENSO"
tb_locacion_recogida = "UBER_ANALISIS.dbo.CAT_LOCACION_RECOGIDA"
tb_proveedor = "UBER_ANALISIS.dbo.CAT_PROVEEDOR"
tb_tarifa = "UBER_ANALISIS.dbo.CAT_TARIFA"
tb_tipo_pago = "UBER_ANALISIS.dbo.CAT_TIPO_PAGO"
tb_fch = "UBER_ANALISIS.dbo.DIM_FCH"
tb_pago_viaje = "UBER_ANALISIS.dbo.FACT_PAGO_VIAJE"
tb_im_locacion_descenso = "UBER_ANALISIS.dbo.IM_CAT_LOCACION_DESCENSO"
tb_im_locacion_recogida = "UBER_ANALISIS.dbo.IM_CAT_LOCACION_RECOGIDA"
tb_im_proveedor = "UBER_ANALISIS.dbo.IM_CAT_PROVEEDOR"
tb_im_tarifa = "UBER_ANALISIS.dbo.IM_CAT_TARIFA"
tb_im_tipo_pago = "UBER_ANALISIS.dbo.IM_CAT_TIPO_PAGO"
tb_im_fch = "UBER_ANALISIS.dbo.IM_DIM_FCH"
tb_im_pago_viaje = "UBER_ANALISIS.dbo.IM_FACT_PAGO_VIAJE"
tb_tmp_pago_viaje = "UBER_ANALISIS.dbo.TMP_FACT_PAGO_VIAJE"
tb_log_carga = "UBER_ANALISIS.dbo.DIM_LOG_CARGA"


# Final tables names
tbf_locacion_descenso = "CAT_LOCACION_DESCENSO"
tbf_locacion_recogida = "CAT_LOCACION_RECOGIDA"
tbf_proveedor = "CAT_PROVEEDOR"
tbf_tarifa = "CAT_TARIFA"
tbf_tipo_pago = "CAT_TIPO_PAGO"
tbf_fch = "DIM_FCH"
tbf_pago_viaje = "FACT_PAGO_VIAJE"
tbf_log_carga = "DIM_LOG_CARGA"


# Define tables schemas

# Schema CAT_PROVEEDOR
schema_proveedor = StructType([
    StructField("ID_PROVEEDOR",IntegerType(),False),
    StructField("PROVEEDOR",StringType(),True)
])

# Schema final CAT_PROVEEDOR
schema_final_proveedor = StructType([
    StructField("ID_PROVEEDOR",IntegerType(),True),
    StructField("PROVEEDOR",StringType(),True),
    StructField("FCH_CARGA", DateType(),True)
])

# Schema CAT_TARIFA
schema_tarifa = StructType([
    StructField("ID_TARIFA",IntegerType(),True),
    StructField("TARIFA",StringType(),True) 
])

# Schema final CAT_TARIFA
schema_final_tarifa = StructType([
    StructField("ID_TARIFA",IntegerType(),True),
    StructField("TARIFA",StringType(),True),
    StructField("FCH_CARGA", DateType(),True)
])

# Schema CAT_TIPO_PAGO
schema_tipo_pago = StructType([
    StructField("ID_TIPO_PAGO",IntegerType(),True),
    StructField("TIPO_PAGO",StringType(),True)
])

# Schema final CAT_TIPO_PAGO
schema_final_tipo_pago = StructType([
    StructField("ID_TIPO_PAGO",IntegerType(),True),
    StructField("TIPO_PAGO",StringType(),True),
    StructField("FCH_CARGA",DateType(),True)
])

# Schema CAT_LOCACION_DESCENSO
schema_locacion_descenso = StructType([
    StructField("LATITUD_DESCENSO",DoubleType(),True),
    StructField("LONGITUD_DESCENSO",DoubleType(),True)
])

# Schema final CAT_LOCACION_DESCENSO
schema_final_loc_descenso = StructType([
    StructField("ID_LOCACION_DESCENSO",IntegerType(),True),
    StructField("LATITUD_DESCENSO",DoubleType(),True),
    StructField("LONGITUD_DESCENSO",DoubleType(),True),
    StructField("FCH_CARGA",DateType(),True)
])

# Schema CAT_LOCACION_RECOGIDA
schema_locacion_recogida = StructType([
    StructField("LATITUD_RECOGIDA",DoubleType(),True),
    StructField("LONGITUD_RECOGIDA",DoubleType(),True)
])

# Schema final CAT_LOCACION_RECOGIDA
schema_final_loc_recogida = StructType([
    StructField("ID_LOCACION_RECOGIDA",IntegerType(),True),
    StructField("LATITUD_RECOGIDA",DoubleType(),True),
    StructField("LONGITUD_RECOGIDA",DoubleType(),True),
    StructField("FCH_CARGA",DateType(),True)
])

# Schema DIM_FCH
schema_fch = StructType([
    StructField("FCH_HRA_RECOGIDA",StringType(),True),
    StructField("FCH_HRA_DESCENSO",StringType(),True)
])

# Schema final DIM_FCH
schema_final_fch = StructType([
    StructField("ID_FCH",IntegerType(),True),
    StructField("FCH_HRA_RECOGIDA",StringType(),True),
    StructField("FCH_RECOGIDA",DateType(),True),
    StructField("ANIO_RECOGIDA",IntegerType(),True),
    StructField("MES_RECOGIDA",IntegerType(),True),
    StructField("DIA_RECOGIDA",IntegerType(),True),
    StructField("DIA_SEMANA_RECOGIDA",IntegerType(),True),
    StructField("HRA_HRA_RECOGIDA",StringType(),True),
    StructField("HRA_RECOGIDA",IntegerType(),True),
    StructField("MIN_RECOGIDA",IntegerType(),True),
    StructField("FCH_HRA_DESCENSO",StringType(),True),
    StructField("FCH_DESCENSO",DateType(),True),
    StructField("ANIO_DESCENSO",IntegerType(),True),
    StructField("MES_DESCENSO",IntegerType(),True),
    StructField("DIA_DESCENSO",IntegerType(),True),
    StructField("DIA_SEMANA_DESCENSO",IntegerType(),True),
    StructField("HRA_HRA_DESCENSO",StringType(),True),
    StructField("HRA_DESCENSO",IntegerType(),True),
    StructField("MIN_DESCENSO",IntegerType(),True),
    StructField("DURACION_VIAJE",StringType(),True),
    StructField("FCH_CARGA",DateType(),True)
])

# Schema FACT_PAGO_VIAJE
schema_pago_viaje = StructType([
    StructField("ID_PROVEEDOR",IntegerType(),True),
    StructField("FCH_HRA_RECOGIDA",StringType(),True),
    StructField("FCH_HRA_DESCENSO",StringType(),True),
    StructField("ID_TARIFA",IntegerType(),True),
    StructField("LATITUD_RECOGIDA",DoubleType(),True),
    StructField("LONGITUD_RECOGIDA",DoubleType(),True),
    StructField("LATITUD_DESCENSO",DoubleType(),True),
    StructField("LONGITUD_DESCENSO",DoubleType(),True),
    StructField("ID_TIPO_PAGO",IntegerType(),True),
    StructField("NUM_PASAJEROS",IntegerType(),True),
    StructField("DISTANCIA_VIAJE",DoubleType(),True),
    StructField("FLG_TIENDA_AVANCE",StringType(),True),
    StructField("MONTO_TARIFA",DoubleType(),True),
    StructField("EXTRA",DoubleType(),True),
    StructField("IMP_MTA",DoubleType(),True),
    StructField("MONTO_PROPINA",DoubleType(),True),
    StructField("MONTO_PEAJE",DoubleType(),True),
    StructField("RECARGO_MEJORA",DoubleType(),True),
    StructField("MONTO_TOTAL",DoubleType(),True)
])

# Schema image FACT_PAGO_VIAJE
schema_image_pago_viaje = StructType([
    StructField("ID_PROVEEDOR",IntegerType(),True),
    StructField("ID_FCH",IntegerType(),True),
    StructField("ID_TARIFA",IntegerType(),True),
    StructField("ID_LOCACION_RECOGIDA",IntegerType(),True),
    StructField("ID_LOCACION_DESCENSO",IntegerType(),True),
    StructField("ID_TIPO_PAGO",IntegerType(),True),
    StructField("NUM_PASAJEROS",IntegerType(),True),
    StructField("DISTANCIA_VIAJE",DoubleType(),True),
    StructField("FLG_TIENDA_AVANCE",StringType(),True),
    StructField("MONTO_TARIFA",DoubleType(),True),
    StructField("EXTRA",DoubleType(),True),
    StructField("IMP_MTA",DoubleType(),True),
    StructField("MONTO_PROPINA",DoubleType(),True),
    StructField("MONTO_PEAJE",DoubleType(),True),
    StructField("RECARGO_MEJORA",DoubleType(),True),
    StructField("MONTO_TOTAL",DoubleType(),True)
])

# Schema final FACT_PAGO_VIAJE
schema_final_pago_viaje = StructType([
    StructField("ID_VIAJE",IntegerType(),True),
    StructField("ID_PROVEEDOR",IntegerType(),True),
    StructField("ID_FCH",IntegerType(),True),
    StructField("ID_TARIFA",IntegerType(),True),
    StructField("ID_LOCACION_RECOGIDA",IntegerType(),True),
    StructField("ID_LOCACION_DESCENSO",IntegerType(),True),
    StructField("ID_TIPO_PAGO",IntegerType(),True),
    StructField("NUM_PASAJEROS",IntegerType(),True),
    StructField("DISTANCIA_VIAJE",DoubleType(),True),
    StructField("FLG_TIENDA_AVANCE",StringType(),True),
    StructField("MONTO_TARIFA",DoubleType(),True),
    StructField("EXTRA",DoubleType(),True),
    StructField("IMP_MTA",DoubleType(),True),
    StructField("MONTO_PROPINA",DoubleType(),True),
    StructField("MONTO_PEAJE",DoubleType(),True),
    StructField("RECARGO_MEJORA",DoubleType(),True),
    StructField("MONTO_TOTAL",DoubleType(),True),
    StructField("FCH_CARGA",DateType(),True)
])

# Schema log DIM_LOG_CARGA
schema_log_carga = StructType([
    StructField('FCH_LOG', StringType()),
    StructField('ID_PROCESO', IntegerType()),
    StructField('NOM_ARCHIVO', StringType()),
    StructField('REGISTROS_NOM_ARCHIVO', IntegerType()),
    StructField('NOM_TABLA', StringType()),
    StructField('REGISTROS_INSERTADOS_TABLA', IntegerType()),
    StructField('COMENTARIO', StringType())
])


# Define files schema

# File json schema
schema_json = StructType([
    StructField("VendorID",IntegerType(),True),
    StructField("tpep_pickup_datetime",StringType(),True),
    StructField("tpep_dropoff_datetime",StringType(),True),
    StructField("passenger_count",IntegerType(),True),
    StructField("trip_distance",DoubleType(),True),
    StructField("pickup_longitude",DoubleType(),True),
    StructField("pickup_latitude",DoubleType(),True),
    StructField("RatecodeID",IntegerType(),True),
    StructField("store_and_fwd_flag",StringType(),True),
    StructField("dropoff_longitude",DoubleType(),True),
    StructField("dropoff_latitude",DoubleType(),True),
    StructField("payment_type",IntegerType(),True),
    StructField("fare_amount",DoubleType(),True),
    StructField("extra",DoubleType(),True),
    StructField("mta_tax",DoubleType(),True),
    StructField("tip_amount",DoubleType(),True),
    StructField("tolls_amount",DoubleType(),True),
    StructField("improvement_surcharge",DoubleType(),True),
    StructField("total_amount",DoubleType(),True)
])

# File csv schema
schema_csv = StructType([
    StructField("ID_PROVEEDOR",IntegerType(),True),
    StructField("FCH_HRA_RECOGIDA",StringType(),True),
    StructField("FCH_HRA_DESCENSO",StringType(),True),
    StructField("NUM_PASAJEROS",IntegerType(),True),
    StructField("DISTANCIA_VIAJE",DoubleType(),True),
    StructField("LONGITUD_RECOGIDA",DoubleType(),True),
    StructField("LATITUD_RECOGIDA",DoubleType(),True),
    StructField("ID_TARIFA",IntegerType(),True),
    StructField("FLG_TIENDA_AVANCE",StringType(),True),
    StructField("LONGITUD_DESCENSO",DoubleType(),True),
    StructField("LATITUD_DESCENSO",DoubleType(),True),
    StructField("ID_TIPO_PAGO",IntegerType(),True),
    StructField("MONTO_TARIFA",DoubleType(),True),
    StructField("EXTRA",DoubleType(),True),
    StructField("IMP_MTA",DoubleType(),True),
    StructField("MONTO_PROPINA",DoubleType(),True),
    StructField("MONTO_PEAJE",DoubleType(),True),
    StructField("RECARGO_MEJORA",DoubleType(),True),
    StructField("MONTO_TOTAL",DoubleType(),True)
])
