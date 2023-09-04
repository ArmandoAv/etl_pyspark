"""
    Files for data model tables are created from 
    the uber_data file
"""

# The libraries are imported
from aux_src.ETL_Param import *
from aux_src.File_Transform import *
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import col
import logging
import time


# Create log file 
logging.basicConfig(filename = path_transform_log_name, \
                               format='%(asctime)s:%(levelname)s:%(message)s', \
                               datefmt='%m/%d/%Y %I:%M:%S %p', \
                               level = logging.DEBUG)
logging.debug('This message should appear on the console')
logging.info('So should this')
logging.warning('And this, too')

# Spark session parameters are initialized
sp = SparkSession.builder.master("local").appName("Create File").getOrCreate()
print("\nThe transformations are being processed...\n")


"""
    Created the csv file to CAT_TARIFA
    this file is created with hard code because
    the info doesn't exist in the input json file
"""

# Delete possible files in temp path
deleteFile()

# Write a DataFrame to file csv
df_tarifa = sp.createDataFrame([
    Row(id_tarifa = 1, tarifa = "Standard rate"),
    Row(id_tarifa = 2, tarifa = "JFK"),
    Row(id_tarifa = 3, tarifa = "Newark"),
    Row(id_tarifa = 4, tarifa = "Nassau or Westchester"),
    Row(id_tarifa = 5, tarifa = "Negotiated fare"),
    Row(id_tarifa = 6, tarifa = "Group ride")
])

print(f"The Dataframe for {file_tarifa_name} has been created")
df_tarifa.show(2)
df_tarifa.write.mode('append').options(delimiter=',').csv(temp_path)
time.sleep(5)

# Move the file from temp path to output path
createFile(file_tarifa_name)


"""
    Create the csv file to CAT_TIPO_PAGO
    this file is created with hard code because
    the info doesn't exist in the input json file
"""
# Delete possible files in temp path
deleteFile()

# Write a DataFrame to file csv
df_tipo_pago = sp.createDataFrame([
    Row(id_tipo_pago = 1, tipo_pago = "Credit card"),
    Row(id_tipo_pago = 2, tipo_pago = "Cash"),
    Row(id_tipo_pago = 3, tipo_pago = "No charge"),
    Row(id_tipo_pago = 4, tipo_pago = "Dispute"),
    Row(id_tipo_pago = 5, tipo_pago = "Unknown"),
    Row(id_tipo_pago = 6, tipo_pago = "Voided trip")
])

print(f"The Dataframe for {file_tipo_pago_name} has been created")
df_tipo_pago.write.mode('append').options(delimiter=',').csv(temp_path)
df_tipo_pago.show(2)
time.sleep(5)

# Move file from temp path to output path
createFile(file_tipo_pago_name)

"""
    Created the csv file to CAT_PROVEEDOR
    this file is created with hard code because
    the info doesn't exist in the input json file
"""
# Delete possible files in temp path
deleteFile()

# Write a DataFrame to file csv
df_proveedor = sp.createDataFrame([
    Row(id_proveedor = 1, proveedor = "Normal"),
    Row(id_proveedor = 2, proveedor = "Black")
])

print(f"The Dataframe for {file_proveedor_name} has been created")
df_proveedor.write.mode('append').options(delimiter=',').csv(temp_path)
df_proveedor.show(2)
time.sleep(5)

# Move file from temp path to output path
createFile(file_proveedor_name)


"""
    The next files are created from the input csv file
"""
# A DataFrame is created wih the csv file
print(f"The schema of the {file_csv_name} file:\n")
df_input_csv = sp.read.schema(schema_csv).csv(path_csv_name)


"""
    Created the csv file to CAT_LOCACION_DESCENSO
"""

# Delete possible files in temp path
deleteFile()

# A DataFrame is created with the necessary columns
df_locacion_descenso = df_input_csv.select(col("LATITUD_DESCENSO"), col("LONGITUD_DESCENSO"))
time.sleep(5)

# Write a DataFrame to file csv
print(f"The Dataframe for {file_locacion_descenso_name} has been created")
df_locacion_descenso.write.mode('append').options(delimiter=',').csv(temp_path)
df_locacion_descenso.show(2)
time.sleep(5)

# Move file from temp path to output path
createFile(file_locacion_descenso_name)


"""
    Created the csv file to CAT_LOCACION_RECOGIDA
"""

# Delete possible files in temp path
deleteFile()

# A DataFrame is created with the necessary columns
df_locacion_recogida = df_input_csv.select(col("LATITUD_RECOGIDA"), col("LONGITUD_RECOGIDA"))
time.sleep(5)

# Write a DataFrame to file csv
print(f"The Dataframe for {file_locacion_recogida_name} has been created")
df_locacion_recogida.write.mode('append').options(delimiter=',').csv(temp_path)
df_locacion_recogida.show(2)
time.sleep(5)

# Move file from temp path to output path
createFile(file_locacion_recogida_name)


"""
    Created the csv file to DIM_FCH
"""

# Delete possible files in temp path
deleteFile()

# A DataFrame is created with the necessary columns
df_fecha = df_input_csv.select(col("FCH_HRA_RECOGIDA"), col("FCH_HRA_DESCENSO"))
time.sleep(5)

# Write a DataFrame to file csv
print(f"The Dataframe for {file_fecha_name} has been created")
df_fecha.write.mode('append').options(delimiter=',').csv(temp_path)
df_fecha.show(2)
time.sleep(5)

# Move file from temp path to output path
createFile(file_fecha_name)


"""
    Created the csv file to FACT_PAGO_VIAJES
"""

# Delete possible files in temp path
deleteFile()

# A DataFrame is created with the necessary columns
df_pago_viajes = df_input_csv.select(col("ID_PROVEEDOR"), col("FCH_HRA_RECOGIDA"), col("FCH_HRA_DESCENSO"), \
                                     col("ID_TARIFA"), col("LATITUD_RECOGIDA"), col("LONGITUD_RECOGIDA"), \
                                     col("LATITUD_DESCENSO"), col("LONGITUD_DESCENSO"), col("ID_TIPO_PAGO"), \
                                     col("NUM_PASAJEROS"), col("DISTANCIA_VIAJE"), col("FLG_TIENDA_AVANCE"), \
                                     col("MONTO_TARIFA"), col("EXTRA"), col("IMP_MTA"), col("MONTO_PROPINA"), \
                                     col("MONTO_PEAJE"), col("RECARGO_MEJORA"), col("MONTO_TOTAL"))
time.sleep(5)

# Write a DataFrame to file csv
print(f"The Dataframe for {file_pago_viaje_name} has been created")
df_pago_viajes.write.mode('append').options(delimiter=',').csv(temp_path)
df_pago_viajes.show(2)
time.sleep(10)

# Move file from temp path to output path
createFile(file_pago_viaje_name)


# Spark session stops
print("The Transform process has finished without errors.")
sp.stop()
