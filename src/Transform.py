"""
    Files for data model tables are created from 
    the uber_data file
"""

# The libraries are imported
from files.ETL_Param import *
from files.File_Transform import *
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
# A DataFrame is created with the list tarifa
df_tarifa = sp.createDataFrame(list_tarifa)

# Delete files in temp path
deleteFile()

# Write a DataFrame to file csv
df_tarifa.write.mode('append').options(delimiter=',').csv(temp_path)
time.sleep(20)

# Move the file from temp path to output path
createFile(file_tarifa_name)


"""
    Create the csv file to CAT_TIPO_PAGO
    this file is created with hard code because
    the info doesn't exist in the input json file
"""
# A DataFrame is created with the list
df_tipo_pago = sp.createDataFrame(list_tipo_pago)

# Delete files in temp path
deleteFile()

# Write a DataFrame to file csv
df_tipo_pago.write.mode('append').options(delimiter=',').csv(temp_path)
time.sleep(20)

# Move file from temp path to output path
createFile(file_tipo_pago_name)

"""
    Created the csv file to CAT_PROVEEDOR
    this file is created with hard code because
    the info doesn't exist in the input json file
"""
# A DataFrame is created with the list
df_proveedor = sp.createDataFrame(list_proveedor)

# Delete files in temp path
deleteFile()

# Write a DataFrame to file csv
df_proveedor.write.mode('append').options(delimiter=',').csv(temp_path)
time.sleep(20)

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

# A DataFrame is created with the necessary columns
df_locacion_descenso = df_input_csv.select(col("LATITUD_DESCENSO"), col("LONGITUD_DESCENSO"))

# Delete files in temp path
deleteFile()

# Write a DataFrame to file csv
df_locacion_descenso.write.mode('append').options(delimiter=',').csv(temp_path)
time.sleep(20)

# Move file from temp path to output path
createFile(file_locacion_descenso_name)


"""
    Created the csv file to CAT_LOCACION_RECOGIDA
"""

# A DataFrame is created with the necessary columns
df_locacion_recogida = df_input_csv.select(col("LATITUD_RECOGIDA"), col("LONGITUD_RECOGIDA"))

# Delete files in temp path
deleteFile()

# Write a DataFrame to file csv
df_locacion_recogida.write.mode('append').options(delimiter=',').csv(temp_path)
time.sleep(20)

# Move file from temp path to output path
createFile(file_locacion_recogida_name)


"""
    Created the csv file to DIM_FCH
"""

# A DataFrame is created with the necessary columns
df_fecha = df_input_csv.select(col("FCH_HRA_RECOGIDA"), col("FCH_HRA_DESCENSO"))

# Delete files in temp path
deleteFile()

# Write a DataFrame to file csv
df_fecha.write.mode('append').options(delimiter=',').csv(temp_path)
time.sleep(20)

# Move file from temp path to output path
createFile(file_fecha_name)


"""
    Created the csv file to FACT_PAGO_VIAJES
"""

# A DataFrame is created with the necessary columns
df_pago_viajes = df_input_csv.select(col("ID_PROVEEDOR"), col("FCH_HRA_RECOGIDA"), col("FCH_HRA_DESCENSO"), \
                                     col("ID_TARIFA"), col("LATITUD_RECOGIDA"), col("LONGITUD_RECOGIDA"), \
                                     col("LATITUD_DESCENSO"), col("LONGITUD_DESCENSO"), col("ID_TIPO_PAGO"), \
                                     col("NUM_PASAJEROS"), col("DISTANCIA_VIAJE"), col("FLG_TIENDA_AVANCE"), \
                                     col("MONTO_TARIFA"), col("EXTRA"), col("IMP_MTA"), col("MONTO_PROPINA"), \
                                     col("MONTO_PEAJE"), col("RECARGO_MEJORA"), col("MONTO_TOTAL"))

# Delete files in temp path
deleteFile()

# Write a DataFrame to file csv
df_pago_viajes.write.mode('append').options(delimiter=',').csv(temp_path)
time.sleep(20)

# Move file from temp path to output path
createFile(file_pago_viaje_name)


# Spark session stops
sp.stop()
