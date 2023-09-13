###################################################################################
#                                                                                 #
# Process: ETL Project (Load)                                                     #
# Created by: Armando Avila                                                       #
# Purpose: Loads the tables with the generated files                              #
# Comment: Runs File_Load.py file functions                                       #
#          loadTable()                                                            #
#          readTables()                                                           #
#          deleteFile()                                                           #
#          Runs File_Load_Bad file functions                                      #
#          createBadFile()                                                        #
#          Runs File_Load_Log file functions                                      #
#          loadLogTable()                                                         #
#          Runs File_Clean_Path file function                                     #
#          cleanPaths()                                                           #
#          Gets parameters from ETL_Param.py file                                 #
#          Gets defined queries from File_Load_Table_Read.py file                 #
# Created: 2023-08                                                                #
# Modified:                                                                       #
#                                                                                 #
###################################################################################


# The libraries are imported
from aux_src.ETL_Param import *
from aux_src.File_Load import *
from aux_src.File_Clean_Path import *
from aux_src.File_Load_Bad import *
from aux_src.File_Load_Log import *
from aux_src.File_Load_Table_Read import *
from pyspark.sql import SparkSession
import logging


# Create log file 
logging.basicConfig(filename = path_load_log_name, \
                               format='%(asctime)s:%(levelname)s:%(message)s', \
                               datefmt='%m/%d/%Y %I:%M:%S %p', \
                               level = logging.DEBUG)
logging.debug('This message should appear on the console')
logging.info('So should this')
logging.warning('And this, too')
logging.error('It is a important message')

# Spark session parameters are initialized
sp = SparkSession.builder.master("local").appName("Load Tables In SQL").getOrCreate()
print("\nThe tables are being loading...\n")


"""
    Load table CAT_TARIFA
    the information about this table 
    was generated as hard code
"""
# Load image table CAT_TARIFA
print(f"The table {tbf_tarifa} is being loading...")
loadTable(schema_tarifa, path_tarifa_name, tb_im_tarifa, overwrite, file_tarifa_name, log_im_tb_tarifa, 1)

# Load log table DIM_LOG_CARGA
print(f"The log table {tbf_log_carga} is being loading...")
loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

# Delete past files in the temp path
deleteFile()

# Create new csv file for CAT_TARIFA
readTables(table_tarifa, path_final_tarifa, file_final_tarifa_name)

# Validating that the file has records
with open(path_final_tarifa) as myfile:
    total_lines = sum(1 for line in myfile)

# Load final table CAT_TARIFA
if total_lines > 0:
    
    loadTable(schema_final_tarifa, path_final_tarifa, tb_tarifa, append, file_final_tarifa_name, log_final_tb_tarifa, 2)
    
    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

else:

    print(f"\tThere aren't new records to insert into the {tbf_tarifa} table\n")

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTableZero(schema_log_carga, path_log_carga, tb_log_carga, append, file_final_tarifa_name, 2)


"""
    Load table CAT_TIPO_PAGO
    the information about this table 
    was generated as hard code
"""
# Load image table CAT_TIPO_PAGO
print(f"The table {tbf_tipo_pago} is being loading...")
loadTable(schema_tipo_pago, path_tipo_pago_name, tb_im_tipo_pago, overwrite, file_tipo_pago_name, log_im_tb_tipo_pago, 3)

# Load log table DIM_LOG_CARGA
print(f"The log table {tbf_log_carga} is being loading...")
loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

# Delete past files in the temp path
deleteFile()

# Create new csv file for CAT_TIPO_PAGO
readTables(table_tipo_pago, path_final_tipo_pago, file_final_tipo_pago_name)

# Validating that the file has records
with open(path_final_tipo_pago) as myfile:
    total_lines = sum(1 for line in myfile)

# Load final table CAT_TIPO_PAGO
if total_lines > 0:

    loadTable(schema_final_tipo_pago, path_final_tipo_pago, tb_tipo_pago, append, file_final_tipo_pago_name, log_final_tb_tipo_pago, 4)
    
    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

else:

    print(f"\tThere aren't new records to insert into the {tbf_tipo_pago} table\n")

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTableZero(schema_log_carga, path_log_carga, tb_log_carga, append, file_final_tipo_pago_name, 4)


"""
    Load table CAT_PROVEEDOR
    the information about this table 
    was generated as hard code
"""
# Load image table CAT_PROVEEDOR
print(f"The table {tbf_proveedor} is being loading...")
loadTable(schema_proveedor, path_proveedor_name, tb_im_proveedor, overwrite, file_proveedor_name, log_im_tb_proveedor, 5)

# Load log table DIM_LOG_CARGA
print(f"The log table {tbf_log_carga} is being loading...")
loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

# Delete past files in the temp path
deleteFile()

# Create new csv file for CAT_PROVEEDOR
readTables(table_proveedor, path_final_proveedor, file_final_proveedor_name)

# Validating that the file has records
with open(path_final_proveedor) as myfile:
    total_lines = sum(1 for line in myfile)

# Load final table CAT_PROVEEDOR
if total_lines > 0:

    loadTable(schema_final_proveedor, path_final_proveedor, tb_proveedor, append, file_final_proveedor_name, log_final_tb_proveedor, 6)
    
    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

else:

    print(f"\tThere aren't new records to insert into the {tbf_proveedor} table\n")

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTableZero(schema_log_carga, path_log_carga, tb_log_carga, append, file_final_proveedor_name, 6)


"""
    Load table CAT_LOCACION_DESCENSO
    the information about this table 
    comes from the json file
"""
# Load image table CAT_LOCACION_DESCENSO
print(f"The table {tbf_locacion_descenso} is being loading...")
loadTable(schema_locacion_descenso, path_locacion_descenso, tb_im_locacion_descenso, overwrite, file_locacion_descenso_name, log_im_tb_loc_descenso, 7)

# Load log table DIM_LOG_CARGA
print(f"The log table {tbf_log_carga} is being loading...")
loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

# Delete past files in the temp path
deleteFile()

# Create new csv file for CAT_LOCACION_DESCENSO
readTables(table_locacion_descenso, path_final_loc_descenso, file_final_loc_descenso_name)

# Validating that the file has records
with open(path_final_loc_descenso) as myfile:
    total_lines = sum(1 for line in myfile)

# Load final table CAT_LOCACION_DESCENSO
if total_lines > 0:

    loadTable(schema_final_loc_descenso, path_final_loc_descenso, tb_locacion_descenso, append, file_final_loc_descenso_name, log_final_tb_loc_descenso, 8)

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

else:

    print(f"\tThere aren't new records to insert into the {tbf_locacion_descenso} table\n")

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTableZero(schema_log_carga, path_log_carga, tb_log_carga, append, file_final_loc_descenso_name, 8)


"""
    Load table CAT_LOCACION_RECOGIDA
    the information about this table 
    comes from the json file
"""
# Load image table CAT_LOCACION_RECOGIDA
print(f"The table {tbf_locacion_recogida} is being loading...")
loadTable(schema_locacion_recogida, path_locacion_recogida, tb_im_locacion_recogida, overwrite, file_locacion_recogida_name, log_im_tb_loc_recogida, 9)

# Load log table DIM_LOG_CARGA
print(f"The log table {tbf_log_carga} is being loading...")
loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

# Delete past files in the temp path
deleteFile()

# Create new csv file for CAT_LOCACION_RECOGIDA
readTables(table_locacion_recogida, path_final_loc_recogida, file_final_loc_recogida_name)

# Validating that the file has records
with open(path_final_loc_recogida) as myfile:
    total_lines = sum(1 for line in myfile)

# Load final table CAT_LOCACION_RECOGIDA
if total_lines > 0:

    loadTable(schema_final_loc_recogida, path_final_loc_recogida, tb_locacion_recogida, append, file_final_loc_recogida_name, log_final_tb_loc_recogida, 10)

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

else:

    print(f"\tThere aren't new records to insert into the {tbf_locacion_recogida} table\n")

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTableZero(schema_log_carga, path_log_carga, tb_log_carga, append, file_final_loc_recogida_name, 10)


"""
    Load table DIM_FCH
    the information about this table 
    comes from the json file
"""
# Load image table DIM_FCH
print(f"The table {tbf_fch} is being loading...")
loadTable(schema_fch, path_fecha, tb_im_fch, overwrite, file_fecha_name, log_im_tb_fch, 11)

# Load log table DIM_LOG_CARGA
print(f"The log table {tbf_log_carga} is being loading...")
loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

# Delete past files in the temp path
deleteFile()

# Create new csv file for DIM_FCH
readTables(table_fch, path_final_fch, file_final_fch_name)

# Validating that the file has records
with open(path_final_fch) as myfile:
    total_lines = sum(1 for line in myfile)

# Load final table DIM_FCH
if total_lines > 0:

    loadTable(schema_final_fch, path_final_fch, tb_fch, append, file_final_fch_name, log_final_tb_fch, 12)

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

else:

    print(f"\tThere aren't new records to insert into the {tbf_fch} table\n")

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTableZero(schema_log_carga, path_log_carga, tb_log_carga, append, file_final_fch_name, 12)


"""
    Load table FACT_PAGO_VIAJE
    the information about this table 
    comes from the json file
"""
# Load temporal table FACT_PAGO_VIAJE
print(f"The table {tbf_pago_viaje} is being loading...")
loadTable(schema_pago_viaje, path_pago_viaje, tb_tmp_pago_viaje, overwrite, file_pago_viaje_name, log_tmp_tb_pago_viaje, 13)

# Load log table DIM_LOG_CARGA
print(f"The log table {tbf_log_carga} is being loading...")
loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

# Delete past files in the temp path
deleteFile()

# Create new image csv file for FACT_PAGO_VIAJE
readTables(table_im_pago_viaje, path_image_pago_viaje, file_image_pago_viaje_name)

# Delete past files in the temp path
deleteFile()

# Load image table FACT_PAGO_VIAJE
loadTable(schema_image_pago_viaje, path_image_pago_viaje, tb_im_pago_viaje, overwrite, file_image_pago_viaje_name, log_im_tb_pago_viaje, 14)

# Load log table DIM_LOG_CARGA
print(f"The log table {tbf_log_carga} is being loading...")
loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

# Delete past files in the temp path
deleteFile()

# Create new csv file for FACT_PAGO_VIAJE
readTables(table_pago_viaje, path_final_pago_viaje, file_final_pago_viaje_name)

# Validating that the file has records
with open(path_final_pago_viaje) as myfile:
    total_lines = sum(1 for line in myfile)

# Load final table FACT_PAGO_VIAJE
if total_lines > 0:

    # Validating that IDs don't contain null values
    createBadFile()

    loadTable(schema_final_pago_viaje, path_final_pago_viaje, tb_pago_viaje, append, file_final_pago_viaje_name, log_final_tb_pago_viaje, 15)

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTable(schema_log_carga, path_log_carga, tb_log_carga, append)

else:
 
    print(f"\tThere aren't new records to insert into the {tbf_pago_viaje} table\n")

    # Load log table DIM_LOG_CARGA
    print(f"The log table {tbf_log_carga} is being loading...")
    loadLogTableZero(schema_log_carga, path_log_carga, tb_log_carga, append, file_final_pago_viaje_name, 15)


"""
    Paths are prepared for a new ETL process
"""
# Clean paths for the new ETL process
print("\nFinally the paths are prepared for a new ETL process...")
cleanPaths()

# Spark session stops
print("\nThe Load process has finished without errors.")
sp.stop()
