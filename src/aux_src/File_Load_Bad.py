"""
    Create bad file from FACT_PAGO_VIAJE
"""

# The libraries are imported
from aux_src.ETL_Param import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import shutil
import time
import os


def createBadFile():

    # Spark session parameters are initialized
    sp = SparkSession.builder.master("local").appName("Load Tables In SQL").getOrCreate()

    # Create the bad DataFrame
    print("\nIt is validated that the IDs of the catalogs and the DIM_FCH table " \
        "don't have null values in the DataFrame of FACT_PAGO_VIAJE...\n")
    df = sp.read.schema(schema_final_pago_viaje).csv(path_final_pago_viaje)

    # Delete possible past file created
    if os.path.exists(path_bad_pago_viaje) == True:
        os.remove(path_bad_pago_viaje)

    # Create new csv file in bad path with IDs columns null
    df_bad_pago_viaje = df.filter(col("ID_PROVEEDOR").isNull() |  \
                                  col("ID_FCH").isNull() | \
                                  col("ID_TARIFA").isNull() | \
                                  col("ID_LOCACION_RECOGIDA").isNull() | \
                                  col("ID_LOCACION_DESCENSO").isNull() | \
                                  col("ID_TIPO_PAGO").isNull())

    df_bad_pago_viaje.write.mode('append').csv(temp_path)
    time.sleep(5)

    # Move file from temp path to output path
    file_created_name = os.listdir(temp_path)[-2]
    
    os.rename(temp_path + file_created_name, path_bad_pago_viaje)

    # Delete temp files in the temp path
    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)

    files_delete = os.listdir(temp_path)

    for delete in files_delete:
        os.remove(temp_path + delete)
    
    # Validating that the file has records
    with open(path_bad_pago_viaje) as new_file:
        total_lines = sum(1 for line in new_file)

    if total_lines > 0:
        raise ValueError (f"The file {file_bad_pago_viaje_name} has been crated in bad path wit {total_lines} records.\n" \
            "Please check because there are records in the bad file.\n")
    
    # Spark session stops
    sp.stop()
