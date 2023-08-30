"""
    Load tables with the csv files created in the output path
    and read tables to create new csv files
"""

# The libraries are imported
from files.ETL_Param import *
from decouple import config
from pyspark.sql import SparkSession
import time
import shutil
import os


# Load table
def loadTable(struct, path_file, table, mode_load):

    # Spark session parameters are initialized
    sp = SparkSession.builder.master("local").appName("Load Table").getOrCreate()

    #df_table = sp.read.schema(schema).csv(path_file)
    df_table = sp.read.load(path_file, schema=struct, format="csv")
    df_table.printSchema()
 
    try:
        df_table.write \
            .format(config('CONN')) \
            .mode(mode_load) \
            .option("url", URL) \
            .option("dbtable", table) \
            .option("user", config('SQL_USER')) \
            .option("password", config('SQL_PWD')) \
            .save()

    except ValueError as error:
        print("Connector write failed." + error)
    
    # Spark session stops
    sp.stop()


# Load table with defined query
def readTables(table, final_path_file, file):

    # Spark session parameters are initialized
    sp = SparkSession.builder.master("local").appName("Load Final Table").getOrCreate()

    try:
        df_read_table = sp.read \
            .format(config('CONN')) \
            .option("url", URL) \
            .option("query", table) \
            .option("user", config('SQL_USER')) \
            .option("password", config('SQL_PWD')) \
            .load()

    except ValueError as error :
        print("Connector write failed." + error)

    # Create new csv file in temp path
    df_read_table.write.mode('append').csv(temp_path)
    time.sleep(2)

    # Delete past file created
    if os.path.exists(final_path_file) == True:
        os.remove(final_path_file)

    # Move file from temp path to output path
    file_created_name = os.listdir(temp_path)[-2]
    
    os.rename(temp_path + file_created_name, final_path_file)

    # Delete temp files in the temp path
    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)

    files_delete = os.listdir(temp_path)

    for delete in files_delete:
        os.remove(temp_path + delete)

    print(f"The file {file} has been crated in output path")

    # Spark session stops
    sp.stop()


# Delete files in the temp path
def deleteFile():

    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)

    delete_all_files = os.listdir(temp_path)
    
    if len(delete_all_files) > 0:
        print("Delete files in temp path\n")
        for delete in delete_all_files:
            os.remove(temp_path + delete)
