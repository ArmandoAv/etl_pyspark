"""
    Load tables with the csv files created in the output path
    and read tables to create new csv files
"""

# The libraries are imported
from aux_src.ETL_Param import *
from decouple import config
from csv import writer
from pyspark.sql import SparkSession
import shutil
import time
import os


# Load table
def loadTable(struct_schema, path_file, table, mode_load, name_file, query, id_proc):

    # Spark session parameters are initialized
    sp = SparkSession.builder.master("local").appName("Load Table").getOrCreate()

    #df_table = sp.read.schema(struct_schema).csv(path_file)
    df_table = sp.read.load(path_file, schema=struct_schema, format="csv")
    df_table.printSchema()
    time.sleep(5)
 
    try:
        if mode_load == overwrite:
            # With truncate option the table keep the original schema
            # including the indexes, only overwrite the data
            df_table.write \
                .option("truncate", "true") \
                .format(config('CONN')) \
                .mode(mode_load) \
                .option("url", URL) \
                .option("dbtable", table) \
                .option("user", config('SQL_USER')) \
                .option("password", config('SQL_PWD')) \
                .save()
        
        else:

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
    
    # The record for the log table is created
    print("The record for the log table is been created...\n")
    time.sleep(5)

    # Validating the records in the file
    comment = None
    with open(path_file) as myfile:
        total_lines = sum(1 for line in myfile)
        
    if total_lines == 0:
        if (table[table.rfind('.') + 1:table.rfind('.') + 4] == 'IM_' | table[table.rfind('.') + 1:table.rfind('.') + 4] == 'TMP'):
            raise ValueError (f"The file {name_file} has been crated without records.\n" \
                "Please check because there aren't records in the file.\n")
    
    # Validating the records loaded in the table
    try:
        df_read_table = sp.read \
            .format(config('CONN')) \
            .option("url", URL) \
            .option("query", query) \
            .option("user", config('SQL_USER')) \
            .option("password", config('SQL_PWD')) \
            .load()

    except ValueError as error :
        print("Connector write failed." + error)
    
    if df_read_table == 0:
        if (table[table.rfind('.') + 1:table.rfind('.') + 4] == 'IM_' | table[table.rfind('.') + 1:table.rfind('.') + 4] == 'TMP'):
            raise ValueError (f"The table {table} hasn't loaded.\n" \
                "Please check why the table hasn't loaded.\n")
    
    # Gets the number of records loaded in the table
    num_rec = df_read_table.collect()[0][0]

    # Validating the log file exists
    if os.path.exists(path_log_carga) == False:
        log_file = open(path_log_carga, 'x')
        log_file.close()
    
    # Create log list
    table_name = table[table.rfind('.') + 1:]
    log_list = [ current_datetime, id_proc, name_file, total_lines, table_name, num_rec, comment ]
    
    # Write the log list to log file
    with open(path_log_carga, 'w', newline = '') as wr_log:
        wr_file = writer(wr_log, delimiter = ',')
        wr_file.writerow(log_list)
        wr_log.close()

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

    # Delete possible past file created
    if os.path.exists(final_path_file) == True:
        os.remove(final_path_file)

    # Create new csv file in temp path
    df_read_table.write.mode('append').csv(temp_path)
    time.sleep(5)

    # Move file from temp path to output path
    file_created_name = os.listdir(temp_path)[-2]
    
    os.rename(temp_path + file_created_name, final_path_file)

    # Delete temp files in the temp path
    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)

    files_delete = os.listdir(temp_path)

    for delete in files_delete:
        os.remove(temp_path + delete)

    # Validating the records in the file
    with open(final_path_file) as myfile:
        total_lines = sum(1 for line in myfile)

    if total_lines == 0:
        print(f"The file {file} has been crated in output path without records.")
    else:
        print(f"The file {file} has been crated in output path with {total_lines} records.")

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
