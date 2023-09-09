"""
    Load log table with csv log file
"""

# The libraries are imported
from aux_src.ETL_Param import *
from decouple import config
from csv import writer
from pyspark.sql import SparkSession
import time
import os


# Load log table
def loadLogTable(struct_schema, path_file, table, mode_load):

    # Spark session parameters are initialized
    sp = SparkSession.builder.master("local").appName("Load Log Table").getOrCreate()

    df_table = sp.read.schema(struct_schema).csv(path_file)
    df_table.printSchema()
    time.sleep(5)
 
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


# Load log table without records
def loadLogTableZero(struct_schema, path_file, table, mode_load, name_file, id_proc):

    # Spark session parameters are initialized
    sp = SparkSession.builder.master("local").appName("Load Log Table").getOrCreate()
    comment = "There aren't new records to insert into the table"

    # Validating the log file exists
    if os.path.exists(path_log_carga) == False:
        log_file = open(path_log_carga, 'x')
        log_file.close()

    # Create log list
    print("The record for the log table is been created...\n")
    table_name = table[table.rfind('.') + 1:]
    log_list = [ current_datetime, id_proc, name_file, 0, table_name, 0, comment ]
    
    # Write the log list to log file
    with open(path_log_carga, 'w', newline = '') as wr_log:
        wr_file = writer(wr_log, delimiter = ',')
        wr_file.writerow(log_list)
        wr_log.close()

    df_table = sp.read.schema(struct_schema).csv(path_file)
    df_table.printSchema()
    time.sleep(5)
 
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
