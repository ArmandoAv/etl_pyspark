"""
    Move the json file from input path to output path
"""

# The libraries are imported
from aux_src.ETL_Param import *
from pyspark.sql import SparkSession
import shutil
import time
import os


# Create file in outpu path
def createFile():

    # Spark session parameters are initialized
    sp = SparkSession.builder.master("local").appName("Create File").getOrCreate()

    # Read json file into dataframe
    print(f"The schema of the {file_json_name} file:\n")
    df = sp.read.schema(schema_json).json(path_json_name)
    df.printSchema()
    
    # Write cvs file in temp path
    df.write.mode('append').csv(temp_path)
    time.sleep(5)

    # Move file from temp path to output path
    print("Rename the file created in the output path...\n")
    file_created_name = os.listdir(temp_path)[-2]
    
    os.rename(temp_path + file_created_name, path_csv_name)

    files_delete = os.listdir(temp_path)

    # Delete temp files in the temp path
    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)
    
    for delete in files_delete:
        os.remove(temp_path + delete)

    # Validating that the file has records
    file_csv = os.listdir(output_path)[0]

    with open(path_csv_name) as new_file:
        total_lines = sum(1 for line in new_file)

    if total_lines > 0:
            print(f"The file {file_csv} has been crated in output path with {total_lines} records.\n")
        
    else:
        raise ValueError (f"The file {file_csv} has been crated in output path without records\n" \
            "Please check because there aren't records in the file.\n")

    # Spark session stops
    sp.stop()


# Delete past files in the temp path and output path
def deleteFile():

    # Spark session parameters are initialized
    sp = SparkSession.builder.master("local").appName("Create File").getOrCreate()

    delete_output_files = os.listdir(output_path)
    delete_temp_files = os.listdir(temp_path)

    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)
    
    if len(delete_output_files) > 0:
        print("Delete past files in output path\n")
        for delete in delete_output_files:
            os.remove(output_path + delete)

    if len(delete_temp_files) > 0:
        print("Delete past files in temp path\n")
        for delete in delete_temp_files:
            os.remove(temp_path + delete)

    # Spark session stops
    sp.stop()
