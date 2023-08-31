"""
    Deletes the created files and moves the input file
    to the processed directory
"""

# The libraries are imported
from files.ETL_Param import *
import shutil
import os


# Clean paths for the new ETL process
def cleanPaths():

    # Copy json file to processed path
    shutil.copy(input_path + file_json_name, proces_path + file_final_json_name)

    # Move csv file to processed path
    os.rename(output_path + file_csv_name, proces_path + file_csv_name)

    # Clean temp path
    delete_temp_path = os.listdir(temp_path)
    
    if len(delete_temp_path) > 0:
        print("\nDelete files in the temp path")
        for delete in delete_temp_path:
            os.remove(temp_path + delete)

    # Clean output path
    delete_output_path = os.listdir(output_path)
    
    if len(delete_output_path) > 0:
        print("Delete files in the output path")
        for delete in delete_output_path:
            os.remove(output_path + delete)
