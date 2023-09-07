"""
    Create files to load in database tables
"""

# The libraries are imported
from aux_src.ETL_Param import *
import shutil
import os


# Create file in the output path
def createFile(file):

    print("Rename the file created in the temp path...\n")

    # Delete past file created
    if os.path.exists(output_path + file) == True:
        os.remove(output_path + file)

    file_created_name = os.listdir(temp_path)[-2]
    os.rename(temp_path + file_created_name, output_path + file)

    # Delete temp files in the temp path
    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)
    
    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)

    files_delete = os.listdir(temp_path)

    for delete in files_delete:
        os.remove(temp_path + delete)

    # Validating that the file has records
    with open(output_path + file) as new_file:
        total_lines = sum(1 for line in new_file)

    if total_lines > 0:
        print(f"The file {file} has been crated in output path with {total_lines} records.\n")
        
    else:
        raise ValueError (f"The file {file} has been crated in output path without records.\n" \
            "Please check because there aren't records in the file.\n")


# Delete files in the temp path
def deleteFile():

    if os.path.exists(tempo_path):
        shutil.rmtree(tempo_path)

    delete_all_files = os.listdir(temp_path)
    
    if len(delete_all_files) > 0:
        print("Delete files in temp path\n")
        for delete in delete_all_files:
            os.remove(temp_path + delete)
