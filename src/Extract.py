"""
    Gets the interface from a json file and transforms it 
    into a flat file for further information handling and
    move the file from input path to output path
    simulating a data extraction
"""

# The libraries are imported
from files.File_Extract import *
from pyspark.sql import SparkSession


# DataFrame data is created in output path
print("\nThe input DataFrame is being created...")

# Delete past files in the output path
deleteFile()

# Create CSV file in the output path
createFile()
