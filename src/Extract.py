"""
    Gets the interface from a json file and transforms it 
    into a flat file for further information handling and
    move the file from input path to output path
    simulating a data extraction
"""

# The libraries are imported
from aux_src.File_Extract import *
from pyspark.sql import SparkSession
import logging


# Create log file 
logging.basicConfig(filename = path_extract_log_name, \
                               format='%(asctime)s:%(levelname)s:%(message)s', \
                               datefmt='%m/%d/%Y %I:%M:%S %p', \
                               level = logging.DEBUG)
logging.debug('This message should appear on the console')
logging.info('So should this')
logging.warning('And this, too')

# DataFrame data is created in output path
print("\nThe input DataFrame is being created...")

# Delete past files in the output path
deleteFile()

# Create CSV file in the output path
createFile()

print("The Extract process has finished without errors.")
