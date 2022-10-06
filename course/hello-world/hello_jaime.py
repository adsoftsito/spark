"""
# Title : hello_jaime.py
# Description : This template can be used to create pyspark script
# Author : Jaime Gutierrez
# Date : Oct 5, 2022
# Version : 1.0 
# Usage : spark-submit --deploy-mode client hello_jaime.py
"""

# import modules
from pyspark.sql import SparkSession
import sys,logging
from datetime import datetime

# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# current time variable to be used for logging purpose
dt_string = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
# change it to your app name
AppName = "Hello" + "_" + "Jaime"


# adding dummy function. change or remove it.
def some_function1():
    logger.info("Inside some_function 1")

# adding dummy function. change or remove it.
def some_function2():
    logger.info("Inside some_function 2")

def main():
    # start spark code
    spark = SparkSession.builder.appName(AppName+"_"+str(dt_string)).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")

    #calling function 1
    some_function1()

    #calling function 2
    some_function2()

    #do something here
    logger.info("Reading ...")
    logger.info("Previewing ...")
    logger.info("Ending spark application")
    # end spark code
    spark.stop()
    return None

# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit()
