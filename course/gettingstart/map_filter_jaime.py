"""
# Title : map_filter.py
# Description : This template can be used to create pyspark script
# Author : Jaime Gutierrez
# Date : oct 5, 2022
# Version : 1.0 (Initial Draft)
# Usage : spark-submit --deploy-mode client map_filter.py
"""

# import modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
AppName = "MapFilter" + "_" + "jaime"


def main():
    # start spark code
    spark = SparkSession.builder.appName(AppName+"_"+str(dt_string)).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")


    #do something here

    my_list = [1,2,3,4,5,6,7,8,9,10]
    squared_my_list = list(map(lambda x: x*x, my_list))
    logger.info(squared_my_list)

    filtered_my_list = list(filter(lambda x: (x%2 != 0), my_list))
    logger.info(filtered_my_list)


    logger.info("Ending spark application")
    # end spark code
    spark.stop()
    return None

# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit()
