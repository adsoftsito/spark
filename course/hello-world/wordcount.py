"""
# Title : wordcount.py
# Description : This template can be used to create pyspark script
# Author : your-name
# Date : Oct 2, 2022
# Version : 1.0 (Initial Draft)
# Usage : spark-submit --deploy-mode cluster wordcount.py
"""

# import modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys,logging
from datetime import datetime

from pyspark import SparkContext
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
AppName = "wordcount" + "_" + "your_name" + "_" + str(dt_string)


def main():
    # start spark code
    #spark = SparkSession.builder.appName(AppName+"_"+str(dt_string)).getOrCreate()
    #spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")

    #do something here
    logger.info("Reading ...")



    sc = SparkContext(appName=AppName)

    start_time = datetime.now()

    f = sc.textFile("hdfs://centos:9000/user/hadoop/books/alice.txt")
    counts = f.flatMap(lambda line:line.split(" ")) \
        .map(lambda word: (word,1)) \
        .reduceByKey(lambda a,b: a+b)


    counts.saveAsTextFile("hdfs://centos:9000/user/hadoop/books/aliceResult2") 

    diff = datetime.now() - start_time
    #print ("Spend %d.%d seconds", (diff.seconds, diff.microseconds))  
    logger.info("Previewing ...")

    logger.info("Ending spark application")
    # end spark code
    sc.stop()
    return None

# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit()
