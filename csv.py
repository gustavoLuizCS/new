"""
This code reads file from HDFS and import to table
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
#from wf_config import log
import sys
import os
import traceback
import subprocess

#below are the credentials for my sql server even though I would prefer using Hive to process big files but sql server was mandatory for this task
SQLserver_name_dest = "jdbc:sqlserver://LOCAHOST"
SQLdatabase_name_dest = "dataLoad"
SQLurl_dest = SQLserver_name_dest + ";" + "databaseName=" + SQLdatabase_name_dest + ";"
SQLusername_dest = "sqltestaccount"
SQLpassword_dest = "pass@"

if __name__ == "__main__":
    #initiating spark session with Hive support for better performance which is my recommendation
    spark = SparkSession.builder.appName("load_data").enableHiveSupport().getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition","true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    #path of the file located on HDFS
    path=('file.csv')
    df_head = spark.read.format("csv").option("header", "false").load(path)
    header=df_head.first()

    #spark will read the csv file below
    dfwhitelist=spark.read.format("csv").option("header", "true").load(path)
    dfwhitelist.show()
    #repartition is what we use to make this solution scalable, we manually define the value below based on the file size we want to import
    df=dfwhitelist.repartition(2)
    #print(df)

    #writting to SQL server below (just to enforce my recommendation is to use Hive and not SQL Server)
    try:
        df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", SQLurl_dest) \
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver') \
        .option("dbtable", "testtable") \
        .option("user", SQLusername_dest) \
        .option("password", SQLpassword_dest) \
        .save()
    except Exception as error:
        print("############# Error found ", error)

