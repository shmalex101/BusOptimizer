#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import time
import postgres
import sys

if __name__ == "__main__":
    # filenames I want to look at. s3a allows me to load data from s3 
    # bucket (permission likelt aleardy setup)
    bucket = 's3a://mtabusdata/'
    filename = 'MTA-Bus-Time_.2014-09-07.txt'
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    #start_time = time.time()
    #df = spark.read.format("csv").option("inferSchema",True). \
     #   option("header",True).option("delimiter", "\t").load(bucket+filename)
    
    #df.write.save(bucket+'/test/'+filename[0:-4],format='parquet')
    #start_time = time.time()
    df = spark.read.load(bucket+'/test/'+filename[0:-4])

    
    df.printSchema()
    '''
    df.select('latitude').show(5)
    #Query!!!!
    df.filter(df.inferred_route_id == 'MTA NYCT_B67').show()
    print(df.columns)
    if isinstance(df, DataFrame):
        print('Dataframe')
    else:
        print("not dataframe")
     '''   
    def write_to_postgres(out_df, table_name):
        table = table_name
        mode = "append"
        connector = postgres.PostgresConnector()
        connector.write(out_df, table, mode)
    
    write_to_postgres(df,'postgres')
    #df.columns
    #print(df.head(5))
    spark.stop()




#spark-submit --master spark://ec2-100-20-48-52.us-west-2.compute.amazonaws.com:7077 test.py 