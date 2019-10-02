#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 13:13:29 2019

@author: monster
"""

from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import unix_timestamp, from_unixtime, hour
from pyspark.sql.types import *
from pyspark.sql.functions import split

import time
import postgres
import sys
import pandas as pd
import math

def write_to_postgres(out_df, table_name):
    table = table_name
    mode = "append"
    connector = postgres.PostgresConnector()
    connector.write(out_df, table, mode)

if __name__ == "__main__":
    time_start = time.time()
    #bucket = 's3a://mtabusdata/'
    #filename = 'MTA-Bus-Time_.2014-08-01.txt'
    bucket = 's3a://mtabusdata/parquet/'
    filename = 'MTA-Bus-Time_.2014-08-01.txt'
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    data = spark.read.load(bucket+filename[0:-4])
    #data = spark.read.format("csv").option("inferSchema",True). \
    #        option("header",True).option("delimiter", "\t").load(bucket+filename)
    
    #data.printSchema()
    #data.show()
    trip = data.groupBy('inferred_trip_id'). \
        agg(F.max("inferred_route_id").alias("route_id"), \
            F.min("time_received").alias("min_time"), \
            F.max("time_received").alias("max_time"))
    timeFmt = "yyyy-MM-dd'T'HH:mm:ss"
    timeDiff = (F.unix_timestamp('max_time', format=timeFmt)-\
                F.unix_timestamp('min_time', format=timeFmt))/60.
    #add hour column
    trip = trip.withColumn('hour',hour(trip['max_time']))
    #compute trip duration
    trip = trip.withColumn("duration", timeDiff)
    #filter out trips more than 20 min and less than 1400 min (~ 1 day)
    trip = trip.filter(trip.duration > 20).filter(trip.duration<1400)
    #stats for trip duration
    tripstat = trip.groupBy('route_id','hour'). \
        agg(F.min('duration').alias("min_dur"), \
            F.max('duration').alias("max_dur"), \
            F.avg('duration').alias("avg_dur"))
    #write_to_postgres(trip,'postgres')
    #tripstat.show(5)
    write_to_postgres(tripstat,'tripstat')
    spark.stop()
    time_end = (time.time()-time_start)/60
    print(time_end)


