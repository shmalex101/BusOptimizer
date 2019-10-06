#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 13:13:29 2019

@author: Alex Soloway
Description: Insight data engineering project. Batch process to calculate the
trip statistics (min, max and avg trip duration) for each bus route in the NYC
bus system. Output of batch process sent to postgres database.
"""

from __future__ import print_function
import time
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import unix_timestamp, from_unixtime, hour, split
from pyspark.sql.types import *
import postgres

def write_to_postgres(out_df, table=table_name):
    """ function to write output to postgres"""
    table = table_name
    mode = "append"
    connector = postgres.PostgresConnector()
    connector.write(out_df, table, mode)

def trip_statistics(data):
    """compute trip statistics """
    #compute trip statistics
    trip = data.groupBy('inferred_trip_id'). \
        agg(F.max("inferred_route_id").alias("route_id"), \
            F.min("time_received").alias("min_time"), \
            F.max("time_received").alias("max_time"))

    #add hour column
    timeFmt = "yyyy-MM-dd'T'HH:mm:ss"
    timeDiff = (F.unix_timestamp('max_time', format=timeFmt) \
                - F.unix_timestamp('min_time', format=timeFmt)) / 60.
    trip = trip.withColumn('hour', hour(trip['max_time']))

    #compute trip duration
    trip = trip.withColumn("duration", timeDiff)

    #edge cases: filter out trips more than 20 min and less than 1400 min
    trip = trip.filter(trip.duration > 20).filter(trip.duration < 1400)

    #stats for trip duration
    tripstat = trip.groupBy('route_id', 'hour'). \
        agg(F.min('duration').alias("min_dur"), \
            F.max('duration').alias("max_dur"), \
            F.avg('duration').alias("avg_dur"))
    return tripstat

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("busoptimize") \
        .getOrCreate()
    
    time_start = time.time()

    BUCKET = 's3a://mtabusdata/'
    FILENAME = '*.txt'
    DATABASE_NAME = 'tripstat2'

    data = spark.read.format("csv").option("inferSchema", True). \
            option("header", True).option("delimiter", "\t").load(BUCKET+FILENAME)

    tripstat = trip_statistics(data)

    write_to_postgres(tripstat, DATABASE_NAME)

    spark.stop()

    time_end = (time.time() - time_start) / 60
    print(time_end)
