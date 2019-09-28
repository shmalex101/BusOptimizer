#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 10:01:06 2019

@author: monster
"""

import requests
import urllib3.request
import bs4
import pandas as pd
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import *

#scrape file download URL from MTA website
def url_scrape(url):
    r = requests.get(url)
    soup = bs4.BeautifulSoup(r.content,'html.parser')
    main_content = soup.find('div', attrs = {'class': 'container'})
    content = main_content.find_all('a')
    dat = []
    for link in content:
        dat.append(link['href'])
    dfname = [item for item in dat if item.startswith('http://s3')]
    return dfname

#schema for csv download
def dat_schema():
    schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("time_received", TimestampType(), True),
        StructField("vehicle_id", IntegerType(), True),
        StructField("distance_along_trip",DoubleType() , True),
        StructField("inferred_direction_id", StringType(), True),
        StructField("inferred_phase",StringType() , True),
        StructField("inferred_route_id",StringType() , True),
        StructField("inferred_trip_id",StringType() , True),
        StructField("next_scheduled_stop_distance",StringType(), True),
        StructField("next_scheduled_stop_id", StringType(), True)])
    return schema

def dat_down(url,bucket):
    # return url links for MTA bus data
    dfname = url_scrape(url)
    
    # download txt file, unpack from xz to txt and save into aws as parquet
    schema = dat_schema()
    for item in dfname:
        filename = item.split('/')[-1][0:-3]
        try:
            os.system('curl ' + item + ' |xz -d >'+ filename)
            df = spark.read.format("csv"). \
                option('inferSchema',True). \
                option('header',True). \
                option('delimiter', '\t'). \
                schema(schema). \
                load(filename)
            df.write.save(bucket+'/parquet/'+filename[0:-4], \
                          format='parquet')
            os.remove(filename)
        except:
            print('Error downloading' + filename )
            pass

if __name__ == "__main__":
    
    spark = SparkSession \
        .builder \
        .appName('Download MTA bus data') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()
           
    url = 'http://web.mta.info/developers/MTA-Bus-Time-historical-data.html'
    bucket = 's3a://mtabusdata/'
    
    dat_down(url,bucket)
    
    spark.stop()
