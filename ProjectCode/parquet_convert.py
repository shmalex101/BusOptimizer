#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 12:46:38 2019

@author: monster
"""
from __future__ import print_function

import requests
import urllib3.request
import time
import bs4
import os
#import lzma
import pandas as pd


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def fname():
    # return list of filenames on mta website
    url = 'http://web.mta.info/developers/MTA-Bus-Time-historical-data.html'
    r = requests.get(url)
    soup = bs4.BeautifulSoup(r.content,'html.parser')
    main_content = soup.find('div', attrs = {'class': 'container'})
    content = main_content.find_all('a')
    dat = []
    for link in content:
        dat.append(link['href'])
    dfname = [item for item in dat if item.startswith('http://s3')]
    return dfname

if __name__ == "__main__":
    
    #s3 bucket
    bucket = 's3a://mtabusdata/'
    #start spark session
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    dfname = fname()
    for item in dfname:
        filename = item.split('/')[-1][0:-3]
        df = spark.read.format("csv").option("inferSchema",True). \
            option("header",True).option("delimiter", "\t").load(bucket+filename)
        df.write.save(bucket+'/parquet/'+filename[0:-4],format='parquet')
        
        
        
        
        
        
        
        
        
        