#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 10:01:06 2019

@author: monster
"""

import requests
import urllib3.request
import time
import bs4
import os
#import lzma
import pandas as pd
import os

url = 'http://web.mta.info/developers/MTA-Bus-Time-historical-data.html'
aws_bucket = 's3://mtabusdata'
r = requests.get(url)
soup = bs4.BeautifulSoup(r.content,'html.parser')
#print(soup.prettify())
main_content = soup.find('div', attrs = {'class': 'container'})
content = main_content.find_all('a')

dat = []
for link in content:
    dat.append(link['href'])
dfname = [item for item in dat if item.startswith('http://s3')]

#this thing right here seems to work. Will expand it out to larger database
#os.system('curl ' + item + ' | gunzip > ' + item.split('/')[-1][0:-3]

#dfname = dfname[0:1]

for item in dfname:
    os.system('curl ' + item + ' |xz -d >'+ item.split('/')[-1][0:-3])
    os.system('aws s3 cp '+item.split('/')[-1][0:-3] +' '+aws_bucket+'/' \
              +item.split('/')[-1][0:-3])
    os.system('rm '+item.split('/')[-1][0:-3])
 
    
    #os.system('curl ' + item + ' | gunzip > ' + item.split('/')[-1][0:-3])
    #os.system('aws s3 mv ' + item.split('/')[-1][0:-3]+' '+aws_bucket+'/'+item.split('/')[-1][0:-3])
    #os.system(' aws s3 mv '+item.split('/')[-1][0:-3]+' '+aws_bucket+'/'+ \
              #item.split('/')[-1][0:-3])
    #os.system('rm '+item.split('/')[-1][0:-3])
    #os.system('curl ' + item + ' | gzip -d > ' + item.split('/')[-1][0:-3])
