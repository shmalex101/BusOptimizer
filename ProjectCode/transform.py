#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 09:05:48 2019

@author: monster
"""

import pandas as pd
import math
import time

def haversine(coord1, coord2):
    R = 6372800  # Earth radius in meters
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    
    phi1, phi2 = math.radians(lat1), math.radians(lat2) 
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    
    a = math.sin(dphi/2)**2 + \
        math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    
    return 2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a))

def dat_load(filename):
    data = pd.read_csv(filename, header = 0,sep="\t",engine='python')
    data['time_received'] = pd.to_datetime(data['time_received'], \
        format='%Y%m%d %H:%M:%S')
    return data

def choke_check(filename,dthresh,dnum):
    
    data = dat_load(filename)
    trip = data.inferred_trip_id.unique()
    # analysis of individual trip
    for tp in trip:
        
        #filter data for trip_id and inferred phase 'IN_PROGRESS' (not layover)
        xx = data[(data.inferred_trip_id == tp) \
                  & (data.inferred_phase == 'IN_PROGRESS') \
                  ].reset_index()
        # placeholder to keep track of choke points
        choke = pd.DataFrame(columns=['latitude', \
                                     'longitude', \
                                     'inferred_route_id'])
        for dex in range(0,xx.shape[0]-1):
            #iterate through pings in the trip and calculte distance between
            #adjacent gps points
            if xx.latitude[dex] != 0 and xx.longitude[dex] != 0:
                d = haversine([xx.latitude[dex],xx.longitude[dex]], \
                             [xx.latitude[dex+1],xx.longitude[dex+1]])
                #check if diatance between adjacent points lessd than threshold
                #if yes update choke 
                if d < dthresh:
                    choke = choke.append(pd.DataFrame([[xx.latitude[dex],
                                                xx.longitude[dex], 
                                                xx.inferred_route_id[dex]]],
                                                columns=['latitude',
                                                         'longitude',
                                                         'inferred_route_id']),
                                                ignore_index=True)
               #if no then check if sufficient number (dnum) of adjacent points 
               #are more than threshold distane. If less than reset choke.
                else:
                    if  choke.shape[0] > dnum:
                        #**********************************
                        # REPLACE PRINT WITH POSTGRES WRITE
                        #**********************************
                        print(round(choke.latitude.mean(),4), \
                              round(choke.longitude.mean(),4), \
                              choke.inferred_route_id[0])
                        #**********************************
                        choke = pd.DataFrame(columns=['latitude',
                                                      'longitude',
                                                      'inferred_route_id'])
                    else:
                        choke = pd.DataFrame(columns=['latitude',
                                                      'longitude',
                                                      'inferred_route_id'])

if __name__ == "__main__":
    tstart = time.time()
    dthresh = 100.
    dnum = 5
    filename = 'MTA-Bus-Time_.2014-10-31.txt'
    choke_check(filename,dthresh,dnum)
    tend = time.time() - tstart
                
            

    