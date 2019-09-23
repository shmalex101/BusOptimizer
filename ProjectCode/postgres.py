#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 15:51:40 2019

@author: monster
"""

from pyspark.sql import DataFrameWriter
import os

class PostgresConnector(object):
    def __init__(self):
        self.database_name = 'postgres'
        self.hostname = 'ip-10-0-0-8'
        self.url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(hostname=self.hostname, db=self.database_name)
        self.properties = {"user":'alex',
                      "password":'password',
                      "driver": "org.postgresql.Driver"
                     }

    def get_writer(self, df):
        return DataFrameWriter(df)

    def write(self, df, table, mode):
        my_writer = self.get_writer(df)
        my_writer.jdbc(self.url_connect, table, mode, self.properties)
        
#check if pipeline working