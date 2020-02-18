#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
"""
Created on Tue May 29 16:42:53 2019

@author: Anurag Sharma
"""

import pymysql
from sqlalchemy import create_engine
 
class DB(object):
    
    def getConnection(self):
        # Open database connection
        db = pymysql.connect("139.59.42.147","******","********","bses_consumption")
        # prepare a cursor object using cursor() method
        # cursor = db.cursor()
        return db
    
    def getEngine(self):
        engine = create_engine('mysql+pymysql://******:********@********:3306/bses_consumption', echo=False)
        return engine
