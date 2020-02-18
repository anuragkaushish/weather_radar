#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
"""
Created on Tue May 29 16:42:53 2018

@author: Manish Sharma
"""

import pymysql
from sqlalchemy import create_engine
 
class DB(object):
    
    def getConnection(self):
        # Open database connection
        db = pymysql.connect("139.59.42.147","albert","albert123","bses_consumption")
        # prepare a cursor object using cursor() method
        # cursor = db.cursor()
        return db
    
    def getEngine(self):
        engine = create_engine('mysql+pymysql://albert:albert123@139.59.42.147:3306/bses_consumption', echo=False)
        return engine