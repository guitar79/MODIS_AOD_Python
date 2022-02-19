# -*- coding: utf-8 -*-
# Auther guitar79@naver.com

"""
CREATE TABLE "hdf_info" (
	"id"	INTEGER NOT NULL UNIQUE,
	"fullname"	TEXT,
	"Wlon"	TEXT,
	"Elon"	TEXT,
	"Slat"	TEXT,
	"Nlat"	TEXT,
	"Clon"	TEXT,
	"Clat"	TEXT,
	"Mean_val"	TEXT,
	"Min_val"	TEXT,
	"Max_val"	TEXT,
	"Attribute"	TEXT,
	PRIMARY KEY('id' AUTOINCREMENT)
);
"""

import numpy as np
import os
from pathlib import Path

import Python_utilities

np.set_printoptions(threshold=100)

import sqlite3
from sqlite3 import Error

#########################################
log_dir = 'logs/'
log_file = '{}{}.log'.format(log_dir, os.path.basename(__file__)[:-3])
err_log_file = '{}{}_err.log'.format(log_dir, os.path.basename(__file__)[:-3])
print ('log_file: {}'.format(log_file))
print ('err_log_file: {}'.format(err_log_file))

#########################################
# Set variables
#########################################
#set directory
base_dr = '../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/'
base_dr = '../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/'
base_drs = ['../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/',
            '../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/',
            '../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 10km/',
            '../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/']

base_dr = '../Aerosol/'
base_dr = ''

DB_fn = 'Aerosel.db'

table_hdf_info = 'hdf_info'
# def create_connection(drbase+drin+infile):

conn = Python_utilities.create_connection(r'{}{}'.format(base_dr, DB_fn))

cur = conn.cursor()

q1 = """CREATE TABLE IF NOT EXISTS '{}' (
	'id'	INTEGER NOT NULL UNIQUE,
	'fullname'	TEXT,
	'Wlon'	TEXT,
	'Elon'	TEXT,
	'Slat'	TEXT,
	'Nlat'	TEXT,
	'Clon'	TEXT,
	'Clat'	TEXT,
	'Mean_val'	TEXT,
	'Min_val'	TEXT,
	'Max_val'	TEXT,
	'Attribute'	TEXT,
	PRIMARY KEY('id' AUTOINCREMENT));""".format(table_hdf_info)

cur.execute(q1)

conn.commit()
conn.close()

