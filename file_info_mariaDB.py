# -*- coding: utf-8 -*-
# Author guitar79@naver.com

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


CREATE TABLE IF NOT EXISTS `MODIS_Aerosol`.`hdf_info` (
    `id` INT NOT NULL AUTO_INCREMENT ,
    `fullname` VARCHAR(16384) default NULL ,
    `Wlon` VARCHAR(16) default NULL ,
    `Elon` VARCHAR(16) default NULL ,
    `Slat` VARCHAR(16) default NULL ,
	`Nlat` VARCHAR(16) default NULL ,
	`Clon` VARCHAR(16) default NULL ,
	`Clat` VARCHAR(16) default NULL ,
	`Mean_val` VARCHAR(16) default NULL ,
	`Min_val` VARCHAR(16) default NULL ,
	`Max_val` VARCHAR(16) default NULL ,
	`Attribute` TEXT default NULL ,
	`histogram_png` INT default NULL ,
	`histogram_png_DT` DATETIME default NULL ,
	`map_png` INT default NULL ,
    `map_png_DT` DATETIME default NULL ,
    PRIMARY KEY (`id`)) ENGINE = InnoDB;
    
"""

import numpy as np
import os
from pathlib import Path

import _Python_utilities
import pymysql

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
#base_dr = ''

DB_fn = 'Aerosel_file_info.sqlite'

#mariaDB info
db_host = '192.168.0.20'
db_user = 'root'
db_pass = 'Pkh19255102@'
db_name = 'MODIS_Aerosol'

table_hdf_info = 'hdf_info'
# def create_connection(drbase+drin+infile):

conn = pymysql.connect(host=db_host, port=3306,
                      user=db_user, password=db_pass, db=db_name,
                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

cur = conn.cursor()

q1 = """CREATE TABLE IF NOT EXIST `{}`.`{}` (
    `id` INT NOT NULL AUTO_INCREMENT ,
    `fullname` VARCHAR(16384) default NULL ,
    `Wlon` VARCHAR(16) default NULL ,
    `Elon` VARCHAR(16) default NULL ,
    `Slat` VARCHAR(16) default NULL ,
	`Nlat` VARCHAR(16) default NULL ,
	`Clon` VARCHAR(16) default NULL ,
	`Clat` VARCHAR(16) default NULL ,
	`Mean_val` VARCHAR(16) default NULL ,
	`Min_val` VARCHAR(16) default NULL ,
	`Max_val` VARCHAR(16) default NULL ,
	`Attribute` TEXT default NULL ,
	`histogram_png` INT default NULL ,
	`histogram_png_DT` DATETIME default NULL ,
	`map_png` INT default NULL ,
    `map_png_DT` DATETIME default NULL ,
    PRIMARY KEY (`id`)) ENGINE = InnoDB;""".format(db_name, table_hdf_info)

cur.execute(q1)
conn.commit()


conn.close()

