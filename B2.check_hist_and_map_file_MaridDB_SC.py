#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################
ALTER USER 'modis'@'%' IDENTIFIED BY 'Modis12345!';
'''

from glob import glob
import numpy as np
import os
import time
from datetime import datetime
import _MODIS_AOD_utilities
import _Python_utilities

#########################################
log_dir = "logs/"
log_file = "{}{}.log".format(log_dir, os.path.basename(__file__)[:-3])
err_log_file = "{}{}_err.log".format(log_dir, os.path.basename(__file__)[:-3])
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))
#########################################

#########################################
# mariaDB info
#########################################
SET_MariaDB = True
if SET_MariaDB == True :
    import pymysql
    db_host = '192.168.0.20'
    db_user = 'root'
    db_pass = 'Pkh19255102@'
    db_user = 'modis'
    db_pass = 'Modis12345!'
    #db_user = 'FB107'
    #db_pass = 'Gses12345!'
    db_name = 'MODIS_Aerosol'
    table_hdf_info = 'hdf_info'

    conn = pymysql.connect(host=db_host, port=3306,
                           user=db_user, password=db_pass, db=db_name,
                           charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    cur = conn.cursor()

    q1 = """CREATE TABLE IF NOT EXISTS `{}`.`{}` (
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
        `Update_DT` TIMESTAMP on update CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
        PRIMARY KEY (`id`)) ENGINE = InnoDB;""".format(db_name, table_hdf_info)

    cur.execute(q1)
    conn.commit()
#########################################

#########################################
# Set variables
#########################################
#set directory
#base_dr = "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/"
#base_dr = "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/"
#base_dr = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/"
#base_dr = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 10km/"

base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/",
            "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/",
            "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/",
            "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/"]
#base_drs = ["../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/",
#            "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/"]
#base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2005/"]
Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MOD04_L2.006"

# Set Datafield name
DATAFIELD_NAME = "Optical_Depth_Land_And_Ocean"
#########################################

#########################################
#single  class
#########################################
class Png_cheker():
    def __init__(self, fullname):
        self.fullname = fullname
        if self.fullname[-4:].lower() == ".hdf":
            print("Starting: {}".format(self.fullname))

            self.fullname_el = self.fullname.split("/")
            self.filename_el = self.fullname_el[-1].split("/")

            try :
                #cur = conn.cursor()

                self.q2 = """SELECT `id` FROM `{}`.`{}` WHERE `fullname`= '{}';""".format(db_name,
                                table_hdf_info, self.fullname)
                self.q2_sel = cur.execute(self.q2)
                print("self.q2: {}".format(self.q2))

                '''
                self.q3 = """SELECT `id` FROM `{}`.`{}` WHERE `histogram_png`= '{}{}_hist.png';""".format(db_name,
                                table_hdf_info, self.fullname[:(self.fullname.find(self.fullname_el[-1]))],
                                self.fullname_el[-1][:-4])
                self.q3_sel = cur.execute(self.q3)
                print("self.q3: {}".format(self.q3))
                print("self.q3_sel: {}".format(self.q3_sel))

                self.q4 = """SELECT `id` FROM `{}`.`{}` WHERE `fullname`= '{}';""".format(db_name,
                                table_hdf_info, self.fullname)
                self.q4_sel = cur.execute(self.q4)
                print("self.q3: {}".format(self.q4))
                print("self.q3_sel: {}".format(self.q4_sel))
                '''

                self.hist_png_exist = os.path.exists(
                                '{}{}_hist.png'.format(self.fullname[:(self.fullname.find(self.fullname_el[-1]))],
                                           self.fullname_el[-1][:-4]))

                if self.q2_sel == 0:
                    print("self.q2_sel: {}".format(self.q2_sel))
                    if self.hist_png_exist == 1:
                        print("self.hist_png_exist: {}".format(self.hist_png_exist))
                        self.hist_png_DT = time.ctime(os.path.getctime('{}{}_hist.png'.format(self.fullname[:(self.fullname.find(self.fullname_el[-1]))],
                                                   self.fullname_el[-1][:-4])))
                        print("self.hist_png_DT: {}".format(self.hist_png_DT))
                        self.hist_png_DT = datetime.strptime(self.hist_png_DT, "%a %b %d %H:%M:%S %Y")
                        print("self.hist_png_DT.strftime('%Y-%m-%d %H:%M:%S'): {}".format(self.hist_png_DT.strftime('%Y-%m-%d %H:%M:%S')))
                        self.q3_insert = """INSERT INTO `{0}`.`{1}`                                         
                                    (`fullname`, `histogram_png`, `histogram_png_DT`) 
                                    VALUES ('{2}', '{3}', '{4}');""".format(db_name, table_hdf_info, self.fullname,
                                                    int(self.hist_png_exist), self.hist_png_DT)

                        print("self.q3_insert: {}".format(self.q3_insert))

                    else :
                        print("self.hist_png_exist: {}".format(self.hist_png_exist))
                        self.q3_insert = """INSERT INTO `{0}`.`{1}`                                         
                                    (`fullname`, `histogram_png`) 
                                    VALUES ('{2}', '{3}');""".format(db_name, table_hdf_info, self.fullname, int(self.hist_png_exist))

                        print("self.q3_insert: {}".format(self.q3_insert))

                    cur.execute(self.q3_insert)

                else:
                    print("self.q2_sel: {}".format(self.q2_sel))
                    if self.hist_png_exist == 1:
                        print("self.hist_png_exist: {}".format(self.hist_png_exist))
                        self.q3_update = """UPDATE `{0}`.`{1}` 
                                        SET `fullname` = '{2}' , 
                                        `histogram_png` = '{3}', 
                                        `histogram_png_DT` = '{4}',  
                                        WHERE `{1}`.`id` = {5};""".format(db_name,
                                            table_hdf_info, self.fullname, int(self.hist_png_exist), self.hist_png_DT, self.q2_sel)
                        print("self.q3_update: {}".format(self.q3_update))
                    else:
                        print("self.hist_png_exist: {}".format(self.hist_png_exist))
                        self.q3_update = """UPDATE `{0}`.`{1}` 
                                        SET `fullname` = '{2}' , 
                                        `histogram_png` = '{3}',   
                                        WHERE `{1}`.`id` = {4};""".format(db_name,
                                            table_hdf_info, self.fullname, int(self.hist_png_exist), self.q2_sel)
                        print("self.q3_update: {}".format(self.q3_update))
                    cur.execute(self.q3_update)

                self.map_png_exist = os.path.exists(
                        '{}{}_map.png'.format(self.fullname[:(self.fullname.find(self.fullname_el[-1]))],
                                              self.fullname_el[-1][:-4]))

                if self.q2_sel == 0:
                    print("self.q2_sel: {}".format(self.q2_sel))
                    if self.map_png_exist == 1:
                        print("self.map_png_exist: {}".format(self.map_png_exist))
                        self.map_png_DT = time.ctime(os.path.getctime('{}{}_map.png'.format(self.fullname[:(self.fullname.find(self.fullname_el[-1]))],
                                                   self.fullname_el[-1][:-4])))
                        print("self.map_png_DT: {}".format(self.map_png_DT))
                        self.map_png_DT = datetime.strptime(self.map_png_DT, "%a %b %d %H:%M:%S %Y")
                        print("self.map_png_DT.strftime('%Y-%m-%d %H:%M:%S'): {}".format(self.map_png_DT.strftime('%Y-%m-%d %H:%M:%S')))
                        self.q4_insert = """INSERT INTO `{0}`.`{1}`                                         
                                    (`fullname`, `map_png`, `map_png_DT`) 
                                    VALUES ('{2}', '{3}', '{4}');""".format(db_name, table_hdf_info, self.fullname,
                                                    int(self.map_png_exist), self.map_png_DT)

                        print("self.q4_insert: {}".format(self.q4_insert))

                    else :
                        print("self.map_png_exist: {}".format(self.map_png_exist))
                        self.q4_insert = """INSERT INTO `{0}`.`{1}`                                         
                                    (`fullname`, `map_png`) 
                                    VALUES ('{2}', '{3}');""".format(db_name, table_hdf_info, self.fullname, int(self.map_png_exist))

                        print("self.q4_insert: {}".format(self.q4_insert))

                    cur.execute(self.q4_insert)

                else:
                    print("self.q2_sel: {}".format(self.q2_sel))
                    if self.map_png_exist == 1:
                        print("self.map_png_exist: {}".format(self.map_png_exist))
                        self.q4_update = """UPDATE `{0}`.`{1}` 
                                        SET `fullname` = '{2}' , 
                                        `map_png` = '{3}', 
                                        `map_png_DT` = '{4}',  
                                        WHERE `{1}`.`id` = {5};""".format(db_name,
                                            table_hdf_info, self.fullname, int(self.map_png_exist), self.map_png_DT, self.q2_sel)
                        print("self.q4_update: {}".format(self.q4_update))
                    else:
                        print("self.map_png_exist: {}".format(self.map_png_exist))
                        self.q4_update = """UPDATE `{0}`.`{1}` 
                                        SET `fullname` = '{2}' , 
                                        `map_png` = '{3}',   
                                        WHERE `{1}`.`id` = {4};""".format(db_name,
                                            table_hdf_info, self.fullname, int(self.map_png_exist), self.q2_sel)
                        print("self.q4_update: {}".format(self.q4_update))
                    cur.execute(self.q4_update)

                conn.commit()
                print("#"*30)

            except Exception as err:
                _Python_utilities.write_log(err_log_file,
                                           "{}, error: {}".format(self.fullname_el[-1], err))

#SELECT `fullname` FROM `hdf_info` WHERE `histogram_png` IS NULL ORDER BY `histogram_png` DESC

fullnames = []
for dirName in base_drs :
    #dirName = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2002/185/"
    try :
        fullnames.extend(_Python_utilities.getFullnameListOfallFiles("{}".format(dirName)))
    except Exception as err :
        #_Python_utilities.write_log(err_log_file, err)
        print(err)
        continue
#fullnames = sorted(fullnames)
#########################################

if __name__ == '__main__' :
	for fullname in fullnames:
		Png_cheker(fullname)
