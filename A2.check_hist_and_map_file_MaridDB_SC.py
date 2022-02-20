#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################
ALTER USER 'modis'@'%' IDENTIFIED BY 'Modis12345!';
'''

from glob import glob
import numpy as np
import os
import MODIS_AOD_utilities
import Python_utilities

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
base_drs = ["../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/",
            "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/"]
base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2005/"]
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
        print("Starting: {}".format(self.fullname))

        self.fullname_el = self.fullname.split("/")
        self.filename_el = self.fullname_el[-1].split("/")

        try :
            #cur = conn.cursor()
            self.q2 = """SELECT `id` FROM `{}`.`{}` WHERE `histogram_png`= '{}{}_hist.png';""".format(db_name, table_hdf_info,
                                                      self.fullname[:(self.fullname.find(self.fullname_el[-1])-1)], self.fullname_el[-1][:-4])
            self.q2_sel = cur.execute(self.q2)
            if self.q2_sel == 0 :

                self.q2_hdf = """INSERT INTO `{0}`.`{1}`                                         
                            (`fullname`, `histogram_png`, `histogram_png_DT`) 
                            VALUES ('{2}', '{3:.03f}', '{4:.03f}');""".format(db_name, table_hdf_info,
                                                    self.fullname, self.Wlon, self.Elon, self.Slat, self.Nlat, self.Clon, self.Clat,
                                                    np.nanmean(self.hdf_value), np.nanmin(self.hdf_value), np.nanmax(self.hdf_value),
                                                    "self.hdf_raw.attributes()")

            else :
                self.q2_hdf = """UPDATE `{0}`.`{1}` 
                            SET `fullname` = '{2}' , 
                            `Wlon` = '{3:.03f}', 
                            `Elon` = '{4:.03f}', 
                            `Slat` = '{5:.03f}', 
                            `Nlat` = '{6:.03f}', 
                            `Clon` = '{7:.03f}', 
                            `Clat` = '{8:.03f}', 
                            `Mean_val` = '{9:.03f}', 
                            `Min_val` = '{10:.03f}', 
                            `Max_val` = '{11:.03f}', 
                            `Attribute` = '{12}'  
                            WHERE `{1}`.`id` = {13};""".format(db_name, table_hdf_info,
                                                    self.fullname, self.Wlon, self.Elon, self.Slat, self.Nlat, self.Clon, self.Clat,
                                                    np.nanmean(self.hdf_value), np.nanmin(self.hdf_value), np.nanmax(self.hdf_value),
                                                    "self.hdf_raw.attributes()", self.q2_sel)
            cur.execute(self.q2_hdf)
            conn.commit()
            print("inserted:\n {}".format(self.q2_hdf))

        except Exception as err:
            Python_utilities.write_log(err_log_file,
                                       "{}, error: {}".format(self.fullname_el[-1], err))

fullnames = []
for dirName in base_drs :
    #dirName = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2002/185/"
    try :
        fullnames.extend(Python_utilities.getFullnameListOfallFiles("{}".format(dirName)))
    except Exception as err :
        #Python_utilities.write_log(err_log_file, err)
        print(err)
        continue
#fullnames = sorted(fullnames)
#########################################

if __name__ == '__main__' :
	for fullname in fullnames:
		Png_cheker(fullname)
