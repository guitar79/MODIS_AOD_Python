#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################

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
    #db_pass = 'Fb10712345!'
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
#base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2005/"]
Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MOD04_L2.006"

# Set Datafield name
DATAFIELD_NAME = "Optical_Depth_Land_And_Ocean"
#########################################

#########################################
#single  class
#########################################
class Plotter():
    def __init__(self, fullname):
        self.fullname = fullname

    #@def fetch(self):
        if self.fullname[-4:].lower() == ".hdf":

            print("Starting...   self.fullname: {}".format(self.fullname))
            self.fullname_el = self.fullname.split("/")
            self.filename_el = self.fullname_el[-1].split(".")
            self.save_dr = self.fullname[:-len(self.fullname_el[-1])]

            #if False and (os.path.exists("{}{}_map.png".format(self.save_dr, self.fullname_el[-1][:-4])) \
            if (os.path.exists("{}{}_map.png".format(self.save_dr, self.fullname_el[-1][:-4])) \
                    and os.path.exists("{}{}_hist.png".format(self.save_dr, self.fullname_el[-1][:-4]))):
                print("{0}{1}_map.png and {0}{1}_hist.png are already exist...".format(self.save_dr, self.fullname_el[-1][:-4]))
            else:
                print("Reading hdf file {0}\n".format(self.fullname))
                try:
                    self.hdf_raw, self.latitude, self.longitude, self.cntl_pt_cols, self.cntl_pt_rows \
                            = MODIS_AOD_utilities.read_MODIS_hdf_to_ndarray(self.fullname, DATAFIELD_NAME)
                    self.hdf_value = self.hdf_raw[:, :]
                    if 'bad_value_scaled' in self.hdf_raw.attributes():
                        # hdf_value[hdf_value == hdf_raw.attributes()['bad_value_scaled']] = np.nan
                        self.hdf_value = np.where(self.hdf_value == self.hdf_raw.attributes()['bad_value_scaled'], np.nan, self.hdf_value)
                        print("'bad_value_scaled' data is changed to np.nan...\n")

                    elif 'fill_value' in self.hdf_raw.attributes():
                        # hdf_value[hdf_value == hdf_raw.attributes()['fill_value']] = np.nan
                        self.hdf_value = np.where(self.hdf_value == self.hdf_raw.attributes()['fill_value'], np.nan, self.hdf_value)
                        print("'fill_value' data is changed to np.nan...\n")

                    elif '_FillValue' in self.hdf_raw.attributes():
                        # hdf_value[hdf_value == hdf_raw.attributes()['_FillValue']] = np.nan
                        self.hdf_value = np.where(self.hdf_value == self.hdf_raw.attributes()['_FillValue'], np.nan, self.hdf_value)
                        print("'_FillValue' data is changed to np.nan...\n")

                    else:
                        # hdf_value = np.where(hdf_value == hdf_value.min(), np.nan, hdf_value)
                        print("Minium value of hdf data is not changed to np.nan ...\n")

                        self.hdf_value = np.where(self.hdf_value == -32767, np.nan, self.hdf_value)
                        print("-32767 value of hdf data is changed to np.nan ...\n")

                    if 'valid_range' in self.hdf_raw.attributes():
                        # hdf_value[hdf_value < hdf_raw.attributes()['valid_range'][0]] = np.nan
                        # hdf_value[hdf_value > hdf_raw.attributes()['valid_range'][1]] = np.nan

                        self.hdf_value = np.where(self.hdf_value < self.hdf_raw.attributes()['valid_range'][0], np.nan, self.hdf_value)
                        self.hdf_value = np.where(self.hdf_value > self.hdf_raw.attributes()['valid_range'][1], np.nan, self.hdf_value)
                        print("invalid_range data changed to np.nan...\n")

                    if 'scale_factor' in self.hdf_raw.attributes() and 'add_offset' in self.hdf_raw.attributes():
                        self.scale_factor = self.hdf_raw.attributes()['scale_factor']
                        self.offset = self.hdf_raw.attributes()['add_offset']

                    elif 'slope' in self.hdf_raw.attributes() and 'intercept' in self.hdf_raw.attributes():
                        self.scale_factor = self.hdf_raw.attributes()['slope']
                        self.offset = self.hdf_raw.attributes()['intercept']

                    else:
                        self.scale_factor, self.offset = 1, 0

                    self.hdf_value = np.asarray(self.hdf_value)
                    self.hdf_value = self.hdf_value * self.scale_factor + self.offset

                    print("latitude: {}".format(self.latitude))
                    print("longitude: {}".format(self.longitude))
                    print("hdf_value: {}".format(self.hdf_value))
                    print("str(hdf_raw.attributes()): {}".format(str(self.hdf_raw.attributes())))

                    self.Wlon, self.Elon, self.Slat, self.Nlat, self.Clon, self.Clat = MODIS_AOD_utilities.findRangeOfMap(self.longitude, self.latitude)
                    #filename, Wlon, Elon, Slat, Nlat, mean(hdf_value), min(hdf_value), max(hdf_value), hdf_raw.attributes()
                    self.hdf_info = "{},{:.03f},{:.03f},{:.03f},{:.03f},{:.03f},{:.03f},{:.03f},{}\n".format(self.fullname_el[-1],
                            self.Wlon, self.Elon, self.Slat, self.Nlat,
                            np.nanmean(self.hdf_value), np.nanmin(self.hdf_value), np.nanmax(self.hdf_value),
                            self.hdf_raw.attributes())
                    print("{}.csv".format(self.fullname[:(self.fullname.find(self.fullname_el[3])-1)]))
                    with open("{}.csv".format(self.fullname[:(self.fullname.find(self.fullname_el[3]) - 1)]),
                              'a') as f_info:
                        f_info.write(self.hdf_info)
                        print("added {}.csv".format(self.fullname[:(self.fullname.find(self.fullname_el[3]) - 1)]))

                        if SET_MariaDB == True:
                            #cur = conn.cursor()
                            self.q2 = """SELECT `id` FROM `{}`.`{}` WHERE `fullname`= '{}';""".format(db_name, table_hdf_info, self.fullname)
                            self.q2_sel = cur.execute(self.q2)
                            if self.q2_sel == 0 :
                                self.q3_insert = """INSERT INTO `{0}`.`{1}`                                         
                                            (`fullname`, `Wlon`, `Elon`, `Slat`, `Nlat`, `Clon`, `Clat`, 
                                            `Mean_val`, `Min_val`, `Max_val`, `Attribute`) 
                                            VALUES ('{2}', '{3:.03f}', '{4:.03f}', '{5:.03f}', '{6:.03f}', 
                                            '{7:.03f}', '{8:.03f}', '{9:.03f}', '{10:.03f}', '{11:.03f}', '{12}');""".format(db_name, table_hdf_info,
                                                                    self.fullname, self.Wlon, self.Elon, self.Slat, self.Nlat, self.Clon, self.Clat,
                                                                    np.nanmean(self.hdf_value), np.nanmin(self.hdf_value), np.nanmax(self.hdf_value),
                                                                    "self.hdf_raw.attributes()")
                                print("self.q2_sel: {}".format(self.q2_sel))
                                print("self.q3_insert: {}".format(self.q3_insert))
                                cur.execute(self.q3_insert)

                            else :
                                self.q3_update = """UPDATE `{0}`.`{1}` 
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
                                print("self.q2_sel: {}".format(self.q2_sel))
                                print("self.q3_update: {}".format(self.q3_update))
                                cur.execute(self.q3_update)

                            conn.commit()
                            print("inserted:\n {}".format(self.q2_hdf))

                            print("plotting histogram {}".format(self.fullname))
                            self.plt_hist = MODIS_AOD_utilities.draw_histogram_hdf(self.hdf_value, self.longitude,
                                                                                   self.latitude, self.fullname,
                                                                                   DATAFIELD_NAME, Dataset_DOI)
                            self.plt_hist.savefig("{}{}_hist.png".format(self.save_dr, self.fullname_el[-1][:-4]),
                                                  overwrite=True)
                            self.plt_hist.close()
                            ######################################################################################
                            Python_utilities.write_log(log_file,
                                                       "{}{}_hist.png is created...".format(self.save_dr,
                                                                                            self.fullname_el[-1][:-4]))

                            # Llon, Rlon, Slat, Nlat = np.min(longitude), np.max(longitude), np.min(latitude), np.max(latitude)
                            print("plotting on the map {}".format(self.fullname))
                            self.plt_map = MODIS_AOD_utilities.draw_map_MODIS_hdf_onefile(self.hdf_value,
                                                                                          self.longitude, self.latitude,
                                                                                          self.fullname,
                                                                                          DATAFIELD_NAME, Dataset_DOI)
                            self.plt_map.savefig("{}{}_map.png".format(self.save_dr, self.fullname_el[-1][:-4]),
                                                 overwrite=True)
                            self.plt_map.close()
                            ######################################################################################
                            Python_utilities.write_log(log_file,
                                                       "{}{}_map.png is created...".format(self.save_dr,
                                                                                           self.fullname_el[-1][:-4]))

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
	#num_thread = 10000
	for fullname in fullnames:
		Plotter(fullname)
