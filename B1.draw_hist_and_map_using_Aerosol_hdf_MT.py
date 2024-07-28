#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################
#runfile('./classify_AVHRR_asc_SST-01.py', 'daily 0.1 2019', wdir='./MODIS_hdf_Python/')
#cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && for yr in {2011..2020}; do python classify_AVHRR_asc_SST-01.py daily 0.05 $yr; done
#conda activate MODIS_hdf_Python_env && cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && python classify_AVHRR_asc_SST.py daily 0.01 2011
#conda activate MODIS_hdf_Python_env && cd /mnt/Rdata/RS-data/KOSC/MODIS_hdf_Python/ && A2.daily_classify_from_DAAC_MOD04_3K_hdf.py 1.0 2019
#conda activate MODIS_hdf_Python_env && cd /mnt/6TB1/RS_data/MODIS_AOD/MODIS_hdf_Python/ && python A2.daily_classify_from_DAAC_MOD04_3K_hdf.py 0.01 2000
'''

from glob import glob
import numpy as np
import os
import _MODIS_AOD_utilities
import _Python_utilities
from datetime import datetime

#threading library
from queue import Queue
import threading
import sys

#########################################
log_dir = "logs/"
log_file = "{}{}.log".format(log_dir, os.path.basename(__file__)[:-3])
err_log_file = "{}{}_err.log".format(log_dir, os.path.basename(__file__)[:-3])
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))

#########################################
# Set variables
#########################################
#set directory
base_dr = "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/"
Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MOD04_L2.006"
base_dr = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/"
Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MYD04_L2.006"

# Set Datafield name
DATAFIELD_NAME = "Optical_Depth_Land_And_Ocean"
#########################################

#########################################
#single thread class
#########################################
class Plotter():
    def __init__(self, working_Date, threadno):
        self.working_Date = working_Date
        self.threadno = threadno
    def fetch(self):
        self.working_Jday = _MODIS_AOD_utilities.datestr_to_JDay(self.working_Date, "%Y-%m-%d")
        self.working_Date = datetime.strptime(self.working_Date, "%Y-%m-%d")

        print("Starting...   working_Date: {}".format(self.working_Date))

        # get fullnames
        self.fullnames = _Python_utilities.getFullnameListOfallFiles("{}{}".format(base_dr, self.working_Date.strftime("%Y/")))

        if len(self.fullnames) == 0:
            print("There is no file in {}...".format(self.fullnames))
        else:
            print(self.fullnames)

            for self.fullname in self.fullnames:
    
                if self.fullname[-4:].lower() == ".hdf":

                    print("Starting...   self.fullname: {}".format(self.fullname))
                    self.fullname_el = self.fullname.split("/")
                    self.filename_el = self.fullname_el[-1].split(".")
                    self.save_dr = self.fullname[:-len(self.fullname_el[-1])]

                    if os.path.exists("{}{}_map.png".format(self.save_dr, self.fullname_el[-1][:-4]))\
                        and os.path.exists("{}{}_hist.png".format(self.save_dr, self.fullname_el[-1][:-4])):
                        print("{0}{1}_map.png and {0}{1}_hist.png are already exist...".format(self.save_dr, self.fullname_el[-1][:-4]))
                    else:
                        print("Reading hdf file {0}\n".format(self.fullname))

                        try:
                            self.hdf_raw, self.latitude, self.longitude, self.cntl_pt_cols, self.cntl_pt_rows \
                                = _MODIS_AOD_utilities.read_MODIS_hdf_to_ndarray(self.fullname, DATAFIELD_NAME)
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
                            
                            #self.Wlon, self.Elon, self.Slat, self.Nlat, self.Clon, self.Clat = _MODIS_AOD_utilities.findRangeOfMap(self.longitude, self.latitude)

                            print("plotting histogram {}".format(self.fullname))
                            self.plt_hist = _MODIS_AOD_utilities.draw_histogram_hdf(self.hdf_value, self.longitude, self.latitude, self.fullname,
                                                                              DATAFIELD_NAME, Dataset_DOI)
                            self.plt_hist.savefig("{}{}_hist.png".format(self.save_dr, self.fullname_el[-1][:-4]), overwrite=True)
                            self.plt_hist.close()
                            ######################################################################################
                            _Python_utilities.write_log(log_file,
                                                          "{}{}_hist.png is created...".format(self.save_dr, self.fullname_el[-1][:-4]))

                            # Llon, Rlon, Slat, Nlat = np.min(longitude), np.max(longitude), np.min(latitude), np.max(latitude)
                            print("plotting on the map {}".format(self.fullname))
                            self.plt_map = _MODIS_AOD_utilities.draw_map_MODIS_hdf_onefile(self.hdf_value, self.longitude, self.latitude, self.fullname,
                                                                                     DATAFIELD_NAME, Dataset_DOI)
                            self.plt_map.savefig("{}{}_map.png".format(self.save_dr, self.fullname_el[-1][:-4]), overwrite=True)
                            self.plt_map.close()
                            ######################################################################################

                            _Python_utilities.write_log(log_file,
                                                          "{}{}_map.png is created...".format(self.save_dr, self.fullname_el[-1][:-4]))

                        except Exception as err:
                            sys.stderr.write("Thread #%d failed...retry\n" % self.threadno)
                            continue


class plot_unit(threading.Thread):
        # def __init__(self, working_Date, threadno):
        def __init__(self, working_datetimes, threadno):
            threading.Thread.__init__(self)
            self.working_datetimes = working_datetimes
            # self.working_Date = working_Date
            self.threadno = threadno
            sys.stderr.write('Thread #{} started...\n'.format(self.threadno))

        def run(self):
            for self.working_Date in self.working_datetimes:
                fetcher = Plotter(self.working_Date, self.threadno)
                fetcher.fetch()
                sys.stderr.write('Thread #{} - fetched {}...\n'.format(self.threadno, self.working_Date))

from dateutil.relativedelta import relativedelta
set_S_datetime = datetime(2003, 1, 1) #convert startdate to date type
set_E_datetime = datetime(2021, 12, 31)

working_datetimes = [set_S_datetime.strftime("%Y-%m-%d")]
date1 = set_S_datetime
while date1 < set_E_datetime :
    date1 += relativedelta(days=1)
    working_datetimes.append(date1.strftime("%Y-%m-%d"))
print(working_datetimes)
#########################################

threadno = 0

num_thread = 1000
num_batches = len(working_datetimes) // num_thread + 1

for batch in range(num_batches):
	#for working_Ho in working_datetimes :
	#mergeunit = merge_unit(working_Ho, threadno)
	plotunit = plot_unit(working_datetimes[batch*num_batches:(batch+1)*num_batches], threadno)
	#merger.daemon = True
	plotunit.start()
	threadno += 1