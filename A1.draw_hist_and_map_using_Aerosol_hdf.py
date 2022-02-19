#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################

'''

import numpy as np
import os
import MODIS_AOD_utilities
import Python_utilities
from datetime import datetime

#########################################
from multiprocessing import Process, Queue

class Multiprocessor():
    def __init__(self):
        self.processes = []
        self.queue = Queue()

    @staticmethod
    def _wrapper(func, queue, args, kwargs):
        ret = func(*args, **kwargs)
        queue.put(ret)

    def restart(self):
        self.processes = []
        self.queue = Queue()

    def run(self, func, *args, **kwargs):
        args2 = [func, self.queue, args, kwargs]
        p = Process(target=self._wrapper, args=args2)
        self.processes.append(p)
        p.start()

    def wait(self):
        rets = []
        for p in self.processes:
            ret = self.queue.get()
            rets.append(ret)
        for p in self.processes:
            p.join()
        return rets

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
base_dr = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/"
base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/",
            "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/",
            "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 10km/",
            "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/"]

#base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2017/001/"]
Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MYD04_L2.006"

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
                    with open("{}.csv".format(self.fullname[:(self.fullname.find(self.fullname_el[3])-1)]), 'a') as f_info:
                        f_info.write(self.hdf_info)
                        print("added {}.csv".format(self.fullname[:(self.fullname.find(self.fullname_el[3]) - 1)]))

                    print("plotting histogram {}".format(self.fullname))
                    self.plt_hist = MODIS_AOD_utilities.draw_histogram_hdf(self.hdf_value, self.longitude, self.latitude, self.fullname,
                                                                      DATAFIELD_NAME, Dataset_DOI)
                    self.plt_hist.savefig("{}{}_hist.png".format(self.save_dr, self.fullname_el[-1][:-4]), overwrite=True)
                    self.plt_hist.close()
                    ######################################################################################
                    Python_utilities.write_log(log_file,
                            "{}{}_hist.png is created...".format(self.save_dr, self.fullname_el[-1][:-4]))

                    # Llon, Rlon, Slat, Nlat = np.min(longitude), np.max(longitude), np.min(latitude), np.max(latitude)
                    print("plotting on the map {}".format(self.fullname))
                    self.plt_map = MODIS_AOD_utilities.draw_map_MODIS_hdf_onefile(self.hdf_value, self.longitude, self.latitude, self.fullname,
                                                                             DATAFIELD_NAME, Dataset_DOI)
                    self.plt_map.savefig("{}{}_map.png".format(self.save_dr, self.fullname_el[-1][:-4]), overwrite=True)
                    self.plt_map.close()
                    ######################################################################################
                    Python_utilities.write_log(log_file,
                            "{}{}_map.png is created...".format(self.save_dr, self.fullname_el[-1][:-4]))

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

myMP = Multiprocessor()
num_cpu = 3
values = []
num_batches = len(fullnames) // num_cpu + 1

for batch in range(num_batches):
    myMP.restart()
    for fullname in fullnames[batch*num_batches:(batch+1)*num_batches]:
        myMP.run(Plotter, fullname)

    print("Batch " + str(batch))
    myMP.wait()
    values.append(myMP.wait())
    print("OK batch" + str(batch))

