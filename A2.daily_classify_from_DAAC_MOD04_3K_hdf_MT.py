#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################

'''

import numpy as np
import pandas as pd
import os
import MODIS_AOD_utilities
import Python_utilities
from datetime import datetime

#threading library
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
#base_dr = "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/"
#Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MOD04_L2.006"
#base_dr = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/"
#Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MYD04_L2.006"

base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/",
              "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/"]
#base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2016/"]

# Set Datafield name
DATAFIELD_NAME = "Optical_Depth_Land_And_Ocean"
resolution = 0.01

#Set lon, lat, resolution
Llon, Rlon, Slat, Nlat = 110, 150, 10, 60

save_dr = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                        str(Slat), str(Nlat), str(resolution), "date_3K")

#########################################  

if not os.path.exists(save_dr):
    os.makedirs(save_dr)
    print ('*'*80)
    print ("{} is created...".format(save_dr))
else :
    print ('*'*80)
    print ("{} is already exist...".format(save_dr))


#########################################
#single thread class
#########################################
class Classifier():
    def __init__(self, proc_date, threadno):
        self.proc_date = proc_date
        self.threadno = threadno
    def fetch(self):
        print("Starting process data in {0} - {1} ...\n" \
              .format(self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d')))
        self.df_proc = df[(df['fullname_dt'] >= self.proc_date[0]) & (df['fullname_dt'] < self.proc_date[1])]

        # check file exist??
        if False and (os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy' \
                                             .format(save_dr, DATAFIELD_NAME, self.proc_date[0].strftime('%Y%m%d'),
                                                     self.proc_date[1].strftime('%Y%m%d'),
                                                     str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) \
                      and os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.csv' \
                                                 .format(save_dr, DATAFIELD_NAME, self.proc_date[0].strftime('%Y%m%d'),
                                                         self.proc_date[1].strftime('%Y%m%d'),
                                                         str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))):

            print(('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are exist...'
                   .format(save_dr, DATAFIELD_NAME, self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d'),
                           str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))))

        else:
            if len(self.df_proc) == 0:
                print("There is no data in {0} - {1} ...\n" \
                      .format(self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d')))

            else:
                print("self.df_proc: {}".format(self.df_proc))
                self.processing_log = "#This file is created using Python : https://github.com/guitar79/MODIS_AOD_Python\n"
                self.processing_log += "#start date = {}, end date = {}\n" \
                    .format(self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d'))
                self.processing_log += "#Llon = {}, Rlon = {}, Slat = {}, Nlat = {}, resolution = {}\n" \
                    .format(str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))

                # make array_data
                print("{0}-{1} Start making grid arrays...\n". \
                      format(self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d')))
                array_data = MODIS_AOD_utilities.make_grid_array(Llon, Rlon, Slat, Nlat, resolution)
                print('Grid arrays are created...........\n')

                self.total_data_cnt = 0
                self.file_no = 0
                self.processing_log += "#processing file Num : {}\n".format(len(self.df_proc["fullname"]))
                self.processing_log += "#processing file list\n"
                self.processing_log += "#file No, total_data_dount, data_count, filename, mean(sst), max(sst), min(sst), min(self.longitude), max(self.longitude), min(self.latitude), max(self.latitude)\n"
                self.array_alldata = array_data.copy()
                print('self.array_alldata is copied...........\n')

                for self.fullname in self.df_proc["fullname"]:
                    # self.fullname = self.df_proc["fullname"][0]
                    self.file_no += 1
                    self.fullname_el = self.fullname.split("/")
                    print("Reading hdf file {0}/{1} : {2}\n".format(self.file_no, len(self.df_proc["fullname"]), self.fullname))

                    try:
                        self.hdf_raw, self.latitude, self.longitude, self.cntl_pt_cols, self.cntl_pt_rows \
                            = MODIS_AOD_utilities.read_MODIS_hdf_to_ndarray(self.fullname, DATAFIELD_NAME)

                        self.hdf_value = self.hdf_raw[:, :]

                        if 'bad_value_scaled' in self.hdf_raw.attributes():
                            # self.hdf_value[self.hdf_value == self.hdf_raw.attributes()['bad_value_scaled']] = np.nan
                            self.hdf_value = np.where(self.hdf_value == self.hdf_raw.attributes()['bad_value_scaled'], np.nan,
                                                 self.hdf_value)
                            # print("'bad_value_scaled' data is changed to np.nan...\n")

                        elif 'fill_value' in self.hdf_raw.attributes():
                            # self.hdf_value[self.hdf_value == self.hdf_raw.attributes()['fill_value']] = np.nan
                            self.hdf_value = np.where(self.hdf_value == self.hdf_raw.attributes()['fill_value'], np.nan, self.hdf_value)
                            # print("'fill_value' data is changed to np.nan...\n")

                        elif '_FillValue' in self.hdf_raw.attributes():
                            # self.hdf_value[self.hdf_value == self.hdf_raw.attributes()['_FillValue']] = np.nan
                            self.hdf_value = np.where(self.hdf_value == self.hdf_raw.attributes()['_FillValue'], np.nan, self.hdf_value)
                            # print("'_FillValue' data is changed to np.nan...\n")\

                        else:
                            self.hdf_value = np.where(self.hdf_value == -32767, np.nan, self.hdf_value)
                            # print("-32767 value of hdf data is changed to np.nan ...\n")

                        if 'valid_range' in self.hdf_raw.attributes():
                            self.hdf_value = np.where(self.hdf_value < self.hdf_raw.attributes()['valid_range'][0], np.nan, self.hdf_value)
                            self.hdf_value = np.where(self.hdf_value > self.hdf_raw.attributes()['valid_range'][1], np.nan, self.hdf_value)
                            # print("invalid_range data changed to np.nan...\n")

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

                        # print("self.latitude: {}".format(self.latitude))
                        # print("self.longitude: {}".format(self.longitude))
                        ### print("self.hdf_value: {}".format(self.hdf_value))
                        ### print("str(self.hdf_raw.attributes()): {}".format(str(self.hdf_raw.attributes())))
                        ### print("np.shape(self.latitude): {}".format(np.shape(self.latitude)))
                        ### print("np.shape(self.longitude): {}".format(np.shape(self.longitude)))
                        ### print("np.shape(self.hdf_value): {}".format(np.shape(self.hdf_value)))
                        ### print("len(self.cntl_pt_cols): {}".format(len(self.cntl_pt_cols)))
                        ### print("len(self.cntl_pt_rows): {}".format(len(self.cntl_pt_rows)))

                        # check self.latitude and self.longitude
                        if np.shape(self.latitude) == np.shape(self.longitude):
                            if np.shape(self.longitude)[0] != np.shape(self.hdf_value)[0]:
                                print("np.shape(self.longitude)[0] != np.shape(self.hdf_value)[0] is true...")
                                row = 0
                                self.longitude_new = np.empty(shape=(np.shape(self.hdf_value)))
                                for row in range(len(self.longitude[0])):
                                    for i in range(len(self.cntl_pt_rows) - 1):
                                        self.longitude_value = np.linspace(self.longitude[row, i], self.longitude[row, i + 1],
                                                                      self.cntl_pt_rows[i])
                                        for j in range(i):
                                            self.longitude_new[row, row + j] = self.longitude_value[j]
                                            # print("np.shape(self.longitude_new): {}".format(np.shape(self.longitude_new)))
                                self.longitude = self.longitude_new.copy()

                            elif np.shape(self.longitude)[1] != np.shape(self.hdf_value)[1]:
                                print("np.shape(self.longitude)[1] != np.shape(self.hdf_value)[1] is true...")
                                col = 0
                                self.longitude_new = np.empty(shape=(np.shape(self.hdf_value)))
                                for row in range(len(self.longitude[1])):
                                    for i in range(len(self.cntl_pt_cols) - 1):
                                        self.longitude_value = np.linspace(self.longitude[row, i], \
                                                                      self.longitude[row, i + 1], \
                                                                      self.cntl_pt_cols[i + 1] - self.cntl_pt_cols[i] + 1)

                                        for j in range(len(self.longitude_value) - 1):
                                            self.longitude_new[row, self.cntl_pt_cols[i] - 1 + j] = self.longitude_value[j]
                                        self.longitude_new[row, np.shape(self.longitude_new)[1] - 1] = self.longitude[
                                            row, np.shape(self.longitude)[1] - 1]
                                # print("np.shape(self.longitude_new): {}".format(np.shape(self.longitude_new)))
                                self.longitude = self.longitude_new.copy()
                            self.longitude = np.asarray(self.longitude)
                            # print("type(self.longitude): {}".format(type(self.longitude)))
                            print("np.shape(self.longitude): {}".format(np.shape(self.longitude)))

                            if np.shape(self.latitude)[0] != np.shape(self.hdf_value)[0]:
                                print("np.shape(self.latitude)[0] != np.shape(self.hdf_value)[0] is not same...")
                                row = 0
                                self.latitude_new = np.empty(shape=(np.shape(self.hdf_value)))
                                for row in range(len(self.latitude[0])):
                                    for i in range(len(self.cntl_pt_rows) - 1):
                                        self.latitude_value = np.linspace(self.latitude[row, i], self.latitude[row, i + 1],
                                                                     self.cntl_pt_rows[i])
                                        for j in range(i):
                                            self.latitude_new[row, row + j] = self.latitude_value[j]
                                print("np.shape(self.latitude_new): {}".format(np.shape(self.latitude_new)))
                                self.latitude = self.latitude_new.copy()

                            elif np.shape(self.latitude)[1] != np.shape(self.hdf_value)[1]:
                                print("np.shape(self.latitude)[1] != np.shape(self.hdf_value)[1] is true...")
                                col = 0
                                self.latitude_new = np.empty(shape=(np.shape(self.hdf_value)))
                                for row in range(len(self.latitude[1])):
                                    for i in range(len(self.cntl_pt_cols) - 1):
                                        self.latitude_value = np.linspace(self.latitude[row, i], \
                                                                     self.latitude[row, i + 1], \
                                                                     self.cntl_pt_cols[i + 1] - self.cntl_pt_cols[i] + 1)

                                        for j in range(len(self.latitude_value) - 1):
                                            self.latitude_new[row, self.cntl_pt_cols[i] - 1 + j] = self.latitude_value[j]
                                        self.latitude_new[row, np.shape(self.latitude_new)[1] - 1] = self.latitude[
                                            row, np.shape(self.latitude)[1] - 1]
                                print("np.shape(self.latitude_new): {}".format(np.shape(self.latitude_new)))
                                self.latitude = self.latitude_new.copy()
                            self.latitude = np.asarray(self.latitude)
                            # print("type(self.latitude): {}".format(type(self.latitude)))
                            print("np.shape(self.latitude): {}".format(np.shape(self.latitude)))

                        # check dimension
                        if not (np.shape(self.latitude) == np.shape(self.hdf_value) \
                                and np.shape(self.longitude) == np.shape(self.hdf_value)):
                            print(
                                "(np.shape(self.latitude) == np.shape(self.hdf_value) and np.shape(self.longitude == np.shape(self.hdf_value)) is not true...")

                        else:

                            self.longitude = np.where(self.longitude < Llon, np.nan, self.longitude)
                            self.longitude = np.where(self.longitude > Rlon, np.nan, self.longitude)
                            self.latitude = np.where(self.latitude > Nlat, np.nan, self.latitude)
                            self.latitude = np.where(self.latitude < Slat, np.nan, self.latitude)

                            self.lon_cood = np.array((self.longitude - Llon) / resolution)
                            self.lat_cood = np.array((Nlat - self.latitude) / resolution)

                            # print("self.longitude: {}".format(self.longitude))
                            ### print("np.shape(self.lon_cood): {}".format(np.shape(self.lon_cood)))
                            # print("self.lon_cood: {}".format(self.lon_cood))

                            # print("self.latitude: {}".format(self.latitude))
                            ### print("np.shape(self.lat_cood): {}".format(np.shape(self.lat_cood)))
                            # print("self.lat_cood: {}".format(self.lat_cood))
                            ### print("self.hdf_value: {}".format(self.hdf_value))

                        if np.isnan(self.hdf_value).all():
                            self.processing_log += "{0}, 0, 0, {1}, \n" \
                                .format(str(self.file_no), str(self.fullname))
                            print("There is no hdf data...")
                            # print("(np.isnan(self.hdf_value).all()) is true...")

                        else:
                            self.data_cnt = 0
                            self.NaN_cnt = 0
                            for i in range(np.shape(self.lon_cood)[0]):
                                for j in range(np.shape(self.lon_cood)[1]):

                                    if (not np.isnan(self.longitude[i, j])) and (not np.isnan(self.latitude[i, j])) \
                                            and (not np.isnan(self.hdf_value[i][j])):
                                        self.data_cnt += 1
                                        # self.array_alldata[int(self.lon_cood[i][j])][int(self.lat_cood[i][j])].append(self.hdf_value[i][j])
                                        self.array_alldata[int(self.lon_cood[i][j])][int(self.lat_cood[i][j])].append(
                                                (self.fullname_el[-1], self.hdf_value[i][j]))

                                        ### print("self.array_alldata[{}][{}].append({}, {})" \
                                        ###        .format(int(self.lon_cood[i][j]), int(self.lat_cood[i][j]), self.fullname_el[-1],
                                        ###            self.hdf_value[i][j]))
                                        # print("{} data added...".format(self.data_cnt))

                            self.total_data_cnt += self.data_cnt
                            self.Wlon1, self.Elon1, self.Slat1, self.Nlat1, self.Clon1, self.Clat1 = MODIS_AOD_utilities.findRangeOfMap(self.longitude,
                                                                                                          self.latitude)
                            self.processing_log += "{0}, {1}, {2}, {3}, {4:.02f}, {5:.02f}, {6:.02f}, {7:.02f}, {8:.02f}, {9:.02f}, {10:.02f}, {11}\n" \
                                    .format(str(self.file_no), str(self.total_data_cnt), str(self.data_cnt), str(self.fullname_el[-1]),
                                        np.nanmean(self.hdf_value), np.nanmax(self.hdf_value), np.nanmin(self.hdf_value),
                                        self.Wlon1, self.Elon1, self.Slat1, self.Nlat1, str(self.hdf_raw.attributes()))

                    except Exception as err:
                        # Python_utilities.write_log(err_log_file, err)
                        print(err)
                        continue

                self.processing_log += "#processing finished!!!\n"
                # print("self.array_alldata: {}".format(self.array_alldata))
                print("self.prodessing_log: {}".format(self.processing_log))

                self.array_alldata = np.array(self.array_alldata)

                print("self.array_alldata: \n{}".format(self.array_alldata))
                print("self.array_alldata.shape: {}".format(self.array_alldata.shape))

                if self.array_alldata.size == 0:
                    print("self.array_alldata.size == 0")
                else:
                    np.save('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy' \
                            .format(save_dr, DATAFIELD_NAME,
                                self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d'),
                                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), self.array_alldata)
                    Python_utilities.write_log(log_file,
                            '{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy is created...' \
                                .format(save_dr, DATAFIELD_NAME,
                                self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d'),
                                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))

                    with open('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.csv' \
                            .format(save_dr, DATAFIELD_NAME,
                                self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d'),
                                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), 'w') as f:
                        f.write(self.processing_log)

                print('#' * 60)
                Python_utilities.write_log(log_file,
                        '{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are is created.\n' \
                            .format(save_dr, DATAFIELD_NAME,
                            self.proc_date[0].strftime('%Y%m%d'), self.proc_date[1].strftime('%Y%m%d'),
                            str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))
                sys.stderr.write("Thread #%d failed...retry\n" % self.threadno)

class Classify_unit(threading.Thread):
    # def __init__(self, working_Date, threadno):
    def __init__(self, proc_dates, threadno):
        threading.Thread.__init__(self)
        self.proc_dates = proc_dates
        # self.working_Date = working_Date
        self.threadno = threadno
        sys.stderr.write('Thread #{} started...\n'.format(self.threadno))

    def run(self):
        for self.proc_date in self.proc_dates:
            fetcher = Classifier(self.proc_date, self.threadno)
            fetcher.fetch()
            sys.stderr.write('Thread #{} - fetched {}...\n'.format(self.threadno, self.proc_date))

fullnames = []
for dirName in base_drs :
    #dirName = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2002/185/"
    try :
        fullnames.extend(Python_utilities.getFullnameListOfallFiles("{}".format(dirName)))
    except Exception as err :
        #Python_utilities.write_log(err_log_file, err)
        print(err)
        continue
fullnames = sorted(fullnames)
df = pd.DataFrame({'fullname':fullnames})

df = df[df.fullname.str.contains(".hdf")]

for idx, row in df.iterrows():
    print(row["fullname"])
    df.at[idx, "fullname_dt"] = MODIS_AOD_utilities.fullname_to_datetime_for_DAAC3K(df.loc[idx, "fullname"])

df.index = df['fullname_dt']
print("make datetime column in df:\n{}".format(df))

#########################################
# make processing period tuple
#########################################
proc_dates = []
#make processing period tuple
from dateutil.relativedelta import relativedelta
set_S_datetime = datetime(2000, 6, 1) #convert startdate to date type
set_E_datetime = datetime(2022, 1, 1)

date1 = set_S_datetime
date2 = set_S_datetime

while date2 < set_E_datetime :
    date2 = date1 + relativedelta(days=1)
    dates = (date1, date2)
    proc_dates.append(dates)
    date1 = date2

print("len(proc_dates): {}".format(len(proc_dates)))
#########################################

threadno = 1

num_thread = 50
num_batches = len(proc_dates) // num_thread + 1

for batch in range(num_batches):
	#for working_Ho in working_datetimes :
	#mergeunit = merge_unit(working_Ho, threadno)
	Classifyunit = Classify_unit(proc_dates[batch*num_batches:(batch+1)*num_batches], threadno)
	#merger.daemon = True
	Classifyunit.start()
	threadno += 1