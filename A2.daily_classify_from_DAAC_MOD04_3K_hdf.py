#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################
#runfile('./A2.daily_classify_from_DAAC_MOD04_3K_hdf.py', '0.1 2019', wdir='./')
#cd '/mnt/14TB1/RS-data/KOSC/MODIS_AOD_Python' && for yr in {2011..2020}; do python classify_AVHRR_asc_SST-01.py daily 0.05 $yr; done
#conda activate MODIS_hdf_Python_env && cd '/mnt/14TB1/RS-data/KOSC/MODIS_AOD_Python' && python classify_AVHRR_asc_SST.py daily 0.01 2011
#conda activate MODIS_hdf_Python_env && cd /mnt/Rdata/RS-data/KOSC/MODIS_AOD_Python/ && A2.daily_classify_from_DAAC_MOD04_3K_hdf.py 1.0 2019
#conda activate MODIS_hdf_Python_env && cd /mnt/6TB1/RS_data/MODIS_AOD/MODIS_AOD_Python/ && python A2.daily_classify_from_DAAC_MOD04_3K_hdf.py 0.01 2000
#conda activate MODIS_hdf_Python_env && cd /mnt/MODIS_AOD/MODIS_AOD_Python/ && python A2.daily_classify_from_DAAC_MOD04_3K_hdf.py 0.01 2002
#conda activate MODIS_AOD_Python_env && cd /mnt/MODIS_AOD/MODIS_AOD_Python/ && for yr in {2000..2020}; do python A2.daily_classify_from_DAAC_MOD04_3K_hdf.py 0.01 $yr; done
'''

import os
import numpy as np
import pandas as pd
from datetime import datetime
import MODIS_AOD_utilities
import Python_utilities

# threading library
from queue import Queue
import threading

#########################################
log_dir = "logs/"
log_file = "{}{}.log".format(log_dir, os.path.basename(__file__)[:-3])
err_log_file = "{}{}_err.log".format(log_dir, os.path.basename(__file__)[:-3])
print("log_file: {}".format(log_file))
print("err_log_file: {}".format(err_log_file))

#########################################
# Set variables
#########################################
base_dr = "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/"
Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MOD04_L2.006"
base_dr = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/"
Dataset_DOI = "http://dx.doi.org/10.5067/MODIS/MYD04_L2.006"

base_drs = ["../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/",
            "../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/"]

# Set Datafield name
DATAFIELD_NAME = "Optical_Depth_Land_And_Ocean"
resolution = 0.01

# Set lon, lat, resolution
Llon, Rlon, Slat, Nlat = 110, 150, 10, 60

save_dr = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                          str(Slat), str(Nlat), str(resolution), "date")

#########################################

if not os.path.exists(save_dr):
    os.makedirs(save_dr)
    print('*' * 80)
    print("{} is created...".format(save_dr))
else:
    print('*' * 80)
    print("{} is already exist...".format(save_dr))

fullnames = []
for dirName in base_drs:
    # dirName = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/2002/185/"
    try:
        fullnames.extend(Python_utilities.getFullnameListOfallFiles("{}".format(dirName)))
    except Exception as err:
        # Python_utilities.write_log(err_log_file, err)
        print(err)
        continue
        
#fullnames = sorted(fullnames)
df = pd.DataFrame({'fullname': fullnames})
df = df[df.fullname.str.contains(".hdf")]

print("Only hdf file in df:\n{}".format(df))

for idx, row in df.iterrows():
    print(row["fullname"])
    df.at[idx, "fullname_dt"] = MODIS_AOD_utilities.fullname_to_datetime_for_DAAC3K(df.loc[idx, "fullname"])
    #df["fullname_dt"] = MODIS_AOD_utilities.fullname_to_datetime_for_DAAC3K(df.fullname.str)

df.index = df['fullname_dt']
print("make datetime column in df:\n{}".format(df))

#########################################
# make processing period tuple
#########################################
proc_dates = []

from dateutil.relativedelta import relativedelta
set_S_datetime = datetime(2000, 6, 1)  # convert startdate to date type
set_E_datetime = datetime(2022, 1, 1)
date1 = set_S_datetime
date2 = set_S_datetime

while date2 < set_E_datetime:
    date2 = date1 + relativedelta(days=1)
    dates = (date1, date2)
    proc_dates.append(dates)
    date1 = date2

print("len(proc_dates): {}".format(len(proc_dates)))
#########################################

created_file_NO = 0
for proc_date in proc_dates[:]:
    # proc_date = proc_dates[0]
    print("Starting process data in {0} - {1} ...\n" \
          .format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
    df_proc = df[(df['fullname_dt'] >= proc_date[0]) & (df['fullname_dt'] < proc_date[1])]

    # check file exist??
    if False or (os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy' \
                              .format(save_dr, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'),
                                      proc_date[1].strftime('%Y%m%d'),
                                      str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) \
            and os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.txt' \
                                       .format(save_dr, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'),
                                               proc_date[1].strftime('%Y%m%d'),
                                               str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))):

        print(('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are exist...'
               .format(save_dr, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'),
                       str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))))

    else:
        if len(df_proc) == 0:
            print("There is no data in {0} - {1} ...\n" \
                  .format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))

        else:
            print("df_proc: {}".format(df_proc))
            processing_log = "#This file is created using Python : https://github.com/guitar79/MODIS_AOD_Python\n"
            processing_log += "#start date = {}, end date = {}\n" \
                .format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'))
            processing_log += "#Llon = {}, Rlon = {}, Slat = {}, Nlat = {}, resolution = {}\n" \
                .format(str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))

            # make array_data
            print("{0}-{1} Start making grid arrays...\n". \
                  format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
            array_data = MODIS_AOD_utilities.make_grid_array(Llon, Rlon, Slat, Nlat, resolution)
            print('Grid arrays are created...........\n')

            total_data_cnt = 0
            file_no = 0
            processing_log += "#processing file Num : {}\n".format(len(df_proc["fullname"]))
            processing_log += "#processing file list\n"
            processing_log += "#file No, total_data_dount, data_count, filename, mean(sst), max(sst), min(sst), min(longitude), max(longitude), min(latitude), max(latitude)\n"
            array_alldata = array_data.copy()
            print('array_alldata is copied...........\n')

            for fullname in df_proc["fullname"]:
                # fullname = df_proc["fullname"][0]
                file_no += 1
                fullname_el = fullname.split("/")
                print("Reading hdf file {0}/{1} : {2}\n".format(file_no, len(df_proc["fullname"]), fullname))

                try:
                    hdf_raw, latitude, longitude, cntl_pt_cols, cntl_pt_rows \
                        = MODIS_AOD_utilities.read_MODIS_hdf_to_ndarray(fullname, DATAFIELD_NAME)

                    hdf_value = hdf_raw[:, :]

                    if 'bad_value_scaled' in hdf_raw.attributes():
                        # hdf_value[hdf_value == hdf_raw.attributes()['bad_value_scaled']] = np.nan
                        hdf_value = np.where(hdf_value == hdf_raw.attributes()['bad_value_scaled'], np.nan, hdf_value)
                        # print("'bad_value_scaled' data is changed to np.nan...\n")

                    elif 'fill_value' in hdf_raw.attributes():
                        # hdf_value[hdf_value == hdf_raw.attributes()['fill_value']] = np.nan
                        hdf_value = np.where(hdf_value == hdf_raw.attributes()['fill_value'], np.nan, hdf_value)
                        # print("'fill_value' data is changed to np.nan...\n")

                    elif '_FillValue' in hdf_raw.attributes():
                        # hdf_value[hdf_value == hdf_raw.attributes()['_FillValue']] = np.nan
                        hdf_value = np.where(hdf_value == hdf_raw.attributes()['_FillValue'], np.nan, hdf_value)
                        # print("'_FillValue' data is changed to np.nan...\n")\

                    else:
                        hdf_value = np.where(hdf_value == -32767, np.nan, hdf_value)
                        # print("-32767 value of hdf data is changed to np.nan ...\n")

                    if 'valid_range' in hdf_raw.attributes():
                        hdf_value = np.where(hdf_value < hdf_raw.attributes()['valid_range'][0], np.nan, hdf_value)
                        hdf_value = np.where(hdf_value > hdf_raw.attributes()['valid_range'][1], np.nan, hdf_value)
                        # print("invalid_range data changed to np.nan...\n")

                    if 'scale_factor' in hdf_raw.attributes() and 'add_offset' in hdf_raw.attributes():
                        scale_factor = hdf_raw.attributes()['scale_factor']
                        offset = hdf_raw.attributes()['add_offset']

                    elif 'slope' in hdf_raw.attributes() and 'intercept' in hdf_raw.attributes():
                        scale_factor = hdf_raw.attributes()['slope']
                        offset = hdf_raw.attributes()['intercept']

                    else:
                        scale_factor, offset = 1, 0

                    hdf_value = np.asarray(hdf_value)
                    hdf_value = hdf_value * scale_factor + offset

                    # print("latitude: {}".format(latitude))
                    # print("longitude: {}".format(longitude))
                    print("hdf_value: {}".format(hdf_value))
                    print("str(hdf_raw.attributes()): {}".format(str(hdf_raw.attributes())))
                    print("np.shape(latitude): {}".format(np.shape(latitude)))
                    print("np.shape(longitude): {}".format(np.shape(longitude)))
                    print("np.shape(hdf_value): {}".format(np.shape(hdf_value)))
                    print("len(cntl_pt_cols): {}".format(len(cntl_pt_cols)))
                    print("len(cntl_pt_rows): {}".format(len(cntl_pt_rows)))

                    # check latitude and longitude
                    if np.shape(latitude) == np.shape(longitude):
                        if np.shape(longitude)[0] != np.shape(hdf_value)[0]:
                            print("np.shape(longitude)[0] != np.shape(hdf_value)[0] is true...")
                            row = 0
                            longitude_new = np.empty(shape=(np.shape(hdf_value)))
                            for row in range(len(longitude[0])):
                                for i in range(len(cntl_pt_rows) - 1):
                                    longitude_value = np.linspace(longitude[row, i], longitude[row, i + 1],
                                                                  cntl_pt_rows[i])
                                    for j in range(i):
                                        longitude_new[row, row + j] = longitude_value[j]
                                        # print("np.shape(longitude_new): {}".format(np.shape(longitude_new)))
                            longitude = longitude_new.copy()

                        elif np.shape(longitude)[1] != np.shape(hdf_value)[1]:
                            print("np.shape(longitude)[1] != np.shape(hdf_value)[1] is true...")
                            col = 0
                            longitude_new = np.empty(shape=(np.shape(hdf_value)))
                            for row in range(len(longitude[1])):
                                for i in range(len(cntl_pt_cols) - 1):
                                    longitude_value = np.linspace(longitude[row, i], \
                                                                  longitude[row, i + 1], \
                                                                  cntl_pt_cols[i + 1] - cntl_pt_cols[i] + 1)

                                    for j in range(len(longitude_value) - 1):
                                        longitude_new[row, cntl_pt_cols[i] - 1 + j] = longitude_value[j]
                                    longitude_new[row, np.shape(longitude_new)[1] - 1] = longitude[
                                        row, np.shape(longitude)[1] - 1]
                            # print("np.shape(longitude_new): {}".format(np.shape(longitude_new)))
                            longitude = longitude_new.copy()
                        longitude = np.asarray(longitude)
                        # print("type(longitude): {}".format(type(longitude)))
                        print("np.shape(longitude): {}".format(np.shape(longitude)))

                        if np.shape(latitude)[0] != np.shape(hdf_value)[0]:
                            print("np.shape(latitude)[0] != np.shape(hdf_value)[0] is not same...")
                            row = 0
                            latitude_new = np.empty(shape=(np.shape(hdf_value)))
                            for row in range(len(latitude[0])):
                                for i in range(len(cntl_pt_rows) - 1):
                                    latitude_value = np.linspace(latitude[row, i], latitude[row, i + 1],
                                                                 cntl_pt_rows[i])
                                    for j in range(i):
                                        latitude_new[row, row + j] = latitude_value[j]
                            print("np.shape(latitude_new): {}".format(np.shape(latitude_new)))
                            latitude = latitude_new.copy()

                        elif np.shape(latitude)[1] != np.shape(hdf_value)[1]:
                            print("np.shape(latitude)[1] != np.shape(hdf_value)[1] is true...")
                            col = 0
                            latitude_new = np.empty(shape=(np.shape(hdf_value)))
                            for row in range(len(latitude[1])):
                                for i in range(len(cntl_pt_cols) - 1):
                                    latitude_value = np.linspace(latitude[row, i], \
                                                                 latitude[row, i + 1], \
                                                                 cntl_pt_cols[i + 1] - cntl_pt_cols[i] + 1)

                                    for j in range(len(latitude_value) - 1):
                                        latitude_new[row, cntl_pt_cols[i] - 1 + j] = latitude_value[j]
                                    latitude_new[row, np.shape(latitude_new)[1] - 1] = latitude[
                                        row, np.shape(latitude)[1] - 1]
                            print("np.shape(latitude_new): {}".format(np.shape(latitude_new)))
                            latitude = latitude_new.copy()
                        latitude = np.asarray(latitude)
                        # print("type(latitude): {}".format(type(latitude)))
                        print("np.shape(latitude): {}".format(np.shape(latitude)))

                    # check dimension
                    if not (np.shape(latitude) == np.shape(hdf_value) \
                            and np.shape(longitude) == np.shape(hdf_value)):
                        print(
                            "(np.shape(latitude) == np.shape(hdf_value) and np.shape(longitude == np.shape(hdf_value)) is not true...")

                    else:

                        longitude = np.where(longitude < Llon, np.nan, longitude)
                        longitude = np.where(longitude > Rlon, np.nan, longitude)
                        latitude = np.where(latitude > Nlat, np.nan, latitude)
                        latitude = np.where(latitude < Slat, np.nan, latitude)

                        lon_cood = np.array((longitude - Llon) / resolution)
                        lat_cood = np.array((Nlat - latitude) / resolution)

                        # print("longitude: {}".format(longitude))
                        print("np.shape(lon_cood): {}".format(np.shape(lon_cood)))
                        # print("lon_cood: {}".format(lon_cood))

                        # print("latitude: {}".format(latitude))
                        print("np.shape(lat_cood): {}".format(np.shape(lat_cood)))
                        # print("lat_cood: {}".format(lat_cood))
                        print("hdf_value: {}".format(hdf_value))

                    if np.isnan(hdf_value).all():
                        processing_log += "{0}, 0, 0, {1}, \n" \
                            .format(str(file_no), str(fullname))
                        print("There is no hdf data...")
                        # print("(np.isnan(hdf_value).all()) is true...")

                    else:
                        data_cnt = 0
                        NaN_cnt = 0
                        for i in range(np.shape(lon_cood)[0]):
                            for j in range(np.shape(lon_cood)[1]):

                                if (not np.isnan(longitude[i, j])) and (not np.isnan(latitude[i, j])) \
                                        and (not np.isnan(hdf_value[i][j])):
                                    data_cnt += 1
                                    # array_alldata[int(lon_cood[i][j])][int(lat_cood[i][j])].append(hdf_value[i][j])
                                    array_alldata[int(lon_cood[i][j])][int(lat_cood[i][j])].append(
                                        (fullname_el[-1], hdf_value[i][j]))

                                    print("array_alldata[{}][{}].append({}, {})" \
                                          .format(int(lon_cood[i][j]), int(lat_cood[i][j]), fullname_el[-1],
                                                  hdf_value[i][j]))
                                    # print("{} data added...".format(data_cnt))

                        total_data_cnt += data_cnt
                        Wlon, Elon, Slat, Nlat, Clon, Clat = MODIS_AOD_utilities.findRangeOfMap(longitude, latitude)
                        processing_log += "{0}, {1}, {2}, {3}, {4:.02f}, {5:.02f}, {6:.02f}, {7:.02f}, {8:.02f}, {9:.02f}, {10:.02f}, {11}\n" \
                            .format(str(file_no), str(total_data_cnt), str(data_cnt), str(fullname),
                                np.nanmean(hdf_value), np.nanmax(hdf_value), np.nanmin(hdf_value),
                                Wlon, Elon, Slat, Nlat, str(hdf_raw.attributes()))

                except Exception as err:
                    # Python_utilities.write_log(err_log_file, err)
                    print(err)
                    continue

            processing_log += "#processing finished!!!\n"
            # print("array_alldata: {}".format(array_alldata))
            print("prodessing_log: {}".format(processing_log))

            array_alldata = np.array(array_alldata)

            print("array_alldata: \n{}".format(array_alldata))
            print("array_alldata.shape: {}".format(array_alldata.shape))

            if array_alldata.size == 0:
                print("array_alldata.size == 0")
            else:
                np.save('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy' \
                    .format(save_dr, DATAFIELD_NAME,
                        proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'),
                        str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), array_alldata)

                with open('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.txt' \
                    .format(save_dr, DATAFIELD_NAME,
                        proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'),
                        str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), 'w') as f:
                    f.write(processing_log)

            print('#' * 60)
            Python_utilities.write_log(log_file,
                '{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are is created.\n{9} files are finished...' \
                .format(save_dr, DATAFIELD_NAME,
                    proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'),
                    str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution),
                    str(created_file_NO)))