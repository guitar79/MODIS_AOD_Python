#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################
#runfile('./classify_AVHRR_asc_SST-01.py', 'daily 0.1 2019', wdir='./MODIS_hdf_Python/')
#cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && for yr in {2011..2020}; do python classify_AVHRR_asc_SST-01.py daily 0.05 $yr; done
#conda activate MODIS_hdf_Python_env && cd '/mnt/6TB1/RS_data/MODIS_AOD/MODIS_hdf_Python' && python A2.statistics_DAAC_MOD043K_AOD_alldata_and_creating_NCfile.py daily 0.05
#conda activate MODIS_hdf_Python_env && cd /mnt/Rdata/RS-data/KOSC/MODIS_hdf_Python/ && python A2.statistics_DAAC_MOD043K_AOD_alldata_and_creating_NCfile.py daily 0.05
'''

from glob import glob
from datetime import datetime
import numpy as np
import netCDF4 as nc
import os
import sys
import MODIS_hdf_utilities

log_file = os.path.basename(__file__)[:-3]+".log"
err_log_file = os.path.basename(__file__)[:-3]+"_err.log"
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))

arg_mode = True
#arg_mode = False

if arg_mode == True:
    from sys import argv  # input option

    print("argv: {}".format(argv))

    if len(argv) < 3:
        print("len(argv) < 2\nPlease input L3_perid and year \n ex) aaa.py daily 0.5")
        sys.exit()
    elif len(argv) > 3:
        print("len(argv) > 2\nPlease input L3_perid and year \n ex) aaa.py daily 0.5")
        sys.exit()
    elif argv[1] == 'daily' or argv[1] == 'weekly' or argv[1] == 'monthly':
        L3_perid, resolution = argv[1], float(argv[2])
        print("{} {} processing started...".format(argv[1], argv[2]))
    else:
        print("Please input L3_perid \n ex) aaa.py daily 0.5")
        sys.exit()
else:
    L3_perid, resolution = 'daily', 0.5

# Set Datafield name
DATAFIELD_NAME = "Optical_Depth_Land_And_Ocean"

#Set lon, lat, resolution
Llon, Rlon = 110, 150
Slat, Nlat = 10, 60

#set directory
base_dir_name = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_date/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                        str(Slat), str(Nlat), str(resolution))
save_dir_name = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                        str(Slat), str(Nlat), str(resolution), L3_perid)
if not os.path.exists(save_dir_name):
    os.makedirs(save_dir_name)
    print('*' * 80)
    print(save_dir_name, 'is created')
else:
    print('*' * 80)
    print(save_dir_name, 'is exist')

proc_dates = []

# make processing period tuple
from dateutil.relativedelta import relativedelta
s_start_date = datetime(2000, 1, 1)  # convert startdate to date type
s_end_date = datetime(2022, 1, 1)

k = 0
date1 = s_start_date
date2 = s_start_date

while date2 < s_end_date:
    k += 1
    if L3_perid == 'daily':
        date2 = date1 + relativedelta(days=1)
    elif L3_perid == 'weekly':
        date2 = date1 + relativedelta(days=8)
    elif L3_perid == 'monthly':
        date2 = date1 + relativedelta(months=1)

    date = (date1, date2, k)
    proc_dates.append(date)
    date1 = date2

#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*alldata.npy')))
print("len(fullnames): {}".format(len(fullnames)))

fullnames_dt = []
for fullname in fullnames :
    fullnames_dt.append(MODIS_hdf_utilities.fullname_to_datetime_for_L3_npyfile(fullname))

import pandas as pd

# Calling DataFrame constructor on list
df = pd.DataFrame({'fullname': fullnames, 'fullname_dt': fullnames_dt})
df.index = df['fullname_dt']
print("fullnames_dt:\n{}".format(fullnames_dt))
print("len(fullnames_dt):\n{}".format(len(fullnames_dt)))

for proc_date in proc_dates[:]:
# proc_date = proc_dates[55]
    df_proc = df[(df['fullname_dt'] >= proc_date[0]) & (df['fullname_dt'] < proc_date[1])]
    if len(df_proc) == 0 :
        print("There is no data in {0} - {1} ...\n"\
                  .format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
        
    else :
        print("df_proc: {}".format(df_proc))

        #check file exist??
        output_fullname = '{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata_mean.nc' \
                    .format(save_dir_name, DATAFIELD_NAME,
                    proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'),
                    str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))

        if os.path.exists('{0}'.format(output_fullname)) :
            # or False :
            print('{0} is already exist...'.format(output_fullname))
                
        else :         
            #if os.path.exists('{0}'.format(output_fullname)):
            #    os.remove('{0}'.format(output_fullname))
            
            print("Starting {0}\n".format(output_fullname))
            output_fullname_el = output_fullname.split("/")
            output_fileneme_el = output_fullname_el[-1].split("_")
            
            alldata_3Ds = np.empty((0, int((Rlon-Llon)/resolution)+1, int((Nlat-Slat)/resolution)+1))
            for fullname in df_proc["fullname"] :
                #fullname = df_proc["fullname"][0]
                       
                alldata = np.load(fullname, allow_pickle=True)
                
                if alldata.size == 0 : 
                    print("alldata.size : {}".format(alldata.size))
                    
                else : 
                    for i in range(alldata.shape[0]):
                        for j in range(alldata.shape[1]):
                            if len(alldata[i,j]) == 0 : 
                                alldata[i,j] = np.nan
                            else : 
                                alldata[i,j] = np.mean(list(map(lambda x:x[1], alldata[i,j])))
            
                    if alldata_3Ds.shape[0] == 0 : 
                        alldata_3Ds = alldata.reshape(1, alldata.shape[0], alldata.shape[1])
                        print("alldata_3Ds.shape : True\n{}".format(alldata_3Ds.shape))
                    else :
                        alldata_3Ds = np.append(alldata_3Ds, alldata.reshape(1, alldata.shape[0], alldata.shape[1]), axis=0)
                        print("alldata_3Ds.shape : False\n{}".format(alldata_3Ds.shape))
                
            print("alldata_3Ds.shape : final\n{}".format(alldata_3Ds.shape))

            alldata_3Ds = alldata_3Ds.astype('float64')

            alldata = np.nanmean(alldata_3Ds, axis=0, keepdims=True)
            print("alldata_3Ds.shape : final\n{}".format(alldata_3Ds.shape))

            # alldata1 = np.nan if np.all(i!=i) else np.nanmean(i)
            print("alldata.shape :\n{}".format(alldata.shape))
            print("alldata :\n{}".format(alldata))

            alldata = alldata.reshape(alldata.shape[1], alldata.shape[2])
            # alldata = alldata.transpose()
            print("alldata.shape :\n{}".format(alldata.shape))
            print("alldata :\n{}".format(alldata))
            ds = nc.Dataset('{0}'.format(output_fullname), 'w', format='NETCDF4')

            #time = ds.createDimension('time', filename_el[2])
            time = ds.createDimension('time', None)
            
            lon = ds.createDimension('longitude', alldata.shape[0])
            lat = ds.createDimension('latitude', alldata.shape[1])
            times = ds.createVariable('time', 'f4', ('time',))
            
            lons = ds.createVariable('longitude', 'f4', ('longitude',))
            lats = ds.createVariable('latitude', 'f4', ('latitude',))
            MODIS_AOD = ds.createVariable('MODIS_AOD', 'f4', ('time', 'latitude', 'longitude',))
            MODIS_AOD.units = ''
            
            lons[:] = np.arange(Llon, Rlon+resolution*0.1, resolution)
            lats[:] = np.arange(Slat, Nlat+resolution*0.1, resolution)
            #lons[:] = np.arange(Llon, Rlon, resolution)
            #lats[:] = np.arange(Slat, Nlat, resolution)
            
            MODIS_AOD[0, :, :] = alldata.transpose()
            
            ds.close()