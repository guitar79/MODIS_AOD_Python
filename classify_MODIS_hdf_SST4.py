#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
Created on Wed Oct 21 14:38:31 2020
@author: guitar79
created by Kevin
#Open hdf file
NameError: name 'SD' is not defined
conda install -c conda-forge pyhdf

runfile('./classify_MODIS_hdf_MP-01.py', 'daily 2011', wdir='./KOSC_MODIS_SST_Python/')


len(npy_data[795,183])
np.mean(npy_data[795,183])

hdf_data = np.load(f_name1, allow_pickle=True)

'''

from glob import glob
from datetime import datetime
import numpy as np
import os
import sys
from sys import argv
import _MODIS_AOD_utilities

#script, L3_perid, yr = argv # Input L3_perid : 'weekly' 'monthly' 'daily'
print("argv: {}".format(argv))
if len(argv) < 3 :
    print ("len(argv) < 2\nPlease input L3_perid and year \n ex) aaa.py daily 2016\n ex) aaa.py weekly 2016\n ex) aaa.py monthly 2016")
    sys.exit()
elif len(argv) > 3 :
    print ("len(argv) > 2\nPlease input L3_perid and year \n ex) aaa.py daily 2016\n ex) aaa.py weekly 2016\n ex) aaa.py monthly 2016")
    sys.exit()
elif argv[1] == 'daily' or argv[1] == 'weekly' or argv[1] == 'monthly' :
    L3_perid, yr = argv[1], int(argv[2])
    print("{}, {} processing started...".format(argv[1], argv[2]))
else :
    print("Please input L3_perid and year \n ex) aaa.py daily 2016\n ex) aaa.py weekly 2016\n ex) aaa.py monthly 2016")
    sys.exit()
#L3_perid, yr = "daily", 2019
add_log = True
if add_log == True :
    log_file = 'MODIS_SST_python.log'
    err_log_file = 'MODIS_SST_python.log'
    
DATAFIELD_NAME = "sst4"
#Set lon, lat, resolution
Llon, Rlon = 110, 150
Slat, Nlat = 10, 60
resolution = 0.05
base_dir_name = '../MODIS_L2_SST/'
save_dir_name = "../{0}_L3/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                        str(Slat), str(Nlat), str(resolution), L3_perid)
if not os.path.exists(save_dir_name):
    os.makedirs(save_dir_name)
    print ('*'*80)
    print (save_dir_name, 'is created')
else :
    print ('*'*80)
    print (save_dir_name, 'is exist')

years = range(yr, yr+1)

proc_dates = []

#make processing period tuple
for year in years:
    dir_name = base_dir_name + str(year) + '/'

    from dateutil.relativedelta import relativedelta
    s_start_date = datetime(year, 1, 1) #convert startdate to date type
    s_end_date = datetime(year+1, 1, 1)

    k=0
    date1 = s_start_date
    date2 = s_start_date
    
    while date2 < s_end_date :
        k += 1
        if L3_perid == 'daily' :
            date2 = date1 + relativedelta(days=1)
        elif L3_perid == 'weekly' :
            date2 = date1 + relativedelta(days=8)
        elif L3_perid == 'monthly' :
            date2 = date1 + relativedelta(months=1)

        date = (date1, date2, k)
        proc_dates.append(date)
        date1 = date2

#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*.hdf')))

fullnames_dt = []
for fullname in fullnames :
    fullnames_dt.append(MODIS_hdf_utilities.fullname_to_datetime_for_KOSC_MODIS_SST(fullname))

import pandas as pd 

len(fullnames)
len(fullnames_dt)

# Calling DataFrame constructor on list 
df = pd.DataFrame({'fullname':fullnames,'fullname_dt':fullnames_dt})
df.index = df['fullname_dt']
df

for proc_date in proc_dates:

    df_proc = df[(df['fullname_dt'] >= proc_date[0]) & (df['fullname_dt'] < proc_date[1])]
    
    if os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy'\
            .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
            str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))\
        and os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.txt'\
            .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
            str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) :
            
        print(('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are exist...'\
            .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
            str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))))
    
    else : 

        if len(df_proc) != 0 :
            
            print("df_proc: {}".format(df_proc))
        
            processing_log = "#This file is created using Python : https://github.com/guitar79/KOSC_MODIS_SST_Python\n"
            processing_log += "#L3_perid = {}, start date = {}, end date = {}\n"\
                .format(L3_perid, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'))
    
            processing_log += "#Llon = {}, Rlon = {}, Slat = {}, Nlat = {}, resolution = {}\n"\
                .format(str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))
            
            # make lat_array, lon_array, array_data
            print("{0}-{1} Start making grid arrays...\n".\
                  format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
            array_data = MODIS_hdf_utilities.make_grid_array(Llon, Rlon, Slat, Nlat, resolution)
            
            print('Grid arrays are created...........\n')
        
            total_data_cnt = 0
            file_no = 0
            processing_log += '#processing file list\n'
            processing_log += '#file No, data_count, filename, hdf_attribute\n'

            for fullname in df_proc["fullname"] : 
                fullname_el = fullname.split("/")
                array_alldata = array_data.copy()

                print("Reading hdf file {0}\n".format(fullname))
                hdf_raw, latitude, longitude, cntl_pt_cols, cntl_pt_rows \
                    = MODIS_hdf_utilities.read_MODIS_hdf_to_ndarray(fullname, DATAFIELD_NAME)
                    
                hdf_value = hdf_raw[:,:]
                
                if 'bad_value_scaled' in hdf_raw.attributes() :
                    #hdf_value[hdf_value == hdf_raw.attributes()['bad_value_scaled']] = np.nan
                    hdf_value = np.where(hdf_value == hdf_raw.attributes()['bad_value_scaled'], np.nan, hdf_value)
                    print("'bad_value_scaled' data is changed to np.nan...\n")

                elif 'fill_value' in hdf_raw.attributes() :
                    #hdf_value[hdf_value == hdf_raw.attributes()['fill_value']] = np.nan
                    hdf_value = np.where(hdf_value == hdf_raw.attributes()['fill_value'], np.nan, hdf_value)
                    print("'fill_value' data is changed to np.nan...\n")
                    
                elif '_FillValue' in hdf_raw.attributes() :
                    #hdf_value[hdf_value == hdf_raw.attributes()['_FillValue']] = np.nan
                    hdf_value = np.where(hdf_value == hdf_raw.attributes()['_FillValue'], np.nan, hdf_value)
                    print("'_FillValue' data is changed to np.nan...\n")
                    
                else :
                    hdf_value = np.where(hdf_value == hdf_value.min(), np.nan, hdf_value)
                    print("Minium value of hdf data is changed to np.nan ...\n")
                
                if 'valid_range' in hdf_raw.attributes() :
                    #hdf_value[hdf_value < hdf_raw.attributes()['valid_range'][0]] = np.nan
                    #hdf_value[hdf_value > hdf_raw.attributes()['valid_range'][1]] = np.nan
                    
                    hdf_value = np.where(hdf_value < hdf_raw.attributes()['valid_range'][0], np.nan, hdf_value)
                    hdf_value = np.where(hdf_value > hdf_raw.attributes()['valid_range'][1], np.nan, hdf_value)
                    print("invalid_range data changed to np.nan...\n")
                    
                if 'scale_factor' in hdf_raw.attributes() and 'add_offset' in hdf_raw.attributes() :
                    scale_factor = hdf_raw.attributes()['scale_factor']
                    offset = hdf_raw.attributes()['add_offset']

                elif 'slope' in hdf_raw.attributes() and 'intercept' in hdf_raw.attributes() :
                    scale_factor = hdf_raw.attributes()['slope']
                    offset = hdf_raw.attributes()['intercept']
                
                else : 
                    scale_factor, offset = 1, 0
                    
                hdf_value = np.asarray(hdf_value)
                hdf_value = hdf_value * scale_factor + offset
                                            
                #print("latitude: {}".format(latitude))
                #print("longitude: {}".format(longitude))
                print("hdf_value: {}".format(hdf_value))
                print("str(hdf_raw.attributes()): {}".format(str(hdf_raw.attributes())))
                print("np.shape(latitude): {}".format(np.shape(latitude)))                        
                print("np.shape(longitude): {}".format(np.shape(longitude)))
                print("np.shape(hdf_value): {}".format(np.shape(hdf_value)))
                print("len(cntl_pt_cols): {}".format(len(cntl_pt_cols)))
                print("len(cntl_pt_rows): {}".format(len(cntl_pt_rows)))
                                   
                if np.shape(latitude) == np.shape(longitude) :                    
                    if np.shape(longitude)[0] != np.shape(hdf_value)[0] :
                        print("np.shape(longitude)[0] != np.shape(hdf_value)[0] is true...")
                        row = 0
                        longitude_new = np.empty(shape=(np.shape(hdf_value)))
                        for row in range(len(longitude[0])) :
                            for i in range(len(cntl_pt_rows)-1) :
                                longitude_value = np.linspace(longitude[row,i], longitude[row,i+1], cntl_pt_rows[i])
                                for j in range(i) :
                                    longitude_new[row, row+j] = longitude_value[j]                        
                        #print("np.shape(longitude_new): {}".format(np.shape(longitude_new)))
                        longitude = longitude_new.copy()
                                                
                    elif np.shape(longitude)[1] != np.shape(hdf_value)[1] :
                        print("np.shape(longitude)[1] != np.shape(hdf_value)[1] is true...")
                        col = 0
                        longitude_new = np.empty(shape=(np.shape(hdf_value)))
                        for row in range(len(longitude[1])) :
                            for i in range(len(cntl_pt_cols)-1) :
                                longitude_value = np.linspace(longitude[row,i], \
                                                             longitude[row,i+1], \
                                                             cntl_pt_cols[i+1]-cntl_pt_cols[i]+1)
                                #print("longitude_value {}: {}".format(i, latitude_value))
                                #print("{0}, cntl_pt_cols[{1}]-cntl_pt_cols[{0}] : {2})"\
                                #      .format(i, i+1, cntl_pt_cols[i+1]-cntl_pt_cols[i]))
                                for j in range(len(longitude_value)-1) :
                                    longitude_new[row, cntl_pt_cols[i]-1+j] = longitude_value[j] 
                                longitude_new[row, np.shape(longitude_new)[1]-1] = longitude[row, np.shape(longitude)[1]-1]
                        #print("np.shape(longitude_new): {}".format(np.shape(longitude_new)))
                        longitude = longitude_new.copy()
                    longitude = np.asarray(longitude)
                    #print("type(longitude): {}".format(type(longitude)))
                    print("np.shape(longitude): {}".format(np.shape(longitude)))
                    
                    if np.shape(latitude)[0] != np.shape(hdf_value)[0] :
                        print("np.shape(latitude)[0] != np.shape(hdf_value)[0] is not same...")
                        row = 0
                        latitude_new = np.empty(shape=(np.shape(hdf_value)))
                        for row in range(len(latitude[0])) :
                            for i in range(len(cntl_pt_rows)-1) :
                                latitude_value = np.linspace(latitude[row,i], latitude[row,i+1], cntl_pt_rows[i])
                                for j in range(i) :
                                    latitude_new[row, row+j] = latitude_value[j]                        
                        print("np.shape(latitude_new): {}".format(np.shape(latitude_new)))
                        latitude = latitude_new.copy()
                                                      
                    elif np.shape(latitude)[1] != np.shape(hdf_value)[1] :
                        print("np.shape(latitude)[1] != np.shape(hdf_value)[1] is true...")
                        col = 0
                        latitude_new = np.empty(shape=(np.shape(hdf_value)))
                        for row in range(len(latitude[1])) :
                            for i in range(len(cntl_pt_cols)-1) :
                                latitude_value = np.linspace(latitude[row,i], \
                                                             latitude[row,i+1], \
                                                             cntl_pt_cols[i+1]-cntl_pt_cols[i]+1)
                                #print("latitude_value {}: {}".format(i, latitude_value))
                                #print("{0}, cntl_pt_cols[{1}]-cntl_pt_cols[{0}] : {2})"\
                                #      .format(i, i+1, cntl_pt_cols[i+1]-cntl_pt_cols[i]))
                                for j in range(len(latitude_value)-1) :
                                    latitude_new[row, cntl_pt_cols[i]-1+j] = latitude_value[j] 
                                latitude_new[row, np.shape(latitude_new)[1]-1] = latitude[row, np.shape(latitude)[1]-1]
                        print("np.shape(latitude_new): {}".format(np.shape(latitude_new)))
                        latitude = latitude_new.copy()
                    latitude = np.asarray(latitude)
                    #print("type(latitude): {}".format(type(latitude)))
                    print("np.shape(latitude): {}".format(np.shape(latitude)))
                

                if np.shape(latitude) == np.shape(hdf_value) \
                    and np.shape(longitude) == np.shape(hdf_value) :
                        
                    #longitude[longitude < Llon] = np.nan
                    #longitude[longitude > Rlon] = np.nan
                    #latitude[latitude > Nlat] = np.nan
                    #latitude[latitude < Slat] = np.nan
                    
                    longitude = np.where(longitude < Llon, np.nan, longitude)
                    longitude = np.where(longitude > Rlon, np.nan, longitude)
                    latitude = np.where(latitude > Nlat, np.nan, latitude)
                    latitude = np.where(latitude < Slat, np.nan, latitude)
                    
                    #lon_cood = np.array((((longitude-Llon)/resolution*100)//100), dtype=np.uint16)
                    #lat_cood = np.array((((Nlat-latitude)/resolution*100)//100), dtype=np.uint16)
                    
                    lon_cood = np.array(((longitude-Llon)/resolution*100)//100)
                    lat_cood = np.array(((Nlat-latitude)/resolution*100)//100)
                    
                    #print("longitude: {}".format(longitude))
                    print("np.shape(lon_cood): {}".format(np.shape(lon_cood)))
                    #print("lon_cood: {}".format(lon_cood))
                    
                    #print("latitude: {}".format(latitude))
                    print("np.shape(lat_cood): {}".format(np.shape(lat_cood)))
                    #print("lat_cood: {}".format(lat_cood))
                    print("hdf_value: {}".format(hdf_value))
                    data_cnt = 0
                    NaN_cnt = 0
                    for i in range(np.shape(lon_cood)[0]) :
                        for j in range(np.shape(lon_cood)[1]) :
                            if longitude[i,j] <= Rlon and longitude[i,j] >= Llon \
                                and latitude[i,j] <= Nlat and latitude[i,j] >= Slat \
                                and not np.isnan(hdf_value[i][j]) :
                                data_cnt += 1
                                #array_alldata[int(lon_cood[i][j])][int(lat_cood[i][j])].append(hdf_value[i][j])
                                array_alldata[int(lon_cood[i][j])][int(lat_cood[i][j])].append((fullname_el[-1], hdf_value[i][j]))
                                
                                #print("array_alldata[{}][{}].append({})"\
                                #      .format(int(lon_cood[i][j]), int(lat_cood[i][j]), hdf_value[i][j]))
                                #print("{} data added...".format(data_cnt))
                           
                    file_no += 1
                    total_data_cnt += data_cnt

                    processing_log += "{0}, {1}, {2}, {3}\n"\
                        .format(str(file_no), str(data_cnt), str(fullname), str(hdf_raw.attributes()))
                else :
                    print("np.shape(latitude) == np.shape(hdf_value) and np.shape(longitude == np.shape(hdf_value) is not true...")
                                
            processing_log += '#total data number =' + str(total_data_cnt) + '\n'
            
            #print("array_alldata: {}".format(array_alldata))
            #print("prodessing_log: {}".format(processing_log))
                                        
            np.save('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy'\
                .format(save_dir_name, DATAFIELD_NAME, 
                proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), array_alldata)
            
            with open('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.txt'\
                  .format(save_dir_name, DATAFIELD_NAME, \
                  proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), \
                  str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), 'w') as f:
                f.write(processing_log)
            print('#'*60)
            MODIS_hdf_utilities.write_log(log_file, \
                '{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are is created.'\
                .format(save_dir_name, DATAFIELD_NAME, \
                proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), \
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))
    
        else :
            print("There is no data in {0} - {1} ...\n"\
                  .format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
                
