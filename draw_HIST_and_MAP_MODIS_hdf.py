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
import _MODIS_AOD_utilities

import sys

from sys import argv # input option

print("argv: {}".format(argv))

if len(argv) == 2 :
    n = int(argv[1])
else :
    print("Please input n")
    sys.exit()
#n = 1        

resolution = 0.01

#Set lon, lat, resolution
Llon, Rlon = 115, 145
Slat, Nlat = 20, 55

# Set Datafield name
DATAFIELD_NAME = "sst4"

#set directory
base_dir_name = '../L2_MODIS_SST/'
save_dir_name = base_dir_name

#L3_perid, resolution, yr = "daily", 0.1, 2019

# long file option
add_log = True
if add_log == True :
    log_file = "AVHRR_{}_python.log".format(DATAFIELD_NAME)
    err_log_file = "AVHRR_{}_python_err.log".format(DATAFIELD_NAME)


#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*.hdf')))

fullnames = fullnames[n*100:(n+1)*100]

fullnames_dt = []
for fullname in fullnames :
    fullnames_dt.append(MODIS_hdf_utilities.fullname_to_datetime_for_KOSC_MODIS_hdf(fullname))


if not os.path.exists(save_dir_name):
    os.makedirs(save_dir_name)
    print ('*'*80)
    print (save_dir_name, 'is created')
else :
    print ('*'*80)
    print (save_dir_name, 'is exist')


fullnames_dt = []
for fullname in fullnames :
    fullnames_dt.append(MODIS_hdf_utilities.fullname_to_datetime_for_KOSC_MODIS_SST(fullname))


import pandas as pd 

print(len(fullnames))
print(len(fullnames_dt))

# Calling DataFrame constructor on list 
df = pd.DataFrame({'fullname':fullnames,'fullname_dt':fullnames_dt})
df.index = df['fullname_dt']
df

process_Num = 0
for fullname in df["fullname"] : 
#fullname = df["fullname"][0]
    fullname_el = fullname.split("/")
    
    if (not os.path.exists("{0}{1}_{2}_hist.pdf"\
            .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)))\
        or (not os.path.exists("{0}{1}_{2}_map.png" \
             .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))) :
        
        try : 
  
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
            

        except Exception as err :
            print("Something got wrecked (1): {}".format(err))
            
        
        try :    
            if False or os.path.exists("{0}{1}_{2}_hist.pdf"\
                .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)) :
                print("{0}{1}_{2}_hist.pdf is already exist..."\
                      .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
            else : 
            
                plt_hist = MODIS_hdf_utilities.draw_histogram_hdf(hdf_value, longitude, latitude, save_dir_name, fullname, DATAFIELD_NAME)
                plt_hist.savefig("{0}{1}_{2}_hist.pdf"\
                    .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                print("{0}{1}_{2}_hist.pdf is created..."\
                    .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                plt_hist.close()
        except Exception as err :
            print("Something got wrecked (2): {}".format(err))
                
                
        try :                
            if os.path.exists("{0}{1}_{2}_map.png" \
                 .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)) :
    
                print("{0}{1}_{2}_map.png is already exist..."\
                      .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
            else : 
            
                plt_map = MODIS_hdf_utilities.draw_map_MODIS_hdf(hdf_value, longitude, latitude, save_dir_name, fullname, DATAFIELD_NAME, Llon, Rlon, Slat, Nlat)
                
                plt_map.savefig("{0}{1}_{2}_map.png"\
                    .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                print("{0}{1}_{2}_map.png is created..."\
                    .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                plt_map.close()
        except Exception as err :
            print("Something got wrecked (4) : {}".format(err))