#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################
#runfile('./classify_AVHRR_asc_SST-01.py', 'daily 0.1 2019', wdir='./MODIS_hdf_Python/')
#cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && for yr in {2011..2020}; do python classify_AVHRR_asc_SST-01.py daily 0.05 $yr; done
#conda activate MODIS_hdf_Python_env && cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && python classify_AVHRR_asc_SST.py daily 0.01 2011
#conda activate MODIS_hdf_Python_env && cd /mnt/Rdata/RS-data/KOSC/MODIS_hdf_Python/ && A1.daily_classify_from_DAAC_MOD04_3K_hdf.py 1.0 2019
#conda activate MODIS_hdf_Python_env && cd /mnt/6TB1/RS_data/MODIS_AOD/MODIS_hdf_Python/ && python A1.daily_classify_from_DAAC_MOD04_3K_hdf.py 0.01 2000
'''

import numpy as np
import os

import MODIS_hdf_utilities

arg_mode = True
#arg_mode =  False

log_file = os.path.basename(__file__)[:-3]+".log"
err_log_file = os.path.basename(__file__)[:-3]+"_err.log"
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))


#set directory
base_dir_name = "../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/"

#### make dataframe from file list
fullnames = MODIS_hdf_utilities.getFullnameListOfallFiles(base_dir_name)

len(fullnames)

# Set Datafield name
DATAFIELD_NAME = "Optical_Depth_Land_And_Ocean"

result_filename = "hdf_file_info.csv"
for fullname in fullnames[:2] :
#fullname = fullnames[0]
    fullname_el = fullname.split("/")
    filename_el = fullname_el[-1].split(".")
    print("Reading hdf file {0}\n".format(fullname))
    if filename_el[-1].lower() == "hdf" :
        
        try : 
            hdf_raw, latitude, longitude, cntl_pt_cols, cntl_pt_rows \
                = MODIS_hdf_utilities.read_MODIS_hdf_to_ndarray(fullname, DATAFIELD_NAME)
                
            save_dir_name = fullname[:-len(fullname_el[-1])]
                
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
                print("'_FillValue' data is changed to np.nan...\n")\
                
            else :
                #hdf_value = np.where(hdf_value == hdf_value.min(), np.nan, hdf_value)
                print("Minium value of hdf data is not changed to np.nan ...\n")
                
                hdf_value = np.where(hdf_value == -32767, np.nan, hdf_value)
                print("-32767 value of hdf data is changed to np.nan ...\n")
                
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
            #print("np.shape(latitude): {}".format(np.shape(latitude)))
            #print("np.shape(longitude): {}".format(np.shape(longitude)))
            #print("np.shape(hdf_value): {}".format(np.shape(hdf_value)))
            #print("len(cntl_pt_cols): {}".format(len(cntl_pt_cols)))
            #print("len(cntl_pt_rows): {}".format(len(cntl_pt_rows)))
            #print("latitude: {} ~ {}".format(np.min(latitude), np.max(latitude)))
            #print("longitude: {} ~ {}".format(np.min(longitude), np.max(longitude)))
            #print("AOD value: {} ~ {}".format(np.nanmin(hdf_value), np.nanmax(hdf_value)))
            
            # check latitude and longitude
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
                
            print("latitude: {} ~ {}".format(np.min(latitude), np.max(latitude)))
            print("longitude: {} ~ {}".format(np.min(longitude), np.max(longitude)))
            print("AOD value: {} ~ {}".format(np.nanmin(hdf_value), np.nanmax(hdf_value)))
            
            plt_hist = MODIS_hdf_utilities.draw_histogram_hdf(hdf_value, longitude, latitude, fullname, DATAFIELD_NAME)
            
            plt_hist.savefig("{}{}_hist.png".format(save_dir_name, fullname_el[-1][:-4]))
            Llon, Rlon, Slat, Nlat = np.min(longitude), np.max(longitude), np.min(latitude), np.max(latitude)
            
            plt_map = MODIS_hdf_utilities.draw_map_MODIS_hdf_onefile(hdf_value, longitude, latitude, fullname, DATAFIELD_NAME, Llon, Rlon, Slat, Nlat)
            plt_map.savefig("{}{}_map.png".format(save_dir_name, fullname_el[-1][:-4]))
                
        except Exception as err :
            #MODIS_hdf_utilities.write_log(err_log_file, err)
            print(err)
            continue
