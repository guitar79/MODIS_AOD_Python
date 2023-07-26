#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
@author: guitar79
created by Kevin

cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && for yr in {2011..2020}; do python classify_AVHRR_asc_SST-01.py daily 0.05 $yr; done

cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && for n in {0..100}; do python draw_HIST_and_MAP_AVHRR_asc_SST_MP.py $n ; done
cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && for n in {100..200}; do python draw_HIST_and_MAP_AVHRR_asc_SST_MP.py $n ; done
'''

from glob import glob
import numpy as np
import os
import _MODIS_AOD_utilities

import sys
from sys import argv # input option

log_file = os.path.basename(__file__)[:-3]+".log"
err_log_file = os.path.basename(__file__)[:-3]+"_err.log"
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))

# 파일을 구간을 나누어 여러개의 프로세스를 돌리기 위한 argv
print("argv: {}".format(argv))

if len(argv) == 2 :
    n = int(argv[1])
else :
    print("Please input n")
    sys.exit()
    
# Set Datafield name
DATAFIELD_NAME = "AVHRR_SST"

#Set lon, lat, resolution
Llon, Rlon = 115, 145
Slat, Nlat = 20, 55
resolution = 0.01

#L3_perid, resolution, yr = "daily", 0.1, 2019


#set directory
base_dir_name = '../L2_AVHRR_SST/'
save_dir_name = base_dir_name

#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*.asc')))

fullnames = fullnames[n*100:(n+1)*100]

fullnames_dt = []
for fullname in fullnames :
    fullnames_dt.append(MODIS_hdf_utilities.fullname_to_datetime_for_KOSC_AVHRR_SST_asc(fullname))

import pandas as pd 

len(fullnames)
len(fullnames_dt)

# Calling DataFrame constructor on list 
df = pd.DataFrame({'fullname':fullnames,'fullname_dt':fullnames_dt})
df.index = df['fullname_dt']
df

process_Num = 0
for fullname in df["fullname"] : 
    
    fullname_el = fullname.split("/")
    
    if (not os.path.exists("{0}{1}_{2}_hist.pdf"\
            .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)))\
        or (not os.path.exists("{0}{1}_{2}_map.png" \
             .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))) :
    
        try : 
            process_Num += 1
            print("#"*60)
            print("{1} file(s)\nReading ascii file: {0} \n".format(fullname, process_Num))
            df_AVHRR_sst = pd.read_table("{}".format(fullname), sep='\t', header=None, index_col=0,
                               names = ['index', 'latitude', 'longitude', 'sst'],
                               engine='python')
            df_AVHRR_sst.loc[df_AVHRR_sst.sst == "***", ['sst']] = np.nan
            
            df_AVHRR_sst["sst"] = df_AVHRR_sst.sst.astype("float16")
            df_AVHRR_sst["longitude"] = df_AVHRR_sst.longitude.astype("float16")
            df_AVHRR_sst["latitude"] = df_AVHRR_sst.latitude.astype("float16")
            print("df_AVHRR_sst : {}".format(df_AVHRR_sst))
            
        except Exception as err :
            MODIS_hdf_utilities.write_log(err_log_file, err)
            continue
            
        
            #check dimension    
        if len(df_AVHRR_sst) == 0 :
            print("There is no sst data...")
                
        else :
            #try :    
                #df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.longitude < Llon].index)
                #df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.longitude > Rlon].index)
                #df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.latitude > Nlat].index)
                #df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.latitude < Slat].index)
                
                #df_AVHRR_sst["lon_cood"] = (((df_AVHRR_sst["longitude"]-Llon)/resolution*100)//100)
                #df_AVHRR_sst["lat_cood"] = (((Nlat-df_AVHRR_sst["latitude"])/resolution*100)//100)
                #df_AVHRR_sst = df_AVHRR_sst.dropna()    
                #print('df_AVHRR_sst["lon_cood"]\n{}'.format(df_AVHRR_sst["lon_cood"]))
                #print('df_AVHRR_sst["lat_cood"]\n{}'.format(df_AVHRR_sst["lat_cood"]))
                #df_AVHRR_sst["lon_cood"] = df_AVHRR_sst.lon_cood.astype("int16")
                #df_AVHRR_sst["lat_cood"] = df_AVHRR_sst.lat_cood.astype("int16")
                
            #except Exception as err :
                #print("Something got wrecked (2): {}".format(err))
                #continue
            
            
            if os.path.exists("{0}{1}_{2}_hist.pdf"\
                .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)) :
                print("{0}{1}_{2}_hist.pdf is already exist..."\
                      .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
            else : 
                try :        
                    plt_hist = MODIS_hdf_utilities.draw_histogram_AVHRR_SST_asc(df_AVHRR_sst, save_dir_name, fullname, DATAFIELD_NAME)            
                    plt_hist.savefig("{0}{1}_{2}_hist.pdf"\
                        .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                    print("{0}{1}_{2}_hist.pdf is created..."\
                        .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                    plt_hist.close()
                except Exception as err :
                    MODIS_hdf_utilities.write_log(err_log_file, err)
                    continue

            
            if os.path.exists("{0}{1}_{2}_map.png" \
                 .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)) :
    
                print("{0}{1}_{2}_map.png is already exist..."\
                      .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
            else : 
                try :                
                
                    plt_map = MODIS_hdf_utilities.draw_map_AVHRR_SST_asc(df_AVHRR_sst, save_dir_name, fullname, DATAFIELD_NAME, Llon, Rlon, Slat, Nlat)
                    
                    plt_map.savefig("{0}{1}_{2}_map.png"\
                        .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                    print("{0}{1}_{2}_map.png is created..."\
                        .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                    plt_map.close()
                except Exception as err :
                    MODIS_hdf_utilities.write_log(err_log_file, err)
                    continue