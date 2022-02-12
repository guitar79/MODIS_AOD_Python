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
from datetime import datetime
from dateutil.relativedelta import relativedelta
set_S_datetime = datetime(2003, 1, 1) #convert startdate to date type
set_E_datetime = datetime(2021, 12, 31)

working_Datetetimes = [set_S_datetime.strftime("%Y-%m-%d")]
date1 = set_S_datetime
while date1 < set_E_datetime :
    date1 += relativedelta(months=1)
    working_Datetetimes.append(date1.strftime("%Y-%m-%d"))
print(working_Datetetimes)
#########################################

n = 0
for working_Date in working_Datetetimes[:]:
    #working_Date = working_Datetetimes[0]
    working_Jday = MODIS_AOD_utilities.datestr_to_JDay(working_Date, "%Y-%m-%d")
    working_Date = datetime.strptime(working_Date, "%Y-%m-%d")

    n += 1
    print('#' * 60,
          "\n{2:.01f}%  ({0}/{1}) {3}".format(n, len(working_Datetetimes), (n / len(working_Datetetimes)) * 100,
                                              os.path.basename(__file__)))
    print("Starting...   working_Date: {}".format(working_Date))

    # get fullnames
    fullnames = Python_utilities.getFullnameListOfallFiles("{}{}".format(base_dr, working_Date.strftime("%Y/")))
    #fullnames = sorted(
    #    glob(os.path.join("{}{}{}/".format(base_dr, working_Date.strftime("%Y/"), working_Jday), '*.hdf')))

    nn = 0

    for fullname in fullnames[:] :
        #fullname = fullnames[8]
        if fullname[-4:].lower() == ".hdf" :
            print('#' * 40,
                  "\n{2:.01f}%  ({0}/{1}) {3}".format(nn, len(fullnames), (nn / len(fullnames)) * 100,
                                                      os.path.basename(__file__)))
            print("Starting...   fullname: {}".format(fullname))

            fullname_el = fullname.split("/")
            filename_el = fullname_el[-1].split(".")
            save_dr = fullname[:-len(fullname_el[-1])]
            print("Reading hdf file {0}\n".format(fullname))

            try :
                hdf_raw, latitude, longitude, cntl_pt_cols, cntl_pt_rows \
                    = MODIS_AOD_utilities.read_MODIS_hdf_to_ndarray(fullname, DATAFIELD_NAME)
                hdf_value = hdf_raw[:, :]
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

                print("latitude: {}".format(latitude))
                print("longitude: {}".format(longitude))
                print("hdf_value: {}".format(hdf_value))
                print("str(hdf_raw.attributes()): {}".format(str(hdf_raw.attributes())))

                #Wlon, Elon, Slat, Nlat, Clon, Clat = MODIS_AOD_utilities.findRangeOfMap(longitude, latitude)
                print("plotting histogram {}".format(fullname))
                plt_hist = MODIS_AOD_utilities.draw_histogram_hdf(hdf_value, longitude, latitude, fullname, DATAFIELD_NAME, Dataset_DOI)
                plt_hist.savefig("{}{}_hist.png".format(save_dr, fullname_el[-1][:-4]), overwrite=True)
                plt_hist.close()
                ######################################################################################
                Python_utilities.write_log(log_file, "{}{}_hist.png is created...".format(save_dr, fullname_el[-1][:-4]))

                print("plotting on the map {}".format(fullname))
                #Llon, Rlon, Slat, Nlat = np.min(longitude), np.max(longitude), np.min(latitude), np.max(latitude)
                plt_map = MODIS_AOD_utilities.draw_map_MODIS_hdf_onefile(hdf_value, longitude, latitude, fullname, DATAFIELD_NAME, Dataset_DOI)
                plt_map.savefig("{}{}_map.png".format(save_dr, fullname_el[-1][:-4]), overwrite=True)
                plt_map.close()
                ######################################################################################

                Python_utilities.write_log(log_file,
                              "{}{}_map.png is created...".format(save_dr, fullname_el[-1][:-4]))

            except Exception as err :
                #MODIS_AOD_utilities.write_log(err_log_file, err)
                print(err)
                continue