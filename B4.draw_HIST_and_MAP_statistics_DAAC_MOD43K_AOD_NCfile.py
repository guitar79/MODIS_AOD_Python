
from glob import glob
import os
import sys

from netCDF4 import Dataset as NetCDFFile 

import _MODIS_AOD_utilities


log_file = os.path.basename(__file__)[:-3]+".log"
err_log_file = os.path.basename(__file__)[:-3]+"_err.log"
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))

arg_mode = True
arg_mode = False

if arg_mode == True :
    from sys import argv # input option
    print("argv: {}".format(argv))

    if len(argv) < 2 :
        print ("len(argv) < 2\nPlease input L3_perid and year \n ex) aaa.py daily")
        sys.exit()
    elif len(argv) > 2 :
        print ("len(argv) > 2\nPlease input L3_perid and year \n ex) aaa.py daily")
        sys.exit()
    elif argv[1] == 'daily' or argv[1] == 'weekly' or argv[1] == 'monthly' :
        L3_perid = argv[1]
        print("{} processing started...".format(argv[1]))
    else :
        print("Please input L3_perid \n ex) aaa.py daily")
        sys.exit()
else :
    L3_perid, resolution = "monthly", 0.5

# Set Datafield name
DATAFIELD_NAME = "AVHRR_SST"

#Set lon, lat, resolution
Llon, Rlon = 115, 145
Slat, Nlat = 20, 55

#Set Datafield name
DATAFIELD_NAME = "Optical_Depth_Land_And_Ocean"

#Set lon, lat, resolution
Llon, Rlon = 110, 150
Slat, Nlat = 10, 60

#set directory
base_dir_name = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                        str(Slat), str(Nlat), str(resolution), L3_perid)
save_dir_name = base_dir_name
#save_dir_name = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                        #str(Slat), str(Nlat), str(resolution), L3_perid)

#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*mean.nc')))

print("len(fullnames): {}".format(len(fullnames)))

for fullname in fullnames : 
    #fullname = fullnames[0]
    print("Starting {0}\n".format(fullname))
    fullname_el = fullname.split("/")
    filename_el = fullname_el[-1].split("_")
    #if os.path.exists('{0}_mean.npy'.format(fullname[:-4])) :
    #    print('{0}_mean.npy is already exist...'.format(fullname[:-4]))
    nc_data = NetCDFFile(fullname) # note this file is 2.5 degree, so low resolution data
    lat = nc_data.variables['latitude'][:]
    lon = nc_data.variables['longitude'][:]
    time = nc_data.variables['time'][:]
    MODIS_AOD = nc_data.variables['MODIS_AOD'][:] # SST
    
    if False and os.path.exists("{0}{1}_{2}_hist.pdf"\
        .format(base_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)) :
        print("{0}{1}_{2}_hist.pdf is already exist..."\
              .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
    else : 
        try :        
            plt_hist = MODIS_hdf_utilities.draw_histogram_SST_NC(MODIS_AOD, lon, lat, fullname, DATAFIELD_NAME)
            plt_hist.savefig('{0}_hist.pdf'.format(fullname[:-3]))
            print('{0}_hist.pdf is created...'.format(fullname[:-3]))
            plt_hist.close()
        except Exception as err :
            MODIS_hdf_utilities.write_log(err_log_file, err)
            continue           
    
    if False and os.path.exists('{0}_map.png'.format(fullname[:-3])) :
        print('{0}_map.png is already exist'.format(fullname[:-3]))
    
    else : 
        
        try :        
        
            plt_map = MODIS_hdf_utilities.draw_map_SST_nc(MODIS_AOD, lon, lat, save_dir_name, fullname, DATAFIELD_NAME, Llon, Rlon, Slat, Nlat)
            
            plt_map.savefig('{0}_map.png'.format(fullname[:-3]))
            print('{0}_map.png is created...'.format(fullname[:-3]))
            plt_map.close()
            
        except Exception as err :
            MODIS_hdf_utilities.write_log(err_log_file, err)
            continue