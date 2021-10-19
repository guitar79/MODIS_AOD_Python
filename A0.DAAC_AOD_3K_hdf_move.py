#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''

'''

from datetime import datetime
import os
import sys
import shutil 
import MODIS_hdf_utilities

arg_mode = True
#arg_mode =  False

log_file = os.path.basename(__file__)[:-3]+".log"
err_log_file = os.path.basename(__file__)[:-3]+"_err.log"
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))

if arg_mode == False :
    from sys import argv # input option
    print("argv: {}".format(argv))

    if len(argv) < 3 :
        print ("len(argv) < 2\nPlease input L3_perid and year \n ex) aaa.py 0.1 2016")
        sys.exit()
    elif len(argv) > 3 :
        print ("len(argv) > 2\nPlease input L3_perid and year \n ex) aaa.py 0.1 2016")
        sys.exit()
    else :
        L3_perid, resolution, year = "daily", float(argv[1]), int(argv[2])
        print("{}, {}, processing started...".format(argv[1], argv[2]))
else :
    L3_perid, resolution, year = "daily", 0.5, 2000
    

#set directory
base_dir_name = "../DAAC_MOD04_L2/"

#fullnames = sorted(glob(os.path.join(base_dir_name, '*.hdf')))

fullnames = MODIS_hdf_utilities.getFullnameListOfallFiles(base_dir_name)

len(fullnames)
    
for fullname in fullnames :
#fullname = df["fullname"][0]
    fullname_el = fullname.split("/")
    filename_el = fullname_el[-1].split(".")
    print("Reading hdf file {0}\n".format(fullname))
    try : 
        if filename_el[-1] == "hdf" : 
            if filename_el[0] == "MOD04_3K" :
                save_filename = r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/{}/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8], fullname_el[-1])
                if not os.path.exists(r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/{}/".format(filename_el[1][1:5])) :
                    os.makedirs(r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/{}/".format(filename_el[1][1:5]))
                if not os.path.exists(r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8])) :
                    os.makedirs(r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 3km/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8]))
                shutil.move(r"{}".format(fullname), r"{}".format(save_filename))
                print("{} is moved to {}".format(fullname, save_filename))        
            elif filename_el[0] == "MYD04_3K" :
                save_filename = r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/{}/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8], fullname_el[-1])
                if not os.path.exists(r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/{}/".format(filename_el[1][1:5])) :
                    os.makedirs(r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/{}/".format(filename_el[1][1:5]))
                if not os.path.exists(r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8])) :
                    os.makedirs(r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 3km/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8]))
                shutil.move(r"{}".format(fullname), r"{}".format(save_filename))
                print("{} is moved to {}".format(fullname, save_filename))
            elif filename_el[0] == "MOD04_L2" :
                save_filename = r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/{}/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8], fullname_el[-1])
                if not os.path.exists(r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/{}/".format(filename_el[1][1:5])) :
                    os.makedirs(r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/{}/".format(filename_el[1][1:5]))
                if not os.path.exists(r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8])) :
                    os.makedirs(r"../Aerosol/MODIS Terra C6.1 - Aerosol 5-Min L2 Swath 10km/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8]))
                shutil.move(r"{}".format(fullname), r"{}".format(save_filename))
                print("{} is moved to {}".format(fullname, save_filename))
            elif filename_el[0] == "MYD04_L2" :
                save_filename = r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 10km/{}/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8], fullname_el[-1])
                if not os.path.exists(r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 10km/{}/".format(filename_el[1][1:5])) :
                    os.makedirs(r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 10km/{}/".format(filename_el[1][1:5]))
                if not os.path.exists(r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 10km/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8])) :
                    os.makedirs(r"../Aerosol/MODIS Aqua C6.1 - Aerosol 5-Min L2 Swath 10km/{}/{}".format(filename_el[1][1:5], filename_el[1][5:8]))
                shutil.move(r"{}".format(fullname), r"{}".format(save_filename))
                print("{} is moved to {}".format(fullname, save_filename))
            else :
                print("xxxxx sonthing get weaked....")
            
    except Exception as err :
        print("X"*60)
        MODIS_hdf_utilities.write_log(err_log_file, \
             '{2} ::: {0} with move {1} '.format(err, fullname, datetime.now()))
    