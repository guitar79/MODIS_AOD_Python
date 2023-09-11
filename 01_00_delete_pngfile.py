# -*- coding: utf-8 -*-
"""
Created on Thu Nov 22 01:00:19 2018
@author: user

"""
#%%
from glob import glob
from pathlib import Path, PosixPath, WindowsPath
import os
from datetime import datetime
import shutil

import _Python_utilities
import _MODIS_AOD_utilities

#%%
#######################################################
# for log file
log_dir = "logs/"
log_file = "{}{}.log".format(log_dir, os.path.basename(__file__)[:-3])
err_log_file = "{}{}_err.log".format(log_dir, os.path.basename(__file__)[:-3])
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))
if not os.path.exists('{0}'.format(log_dir)):
    os.makedirs('{0}'.format(log_dir))
#######################################################
#%%
#######################################################
# read all files in base directory for processing
BASEDIR = Path("/mnt/Rdata/MODIS_AOD/Aerosol/") 
###
DOINGDIR = Path(BASEDIR/ "MOD04_3K")
DOINGDIR = Path(BASEDIR/ "MOD04_L2")
DOINGDIR = Path(BASEDIR/ "MYD04_3K")
DOINGDIR = Path(BASEDIR/ "MYD04_L2")

DOINGDIRs = sorted(_Python_utilities.getFullnameListOfsubDirs(DOINGDIR))
#print ("DOINGDIRs: ", format(DOINGDIRs))
print ("len(DOINGDIRs): ", format(len(DOINGDIRs)))
#######################################################
#%%
#DOINGDIR = DOINGDIRs[0]
for DOINGDIR in DOINGDIRs :
    #DOINGDIR = Path(DOINGDIR)
    print(DOINGDIR)
    YDDIRs = sorted(_Python_utilities.getFullnameListOfsubDirs(DOINGDIR))

    for YDDIR in YDDIRs:
        YDDIR = Path(YDDIR)
        print(YDDIR)
        fpaths = sorted(list(YDDIR.glob('*.png')))
        print(fpaths)
        for fpath in fpaths:
            os.remove(str(fpath))
            print(f"{str(fpath.stem)} is deleted...")