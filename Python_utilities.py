# -*- coding: utf-8 -*-
"""
Created on Thu Nov 22 01:00:19 2018
@author: guitar79@naver.com

ModuleNotFoundError: No module named 'ccdproc'
conda install -c condaforge ccdproc
"""

from datetime import datetime

# =============================================================================
# creat log
# =============================================================================

def write_log(log_file, log_str):
    import time
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    msg = '[' + timestamp + '] ' + log_str
    print(msg)
    with open(log_file, 'a') as f:
        f.write(msg + '\n')

def write_log2(log_file, log_str):
    import os
    with open(log_file, 'a') as log_f:
        log_f.write("{}, {}\n".format(os.path.basename(__file__), log_str))
    return print ("{}, {}\n".format(os.path.basename(__file__), log_str))

        
# =============================================================================
# for checking time
# =============================================================================
cht_start_time = datetime.now()
def print_working_time(cht_start_time):
    working_time = (datetime.now() - cht_start_time) #total days for downloading
    return print('working time ::: %s' % (working_time))

master_file_dir_name = 'master_file_Python/'
processing_dir_name = 'processing_Python/'
integration_dir_name = 'integration_Python/'
alignment_dir_name = 'alignment_Python/'


# =============================================================================
# getFullnameListOfallFiles
# =============================================================================
def getFullnameListOfallFiles(dirName):
    ##############################################3
    import os
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = sorted(os.listdir(dirName))
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getFullnameListOfallFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles


# =============================================================================
# getFullnameListOfallFiles
# =============================================================================

def getFullnameListOfsubDirs(dirName):
    ##############################################3
    import os
    allFiles = list()
    for file in sorted(os.listdir(dirName)):
        d = os.path.join(dirName, file)
        if os.path.isdir(d):
            allFiles.append(d)

    return allFiles


# =============================================================================
# getFullnameListOfallsubDirs
# =============================================================================
def getFullnameListOfallsubDirs(dirName):
    ##############################################3
    import os
    allFiles = list()
    for it in os.scandir(dirName):
        if it.is_dir():
            allFiles.append(it.path)
            allFiles.extend(getFullnameListOfallsubDirs(it))
    return allFiles
