'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
Created on Sat Nov  3 20:34:47 2018
@author: guitar79
created by Kevin
#Open hdf file
NameError: name 'SD' is not defined
conda install -c conda-forge pyhdf
'''

from glob import glob
import numpy as np
from datetime import datetime

import os
from pyhdf.SD import SD, SDC

def write_log2(log_file, log_str):
    import os
    with open(log_file, 'a') as log_f:
        log_f.write("{}, {}\n".format(os.path.basename(__file__), log_str))
    return print ("{}, {}\n".format(os.path.basename(__file__), log_str))

def write_log(log_file, log_str):
    import time
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    msg = '[' + timestamp + '] ' + log_str
    print (msg)
    with open(log_file, 'a') as f:
        f.write(msg + '\n')

#for checking time
cht_start_time = datetime.now()


def getFullnameListOfallsubDirs(dirName):
    ##############################################3
    import os
    allFiles = list()
    for it in os.scandir(dirName):
        if it.is_dir():
            allFiles.append(it.path)
            allFiles.extend(getFullnameListOfallsubDirs(it))

    return allFiles


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

#JulianDate_to_date(2018, 131) -- '20180511'
def JulianDate_to_date(y, jd):
    import calendar
    ############################################################
    #JulianDate_to_date(2018, 131) -- '20180511'
    #
    month = 1
    while jd - calendar.monthrange(y, month)[1] > 0 and month <= 12:
        jd = jd - calendar.monthrange(y, month)[1]
        month += 1
    #return datetime(y, month, jd).strftime('%Y%m%d')
    return datetime(y, month, jd)

def date_to_JulianDate(dt, fmt):
    ############################################################
    #date_to_JulianDate('20180201', '%Y%m%d') -- 2018032
    #
    dt = datetime.strptime(dt, fmt)
    tt = dt.timetuple()
    return int('%d%03d' % (tt.tm_year, tt.tm_yday))



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

def fullname_to_datetime_for_DAAC3K(fullname):
    ############################################################
    #for modis hdf file, filename = 'DAAC_MOD04_3K/MOD04_3K.A2014003.0105.006.2015072123557.hdf'
    #
    import calendar
    from datetime import datetime
    fullname_el = fullname.split("/")
    filename_el = fullname_el[-1].split(".")
    y = int(filename_el[1][1:5])
    jd = int(filename_el[1][5:])
    month = 1
    while jd - calendar.monthrange(y, month)[1] > 0 and month <= 12:
        jd = jd - calendar.monthrange(y, month)[1]
        month += 1
    print("filename_el: {}".format(filename_el))
    print(y, month, jd, int(filename_el[2][:2]), int(filename_el[2][2:]))
    return datetime(y, month, jd, int(filename_el[2][:2]), int(filename_el[2][2:]))


def fullname_to_datetime_for_DAAC3K_DF(Series):
    ############################################################
    #for modis hdf file, filename = 'DAAC_MOD04_3K/MOD04_3K.A2014003.0105.006.2015072123557.hdf'
    #
    import calendar
    fullname_el = Series.str.split("/")
    filename_el = fullname_el[-1].split(".")
    y = int(filename_el[1][1:5])
    jd = int(filename_el[1][5:])
    month = 1
    while jd - calendar.monthrange(y, month)[1] > 0 and month <= 12:
        jd = jd - calendar.monthrange(y, month)[1]
        month += 1
    #print("filename_el: {}".format(filename_el))
    #print(y, month, jd, int(filename_el[2][:2]), int(filename_el[2][2:]))
    return datetime(y, month, jd, int(filename_el[2][:2]), int(filename_el[2][2:]))
     

def fullname_to_datetime_for_KOSC_MODIS_SST(fullname):
    ############################################################
    #for modis hdf file, filename = '../folder/MYDOCT.2018.0724.0515.aqua-1.hdf'
    #
    from datetime import datetime
    
    fullname_info = fullname.split('/')
    fileinfo = fullname_info[-1].split('.')
    filename_dt = datetime(int(fileinfo[-5]), int(fileinfo[-4][:2]), int(fileinfo[-4][2:]), int(fileinfo[-3][:2]), int(fileinfo[-3][2:]))
    return filename_dt

def fullname_to_datetime_for_KOSC_AVHRR_SST_asc(fullname):
    ############################################################
    #for modis hdf file, filename = '../folder/MYDOCT.2018.0724.0515.aqua-1.hdf'
    #
    from datetime import datetime
    
    fullname_info = fullname.split('/')
    fileinfo = fullname_info[-1].split('.')
    filename_dt = datetime(int(fileinfo[0]), int(fileinfo[1][:2]), int(fileinfo[1][2:]), int(fileinfo[2][:2]), int(fileinfo[2][2:]))
    return filename_dt

def fullname_to_datetime_for_L3_npyfile(fullname):
    ############################################################
    #for modis hdf file, filename = '../folder/AVHRR_SST_20110901_20110902_115_145_20_55_0.5_alldata.npy'
    #
    from datetime import datetime
    
    fullname_info = fullname.split('/')
    fileinfo = fullname_info[-1].split('_')
    filename_dt = datetime(int(fileinfo[-8][0:4]), int(fileinfo[-8][4:6]), int(fileinfo[-8][6:]))
    return filename_dt

def fullname_to_datetime_for_KOSC_MODIS_hdf(fullname):
    ############################################################
    #for modis hdf file, filename = '../folder/MYDOCT.2018.0724.0515.aqua-1.hdf'
    #
    from datetime import datetime
    
    fullname_info = fullname.split('/')
    fileinfo = fullname_info[-1].split('.')
    filename_dt = datetime(int(fileinfo[1]), int(fileinfo[2][:2]), int(fileinfo[2][2:]), int(fileinfo[3][:2]), int(fileinfo[3][2:]))
    return filename_dt


def draw_histogram_hdf(hdf_value, longitude, latitude, save_dir_name, fullname, DATAFIELD_NAME):
    fullname_el = fullname.split("/")
    import matplotlib.pyplot as plt
    import numpy as np
    plt.figure(figsize=(12, 8))
    plt.title("Histogram of {0}: \n{1}\nmean : {2:.02f}, max: {3:.02f}, min: {4:.02f}\n\
              longigude : {5:.02f}~{6:.02f}, latitude: {7:.02f}~{8:.02f}".format(DATAFIELD_NAME, fullname_el[-1],\
                                      np.nanmean(hdf_value), np.nanmax(hdf_value), np.nanmin(hdf_value),\
                                      np.nanmin(longitude), np.nanmax(longitude),\
                                      np.nanmin(latitude), np.nanmax(latitude)), fontsize=9)
    plt.hist(hdf_value)
    plt.grid(True)

    return plt

def draw_histogram(hdf_value, longitude, latitude, save_dir_name, fullname, DATAFIELD_NAME):
    fullname_el = fullname.split("/")
    import matplotlib.pyplot as plt
    import numpy as np
    plt.figure(figsize=(12, 8))
    plt.title("Histogram of {0}: \n{1}\nmean : {2:.02f}, max: {3:.02f}, min: {4:.02f}\n\
              longigude : {5:.02f}~{6:.02f}, latitude: {7:.02f}~{8:.02f}".format(DATAFIELD_NAME, fullname_el[-1],\
                                      np.nanmean(hdf_value), np.nanmax(hdf_value), np.nanmin(hdf_value),\
                                      np.nanmin(longitude), np.nanmax(longitude),\
                                      np.nanmin(latitude), np.nanmax(latitude)), fontsize=9)
    plt.hist(hdf_value)
    plt.grid(True)
    plt.savefig("{0}{1}_{2}_hist.png"\
        .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
    print("{0}{1}_{2}_hist.png is created..."\
        .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
    plt.close()
    return None


def draw_map_MODIS_hdf(hdf_value, longitude, latitude, save_dir_name, fullname, DATAFIELD_NAME, Llon, Rlon, Slat, Nlat):
    fullname_el = fullname.split("/")
    import numpy as np
    #if np.isnan(hdf_value).any() :
    #    print("(np.isnan(hdf_value).any()) is true...")                    
    #else :
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt

    plt.figure(figsize=(10, 10))
    
    # sylender map
    m = Basemap(projection='cyl', resolution='l', \
                llcrnrlat = Slat, urcrnrlat = Nlat, \
                llcrnrlon = Llon, urcrnrlon = Rlon)
    
    m.drawcoastlines(linewidth=0.25, color='white')
    m.drawcountries(linewidth=0.25, color='white')
    m.fillcontinents(color='black', lake_color='black')
    m.drawmapboundary()
    
    m.drawparallels(np.arange(-90., 90., 10.), labels=[1, 0, 0, 0], color='white')
    m.drawmeridians(np.arange(-180., 181., 15.), labels=[0, 0, 0, 1], color='white')
    
    x, y = m(longitude, latitude) # convert to projection map
    
    m.pcolormesh(x, y, hdf_value, vmin=0, vmax=40, cmap='coolwarm')
    m.colorbar(fraction=0.0455, pad=0.044, ticks=(np.arange(-5, 40.1, step=5)))
    
    plt.title('MODIS {}'.format(DATAFIELD_NAME), fontsize=20)      
    
    x1, y1 = m(Llon, Slat-1.5)
    plt.text(x1, y1, "Maximun value: {0:.1f}\nMean value: {1:.1f}\nMin value: {2:.1f}\n"\
            .format(np.nanmax(hdf_value), np.nanmean(hdf_value), 
                    np.nanmin(hdf_value)), 
            horizontalalignment='left',
            verticalalignment='top', 
            fontsize=9, style='italic', wrap=True)

    x2, y2 = m(Rlon, Slat-1.5)
    plt.text(x2, y2, "created by guitar79@gs.hs.kr\nAVHRR SST procuct using KOSC data\n{}"\
             .format(fullname_el[-1]), 
            horizontalalignment='right',
            verticalalignment='top', 
            fontsize=10, style='italic', wrap=True)    
    
    return plt

def draw_map_SST_nc(hdf_value, longitude, latitude, save_dir_name, fullname, DATAFIELD_NAME, Llon, Rlon, Slat, Nlat):
    fullname_el = fullname.split("/")
    import numpy as np
    #if np.isnan(hdf_value).any() :
    #    print("(np.isnan(hdf_value).any()) is true...")                    
    #else :
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt

    plt.figure(figsize=(10, 10))
    
    # sylender map
    m = Basemap(projection='cyl', resolution='l', \
                llcrnrlat = Slat, urcrnrlat = Nlat, \
                llcrnrlon = Llon, urcrnrlon = Rlon)
    
    m.drawcoastlines(linewidth=0.25, color='white')
    m.drawcountries(linewidth=0.25, color='white')
    m.fillcontinents(color='black', lake_color='black')
    m.drawmapboundary()
    
    m.drawparallels(np.arange(-90., 90., 10.), labels=[1, 0, 0, 0], color='white')
    m.drawmeridians(np.arange(-180., 181., 15.), labels=[0, 0, 0, 1], color='white')
    
    lons,lats= np.meshgrid(longitude, latitude) # for this dataset, longitude is 0 through 360, so you need to subtract 180 to properly display on map
    x,y = m(lons,lats)
    
    #x, y = m(longitude, latitude) # convert to projection map
    
    m.pcolormesh(x, y, hdf_value[0,:,:], vmin=0, vmax=40, cmap='coolwarm')
    m.colorbar(fraction=0.0455, pad=0.044, ticks=(np.arange(-5, 40.1, step=5)))
    
    plt.title('MODIS {}'.format(DATAFIELD_NAME), fontsize=20, y=1.03)      
    
    x1, y1 = m(Llon, Slat-1.5)
    plt.text(x1, y1, "Maximun value: {0:.1f}\nMean value: {1:.1f}\nMin value: {2:.1f}\n"\
            .format(np.nanmax(hdf_value), np.nanmean(hdf_value), 
                    np.nanmin(hdf_value)), 
            horizontalalignment='left',
            verticalalignment='top', 
            fontsize=9, style='italic', wrap=True)

    x2, y2 = m(Rlon, Slat-1.5)
    plt.text(x2, y2, "created by guitar79@gs.hs.kr\nAVHRR SST procuct using KOSC data\n{}"\
             .format(fullname_el[-1]), 
            horizontalalignment='right',
            verticalalignment='top', 
            fontsize=10, style='italic', wrap=True)    
    
    return plt

def draw_histogram_SST_NC(SST, longitude, latitude, fullname, DATAFIELD_NAME):
    fullname_el = fullname.split("/")
    import matplotlib.pyplot as plt
    import numpy as np
    plt.figure(figsize=(12, 8))
    #plt.title("Histogram of {0}: \n{1}\nmean : {2:.02f}, max: {3:.02f}, min: {4:.02f}\n\
    #          longigude : {5:.02f}~{6:.02f}, latitude: {7:.02f}~{8:.02f}".format(DATAFIELD_NAME, fullname,\
    #                                  np.nanmean(SST[0,:,:]), np.nanmax(SST[0,:,:]), np.nanmin(SST[0,:,:]),\
    #                                  np.nanmin(longitude), np.nanmax(longitude),\
    #                                  np.nanmin(latitude), np.nanmax(latitude)), fontsize=9)
    
    plt.title("Histogram of {0}".format(DATAFIELD_NAME), fontsize=20, y=1.03)
    
    ys, xs, patches =plt.hist(SST[0,:,:])
    plt.xlim(int(np.min(xs)/10)*10, (int(np.max(xs)/10)+1)*10)
    plt.ylim(int(np.min(ys)/10)*10, (int(np.max(ys)/10)+1)*10)
    
    plt.grid(True)
   
    plt.text(int(np.min(xs)/10)*10, (-1/np.max(ys))*25, "Maximun value: {0:.1f}\nMean value: {1:.1f}\nMin value: {2:.1f}\n"\
            .format(np.nanmax(SST[0,:,:]), np.nanmean(SST[0,:,:]), 
                    np.nanmin(SST[0,:,:])), 
            horizontalalignment='left',
            verticalalignment='top', 
            fontsize=9, style='italic', wrap=True)
    plt.text((int(np.max(xs)/10)+1)*10, (-1/np.max(ys))*25, "created by guitar79@gs.hs.kr\nAVHRR SST procuct using KOSC data\n{}"\
             .format(fullname_el[-1]), 
            horizontalalignment='right',
            verticalalignment='top', 
            fontsize=10, style='italic', wrap=True)  
    #plt.text(min(xs), -0.25, "Maximun value: {0:.1f}\nMean value: {1:.1f}\nMin value: {2:.1f}\n"\
    #        .format(np.nanmax(SST[0,:,:]), np.nanmean(SST[0,:,:]), 
    #                np.nanmin(SST[0,:,:])), 
    #        horizontalalignment='left',
    #        verticalalignment='top', 
    #        fontsize=9, style='italic', wrap=True)
   
    #plt.text(np.nanmax(SST[0,:,:]), -0.25, "created by guitar79@gs.hs.kr\nAVHRR SST procuct using KOSC data\n{}"\
    #         .format(fullname_el[-1]), 
    #        horizontalalignment='right',
    #        verticalalignment='top', 
    #        fontsize=10, style='italic', wrap=True)  
    #plt.text(0, -0.25, "Maximun value: {0:.1f}\nMean value: {1:.1f}\nMin value: {2:.1f}\n"\
    #        .format(np.nanmax(SST[0,:,:]), np.nanmean(SST[0,:,:]), 
    #                np.nanmin(SST[0,:,:])), 
    #        horizontalalignment='left',
    #        verticalalignment='top', 
    #        fontsize=9, style='italic', wrap=True)

    return plt

def draw_map_AVHRR_SST_asc(df_AVHRR_sst, save_dir_name, fullname, DATAFIELD_NAME, Llon, Rlon, Slat, Nlat):
    fullname_el = fullname.split("/")
    import numpy as np
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt
    
    width = []
    for i in range(len(df_AVHRR_sst)-1): 
        #print("index : {}".format(df_AVHRR_sst['latitude'].iloc[i]))
        if abs(df_AVHRR_sst['latitude'].iloc[i] - df_AVHRR_sst['latitude'].iloc[i+1]) > 0.001 :
            width.append(i)
            #print("index : {}".format(i))
        if i == 10000 : break        
    longitude = df_AVHRR_sst['longitude'].to_numpy()
    longitude =  np.array(longitude, dtype=np.float32)
    longitude = longitude.reshape((longitude.shape[0]//(width[0]+1)), width[0]+1)
    latitude = df_AVHRR_sst['latitude'].to_numpy()
    latitude = np.array(latitude, dtype=np.float32)    
    latitude = latitude.reshape((latitude.shape[0]//(width[0]+1)), width[0]+1)
    print("latitude.shape: {}".format(latitude.shape))
    print("type(latitude) : {}".format(type(latitude)))
    print("latitude: {}".format(latitude))
    print("np.nanmax(latitude): {}".format(np.nanmax(latitude)))
    print("np.nanmin(latitude): {}".format(np.nanmin(latitude)))
    
    sst = df_AVHRR_sst['sst'].to_numpy()
    sst = np.array(sst, dtype=np.float32)
    sst = sst.reshape((sst.shape[0]//(width[0]+1)), width[0]+1)   
    
    #Plot data on the map
    print("="*80)
    print("Plotting data on the map")
    
    plt.figure(figsize=(10, 10))
    
    # sylender map
    m = Basemap(projection='cyl', resolution='l', \
                llcrnrlat = Slat, urcrnrlat = Nlat, \
                llcrnrlon = Llon, urcrnrlon = Rlon)
    
    m.drawcoastlines(linewidth=0.25, color='white')
    m.drawcountries(linewidth=0.25, color='white')
    m.fillcontinents(color='black', lake_color='black')
    m.drawmapboundary()
    
    m.drawparallels(np.arange(-90., 90., 10.), labels=[1, 0, 0, 0], color='white')
    m.drawmeridians(np.arange(-180., 181., 15.), labels=[0, 0, 0, 1], color='white')
    
    x, y = m(longitude, latitude) # convert to projection map
    
    m.pcolormesh(x, y, sst, vmin=0, vmax=40, cmap='coolwarm')
    m.colorbar(fraction=0.0455, pad=0.044, ticks=(np.arange(-5, 40.1, step=5)))
    
    plt.title('{}'.format(DATAFIELD_NAME), fontsize=20)
    
    x1, y1 = m(Llon, Slat-1.5)
    plt.text(x1, y1, "Maximun value: {0:.1f}\nMean value: {1:.1f}\nMin value: {2:.1f}\n"\
            .format(np.nanmax(df_AVHRR_sst["sst"]), np.nanmean(df_AVHRR_sst["sst"]), 
                    np.nanmin(df_AVHRR_sst["sst"])), 
            horizontalalignment='left',
            verticalalignment='top', 
            fontsize=9, style='italic', wrap=True)

    x2, y2 = m(Rlon, Slat-1.5)
    plt.text(x2, y2, "created by guitar79@gs.hs.kr\nAVHRR SST procuct using KOSC data\n{}"\
             .format(fullname_el[-1]), 
            horizontalalignment='right',
            verticalalignment='top', 
            fontsize=10, style='italic', wrap=True)    
    
    return plt


def draw_histogram_AVHRR_SST_asc(df_AVHRR_sst, save_dir_name, fullname, DATAFIELD_NAME):
    fullname_el = fullname.split("/")
    import matplotlib.pyplot as plt
    import numpy as np
    plt.figure(figsize=(12, 8))
    plt.title("Histogram of {0}".format(DATAFIELD_NAME), fontsize=20)
    
    #plt.title("Histogram of {0}: \n{1}\nmean : {2:.02f}, max: {3:.02f}, min: {4:.02f}\n\
    #          longigude : {5:.02f}~{6:.02f}, latitude: {7:.02f}~{8:.02f}".format(DATAFIELD_NAME, fullname,\
    #                                  np.nanmean(df_AVHRR_sst["sst"]), np.nanmax(df_AVHRR_sst["sst"]), np.nanmin(df_AVHRR_sst["sst"]),\
    #                                  np.nanmin(df_AVHRR_sst["longitude"]), np.nanmax(df_AVHRR_sst["longitude"]),\
    #                                  np.nanmin(df_AVHRR_sst["latitude"]), np.nanmax(df_AVHRR_sst["latitude"])), fontsize=9)
    plt.hist(df_AVHRR_sst["sst"])
    plt.grid(True)
    
    plt.text(0, -0.2, "Maximun value: {0:.1f}\nMean value: {1:.1f}\nMin value: {2:.1f}\n"\
            .format(np.nanmax(df_AVHRR_sst["sst"]), np.nanmean(df_AVHRR_sst["sst"]), 
                    np.nanmin(df_AVHRR_sst["sst"])), 
            horizontalalignment='left',
            verticalalignment='top', 
            fontsize=9, style='italic', wrap=True)
   
    plt.text(np.nanmax(df_AVHRR_sst["sst"]), -0.2, "created by guitar79@gs.hs.kr\nAVHRR SST procuct using KOSC data\n{}"\
             .format(fullname_el[-1]), 
            horizontalalignment='right',
            verticalalignment='top', 
            fontsize=10, style='italic', wrap=True)  



    return plt

def draw_histogram_AVHRR_SST_asc1(df_AVHRR_sst, save_dir_name, fullname, DATAFIELD_NAME):
    fullname_el = fullname.split("/")
    import matplotlib.pyplot as plt
    import numpy as np
    plt.figure(figsize=(12, 8))
    plt.title("Histogram of {0}: \n{1}\nmean : {2:.02f}, max: {3:.02f}, min: {4:.02f}\n\
              longigude : {5:.02f}~{6:.02f}, latitude: {7:.02f}~{8:.02f}".format(DATAFIELD_NAME, fullname,\
                                      np.nanmean(df_AVHRR_sst["sst"]), np.nanmax(df_AVHRR_sst["sst"]), np.nanmin(df_AVHRR_sst["sst"]),\
                                      np.nanmin(df_AVHRR_sst["longitude"]), np.nanmax(df_AVHRR_sst["longitude"]),\
                                      np.nanmin(df_AVHRR_sst["latitude"]), np.nanmax(df_AVHRR_sst["latitude"])), fontsize=9)
    plt.hist(df_AVHRR_sst["sst"])
    plt.grid(True)

    #plt.savefig("{}_{}_hist.png".format(fullname[:-4], DATAFIELD_NAME))
    #print("{}_{}_hist.png is created...".format(fullname[:-4], DATAFIELD_NAME))
    plt.savefig("{0}{1}_{2}_hist.png"\
        .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
    print("{0}{1}_{2}_hist.png is created..."\
        .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
    plt.close()
    return None      

def npy_filename_to_fileinfo(fullname):
    ############################################################
    # for modis hdf file, filename = 'DAAC_MOD04_3K/daily/sst_20110901_20110902_110_150_10_60_0.05_alldata.npy'    
    #
    fileinfo = fullname.split('_')
    start_date = fileinfo[-8]
    end_date = fileinfo[-7]
    Llon = fileinfo[-6]
    Rlon = fileinfo[-5]
    Slat = fileinfo[-4]
    Nlat = fileinfo[-3]
    resolution = fileinfo[-2]
    return start_date, end_date, Llon, Rlon, Slat, Nlat, resolution

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


def calculate_mean_using_result_array(result_array):
    mean_array = result_array.copy()
    cnt_array = result_array.copy()
    for i in range(np.shape(result_array)[0]):
        for j in range(np.shape(result_array)[1]):
            
            if len(result_array[i][j])>0: mean_array[i][j] = np.mean(result_array[i][j])
            else : mean_array[i][j] = np.nan
            cnt_array[i][j] = len(result_array[i][j])
    
    mean_array = np.array(mean_array)
    cnt_array = np.array(cnt_array)
    return mean_array, cnt_array

def make_grid_array(Llon, Rlon, Slat, Nlat, resolution) :
    ############################################################
    # Llon, Rlon = 90, 150
    # Slat, Nlat = 10, 60
    # resolution = 0.025
    #
    
    import numpy as np
    
    ni = np.int((Rlon-Llon)/resolution+1.00)
    nj = np.int((Nlat-Slat)/resolution+1.00)
    array_data = []
    for i in range(ni):
        line_data = []
        for j in range(nj):
            line_data.append([])
        array_data.append(line_data)
    
    return array_data


def make_grid_array1(Llon, Rlon, Slat, Nlat, resolution) :
    ############################################################
    # Llon, Rlon = 90, 150
    # Slat, Nlat = 10, 60
    # resolution = 0.025
    #
    
    import numpy as np
    
    ni = np.int((Rlon-Llon)/resolution+1.00)
    nj = np.int((Nlat-Slat)/resolution+1.00)
    array_lon = []
    array_lat = []
    array_data = []
    for i in range(ni):
        line_lon = []
        line_lat = []
        line_data = []
        for j in range(nj):
            line_lon.append(Llon+resolution*i)
            line_lat.append(Nlat-resolution*j)
            line_data.append([])
        array_lon.append(line_lon)
        array_lat.append(line_lat)
        array_data.append(line_data)
    array_lon = np.array(array_lon)
    array_lat = np.array(array_lat)
    
    return array_lon, array_lat, array_data



def read_MODIS_hdf_to_ndarray(fullname, DATAFIELD_NAME):
    ##########################################################
    #
    #
    import numpy as np
    from pyhdf.SD import SD, SDC
    hdf = SD(fullname, SDC.READ)
    
    # Read AOD dataset.
    if DATAFIELD_NAME.upper() in hdf.datasets() :
        DATAFIELD_NAME = DATAFIELD_NAME.upper()
    
    if DATAFIELD_NAME in hdf.datasets() :
        hdf_raw = hdf.select(DATAFIELD_NAME)
        print("found data set of {}: {}".format(DATAFIELD_NAME, hdf_raw))
        
    else : 
        print("There is no data set of {}: {}".format(DATAFIELD_NAME, hdf_raw))
        hdf_raw = np.arange(0)
    
    # Read geolocation dataset.
    if 'Latitude' in hdf.datasets() and 'Longitude' in hdf.datasets():
        lat = hdf.select('Latitude')
        latitude = lat[:,:]
        lon = hdf.select('Longitude')
        longitude = lon[:,:]
        
    elif 'Latitude'.lower() in hdf.datasets() and 'Longitude'.lower()  in hdf.datasets():
        lat = hdf.select('Latitude'.lower())
        latitude = lat[:,:]
        lon = hdf.select('Longitude'.lower())
        longitude = lon[:,:]
    else : 
        latitude, longitude  \
            = np.arange(0), np.arange(0)
        
    if 'cntl_pt_cols' in hdf.datasets() and 'cntl_pt_rows' in hdf.datasets():
        cntl_pt_cols = hdf.select('cntl_pt_cols')
        cntl_pt_cols = cntl_pt_cols[:]
        cntl_pt_rows = hdf.select('cntl_pt_rows')
        cntl_pt_rows = cntl_pt_rows[:]
    else :
        cntl_pt_cols, cntl_pt_rows =  np.arange(0), np.arange(0)    
        
    return hdf_raw, latitude, longitude, cntl_pt_cols, cntl_pt_rows
                    
                    

def read_MODIS_hdf_and_make_statistics_array(dir_name, DATAFIELD_NAME, proc_date, 
                             resolution, Llon, Rlon, Slat, Nlat):
    
    proc_start_date = proc_date[0]
    proc_end_date = proc_date[1]
    thread_number = proc_date[2]
    processing_log = '#This file is created using python \n' \
                '#https://github.com/guitar79/KOSC_MODIS_SST_Python \n' \
                + '#start date = ' + str(proc_date[0]) +'\n'\
                + '#end date = ' + str(proc_date[1]) +'\n'
    
    #convert start_date and end_date to date type
    start_date = datetime(int(proc_start_date[:4]), 
                          int(proc_start_date[4:6]), 
                          int(proc_start_date[6:8])) 
    end_date = datetime(int(proc_end_date[:4]), 
                        int(proc_end_date[4:6]), 
                        int(proc_end_date[6:8]))
    
    processing_log += '#Llon =' + str(Llon) + '\n' \
    + '#Rlon =' + str(Rlon) + '\n' \
    + '#Slat =' + str(Slat) + '\n' \
    + '#Nlat =' + str(Nlat) + '\n' \
    + '#resolution =' + str(resolution) + '\n'

    print("{0}-{1} Start making grid arrays...\n".format(proc_start_date, proc_end_date))
    ni = np.int((Rlon-Llon)/resolution+1.00)
    nj = np.int((Nlat-Slat)/resolution+1.00)
    array_lon = []
    array_lat = []
    array_data = []
    for i in range(ni):
        line_lon = []
        line_lat = []
        line_data = []
        for j in range(nj):
            line_lon.append(Llon+resolution*i)
            line_lat.append(Nlat-resolution*j)
            line_data.append([])
        array_lon.append(line_lon)
        array_lat.append(line_lat)
        array_data.append(line_data)
    array_lat = np.array(array_lat)
    array_lon = np.array(array_lon)
    print('Grid arrays are created...........\n')

    total_data_cnt = 0
    file_no = 0
    processing_log += '#processing file list\n'
    processing_log += '#No, data_count, filename \n'

    result_array = np.zeros((1, 1, 1))
    fullnames = sorted(glob(os.path.join(dir_name, '*.hdf')))
    if not fullnames  :
        for fullname in fullnames:
            result_array = array_data
            file_date = fullname_to_datetime_for_MODIS_3K(fullname)
            #print('fileinfo', file_date)
    
            if file_date >= start_date \
                and file_date < end_date :
    
                try:
                    print('reading file {0}\n'.format(fullname))
                    hdf = SD(fullname, SDC.READ)
                    # Read AOD dataset.
                    hdf_raw = hdf.select(DATAFIELD_NAME)
                    hdf_data = hdf_raw[:,:]
                    scale_factor = hdf_raw.attributes()['scale_factor']
                    offset = hdf_raw.attributes()['add_offset']
                    hdf_value = hdf_data * scale_factor + offset
                    hdf_value[hdf_value < 0] = np.nan
                    hdf_value = np.asarray(hdf_value)
    
                    # Read geolocation dataset.
                    lat = hdf.select('Latitude')
                    latitude = lat[:,:]
                    lon = hdf.select('Longitude')
                    longitude = lon[:,:]
                except Exception as err :
                    print("Something got wrecked : {}".format(err))
                    continue
    
                if np.shape(longitude) != np.shape(latitude) or np.shape(latitude) != np.shape(hdf_value) :
                    print('data shape is different!! \n')
                    print('='*80)
                else :
                    lon_cood = np.array(((longitude-Llon)/resolution*100//100), dtype=np.uint16)
                    lat_cood = np.array(((Nlat-latitude)/resolution*100//100), dtype=np.uint16)
                    data_cnt = 0
                    for i in range(np.shape(lon_cood)[0]) :
                        for j in range(np.shape(lon_cood)[1]) :
                            if int(lon_cood[i][j]) < np.shape(array_lon)[0] \
                                and int(lat_cood[i][j]) < np.shape(array_lon)[1] \
                                and not np.isnan(hdf_value[i][j]) :
                                data_cnt += 1 #for debug
                                result_array[int(lon_cood[i][j])][int(lat_cood[i][j])].append(hdf_value[i][j])
                    file_no += 1
                    total_data_cnt += data_cnt
                processing_log += str(file_no) + ',' + str(data_cnt) +',' + str(fullname) + '\n'
                print(thread_number,  proc_date[0], 'number of files: ', 
                      file_no, 'tatal data cnt :' , data_cnt)
        processing_log += '#total data number =' + str(total_data_cnt) + '\n'        
    
    else  :
        print("No file exist...")

    return result_array, processing_log




def read_MODIS_SST_hdf_and_array_by_date(save_dir_name, dir_name, proc_date, 
                             resolution, Llon, Rlon, Slat, Nlat):
    add_log = True
    if add_log == True :
        log_file = 'read_MODIS_AOD_hdf_and_array_by_date.log'
        err_log_file = 'read_MODIS_AOD_hdf_and_array_by_date_err.log'
    
    proc_start_date = proc_date[0]
    proc_end_date = proc_date[1]
    thread_number = proc_date[2]
    processing_log = '#This file is created using python \n' \
                '#https://github.com/guitar79/MODIS_AOD \n' \
                + '#start date = ' + str(proc_date[0]) +'\n'\
                + '#end date = ' + str(proc_date[1]) +'\n'
    #variables for downloading
    start_date = datetime(int(proc_start_date[:4]), 
                          int(proc_start_date[4:6]), 
                          int(proc_start_date[6:8])) #convert startdate to date type
    end_date = datetime(int(proc_end_date[:4]), 
                        int(proc_end_date[4:6]), 
                        int(proc_end_date[6:8])) #convert startdate to date type
    
    print('checking... {0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_result.npy\n'\
          .format(save_dir_name, proc_start_date, proc_end_date, 
          str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) 
    if os.path.exists('{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_result.npy'\
          .format(save_dir_name, proc_start_date, proc_end_date, 
          str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) \
        and os.path.exists('{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_info.txt'\
          .format(save_dir_name, proc_start_date, proc_end_date, 
          str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))):
            
        print('='*80)
        write_log(log_file, '{8} ::: {0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7} files are already exist'\
                  .format(save_dir_name, proc_start_date, proc_end_date, 
                  str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution), datetime.now()))
        return 0
    
    else : 
        processing_log += '#Llon =' + str(Llon) + '\n' \
        + '#Rlon =' + str(Rlon) + '\n' \
        + '#Slat =' + str(Slat) + '\n' \
        + '#Nlat =' + str(Nlat) + '\n' \
        + '#resolution =' + str(resolution) + '\n'

        print('{0}-{1} Start making grid arrays...\n'\
              .format(proc_start_date, proc_end_date))
        ni = np.int((Rlon-Llon)/resolution+1.00)
        nj = np.int((Nlat-Slat)/resolution+1.00)
        array_lon = []
        array_lat = []
        array_data = []
        for i in range(ni):
            line_lon = []
            line_lat = []
            line_data = []
            for j in range(nj):
                line_lon.append(Llon+resolution*i)
                line_lat.append(Nlat-resolution*j)
                line_data.append([])
            array_lon.append(line_lon)
            array_lat.append(line_lat)
            array_data.append(line_data)
        array_lat = np.array(array_lat)
        array_lon = np.array(array_lon)
        print('grid arrays are created...........\n')

        total_data_cnt = 0
        file_no=0
        processing_log += '#processing file list\n'
        processing_log += '#No, data_count, filename \n'

        result_array = np.zeros((1, 1, 1))
        for fullname in sorted(glob(os.path.join(dir_name, '*.hdf'))):
            
            result_array = array_data
            file_date = fullname_to_datetime_for_MODIS_3K(fullname)
            #print('fileinfo', file_date)

            if file_date >= start_date \
                and file_date < end_date :

                try:
                    print('reading file {0}\n'.format(fullname))
                    hdf = SD(fullname, SDC.READ)
                    # Read AOD dataset.
                    DATAFIELD_NAME = 'Optical_Depth_Land_And_Ocean'
                    hdf_raw = hdf.select(DATAFIELD_NAME)
                    hdf_data = hdf_raw[:,:]
                    scale_factor = hdf_raw.attributes()['scale_factor']
                    offset = hdf_raw.attributes()['add_offset']
                    hdf_value = hdf_data * scale_factor + offset
                    hdf_value[hdf_value < 0] = np.nan
                    hdf_value = np.asarray(hdf_value)

                    # Read geolocation dataset.
                    lat = hdf.select('Latitude')
                    latitude = lat[:,:]
                    lon = hdf.select('Longitude')
                    longitude = lon[:,:]
                except Exception as err :
                    print("Something got wrecked \n")
                    write_log(err_log_file, '{2} ::: {0} with {1}'\
                      .format(err, fullname, datetime.now()))
                    continue

                if np.shape(longitude) != np.shape(latitude) or np.shape(latitude) != np.shape(hdf_value) :
                    print('data shape is different!! \n')
                    print('='*80)
                else :
                    lon_cood = np.array(((longitude-Llon)/resolution*100//100), dtype=np.uint16)
                    lat_cood = np.array(((Nlat-latitude)/resolution*100//100), dtype=np.uint16)
                    data_cnt = 0
                    for i in range(np.shape(lon_cood)[0]) :
                        for j in range(np.shape(lon_cood)[1]) :
                            if int(lon_cood[i][j]) < np.shape(array_lon)[0] \
                                and int(lat_cood[i][j]) < np.shape(array_lon)[1] \
                                and not np.isnan(hdf_value[i][j]) :
                                data_cnt += 1 #for debug
                                result_array[int(lon_cood[i][j])][int(lat_cood[i][j])].append(hdf_value[i][j])
                    file_no += 1
                    total_data_cnt += data_cnt
                processing_log += str(file_no) + ',' + str(data_cnt) +',' + str(fullname) + '\n'
                print(thread_number,  proc_date[0], 'number of files: ', 
                      file_no, 'tatal data cnt :' , data_cnt)
        processing_log += '#total data number =' + str(total_data_cnt) + '\n'

        np.save('{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_result.npy'\
                .format(save_dir_name, proc_start_date, proc_end_date, 
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), result_array)
        
        with open('{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_info.txt'\
                  .format(save_dir_name, proc_start_date, proc_end_date, 
                  str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), 'w') as f:
            f.write(processing_log)
        print('#'*60)
        write_log(log_file, '{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7} files are is created.'\
              .format(save_dir_name, proc_start_date, proc_end_date, 
                  str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))

    return 0 # Return a dummy value
    # Putting large values in Queue was slow than expected(~10min)
    #return result_array, processing_log

