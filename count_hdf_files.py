'''
Created on Mon Nov  5 18:47:09 2018
@author: guitar79
created by Kevin 

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''

from glob import glob
import os

years = range(2000, 2018)

total_num = 0
for year in years :
    try:
        dir_name = 'DAAC_MOD04_3K/'+str(year)+'/'
        list_of_hdf = sorted(glob(os.path.join(dir_name, '*.hdf')))
        print(year, len(list_of_hdf))
        total_num += len(list_of_hdf)             
    except:
        print("Something got wrecked \n")
        continue
    
print('total', total_num)