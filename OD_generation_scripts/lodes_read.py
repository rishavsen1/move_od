#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd

# county_lodes_paths = ['../data/lodes/tn_od_main_JT00_2019.csv', '../data/lodes/tn_od_main_JT01_2019.csv',
#                     '../data/lodes/tn_od_main_JT02_2019.csv', '../data/lodes/tn_od_main_JT03_2019.csv',
#                     '../data/lodes/tn_od_main_JT04_2019.csv', '../data/lodes/tn_od_main_JT05_2019.csv']
# COUNTY = '037'

class Lodes_gen:

    print('\n Running lodes_read.py')
    
    def __init__(self, county, county_lodes_paths, county_cbg, data_path):
        self.COUNTY = county
        self.county_lodes_paths = county_lodes_paths
        self.county_cbg = county_cbg
        self.data_path = data_path
        self.df = pd.DataFrame()


    def generate(self):
    #loading parts of the available data
    #these files are not included here, can be downloaded from: https://lehd.ces.census.gov/data/lodes/LODES7/tn/od/
        
        #appending all the sources
        print(self.county_lodes_paths)

        for i, lodes_path in enumerate(self.county_lodes_paths):
            temp_df = pd.read_csv(lodes_path).rename(columns = {'S000':'total_jobs'})
            self.df = self.df.append(temp_df)
            
        #filtering out duplicates 
        tn_lodes = self.df.drop_duplicates()
        tn_lodes.h_geocode = tn_lodes.h_geocode.astype(str)
        tn_lodes.w_geocode = tn_lodes.w_geocode.astype(str)
        # initially in form of blocks - converting to match cbgs 
        tn_lodes.h_geocode = tn_lodes.h_geocode.apply(lambda x: x[0:-3])
        tn_lodes.w_geocode = tn_lodes.w_geocode.apply(lambda x: x[0:-3])

        print(tn_lodes.head())

        #read Hamilton county blocks (too large to store in github)
        # can be downloaded from : https://vanderbilt365-my.sharepoint.com/:f:/g/personal/rishav_sen_vanderbilt_edu/EuB8qV7yx3ZDoxpXq232E1cBJ1Q3Qlzr1cQOvP3UKWqmHw?e=cc1z5h

        cbgs  = gpd.read_file(self.county_cbg)
        cbgs = cbgs[cbgs.COUNTYFP == self.COUNTY][['GEOID', 'geometry']]
        cbgs.GEOID = cbgs.GEOID.astype(str)

        print(cbgs.head())

        # filtering TN LODES data for cbgs only in selected county
        area_lodes = pd.merge(tn_lodes, cbgs, left_on='h_geocode', right_on='GEOID', how='inner').merge(cbgs, left_on='w_geocode', right_on='GEOID', how='inner').sort_values('total_jobs', ascending=False).reset_index(drop=True)
        area_lodes = area_lodes.drop(['GEOID_x', 'GEOID_y', 'geometry_x', 'geometry_y'], axis=1)
        # print(area_lodes.head())
        # it is stored at the block level (smaller area than CBG)
        area_lodes.to_csv(f'{self.data_path}/county_lodes_2019.csv', index=False)

        #additional 

        # pd.merge(tn_lodes, blocks, left_on='h_geocode', right_on='GEOID', how='inner').groupby('h_geocode').sum().merge(blocks, left_on='h_geocode', right_on='GEOID', how='inner').sort_values('total_jobs', ascending=False).reset_index().to_csv('homes_blocks.csv')
        # pd.merge(tn_lodes, blocks, left_on='w_geocode', right_on='GEOID', how='inner').groupby('w_geocode').sum().merge(blocks, left_on='w_geocode', right_on='GEOID', how='inner').sort_values('total_jobs', ascending=False).reset_index().to_csv('work_blocks.csv')

        cbg  = gpd.read_file(self.county_cbg)
        cbg = cbg[cbg.COUNTYFP == self.COUNTY]
        cbg.GEOID = cbg.GEOID.astype(str)

        cbg.to_csv(f'{self.data_path}/county_cbg.csv', index=False)