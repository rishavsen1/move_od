#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import geopandas as gpd
import numpy as np
import random
import tqdm
from tqdm.notebook import tqdm_notebook
from shapely.geometry import Point
from datetime  import datetime, timedelta



class Lodes_comb:

    print('Running lodes_comb.py')

    def __init__(self, county_cbg,  data_path, ms_enabled, start_time, end_time, timedelta) -> None:
        self.county_cbg = county_cbg
        self.data_path =data_path
        self.ms_enabled = ms_enabled
        self.start_time = start_time
        self.end_time = end_time
        self.timedelta = timedelta

    def intpt_func(row):
        return Point(row['INTPTLON'], row['INTPTLAT'])

    def func_home_pt(row):
        return Point(row.home_loc_lon, row.home_loc_lat)
    
    def func_work_pt(row):
        return Point(row.work_loc_lon, row.work_loc_lat)

    def datetime_range(start, end, delta):
        current = start
        while current < end:
            yield current
            current += delta


    def generate_combs(self):

        print('Running lodes_comb.py func')

        #loading LODES data

        county_lodes = pd.read_csv(f'{self.data_path}/county_lodes_2019.csv', dtype={"TRACTCE20_home":"string", "TRACTCE20_work":"string"})
        # county_lodes.h_geocode = county_lodes.h_geocode.apply(lambda x: int(x/1000))
        # county_lodes.w_geocode = county_lodes.w_geocode.apply(lambda x: int(x/1000))
        county_lodes.w_geocode = county_lodes.w_geocode.astype(str)
        county_lodes.h_geocode = county_lodes.h_geocode.astype(str)

        #loading Hamilton county geodata
        county_cbg = pd.read_csv(f'{self.data_path}/county_cbg.csv')
        county_cbg['intpt'] = county_cbg[['INTPTLAT', 'INTPTLON']].apply(lambda p: Lodes_comb.intpt_func(p), axis=1)
        county_cbg = gpd.GeoDataFrame(county_cbg, geometry=gpd.GeoSeries.from_wkt(county_cbg.geometry))
        county_cbg.GEOID = county_cbg.GEOID.astype(str)
        county_cbg['location'] = county_cbg.intpt.apply(lambda p: [p.y, p.x])


        #loading residential buildings
        res_build = pd.read_csv(f'{self.data_path}/county_residential_buildings.csv', index_col=0)
        res_build = gpd.GeoDataFrame(res_build, geometry=gpd.GeoSeries.from_wkt(res_build.geometry))
        res_build['location'] = res_build.geometry.apply(lambda p: [p.y, p.x])

        #loading work buildings
        com_build = pd.read_csv(f'{self.data_path}/county_work_loc_poi_com_civ.csv', index_col=0)
        com_build = gpd.GeoDataFrame(com_build, geometry=gpd.GeoSeries.from_wkt(com_build.geometry))
        com_build['location'] = com_build.geometry.apply(lambda p: [p.y, p.x])
        com_build = com_build.reset_index()
        com_build.GEOID = com_build.GEOID.astype(str)

        #loading all buildings (MS dataset)

        if self.ms_enabled:
            ms_build = pd.read_csv(f'{self.data_path}/county_buildings_MS.csv')
            ms_build = gpd.GeoDataFrame(ms_build, geometry=gpd.GeoSeries.from_wkt(ms_build.geo_centers))
            ms_build.GEOID = ms_build.GEOID.astype(str)
            ms_build['location'] = ms_build.geometry.apply(lambda p: [p.y, p.x])

        # print(county_lodes.head())
        # print(county_cbg[['GEOID', 'geometry']].head())
        
        #aggregating total jobs for each combination of home and work cbg 
        county_lodes = county_lodes.groupby(['h_geocode', 'w_geocode']).agg(total_jobs=('total_jobs', sum)).reset_index().merge(county_cbg[['GEOID', 'geometry']], left_on='h_geocode', right_on='GEOID').rename({'geometry':'home_geom'}, axis=1).drop('GEOID', axis=1).merge(county_cbg[['GEOID', 'geometry']], left_on='w_geocode', right_on='GEOID').rename({'geometry':'work_geom'}, axis=1).drop('GEOID', axis=1).sort_values('total_jobs', ascending=False).reset_index(drop=True)
        county_lodes = gpd.GeoDataFrame(county_lodes)

        #generating array of start and return times (in 15 min intervals)
        times_morning = [datetime.strptime(dt.strftime('%H%M%S'), '%H%M%S') for dt in 
            Lodes_comb.datetime_range(datetime(2021, 9, 1, 7), datetime(2021, 9, 1, 9, 10), 
            timedelta(seconds=15))]
        times_evening = [datetime.strptime(dt.strftime('%H%M%S'), '%H%M%S') for dt in 
            Lodes_comb.datetime_range(datetime(2021, 9, 1, 16), datetime(2021, 9, 1, 18, 10), 
            timedelta(seconds=15))]



        # (times_morning[5] - datetime(1900, 1, 1)).total_seconds()
        # times_morning[1].strftime('%H:%M:%S')

        res_build.GEOID = res_build.GEOID.astype(str)
        com_build.GEOID = com_build.GEOID.astype(str)

        #setting the random seed
        np.random.seed(42)
        random.seed(42)

        prob_matrix = gpd.GeoDataFrame()

        # print(county_lodes.head())
        for idx, movement in county_lodes.iterrows():

            res = res_build[res_build.GEOID == movement.h_geocode].reset_index(drop=True)
            if res.empty:
                try:
                    res = ms_build[ms_build.GEOID == movement.h_geocode].sample(n=movement.total_jobs, random_state=42).reset_index(drop=True)
                except:
                    if self.ms_enabled:
                        try:
                            res = ms_build[ms_build.GEOID == movement.h_geocode].sample(n=movement.total_jobs, random_state=42, replace=True).reset_index(drop=True)
                        except:
                            res = county_cbg[county_cbg.GEOID == movement.h_geocode].reset_index(drop=True)

            com = com_build[com_build.GEOID == movement.w_geocode].reset_index(drop=True)
            if com.empty:
                try:
                    com = ms_build[ms_build.GEOID == movement.w_geocode].sample(n=movement.total_jobs, random_state=42).reset_index(drop=True)
                except:
                    if self.ms_enabled:
                        try:
                            com = ms_build[ms_build.GEOID == movement.w_geocode].sample(n=movement.total_jobs, random_state=42, replace=True).reset_index(drop=True)
                        except:
                            com = county_cbg[county_cbg.GEOID == movement.w_geocode].reset_index(drop=True)
            r = res
            c = com
        
            for job in range(movement.total_jobs):
            
                if c.empty:
                    c = com
                if r.empty:
                    r = res

                rand_r = random.randrange(0, r.shape[0])
                rand_c = random.randrange(0, c.shape[0])
                r_df = r.iloc[rand_r]
                c_df = c.iloc[rand_c]
                r = r.drop([rand_r]).reset_index(drop=True)
                c = c.drop([rand_c]).reset_index(drop=True)
                
                time_slot1 = np.random.choice(times_morning, size=1, replace=True)
                time_slot2 = np.random.choice(times_evening, size=1, replace=True)

                temp = gpd.GeoDataFrame()

                temp.loc[job, 'h_geocode'] = movement.h_geocode
                temp.loc[job, 'w_geocode'] = movement.w_geocode
                temp.loc[job, 'total_jobs'] = movement.total_jobs
                temp.loc[job, 'home_loc_lat'] = r_df.location[0]
                temp.loc[job, 'home_loc_lon'] = r_df.location[1]
                temp.loc[job, 'work_loc_lat'] = c_df.location[0]
                temp.loc[job, 'work_loc_lon'] = c_df.location[1]
                temp.loc[job, 'go_time'] = time_slot1[0].time()
                temp.loc[job, 'go_time_secs'] = (time_slot1[0] - datetime(1900, 1, 1)).total_seconds()
                temp.loc[job, 'go_time_str'] = time_slot1[0].strftime('%H:%M:%S')
                temp.loc[job, 'return_time'] = time_slot2[0].time()         
                temp.loc[job, 'return_time_secs'] = (time_slot2[0] - datetime(1900, 1, 1)).total_seconds()
                temp.loc[job, 'return_time_str'] = time_slot2[0].strftime('%H:%M:%S')

                prob_matrix = prob_matrix.append(temp, ignore_index=True)
            
        # print(prob_matrix.head())

        # convert the lat and lon points to shapely Points
        prob_matrix['home_geom'] = prob_matrix[['home_loc_lat', 'home_loc_lon']].apply(lambda row: Lodes_comb.func_home_pt(row), axis=1)
        prob_matrix['work_geom'] = prob_matrix[['work_loc_lat', 'work_loc_lon']].apply(lambda row: Lodes_comb.func_work_pt(row), axis=1)
        prob_matrix.h_geocode = prob_matrix.h_geocode.astype(str)
        prob_matrix.w_geocode = prob_matrix.w_geocode.astype(str)

        prob_matrix.to_csv(f'{self.data_path}/county_lodes_combinations.csv', index=False)
        # prob_matrix.to_parquet(f'{self.data_path}/county_lodes_combinations.parquet', engine='pyarrow', index=False)