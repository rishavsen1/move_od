#!/usr/bin/env python
# coding: utf-8

import osmnx as ox
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, Point
import geopandas as gpd
import pandas as pd
# from logger import logger

# COUNTY = '037'
# AREA = 'Davidson'

# print(COUNTY)

# input Hamilton county geo file
# county_cbg = gpd.read_file('../data/Tennessee Census Block Groups/tl_2020_47_bg.shp')

# sg_enabled = False
# sg = pd.read_csv('../data/sg_poi_cbgs.csv') # path removed due to privacy concerns


class locations_OSM_SG:


    def __init__(self, county, area, county_cbg, sg_enabled, data_path):
        print('Initliazing locations_OSM_SG.py')
        self.COUNTY = county
        self.AREA = area
        self.county_cbg = gpd.read_file(county_cbg)
        # self.sg = pd.read_csv(sg)
        self.sg_enabled = sg_enabled
        self.data_path = data_path


    def func(row):
        str(Point(gpd.points_from_xy(row.INTPTLAT, row.INTPTLON)[0]))


    def find_locations_OSM(self):
        
        print('Running locations_OSM_SG.py func')
        self.county_cbg = self.county_cbg[self.county_cbg.COUNTYFP==self.COUNTY]
        self.county_cbg = self.county_cbg.to_crs('epsg:4326')

        minx, miny, maxx, maxy = self.county_cbg.geometry.total_bounds

        #finding all buildings
        tags = {'building': True}
        buildings = ox.geometries_from_bbox(miny, maxy, maxx, minx, tags)
        # buildings = ox.geometries_from_bbox(34.854382885097905, 35.935532323321, -84.19759521484375, -85.553161621093756, tags)


        #aggregating all residential tags
        pd.set_option('display.max_columns', None)
        res_build = buildings[(buildings.building == 'residential') | (buildings.building == 'bungalow') | (buildings.building == 'cabin') | (buildings.building == 'dormitory') | (buildings.building == 'hotel') | (buildings.building == 'house') | (buildings.building == 'semidetached_house') | (buildings.building == 'barracks') | (buildings.building == 'farm') | (buildings.building == 'ger') | (buildings.building == 'houseboat') | (buildings.building == 'static_caravan') | (buildings.building == 'terrace')].reset_index()[['osmid', 'geometry', 'nodes', 'building', 'name', 'source']].sjoin(self.county_cbg)
        res_build.geometry = res_build.geometry.apply(lambda x: x.centroid if x.geom_type =='Polygon' else x)

        #saving residential buildings
        #TODO: Error Handling
        try:
            res_build.to_csv(f'{self.data_path}/county_residential_buildings.csv', index=False)
        except FileNotFoundError:
            print(f'File not found: {self.data_path}/county_residential_buildings.csv')
        except:
            print('General exception')


        #work tags

        com_build = buildings[(buildings.building == 'commercial') | (buildings.building == 'industrial') | (buildings.building == 'kiosk') | (buildings.building == 'office') | (buildings.building == 'retail') | (buildings.building == 'supermarket') | (buildings.building == 'warehouse')].reset_index()[['osmid', 'geometry', 'nodes', 'building', 'name', 'source']].sjoin(self.county_cbg)
        civ_build = buildings[(buildings.building == 'bakehouse') | (buildings.building == 'civic') | (buildings.building == 'college') | (buildings.building == 'fire_station') | (buildings.building == 'government') | (buildings.building == 'hospital') | (buildings.building == 'kindergarten') | (buildings.building == 'public') | (buildings.building == 'school') | (buildings.building == 'train_station') | (buildings.building == 'transportation') | (buildings.building == 'university')].reset_index()[['osmid', 'geometry', 'nodes', 'building', 'name', 'source']].sjoin(self.county_cbg)

        com_build.geometry = com_build.geometry.apply(lambda x: x.centroid if x.geom_type =='Polygon' else x)
        civ_build.geometry = civ_build.geometry.apply(lambda x: x.centroid if x.geom_type =='Polygon' else x)
 
        #converting the default internal point of each cbg to a shapely Point
        res_build['intpt'] = res_build[['INTPTLAT', 'INTPTLON']].apply(lambda x: locations_OSM_SG.func, axis=1)

        if self.sg_enabled:
            locations_OSM_SG.find_locations_SG(self, com_build, civ_build)

    # ## adding safegraph poi locations
    def find_locations_SG(self, com_build, civ_build):
        

        # sg = gpd.read_file('path to safegraph file') # path removed due to privacy concerns
        self.sg = pd.read_csv(f'{self.data_path}/sg_poi_cbgs.csv')
        self.sg.longitude = self.sg.longitude.astype(float)
        self.sg.latitude = self.sg.latitude.astype(float)
        geom = [Point(xy) for xy in zip(self.sg.longitude, self.sg.latitude)]
        self.sg = gpd.GeoDataFrame(self.sg, geometry = geom, crs='epsg:4326')

        #adding in safegraph POI locations to OSM work locations

        t1 = (self.sg[['poi_cbg', 'geometry']].rename({'poi_cbg':'GEOID'},axis=1).append(com_build[['GEOID', 'geometry']]).append(civ_build[['GEOID', 'geometry']]))
        # t1 = com_build[['GEOID', 'geometry']].append(civ_build[['GEOID', 'geometry']])
        t1.GEOID = t1.GEOID.astype(str).apply(lambda x: x.split('.')[0])

        # saving work buildings to file 
        t1.to_csv(f'{self.data_path}/county_work_loc_poi_com_civ.csv', index=False)


# def preproc(x, attr:str, attr_name:str, ret_attr:str) -> any:
#     if getattr(x, attr) == attr_name:
#         return getattr(x, ret_attr)
#     return x

# Mixin