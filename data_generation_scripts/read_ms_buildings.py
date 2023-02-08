#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd

#Tennessee buildings geojson from Microsoft Buildings github 
#can be downloaded here: https://usbuildingdata.blob.core.windows.net/usbuildings-v2/Tennessee.geojson.zip

class MS_Buildings:

    print("Running read_ms_buildings.py")

    def __init__(self, county, county_cbg, builds, data_path) -> None:
        self.COUNTY = county
        self.builds = gpd.read_file(builds)
        self.state_cbg = gpd.read_file(county_cbg)
        self.data_path = data_path

    
    def buildings(self):
        # builds = gpd.read_file('../data/Tennessee.geojson')
        # COUNTY = '037'
        # state_cbg = gpd.read_file('../data/Tennessee Census Block Groups/tl_2020_47_bg.shp')

        #input the cbgs and filter for the county
        county_cbg = self.state_cbg[self.state_cbg['COUNTYFP']==self.COUNTY].reset_index(drop=True)
        county_cbg.GEOID = county_cbg.GEOID.astype(str)
        county_cbg = county_cbg.to_crs('epsg:4326')

        #choosing buildings only in the county
        county_builds = self.builds.sjoin(county_cbg[['GEOID', 'geometry']])

        #finding center of buildings from MS building footprints which are usually Polygons
        county_builds['geo_centers'] = county_builds.geometry.centroid

        # ax=county_cbg.plot(figsize=(10, 10), color='None', edgecolor="black", linewidth=0.4)
        # county_builds.geo_centers.plot(figsize=(10, 10), ax=ax, markersize=0.1)

        county_builds['location'] = county_builds.geo_centers.apply(lambda p: [p.y, p.x])
        county_builds.to_csv(f'{self.data_path}/county_buildings_MS.csv', index=False)