#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd

# Tennessee buildings geojson from Microsoft Buildings github
# can be downloaded here: https://usbuildingdata.blob.core.windows.net/usbuildings-v2/Tennessee.geojson.zip


class MSBuildings:

    def __init__(self, county_fips, county_geoid_df, ms_buildings_path, output_path, logger) -> None:
        self.county_fips = county_fips
        self.ms_buildings_df = gpd.read_file(ms_buildings_path)
        self.county_geoid_df = county_geoid_df
        self.output_path = output_path
        self.logger = logger

    def buildings(self):
        print("Running read_ms_buildings.py")
        county_geoid_df = self.county_geoid_df[self.county_geoid_df["COUNTYFP"] == self.county_fips].reset_index(
            drop=True
        )
        county_geoid_df.GEOID = county_geoid_df.GEOID.astype(str)
        county_geoid_df = county_geoid_df.to_crs("epsg:4326")

        ms_buildings_df = self.ms_buildings_df.sjoin(county_geoid_df[["GEOID", "geometry"]])

        ms_buildings_df.to_file(f"{self.output_path}/county_buildings_MS.geojson", driver="GeoJSON")

        ms_buildings_df["geo_centers"] = ms_buildings_df.geometry.centroid
        ms_buildings_df["location"] = ms_buildings_df.geometry.centroid.apply(lambda p: [p.y, p.x])
        ms_buildings_df = ms_buildings_df[["geometry", "GEOID", "geo_centers", "location"]]

        success = False
        if type(ms_buildings_df) == gpd.GeoDataFrame and ms_buildings_df.shape[0] > 0:
            success = True

        return ms_buildings_df, success
