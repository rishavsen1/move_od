#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd

# Tennessee buildings geojson from Microsoft Buildings github
# can be downloaded here: https://usbuildingdata.blob.core.windows.net/usbuildings-v2/Tennessee.geojson.zip


class MSBuildings:
    print("Running read_ms_buildings.py")

    def __init__(self, county_fips, county_cbg, ms_buildings_path, output_path, logger) -> None:
        self.county_fips = county_fips
        self.ms_buildings_df = gpd.read_file(ms_buildings_path)
        self.county_cbg = gpd.read_file(county_cbg)
        self.output_path = output_path
        self.logger = logger

    def buildings(self):
        county_cbg = self.county_cbg[self.county_cbg["COUNTYFP"] == self.county_fips].reset_index(drop=True)
        county_cbg.GEOID = county_cbg.GEOID.astype(str)
        county_cbg = county_cbg.to_crs("epsg:4326")

        county_builds = self.ms_buildings_df.sjoin(county_cbg[["GEOID", "geometry"]])

        county_builds["geo_centers"] = county_builds.geometry.centroid
        county_builds["location"] = county_builds.geo_centers.apply(lambda p: [p.y, p.x])
        county_builds.to_csv(f"{self.output_path}/county_buildings_MS.csv", index=False)
