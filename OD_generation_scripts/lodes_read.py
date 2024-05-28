#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from utils import intpt_func


class LodesGen:
    def __init__(self, county_fips, county_lodes_paths, county_cbg, output_path, logger, od_option):
        self.county_fips = county_fips
        self.county_lodes_paths = county_lodes_paths
        self.county_cbg = county_cbg
        self.output_path = output_path
        self.logger = logger
        self.od_option = od_option

    def read_and_process_lodes_file(self, lodes_path):
        # Read CSV file and rename columns
        temp_df = pd.read_csv(lodes_path).rename(columns={"S000": "total_jobs"})[
            ["h_geocode", "w_geocode", "total_jobs"]
        ]

        # Convert geocode columns
        temp_df.h_geocode = temp_df.h_geocode.astype(str).str[:-3].str.lstrip("0")
        temp_df.w_geocode = temp_df.w_geocode.astype(str).str[:-3].str.lstrip("0")

        return temp_df

    def generate(self):
        self.logger.info("Running lodes_read.py")
        # loading parts of the available data
        # these files are not included here, can be downloaded from: https://lehd.ces.census.gov/data/lodes/LODES7/tn/od/

        # appending all the sources
        self.logger.info(self.county_lodes_paths)
        all_lodes = pd.DataFrame()

        pool = Pool(processes=multiprocessing.cpu_count())
        results = pool.map(self.read_and_process_lodes_file, self.county_lodes_paths)
        pool.close()
        pool.join()

        all_lodes = pd.concat(results)
        lodes = all_lodes.drop_duplicates()

        cbgs = gpd.read_file(self.county_cbg)[["GEOID", "COUNTYFP", "geometry"]]
        cbgs.GEOID = cbgs.GEOID.astype(str)
        cbgs.GEOID = cbgs.GEOID.str.lstrip("0")
        
        cbgs = gpd.read_file(self.county_cbg)[["GEOID", "COUNTYFP", "geometry"]]
        cbgs.GEOID = cbgs.GEOID.astype(str)
        cbgs.GEOID = cbgs.GEOID.str.lstrip("0")

        if self.od_option == "Origin and Destination in same County":
            cbgs = cbgs[cbgs.COUNTYFP == self.county_fips][["GEOID", "geometry"]]
            area_lodes = (
                pd.merge(lodes, cbgs, left_on="h_geocode", right_on="GEOID", how="inner")
                .merge(cbgs, left_on="w_geocode", right_on="GEOID", how="inner")
                .sort_values("total_jobs", ascending=False)
                .reset_index(drop=True)
            )
        elif self.od_option == "Only Origin in County":
            cbgs_origin = cbgs[cbgs.COUNTYFP == self.county_fips][["GEOID", "geometry"]]
            area_lodes = (
                pd.merge(
                    lodes,
                    cbgs_origin,
                    left_on="h_geocode",
                    right_on="GEOID",
                    how="inner",
                )
                .merge(cbgs, left_on="w_geocode", right_on="GEOID", how="inner")
                .sort_values("total_jobs", ascending=False)
                .reset_index(drop=True)
            )
        elif self.od_option == "Only Destination in County":
            cbgs_dest = cbgs[cbgs.COUNTYFP == self.county_fips][["GEOID", "geometry"]]
            area_lodes = (
                pd.merge(lodes, cbgs, left_on="h_geocode", right_on="GEOID", how="inner")
                .merge(cbgs_dest, left_on="w_geocode", right_on="GEOID", how="inner")
                .sort_values("total_jobs", ascending=False)
                .reset_index(drop=True)
            )

        area_lodes = area_lodes.drop(["GEOID_x", "GEOID_y", "geometry_x", "geometry_y"], axis=1)

        # it is stored at the block level (smaller area than CBG)
        area_lodes.to_csv(f"{self.output_path}/county_lodes.csv", index=False)
        self.logger.info(f"County has {area_lodes.shape[0]} LODES entries")

        # additional

        # pd.merge(lodes, blocks, left_on='h_geocode', right_on='GEOID', how='inner').groupby('h_geocode').sum().merge(blocks, left_on='h_geocode', right_on='GEOID', how='inner').sort_values('total_jobs', ascending=False).reset_index().to_csv('homes_blocks.csv')
        # pd.merge(lodes, blocks, left_on='w_geocode', right_on='GEOID', how='inner').groupby('w_geocode').sum().merge(blocks, left_on='w_geocode', right_on='GEOID', how='inner').sort_values('total_jobs', ascending=False).reset_index().to_csv('work_blocks.csv')

        cbg = gpd.read_file(self.county_cbg)
        cbg = cbg[cbg.COUNTYFP == self.county_fips]
        cbg.GEOID = cbg.GEOID.astype(str)
        cbg["intpt"] = cbg[["INTPTLAT", "INTPTLON"]].apply(lambda p: intpt_func(p), axis=1)
        cbg["location"] = cbg.intpt.apply(lambda p: [p.y, p.x])

        cbg = cbg[["GEOID", "COUNTYFP", "geometry", "intpt", "location"]]

        cbg.to_csv(f"{self.output_path}/county_cbg.csv", index=False)
        self.logger.info(f"County has {cbg.shape[0]} census block groups")
