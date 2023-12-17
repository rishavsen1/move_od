#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd

# county_lodes_paths = ['../data/lodes/tn_od_main_JT00_2019.csv', '../data/lodes/tn_od_main_JT01_2019.csv',
#                     '../data/lodes/tn_od_main_JT02_2019.csv', '../data/lodes/tn_od_main_JT03_2019.csv',
#                     '../data/lodes/tn_od_main_JT04_2019.csv', '../data/lodes/tn_od_main_JT05_2019.csv']
# COUNTY = '037'


class Lodes_gen:
    def __init__(self, county, county_lodes_paths, county_cbg, output_path, logger, od_option):
        self.COUNTY = county
        self.county_lodes_paths = county_lodes_paths
        self.county_cbg = county_cbg
        self.output_path = output_path
        self.df = pd.DataFrame()
        self.logger = logger
        self.od_option = od_option
        self.logger.info("\n Running lodes_read.py")

    def generate(self):
        # loading parts of the available data
        # these files are not included here, can be downloaded from: https://lehd.ces.census.gov/data/lodes/LODES7/tn/od/

        # appending all the sources
        self.logger.info(self.county_lodes_paths)
        temp_dfs = []

        for i, lodes_path in enumerate(self.county_lodes_paths):
            temp_df = pd.read_csv(lodes_path).rename(columns={"S000": "total_jobs"})[
                ["h_geocode", "w_geocode", "total_jobs"]
            ]
            # self.df = self.df.append(temp_df)
            temp_dfs.append(temp_df)

        self.df = pd.concat(temp_dfs, ignore_index=True)

        # filtering out duplicates
        lodes = self.df.drop_duplicates()

        lodes.h_geocode = lodes.h_geocode.astype(str)
        lodes.w_geocode = lodes.w_geocode.astype(str)
        # initially in form of blocks - converting to match cbgs
        lodes["h_geocode"] = lodes["h_geocode"].str[:-3]
        lodes["w_geocode"] = lodes["w_geocode"].str[:-3]

        lodes.h_geocode = lodes.h_geocode.str.lstrip("0")
        lodes.w_geocode = lodes.w_geocode.str.lstrip("0")

        # self.logger.info(lodes.head())

        # read Hamilton county blocks (too large to store in github)
        # can be downloaded from : https://vanderbilt365-my.sharepoint.com/:f:/g/personal/rishav_sen_vanderbilt_edu/EuB8qV7yx3ZDoxpXq232E1cBJ1Q3Qlzr1cQOvP3UKWqmHw?e=cc1z5h

        cbgs = gpd.read_file(self.county_cbg)[["GEOID", "COUNTYFP", "geometry"]]
        cbgs.GEOID = cbgs.GEOID.astype(str)
        cbgs.GEOID = cbgs.GEOID.str.lstrip("0")

        if self.od_option == "Origin and Destination in same County":
            cbgs = cbgs[cbgs.COUNTYFP == self.COUNTY][["GEOID", "geometry"]]
        elif self.od_option == "Only Origin in County":
            cbgs_origin = cbgs[cbgs.COUNTYFP == self.COUNTY][["GEOID", "geometry"]]
        elif self.od_option == "Only Destination in County":
            cbgs_dest = cbgs[cbgs.COUNTYFP == self.COUNTY][["GEOID", "geometry"]]

        # self.logger.info(cbgs.head())

        # filtering TN LODES data for cbgs only in selected county
        if self.od_option == "Origin and Destination in same County":
            area_lodes = (
                pd.merge(lodes, cbgs, left_on="h_geocode", right_on="GEOID", how="inner")
                .merge(cbgs, left_on="w_geocode", right_on="GEOID", how="inner")
                .sort_values("total_jobs", ascending=False)
                .reset_index(drop=True)
            )
        elif self.od_option == "Only Origin in County":
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
            area_lodes = (
                pd.merge(lodes, cbgs, left_on="h_geocode", right_on="GEOID", how="inner")
                .merge(cbgs_dest, left_on="w_geocode", right_on="GEOID", how="inner")
                .sort_values("total_jobs", ascending=False)
                .reset_index(drop=True)
            )

        area_lodes = area_lodes.drop(["GEOID_x", "GEOID_y", "geometry_x", "geometry_y"], axis=1)
        # self.logger.info(area_lodes.head())
        # it is stored at the block level (smaller area than CBG)
        area_lodes.to_csv(f"{self.output_path}/county_lodes.csv", index=False)

        # additional

        # pd.merge(lodes, blocks, left_on='h_geocode', right_on='GEOID', how='inner').groupby('h_geocode').sum().merge(blocks, left_on='h_geocode', right_on='GEOID', how='inner').sort_values('total_jobs', ascending=False).reset_index().to_csv('homes_blocks.csv')
        # pd.merge(lodes, blocks, left_on='w_geocode', right_on='GEOID', how='inner').groupby('w_geocode').sum().merge(blocks, left_on='w_geocode', right_on='GEOID', how='inner').sort_values('total_jobs', ascending=False).reset_index().to_csv('work_blocks.csv')

        cbg = gpd.read_file(self.county_cbg)
        cbg = cbg[cbg.COUNTYFP == self.COUNTY]
        cbg.GEOID = cbg.GEOID.astype(str)

        cbg.to_csv(f"{self.output_path}/county_cbg.csv", index=False)
