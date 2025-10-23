#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from generate.utils import intpt_func


class LodesGen:
    def __init__(self, county_fips, county_lodes_paths, county_geoid_df, output_path, logger, od_option):
        self.county_fips = county_fips
        self.county_lodes_paths = county_lodes_paths
        self.county_geoid_df = county_geoid_df
        self.output_path = output_path
        self.logger = logger
        self.od_option = od_option

    def read_and_process_lodes_file(self, lodes_path):
        # Read CSV file and rename columns
        temp_df = pd.read_csv(lodes_path).rename(columns={"S000": "total_jobs"})[
            ["h_geocode", "w_geocode", "total_jobs"]
        ]

        # Convert geocode columns
        temp_df.h_geocode = temp_df.h_geocode.astype(str).str[:-3]
        temp_df.w_geocode = temp_df.w_geocode.astype(str).str[:-3]

        temp_df = temp_df.groupby(["h_geocode", "w_geocode"]).sum().reset_index()

        return temp_df

    def generate(self):
        self.logger.info("Running lodes_read.py")
        self.logger.info(self.county_lodes_paths)

        results = []
        for county_lodes_path in self.county_lodes_paths:
            results.append(self.read_and_process_lodes_file(county_lodes_path))

        lodes_df = pd.concat(results)
        lodes_df = lodes_df.drop_duplicates()

        county_geoid_df = self.county_geoid_df

        if self.od_option == "Origin and Destination in same County":
            county_geoid_df_temp = county_geoid_df[county_geoid_df.COUNTYFP == self.county_fips][["GEOID", "COUNTYFP"]]
            county_lodes_df = (
                pd.merge(lodes_df, county_geoid_df_temp, left_on="h_geocode", right_on="GEOID", how="inner")
                .merge(county_geoid_df_temp, left_on="w_geocode", right_on="GEOID", how="inner")
                .sort_values("total_jobs", ascending=False)
                .reset_index(drop=True)
            )
        elif self.od_option == "Only Origin in County":
            county_geoid_origin = county_geoid_df[county_geoid_df.COUNTYFP == int(self.county_fips)][
                ["GEOID", "COUNTYFP"]
            ]
            county_lodes_df = (
                pd.merge(
                    lodes_df,
                    county_geoid_origin[["GEOID", "COUNTYFP"]],
                    left_on="h_geocode",
                    right_on="GEOID",
                    how="inner",
                )
                .merge(county_geoid_df[["GEOID", "COUNTYFP"]], left_on="w_geocode", right_on="GEOID", how="inner")
                .sort_values("total_jobs", ascending=False)
                .reset_index(drop=True),
            )

        elif self.od_option == "Only Destination in County":
            county_geoid_dest = county_geoid_df[county_geoid_df.COUNTYFP == int(self.county_fips)][
                ["GEOID", "COUNTYFP"]
            ]
            county_lodes_df = (
                pd.merge(
                    lodes_df,
                    county_geoid_df[["GEOID", "COUNTYFP"]],
                    left_on="h_geocode",
                    right_on="GEOID",
                    how="inner",
                )
                .merge(county_geoid_dest[["GEOID", "COUNTYFP"]], left_on="w_geocode", right_on="GEOID", how="inner")
                .sort_values("total_jobs", ascending=False)
                .reset_index(drop=True),
            )

        # it is stored at the block level (smaller area than GEOID)

        county_lodes_df.to_parquet(f"{self.output_path}/county_lodes.parquet")

        unique_countyfps = list(
            set(county_lodes_df["COUNTYFP_x"].astype(str).unique()).union(
                set(county_lodes_df["COUNTYFP_y"].astype(str).unique())
            )
        )
        county_lodes_df = county_lodes_df.drop(["GEOID_x", "GEOID_y", "COUNTYFP_x", "COUNTYFP_y"], axis=1)

        self.logger.info(f"County has {county_lodes_df['total_jobs'].sum()} job movements")

        success = False
        if type(county_lodes_df) == pd.DataFrame and county_lodes_df.shape[0] > 0:
            success = True

        return county_lodes_df, unique_countyfps, success
