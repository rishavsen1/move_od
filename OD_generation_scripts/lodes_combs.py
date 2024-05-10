#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import geopandas as gpd
import numpy as np
import random
import dask
from shapely.geometry import Point
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from dask import delayed, compute
from utils import marginal_dist, get_travel_time_dict


class LodesComb:
    def __init__(
        self,
        county_cbg,
        data_path,
        ms_enabled,
        timedelta,
        datetime_ranges,
        logger,
    ) -> None:
        self.county_cbg = county_cbg
        self.data_path = data_path
        self.ms_enabled = ms_enabled
        self.timedelta = timedelta
        self.datetime_ranges = datetime_ranges
        self.logger = logger
        self.logger.info("Initalizing lodes_comb.py")

    def intpt_func(self, row):
        return Point(row["INTPTLON"], row["INTPTLAT"])

    def func_origin_pt(self, row):
        return Point(row.origin_loc_lon, row.origin_loc_lat)

    def func_dest_pt(self, row):
        return Point(row.dest_loc_lon, row.dest_loc_lat)

    def datetime_range(self, start, end, delta):
        current = start
        while current < end:
            yield current
            current += delta

    def read_county_lodes(self, county_lodes, county_cbg):
        self.logger.info("Running lodes_comb.py func")

        county_lodes.w_geocode = county_lodes.w_geocode.astype(str)
        county_lodes.h_geocode = county_lodes.h_geocode.astype(str)

        # aggregating total jobs for each combination of home and work cbg
        county_lodes = (
            county_lodes.groupby(["h_geocode", "w_geocode"])
            .agg(total_jobs=("total_jobs", sum))
            .reset_index()
            .merge(county_cbg[["GEOID", "geometry"]], left_on="h_geocode", right_on="GEOID")
            .rename({"geometry": "origin_geom"}, axis=1)
            .drop("GEOID", axis=1)
            .merge(county_cbg[["GEOID", "geometry"]], left_on="w_geocode", right_on="GEOID")
            .rename({"geometry": "dest_geom"}, axis=1)
            .drop("GEOID", axis=1)
            .sort_values("total_jobs", ascending=False)
            .reset_index(drop=True)
        )
        county_lodes = gpd.GeoDataFrame(county_lodes)

        return county_lodes

    def generate_OD(self, params):
        day, county_lodes, county_cbg, res_build, com_build, ms_build, datetime_ranges = params
        prob_matrix = pd.DataFrame()
        mode_type = "drive"

        specific_list = self.datetime_ranges[0]

        temp_data = []  # List to store all temporary data frames

        # Pre-filtering based on unique GEOIDs to reduce lookup times
        unique_h_geocodes = county_lodes["h_geocode"].unique()
        unique_w_geocodes = county_lodes["w_geocode"].unique()

        res_build_filtered = res_build[res_build["GEOID"].isin(unique_h_geocodes)]
        com_build_filtered = com_build[com_build["GEOID"].isin(unique_w_geocodes)]
        ms_build_filtered = ms_build[ms_build["GEOID"].isin(np.concatenate((unique_h_geocodes, unique_w_geocodes)))]
        county_cbg_filtered = county_cbg[
            county_cbg["GEOID"].isin(np.concatenate((unique_h_geocodes, unique_w_geocodes)))
        ]

        # Creating dictionaries for quick look-up
        res_dict = {geo_id: df for geo_id, df in res_build_filtered.groupby("GEOID")}
        com_dict = {geo_id: df for geo_id, df in com_build_filtered.groupby("GEOID")}
        ms_dict = {geo_id: df for geo_id, df in ms_build_filtered.groupby("GEOID")}
        cbg_dict = {geo_id: df for geo_id, df in county_cbg_filtered.groupby("GEOID")}

        for index, movement in county_lodes.iterrows():
            res = res_dict.get(movement.h_geocode, pd.DataFrame())
            if res.empty and self.ms_enabled:
                res = ms_dict.get(movement.h_geocode, pd.DataFrame())
            if res.empty:
                res = cbg_dict.get(movement.h_geocode, pd.DataFrame())

            com = com_dict.get(movement.w_geocode, pd.DataFrame())
            if com.empty and self.ms_enabled:
                com = ms_dict.get(movement.w_geocode, pd.DataFrame())
            if com.empty:
                com = cbg_dict.get(movement.w_geocode, pd.DataFrame())

            num_res = len(res)
            num_com = len(com)
            repeat_res = movement.total_jobs // num_res if num_res else 0
            repeat_com = movement.total_jobs // num_com if num_com else 0
            additional_res = movement.total_jobs % num_res
            additional_com = movement.total_jobs % num_com

            sampled_res = pd.concat(
                [res] * repeat_res + [res.sample(n=additional_res, random_state=42)], ignore_index=True
            )
            sampled_com = pd.concat(
                [com] * repeat_com + [com.sample(n=additional_com, random_state=42)], ignore_index=True
            )

            sampled_res = sampled_res.sample(frac=1, random_state=42).reset_index(drop=True)
            sampled_com = sampled_com.sample(frac=1, random_state=42).reset_index(drop=True)

            for i in range(movement.total_jobs):
                temp_dict = {
                    "h_geocode": movement.h_geocode,
                    "w_geocode": movement.w_geocode,
                    "total_jobs": movement.total_jobs,
                    "origin_loc_lat": sampled_res.iloc[i]["location"][0],
                    "origin_loc_lon": sampled_res.iloc[i]["location"][1],
                    "dest_loc_lat": sampled_com.iloc[i]["location"][0],
                    "dest_loc_lon": sampled_com.iloc[i]["location"][1],
                }

                pickup_time = random.choice(specific_list)
                time_part = pickup_time.time()
                seconds_since_midnight = (time_part.hour * 3600) + (time_part.minute * 60) + time_part.second
                temp_dict.update(
                    {
                        "pickup_time": pickup_time.time(),
                        "pickup_time_secs": seconds_since_midnight,
                        "pickup_time_str": pickup_time.time().strftime("%H:%M:%S"),
                    }
                )
                move_time, total_distance, distance_miles = list(get_travel_time_dict(mode_type, temp_dict).values())

                temp_dict.update(
                    {
                        "time_taken": move_time,
                        "total_distance": total_distance,
                        "distance_miles": distance_miles,
                    }
                )

                temp_data.append(temp_dict)

        prob_matrix = gpd.GeoDataFrame(temp_data)

        return (day, prob_matrix)

    def main(self, county_cbg, res_build, com_build, ms_build, county_lodes, sample_size):
        county_lodes = self.read_county_lodes(county_lodes, county_cbg)

        np.random.seed(42)
        random.seed(42)

        county_lodes = marginal_dist(county_lodes, "h_geocode", "w_geocode", sample_size)

        days = sorted(set(day[0].date() for day in self.datetime_ranges))
        num_cpus = cpu_count() - 1
        num_tasks = max(len(days), num_cpus)
        chunk_size = max(1, len(county_lodes) // num_tasks)

        delayed_tasks = []
        for day in days:
            current_index = 0
            while current_index < len(county_lodes):
                end_index = min(current_index + chunk_size, len(county_lodes))
                delayed_task = delayed(self.generate_OD)(
                    (
                        day,
                        county_lodes.iloc[current_index:end_index],
                        county_cbg,
                        res_build,
                        com_build,
                        ms_build,
                        self.datetime_ranges,
                    )
                )
                delayed_tasks.append(delayed_task)
                current_index = end_index

        results = compute(*delayed_tasks)

        results_by_day = defaultdict(list)
        for result in results:
            day, df = result
            results_by_day[day].append(df)

        for day, dataframes in results_by_day.items():
            combined_df = pd.concat(dataframes, ignore_index=True)
            combined_df.to_csv(f"{self.data_path}/lodes_combs/lodes_{day}.csv", index=False)
            self.logger.info(f"Saved results for day {day}")

        self.logger.info("All days generated")
