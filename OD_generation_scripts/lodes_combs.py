#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import geopandas as gpd
import numpy as np
import math
import random
from shapely.geometry import Point
from collections import defaultdict
from dask import delayed, compute
from datetime import datetime, timedelta

from utils import (
    marginal_dist,
    get_travel_time_dict,
    get_census_data_wrapper,
    get_census_travel_time_data,
    parse_time_range,
    preprocessing_probabilites,
    define_time_blocks,
    get_census_work_time,
    sample_gaussian_dist,
    get_OSM_graph,
    find_shortest_path_dict,
    get_travel_time_osmnx,
)


class LodesComb:
    def __init__(
        self,
        county_cbg,
        data_path,
        ms_enabled,
        datetime_ranges,
        logger,
    ) -> None:
        self.county_cbg = county_cbg
        self.data_path = data_path
        self.ms_enabled = ms_enabled
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

    def od_assign_start_end(self, params):
        (
            G,
            day,
            county_lodes,
            county_cbg,
            res_build,
            com_build,
            ms_build,
        ) = params

        od_data = []
        mode_type = "drive"

        for index, movement in county_lodes.iterrows():
            # Retrieve location data
            res = res_build[res_build["GEOID"] == movement["h_geocode"]]
            com = com_build[com_build["GEOID"] == movement["w_geocode"]]

            # In case of missing data, use fallback data structures
            if res.empty and hasattr(self, "ms_enabled") and self.ms_enabled:
                res = ms_build[ms_build["GEOID"] == movement["w_geocode"]]
            if com.empty and hasattr(self, "ms_enabled") and self.ms_enabled:
                com = ms_build[ms_build["GEOID"] == movement["w_geocode"]]

            if res.empty:
                res = county_cbg[county_cbg["GEOID"] == movement["w_geocode"]]
            if com.empty:
                com = county_cbg[county_cbg["GEOID"] == movement["w_geocode"]]

            # Calculate proportions for origin and destination
            num_res = len(res)
            num_com = len(com)
            repeat_res = movement["scaled_total_jobs"] // num_res if num_res else 0
            repeat_com = movement["scaled_total_jobs"] // num_com if num_com else 0
            additional_res = movement["scaled_total_jobs"] % num_res
            additional_com = movement["scaled_total_jobs"] % num_com

            # Sample residential and commercial buildings accordingly
            sampled_res = pd.concat(
                [res] * repeat_res + [res.sample(n=additional_res, random_state=42)], ignore_index=True
            )
            sampled_com = pd.concat(
                [com] * repeat_com + [com.sample(n=additional_com, random_state=42)], ignore_index=True
            )

            sampled_res = sampled_res.sample(frac=1, random_state=42).reset_index(drop=True)
            sampled_com = sampled_com.sample(frac=1, random_state=42).reset_index(drop=True)

            for i in range(movement["scaled_total_jobs"]):

                od_record = {
                    "h_geocode": movement["h_geocode"],
                    "w_geocode": movement["w_geocode"],
                    "total_jobs": movement["scaled_total_jobs"],
                    "origin_loc": [sampled_res.iloc[i]["location"][0], sampled_res.iloc[i]["location"][1]],
                    "origin_loc_lat": sampled_res.iloc[i]["location"][0],
                    "origin_loc_lon": sampled_res.iloc[i]["location"][1],
                    "dest_loc": [sampled_com.iloc[i]["location"][0], sampled_com.iloc[i]["location"][1]],
                    "dest_loc_lat": sampled_com.iloc[i]["location"][0],
                    "dest_loc_lon": sampled_com.iloc[i]["location"][1],
                }

                # shortest_path = find_shortest_path_dict(G, od_record)
                # moving the time calculation to later - need to assign time based on the combined probabilities
                move_time, total_distance, distance_miles = list(get_travel_time_dict(mode_type, od_record).values())
                od_record.update(
                    {
                        "time_taken": move_time,
                        "total_distance": total_distance,
                        "distance_miles": distance_miles,
                    }
                )

                od_data.append(od_record)

        # Convert the list of dictionaries to a GeoDataFrame
        od_frame = gpd.GeoDataFrame(od_data)
        return od_frame

    def od_assign_time(
        self,
        od_df,
        departure_counts,
        start_datetime,
        end_datetime,
        total_travel_time_to_work,
        total_travel_time_to_work_proportion,
        travel_time_block_probabilities,
        time_blocks,
        hours_worked,
    ):
        od_time_blocks = define_time_blocks(time_blocks)
        travel_time_block_probabilities = np.array(travel_time_block_probabilities)
        travel_time_block_probabilities /= travel_time_block_probabilities.sum()

        bins = [block[0] for block in od_time_blocks] + [np.inf]

        counts, _ = np.histogram(od_df["time_taken"], bins=bins)

        bin_probabilities = np.array(travel_time_block_probabilities)
        bin_probabilities *= counts
        bin_probabilities /= bin_probabilities.sum()

        od_df["bin_index"] = np.digitize(od_df["time_taken"], bins=bins[:-1], right=True) - 1
        od_df["sample_probability"] = od_df["bin_index"].apply(lambda x: bin_probabilities[x])

        expected_sum_travel_time = total_travel_time_to_work * total_travel_time_to_work_proportion
        threshold = 0.3 * expected_sum_travel_time

        selected_od_indices = []
        current_sum_travel_time = 0
        od_df = od_df.reset_index().drop("index", axis=1)
        available_od_df = od_df.copy()

        while len(selected_od_indices) < min(departure_counts, len(available_od_df)):
            remaining_departures = departure_counts - len(selected_od_indices)
            required_average_remaining = (expected_sum_travel_time - current_sum_travel_time) / remaining_departures

            if available_od_df.empty or available_od_df["sample_probability"].sum() == 0:
                available_od_df["sample_probability"] = 1

            available_od_df["sample_probability"] /= available_od_df["sample_probability"].sum()

            if (
                current_sum_travel_time > threshold
                or len(selected_od_indices) > 0.5 * departure_counts
                or (required_average_remaining * remaining_departures) < current_sum_travel_time
            ):
                if required_average_remaining < 0:
                    inverse_weights = 1 / available_od_df["time_taken"]
                    if inverse_weights.sum() == 0:
                        inverse_weights.fillna(0.001, inplace=True)
                    available_od_df["adjusted_probability"] = available_od_df["sample_probability"] * inverse_weights
                else:
                    adjustment_factor = (required_average_remaining / available_od_df["time_taken"]).clip(0, 2)
                    available_od_df["adjusted_probability"] = available_od_df["sample_probability"] * adjustment_factor
            else:
                available_od_df["adjusted_probability"] = available_od_df["sample_probability"]

            available_od_df["adjusted_probability"] /= available_od_df["adjusted_probability"].sum()
            sampled_index = available_od_df.sample(weights="adjusted_probability", random_state=42).index[0]
            selected_od_indices.append(sampled_index)

            current_sum_travel_time += available_od_df.loc[sampled_index, "time_taken"] / 60  # seconds to minutes
            available_od_df = available_od_df.drop(sampled_index)

        selected_od_df = od_df.loc[selected_od_indices]
        remaining_od_df = od_df.drop(selected_od_indices)

        selected_od_df["departure_time"] = selected_od_df.apply(
            lambda row: start_datetime
            + timedelta(seconds=random.randint(0, int((end_datetime - start_datetime).total_seconds()))),
            axis=1,
        )
        selected_od_df["departure_time_secs"] = selected_od_df["departure_time"].apply(
            lambda dt: dt.hour * 3600 + dt.minute * 60 + dt.second
        )
        selected_od_df["departure_time_str"] = selected_od_df["departure_time"].apply(
            lambda dt: dt.strftime("%H:%M:%S")
        )

        stay_duration_samples = sample_gaussian_dist(hours_worked, 1)
        stay_duration_secs = stay_duration_samples[0] * 3600  # Convert hours to seconds

        selected_od_df["return_time"] = selected_od_df.apply(
            lambda row: row["departure_time"] + timedelta(seconds=row["time_taken"] + stay_duration_secs), axis=1
        )

        # delta = int((end_datetime - start_datetime).total_seconds())
        # random_second = random.randint(0, delta)
        # final_departure_time = start_datetime + timedelta(seconds=random_second)
        # seconds_from_midnight = start_datetime.hour * 3600 + start_datetime.minute * 60 + random_second
        # "departure_time": final_departure_time,
        # "departure_time_secs": seconds_from_midnight,
        # "departure_time_str": final_departure_time.strftime("%H:%M:%S"),

        return selected_od_df, remaining_od_df

    def main(
        self,
        county_cbg,
        res_build,
        com_build,
        ms_build,
        county_lodes,
        state,
        county,
        state_fips,
        county_fips,
        block_groups,
    ):
        # make this common
        county_lodes = self.read_county_lodes(county_lodes, county_cbg)

        np.random.seed(42)
        random.seed(42)

        # make this common
        census_depart_times_df = get_census_data_wrapper(
            table="B08302",
            api_url="https://api.census.gov/data/2021/acs/acs5",
            state_fips=state_fips,
            county_fips=county_fips,
            block_groups=block_groups,
            county_only=False,
        )
        travel_time_to_work_by_departure_df = get_census_data_wrapper(
            table="B08133",
            api_url="https://api.census.gov/data/2021/acs/acs1",
            state_fips=state_fips,
            county_fips=county_fips,
            block_groups=block_groups,
            county_only=True,
        )
        hours_worked = get_census_work_time(
            table="B23020",
            api_url="https://api.census.gov/data/2021/acs/acs1",
            state_fips=state_fips,
            county_fips=county_fips,
            block_groups=block_groups,
            county_only=True,
        )
        travel_time_to_work_df = get_census_travel_time_data(
            table="B08303",
            api_url="https://api.census.gov/data/2021/acs/acs5",
            state_fips=state_fips,
            county_fips=county_fips,
            block_groups=block_groups,
            county_only=False,
        )
        census_depart_times_df.to_csv("census_depart_times.csv")
        # if sample_size < county_lodes.shape[0]:
        #     county_lodes = marginal_dist(county_lodes, "h_geocode", "w_geocode", sample_size)

        G = get_OSM_graph(county, state)

        # days = sorted(set(day for day in self.datetime_ranges))
        days = list(range(4))

        delayed_tasks = []
        results = []
        total_census_departs = census_depart_times_df["total_estimate"].sum()
        time_columns = [col for col in census_depart_times_df.columns if "estimate" in col and "total" not in col]
        census_depart_times_current_df = census_depart_times_df[time_columns]
        departure_times = census_depart_times_current_df.columns.tolist()

        current_totals = county_lodes.groupby("h_geocode")["total_jobs"].sum().reset_index()
        current_totals = current_totals.rename(columns={"total_jobs": "current_total_jobs"})

        b08302_totals = census_depart_times_df.groupby("GEO_ID")["total_estimate"].sum().reset_index()
        b08302_totals = b08302_totals.rename(
            columns={"GEO_ID": "h_geocode", "total_estimate": "b08302_total_estimate"}
        )

        merged_lodes_acs_df = pd.merge(current_totals, b08302_totals, on="h_geocode", how="inner")
        merged_lodes_acs_df["scaling_factor"] = (
            merged_lodes_acs_df["b08302_total_estimate"] / merged_lodes_acs_df["current_total_jobs"]
        )
        county_lodes = pd.merge(
            county_lodes, merged_lodes_acs_df[["h_geocode", "scaling_factor"]], on="h_geocode", how="left"
        )
        county_lodes["scaled_total_jobs"] = (
            (county_lodes["total_jobs"] * county_lodes["scaling_factor"]).round().astype(int)
        )

        # create dict of average_speeds for each timestamp, pass to od_assign_start_end

        for day in days:
            county_h_geocodes = county_cbg["GEOID"].to_list()

            for h_geocode in county_h_geocodes:

                delayed_task = delayed(self.od_assign_start_end)(
                    (
                        G,
                        day,
                        county_lodes[county_lodes["h_geocode"] == h_geocode],
                        county_cbg,
                        res_build,
                        com_build,
                        ms_build,
                    )
                )
                delayed_tasks.append(delayed_task)

            result = compute(*delayed_tasks)
            result_df = pd.DataFrame()
            for res in result:
                result_df = pd.concat([result_df, res])

            assigned_od = pd.DataFrame()
            original_od_count = result_df.shape[0]
            result_df = result_df.dropna(subset=["time_taken"])
            for departure_time in departure_times:
                start_datetime, end_datetime = parse_time_range(day, departure_time)
                departure_counts = math.ceil(
                    (census_depart_times_df[departure_time].sum() / total_census_departs) * original_od_count
                )
                # travel_time_to_work_subset_df = travel_time_to_work_df[travel_time_to_work_df["GEO_ID"] == h_geocode]
                travel_time_block_probabilities, time_blocks = preprocessing_probabilites(travel_time_to_work_df)
                total_travel_time_to_work = travel_time_to_work_by_departure_df[departure_time].sum()
                total_travel_time_to_work_proportion = (
                    result_df["time_taken"].sum() / travel_time_to_work_by_departure_df["total_estimate"].sum()
                )

                selected_od_df, remaining_od_df = self.od_assign_time(
                    result_df,
                    departure_counts,
                    start_datetime,
                    end_datetime,
                    total_travel_time_to_work,
                    total_travel_time_to_work_proportion,
                    travel_time_block_probabilities,
                    time_blocks,
                    hours_worked,
                )

                result_df = remaining_od_df
                assigned_od = pd.concat([assigned_od, selected_od_df])

            if len(result_df) > 0:
                self.logger.warning(f"Warning: Not all ODs were assigned. {len(result_df)} ODs remain unassigned.")
            if not assigned_od.shape[0] == original_od_count:
                self.logger.error(f"Error: The number of assigned ODs does not match the original count")

            results.append((day, assigned_od))
            self.logger.info(f"Saved results for day {day}")

        results_by_day = defaultdict(list)
        for result in results:
            day, df = result
            results_by_day[day].append(df)

        for day, dataframes in results_by_day.items():
            combined_df = pd.concat(dataframes, ignore_index=True)
            combined_df.to_csv(f"{self.data_path}/lodes_combs/lodes_{day}.csv", index=False)
            self.logger.info(f"Saved results for day {day}")

        self.logger.info("All days generated")
