#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import geopandas as gpd
import numpy as np
import random
from shapely.geometry import Point
from datetime import datetime, timedelta
import os
import multiprocessing


class Lodes_comb:
    def __init__(
        self,
        county_cbg,
        data_path,
        ms_enabled,
        timedelta,
        time_start,
        time_end,
        start_date,
        end_date,
        logger,
    ) -> None:
        self.county_cbg = county_cbg
        self.data_path = data_path
        self.ms_enabled = ms_enabled
        self.timedelta = timedelta
        self.time_start = time_start
        self.time_end = time_end
        self.start_date = start_date
        self.end_date = end_date
        self.logger = logger
        self.logger.info("Initalizing lodes_comb.py")

    def intpt_func(row):
        return Point(row["INTPTLON"], row["INTPTLAT"])

    def func_home_pt(row):
        return Point(row.home_loc_lon, row.home_loc_lat)

    def func_work_pt(row):
        return Point(row.work_loc_lon, row.work_loc_lat)

    def datetime_range(start, end, delta):
        current = start
        while current < end:
            yield current
            current += delta

    def read_data(self, county_lodes, county_cbg, res_build, com_build, ms_build):
        self.logger.info("Running lodes_comb.py func")

        # # loading LODES data
        # county_lodes = pd.read_csv(
        #     f"{self.data_path}/county_lodes_2019.csv",
        #     dtype={"TRACTCE20_home": "string", "TRACTCE20_work": "string"},
        # )
        county_lodes.w_geocode = county_lodes.w_geocode.astype(str)
        county_lodes.h_geocode = county_lodes.h_geocode.astype(str)

        # # loading Hamilton county geodata
        # county_cbg = pd.read_csv(f"{self.data_path}/county_cbg.csv")
        # county_cbg["intpt"] = county_cbg[["INTPTLAT", "INTPTLON"]].apply(
        #     lambda p: Lodes_comb.intpt_func(p), axis=1
        # )
        # county_cbg = gpd.GeoDataFrame(
        #     county_cbg, geometry=gpd.GeoSeries.from_wkt(county_cbg.geometry)
        # )
        # county_cbg.GEOID = county_cbg.GEOID.astype(str)
        # county_cbg["location"] = county_cbg.intpt.apply(lambda p: [p.y, p.x])

        # # loading residential buildings
        # res_build = pd.read_csv(
        #     f"{self.data_path}/county_residential_buildings.csv", index_col=0
        # )
        # res_build = gpd.GeoDataFrame(
        #     res_build, geometry=gpd.GeoSeries.from_wkt(res_build.geometry)
        # )
        # res_build["location"] = res_build.geometry.apply(lambda p: [p.y, p.x])
        # res_build.GEOID = res_build.GEOID.astype(str)

        # # loading work buildings
        # com_build = pd.read_csv(
        #     f"{self.data_path}/county_work_locations.csv", index_col=0
        # )
        # com_build = gpd.GeoDataFrame(
        #     com_build, geometry=gpd.GeoSeries.from_wkt(com_build.geometry)
        # )
        # com_build["location"] = com_build.geometry.apply(lambda p: [p.y, p.x])
        # com_build = com_build.reset_index()
        # com_build.GEOID = com_build.GEOID.astype(str)

        # # loading all buildings (MS dataset)

        # if self.ms_enabled:
        #     ms_build = pd.read_csv(f"{self.data_path}/county_buildings_MS.csv")
        # ms_build = gpd.GeoDataFrame(
        #     ms_build, geometry=gpd.GeoSeries.from_wkt(ms_build.geo_centers)
        # )
        # ms_build.GEOID = ms_build.GEOID.astype(str)
        # ms_build["location"] = ms_build.geometry.apply(lambda p: [p.y, p.x])

        # aggregating total jobs for each combination of home and work cbg
        county_lodes = (
            county_lodes.groupby(["h_geocode", "w_geocode"])
            .agg(total_jobs=("total_jobs", sum))
            .reset_index()
            .merge(
                county_cbg[["GEOID", "geometry"]], left_on="h_geocode", right_on="GEOID"
            )
            .rename({"geometry": "home_geom"}, axis=1)
            .drop("GEOID", axis=1)
            .merge(
                county_cbg[["GEOID", "geometry"]], left_on="w_geocode", right_on="GEOID"
            )
            .rename({"geometry": "work_geom"}, axis=1)
            .drop("GEOID", axis=1)
            .sort_values("total_jobs", ascending=False)
            .reset_index(drop=True)
        )
        county_lodes = gpd.GeoDataFrame(county_lodes)

        # generating array of start and return times (in 15 min intervals)
        times = []
        for time in range(len(self.time_start)):
            times.append(
                [
                    datetime.strptime(dt.strftime("%H:%M"), "%H:%M")
                    for dt in Lodes_comb.datetime_range(
                        datetime(
                            2023,
                            9,
                            1,
                            self.time_start[time].hour,
                            self.time_start[time].minute,
                            self.time_start[time].second,
                        ),
                        datetime(
                            2023,
                            9,
                            1,
                            self.time_end[time].hour,
                            self.time_end[time].minute,
                            self.time_end[time].second,
                        ),
                        timedelta(seconds=self.timedelta),
                    )
                ]
            )

        # times_evening = [datetime.strptime(dt.strftime('%H:%M'), '%H:%M') for dt in
        #     datetime_range(datetime(2016, 9, 1, self.time_start[time].hour, self.time_start[time].minute, self.time_start[time].second), datetime(2016, 9, 1, self.time_end[time].hour, self.time_end[time].minute, self.time_end[time].second),
        #     timedelta(seconds=self.timedelta))]

        # return county_lodes, county_cbg, res_build, com_build, ms_build, times
        return county_lodes, county_cbg, res_build, com_build, ms_build, times

    def generate_OD(
        self, day, county_lodes, county_cbg, res_build, com_build, ms_build, times
    ):
        # for day in day_count:
        self.logger.info(f"Generating results for day {day.date()}")
        prob_matrix = pd.DataFrame()

        # self.logger.info(county_lodes.head())
        for idx, movement in county_lodes.iterrows():
            res = res_build[res_build.GEOID == movement.h_geocode].reset_index(
                drop=True
            )
            if res.empty:
                if self.ms_enabled:
                    res = (
                        ms_build[ms_build.GEOID == movement.h_geocode]
                        .sample(n=movement.total_jobs, random_state=42, replace=True)
                        .reset_index(drop=True)
                    )
                if res.empty:
                    res = county_cbg[
                        county_cbg.GEOID == movement.h_geocode
                    ].reset_index(drop=True)

            com = com_build[com_build.GEOID == movement.w_geocode].reset_index(
                drop=True
            )
            if com.empty:
                if self.ms_enabled:
                    com = (
                        ms_build[ms_build.GEOID == movement.w_geocode]
                        .sample(n=movement.total_jobs, random_state=42, replace=True)
                        .reset_index(drop=True)
                    )
                if com.empty:
                    com = county_cbg[
                        county_cbg.GEOID == movement.w_geocode
                    ].reset_index(drop=True)

            r = res.reset_index(drop=True)
            c = com.reset_index(drop=True)

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

                time_slot = []

                for time in range(len(times)):
                    # self.logger.info(times[time])
                    time_slot.append(
                        np.random.choice(times[time], size=1, replace=True)
                    )

                # time_slot1 = np.random.choice(times_morning, size=1, replace=True)
                # time_slot2 = np.random.choice(times_evening, size=1, replace=True)

                temp = gpd.GeoDataFrame()

                temp.loc[job, "h_geocode"] = movement.h_geocode
                temp.loc[job, "w_geocode"] = movement.w_geocode
                temp.loc[job, "total_jobs"] = movement.total_jobs
                temp.loc[job, "home_loc_lat"] = r_df.location[0]
                temp.loc[job, "home_loc_lon"] = r_df.location[1]
                temp.loc[job, "work_loc_lat"] = c_df.location[0]
                temp.loc[job, "work_loc_lon"] = c_df.location[1]

                for time in range(len(times)):
                    temp.loc[job, f"time_{time}"] = time_slot[time][0].time()
                    temp.loc[job, f"time_{time}_secs"] = (
                        time_slot[time][0] - datetime(1900, 1, 1)
                    ).total_seconds()
                    temp.loc[job, f"time_{time}_str"] = time_slot[time][0].strftime(
                        "%H:%M"
                    )

                prob_matrix = pd.concat([prob_matrix, temp], ignore_index=True)

        # convert the lat and lon points to shapely Points
        prob_matrix["home_geom"] = prob_matrix[["home_loc_lat", "home_loc_lon"]].apply(
            lambda row: Lodes_comb.func_home_pt(row), axis=1
        )
        prob_matrix["work_geom"] = prob_matrix[["work_loc_lat", "work_loc_lon"]].apply(
            lambda row: Lodes_comb.func_work_pt(row), axis=1
        )
        prob_matrix.h_geocode = prob_matrix.h_geocode.astype(str)
        prob_matrix.w_geocode = prob_matrix.w_geocode.astype(str)

        prob_matrix.to_csv(
            f"{self.data_path}/lodes_combs/lodes_{day.date()}.csv", index=False
        )
        self.logger.info(f"LODES - Day {day.date()} generated")

    def main(
        self,
        county_cbg,
        res_build,
        com_build,
        ms_build,
        county_lodes,
        lodes_cpu_max,
    ):
        (
            county_lodes,
            county_cbg,
            res_build,
            com_build,
            ms_build,
            times,
        ) = Lodes_comb.read_data(
            self, county_lodes, county_cbg, res_build, com_build, ms_build
        )

        # setting the random seed
        np.random.seed(42)
        random.seed(42)

        days = pd.date_range(self.start_date, self.end_date, freq="d").to_list()

        # Lodes_comb.generate_OD(county_lodes, county_cbg, res_build, com_build, ms_build, times)
        weekdays = []
        weekends = []

        # weekdays = days

        # self.logger.info(days)
        for day in days:
            if day.date().weekday() <= 4:
                self.logger.info(day.date().weekday())
                weekdays.append(day)
            else:
                weekends.append(day)
                pd.DataFrame().to_csv(
                    f"{self.data_path}/lodes_combs/lodes_{day.date()}.csv", index=False
                )

        day_count = len(weekdays)

        processes = []
        cpu_count = os.cpu_count()
        while day_count > 0:
            num_processes = 1
            if day_count >= cpu_count:
                num_processes = cpu_count
            else:
                num_processes = day_count

            day_sub = weekdays[:num_processes]
            weekdays = weekdays[num_processes:]
            day_count -= num_processes

            self.logger.info(
                f"Running {num_processes} day(s) in parallel. {day_count} day(s) left."
            )

            for proc in range(num_processes):
                process = multiprocessing.Process(
                    target=Lodes_comb.generate_OD,
                    args=(
                        self,
                        day_sub[proc],
                        county_lodes,
                        county_cbg,
                        res_build,
                        com_build,
                        ms_build,
                        times,
                    ),
                )
                process.start()
                processes.append(process)

                # Wait for all processes to finish
            for process in processes:
                process.join()
        self.logger.info(f"All days generated")
