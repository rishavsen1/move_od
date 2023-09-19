import os
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import datetime, timedelta
import random

# from logger import logger
from shapely.geometry import Point
import multiprocessing


import warnings

warnings.filterwarnings("ignore")


class Sg_combs:
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
        # self.day_count = day_count
        self.start_date = start_date
        self.end_date = end_date
        self.logger = logger
        self.logger.info("Initliazing safegraph_combs.py")

    def intpt_func(row):
        return Point(row["INTPTLON"], row["INTPTLAT"])

    def func_home_pt(row):
        return Point(float(row.home_loc_lon), float(row.home_loc_lat))

    def func_work_pt(row):
        return Point(float(row.work_loc_lon), float(row.work_loc_lat))

    def datetime_range(start, end, delta):
        current = start
        while current < end:
            yield current
            current += delta

    def read_data(self):
        self.logger.info("Running safegraph_comb.py")

        # # loading geometry data
        # self.county_cbg = pd.read_csv(f"{self.data_path}/county_cbg.csv")
        # self.county_cbg["intpt"] = self.county_cbg[["INTPTLAT", "INTPTLON"]].apply(
        #     lambda p: Sg_combs.intpt_func(p), axis=1
        # )
        # self.county_cbg = gpd.GeoDataFrame(
        #     self.county_cbg, geometry=gpd.GeoSeries.from_wkt(self.county_cbg.geometry)
        # )
        # self.county_cbg.GEOID = self.county_cbg.GEOID.astype(str)
        # self.county_cbg["location"] = self.county_cbg.intpt.apply(lambda p: [p.y, p.x])

        # # loading residential buildings
        # self.res_build = pd.read_csv(
        #     f"{self.data_path}/county_residential_buildings.csv", index_col=[0]
        # )
        # self.res_build = gpd.GeoDataFrame(
        #     self.res_build, geometry=gpd.GeoSeries.from_wkt(self.res_build.geometry)
        # )
        # self.res_build["location"] = self.res_build.geometry.apply(lambda p: [p.y, p.x])
        # self.res_build.GEOID = self.res_build.GEOID.astype(str)

        # # loading work buildings
        # self.com_build = pd.read_csv(
        #     f"{self.data_path}/county_work_locations.csv", index_col=[0]
        # )
        # self.com_build = gpd.GeoDataFrame(
        #     self.com_build, geometry=gpd.GeoSeries.from_wkt(self.com_build.geometry)
        # )
        # self.com_build["location"] = self.com_build.geometry.apply(lambda p: [p.y, p.x])
        # self.com_build = self.com_build.reset_index()
        # self.com_build.GEOID = self.com_build.GEOID.astype(str)

        # # loading all buildings (MS dataset)
        # self.ms_build = pd.read_csv(f"{self.data_path}/county_buildings_MS.csv")
        # self.ms_build = gpd.GeoDataFrame(
        #     self.ms_build, geometry=gpd.GeoSeries.from_wkt(self.ms_build.geo_centers)
        # )
        # self.ms_build.GEOID = self.ms_build.GEOID.astype(str)
        # self.ms_build["location"] = self.ms_build.geometry.apply(lambda p: [p.y, p.x])

        self.time_slot1 = [
            datetime.strptime(dt.strftime("%H:%M"), "%H:%M")
            for dt in Sg_combs.datetime_range(
                datetime(2016, 9, 1, 4, 0),
                datetime(2016, 9, 1, 20, 0),
                timedelta(seconds=self.timedelta),
            )
        ]

        # TODO: Add self.start_time (morning and evening), and self.end_time (morning and evening), self.timedelta to times_morning (or, times_evening)

    def generate_OD(self, day, sg):
        # for day in days:
        self.logger.info(f"Generating OD for {day}")
        prob_matrix_sg = gpd.GeoDataFrame()

        for idx, movement in sg.iterrows():
            res = self.res_build[self.res_build.GEOID == movement.home_cbg].reset_index(
                drop=True
            )
            if res.empty:
                if self.ms_enabled:
                    res = (
                        self.ms_build[self.ms_build.GEOID == movement.home_cbg]
                        .sample(n=int(movement.visits), random_state=42, replace=True)
                        .reset_index(drop=True)
                    )
                if res.empty:
                    res = self.county_cbg[
                        self.county_cbg.GEOID == movement.home_cbg
                    ].reset_index(drop=True)

            com = self.com_build[self.com_build.GEOID == movement.poi_cbg].reset_index(
                drop=True
            )
            if com.empty:
                if self.ms_enabled:
                    com = (
                        self.ms_build[self.ms_build.GEOID == movement.poi_cbg]
                        .sample(n=int(movement.visits), random_state=42, replace=True)
                        .reset_index(drop=True)
                    )
                if com.empty:
                    com = self.county_cbg[
                        self.county_cbg.GEOID == movement.poi_cbg
                    ].reset_index(drop=True)

            r = res.reset_index(drop=True)
            c = com.reset_index(drop=True)

            for freq in range(int(movement.visits)):
                if c.empty:
                    c = com
                if r.empty:
                    r = res

                rand_r = random.randrange(r.shape[0])
                rand_c = random.randrange(c.shape[0])
                r_df = r.iloc[rand_r]
                c_df = c.iloc[rand_c]
                r = r.drop([rand_r]).reset_index(drop=True)
                c = c.drop([rand_c]).reset_index(drop=True)

                temp = gpd.GeoDataFrame()

                temp.loc[freq, "home_cbg"] = movement.home_cbg
                temp.loc[freq, "poi_cbg"] = movement.poi_cbg
                temp.loc[freq, "visits"] = movement.visits
                temp.loc[freq, "home_loc_lat"] = r_df.location[0]
                temp.loc[freq, "home_loc_lon"] = r_df.location[1]
                temp.loc[freq, "work_loc_lat"] = c_df.location[0]
                temp.loc[freq, "work_loc_lon"] = c_df.location[1]

                # TODO: use realistic starting times instead of randomly sampled ones

                time_slot = np.random.choice(self.time_slot1, size=1, replace=True)

                temp.loc[freq, "go_time"] = time_slot[0].time()
                temp.loc[freq, "go_time_secs"] = (
                    time_slot[0] - datetime(1900, 1, 1)
                ).total_seconds()
                temp.loc[freq, "go_time_str"] = time_slot[0].strftime("%H:%M")

                ret_time = time_slot[0] + timedelta(minutes=(movement["dwell_time"]))
                temp.loc[freq, "return_time"] = ret_time.time()
                temp.loc[freq, "return_time_secs"] = (
                    ret_time - datetime(1900, 1, 1)
                ).total_seconds()
                temp.loc[freq, "return_time_str"] = ret_time.strftime("%H:%M")
                temp = temp[temp["return_time_secs"] <= 86400]
                temp = temp[temp["go_time_secs"] <= 86400]

                prob_matrix_sg = pd.concat([prob_matrix_sg, temp], ignore_index=True)

        prob_matrix_sg["home_geom"] = prob_matrix_sg[
            ["home_loc_lat", "home_loc_lon"]
        ].apply(lambda row: Sg_combs.func_home_pt(row), axis=1)

        prob_matrix_sg["work_geom"] = prob_matrix_sg[
            ["work_loc_lat", "work_loc_lon"]
        ].apply(lambda row: Sg_combs.func_work_pt(row), axis=1)

        # convert the lat and lon points to shapely Points
        prob_matrix_sg.to_csv(
            f"{self.data_path}/safegraph_combs/sg_{day}.csv", index=False
        )
        self.logger.info(f"Generated for day {day}")

    def main(self, county_cbg, res_build, com_build, ms_build, sg, sg_cpu_max):
        self.county_cbg = county_cbg
        self.res_build = res_build
        self.com_build = com_build
        self.ms_build = ms_build

        Sg_combs.read_data(self)

        sg = pd.read_csv(f"{self.data_path}/sg_visits_by_day.csv")
        sg["home_cbg"] = sg["home_cbg"].astype(str)
        sg["poi_cbg"] = sg["poi_cbg"].astype(str)

        days = sg.date.unique()

        # Sg_combs.generate_OD(sg, days)
        # Sg_combs.generate_OD(county_lodes, county_cbg, res_build, com_build, ms_build, times)

        # setting the random seed
        np.random.seed(42)
        random.seed(42)
        day_count = len(days)

        processes = []
        cpu_count = os.cpu_count()
        while day_count > 0:
            num_processes = 1
            if day_count >= cpu_count:
                num_processes = cpu_count
            else:
                num_processes = day_count

            day_sub = days[:num_processes]
            days = days[num_processes:]
            day_count -= num_processes

            self.logger.info(
                f"Running {num_processes} days in parallel. {day_count} days left."
            )
            for proc in range(num_processes):
                process = multiprocessing.Process(
                    target=Sg_combs.generate_OD,
                    args=(self, day_sub[proc], sg[sg.date == day_sub[proc]]),
                )
                process.start()
                processes.append(process)

                # Wait for all processes to finish
            for process in processes:
                process.join()
        self.logger.info(f"All days generated")
        num_processes = os.cpu_count() if len(days) >= os.cpu_count() else len(days)
