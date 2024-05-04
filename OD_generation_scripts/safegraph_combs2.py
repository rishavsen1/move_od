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

from utils import write_to_file


class Sg_combs:

    def __init__(
        self, county_cbg, output_path, ms_enabled, timedelta, time_start, time_end, start_date, end_date
    ) -> None:
        print("Initliazing safegraph_combs.py")
        self.county_cbg = county_cbg
        self.output_path = output_path
        self.ms_enabled = ms_enabled
        self.timedelta = timedelta
        self.time_start = time_start
        self.time_end = time_end
        # self.day_count = day_count
        self.start_date = start_date
        self.end_date = end_date

    def generate_time_slots(self):

        time_slot1 = [
            datetime.strptime(dt.strftime("%H:%M"), "%H:%M")
            for dt in Sg_combs.datetime_range(
                datetime(2016, 9, 1, 7, 0), datetime(2016, 9, 1, 18, 0), timedelta(seconds=self.timedelta)
            )
        ]
        return time_slot1

    def generate_OD(self, day, sg, county_cbg, res_build, com_build, ms_build, ms_enabled, time_slot1):

        print(f"Generating OD for {day}")

        prob_matrix_sg = gpd.GeoDataFrame()

        for idx, movement in sg.iterrows():

            res = res_build[res_build.GEOID == movement.home_cbg].reset_index(drop=True)
            if res.empty:
                if ms_enabled:
                    res = (
                        ms_build[ms_build.GEOID == movement.home_cbg]
                        .sample(n=int(movement.visits), random_state=42, replace=True)
                        .reset_index(drop=True)
                    )
                if res.empty:
                    res = county_cbg[county_cbg.GEOID == movement.home_cbg].reset_index(drop=True)

            com = com_build[com_build.GEOID == movement.poi_cbg].reset_index(drop=True)
            if com.empty:
                if ms_enabled:
                    com = (
                        ms_build[ms_build.GEOID == movement.poi_cbg]
                        .sample(n=int(movement.visits), random_state=42, replace=True)
                        .reset_index(drop=True)
                    )
                if com.empty:
                    com = county_cbg[county_cbg.GEOID == movement.poi_cbg].reset_index(drop=True)

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

                time_slot = np.random.choice(time_slot1, size=1, replace=True)

                temp.loc[freq, "go_time"] = time_slot[0].time()
                temp.loc[freq, "go_time_secs"] = (time_slot[0] - datetime(1900, 1, 1)).total_seconds()
                temp.loc[freq, "go_time_str"] = time_slot[0].strftime("%H:%M")

                ret_time = time_slot[0] + timedelta(minutes=(movement["dwell_time"]))
                temp.loc[freq, "return_time"] = ret_time.time()
                temp.loc[freq, "return_time_secs"] = (ret_time - datetime(1900, 1, 1)).total_seconds()
                temp.loc[freq, "return_time_str"] = ret_time.strftime("%H:%M")

                prob_matrix_sg = prob_matrix_sg.append(temp, ignore_index=True)

        # convert the lat and lon points to shapely Points
        prob_matrix_sg["home_geom"] = prob_matrix_sg[["home_loc_lat", "home_loc_lon"]].apply(
            lambda row: Sg_combs.func_home_pt(row), axis=1
        )
        prob_matrix_sg["work_geom"] = prob_matrix_sg[["work_loc_lat", "work_loc_lon"]].apply(
            lambda row: Sg_combs.func_work_pt(row), axis=1
        )

        write_to_file(output_path=self.output_path, file_path="safegraph_combs", file_name=f"sg_{day}.csv")

        print(f"Generated for day {day}")

    def main(self, county_cbg, res_build, com_build, ms_build, sg, cpus):

        print("Running safegraph_comb.py")

        # TODO: Add self.start_time (morning and evening), and self.end_time (morning and evening), self.timedelta to times_morning (or, times_evening)

        days = sg.date.unique()
        time_slot1 = Sg_combs.generate_time_slots()

        # setting the random seed
        np.random.seed(42)
        random.seed(42)
        day_count = len(days)

        processes = []
        cpu_count = cpus
        while day_count > 0:

            num_processes = 1
            if day_count >= cpu_count:
                num_processes = cpu_count
            else:
                num_processes = day_count

            day_sub = days[:num_processes]
            days = days[num_processes:]
            day_count -= num_processes

            print(f"Running {num_processes} days in parallel. {day_count} days left.")
            for proc in range(num_processes):
                process = multiprocessing.Process(
                    target=Sg_combs.generate_OD,
                    args=(
                        self,
                        day_sub[proc],
                        sg[sg.date == day_sub[proc]],
                        county_cbg,
                        res_build,
                        com_build,
                        ms_build,
                        time_slot1,
                    ),
                )
                process.start()
                processes.append(process)

                # Wait for all processes to finish
            for process in processes:
                process.join()
        print(f"All days generated")
        num_processes = os.cpu_count() if len(days) >= os.cpu_count() else len(days)
