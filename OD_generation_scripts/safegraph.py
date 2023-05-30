#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime as dt
import ast
import geopandas as gpd
import numpy as np
from scipy.stats import norm
import math
from datetime import datetime, timedelta
import random
from itertools import chain
import json
import glob

pd.options.display.float_format = "{:.2f}".format


# COUNTY = '037'
# CITY = 'Nashville'
# county_cbg  = gpd.read_file('../data/Tennessee Census Block Groups/tl_2020_47_bg.shp')
# safe_df =  pd.read_parquet(f'../data/safegraph.parquet/year=2021/region=TN/city={CITY}/', engine='pyarrow')


class Safegraph:
    def __init__(
        self, county, city, county_cbg, safe_df, data_path, start_date, end_date, logger
    ):
        self.COUNTY = county
        self.county_cbg = county_cbg
        self.CITY = city
        self.safe_df = safe_df
        self.data_path = data_path
        self.start_date = start_date
        self.end_date = end_date
        self.logger = logger

        self.logger.info("Initliazing safegraph.py")

    def find_norm_dist(bucketed_data):
        total_values = 0
        total_sum = 0
        squared_diff_sum = 0

        for key, count in bucketed_data.items():
            if "-" in key:
                lower, upper = map(int, key.split("-"))
            else:
                lower = 0
                upper = int(key[1:])
            mid_point = (lower + upper) / 2
            total_values += count
            total_sum += mid_point * count

        mean = total_sum / total_values

        squared_diff_sum = 0

        for key, count in bucketed_data.items():
            if "-" in key:
                lower, upper = map(int, key.split("-"))
            else:
                lower = 0
                upper = int(key[1:])
            mid_point = (lower + upper) / 2
            squared_diff = ((mid_point - mean) ** 2) * count
            squared_diff_sum += squared_diff

        variance = squared_diff_sum / total_values
        std_dev = np.sqrt(variance)

        # Create a normal distribution using the mean and standard deviation
        normal_distribution = norm(loc=mean, scale=std_dev)
        return normal_distribution

    def prev_weekday(d, weekday):
        days_behind = d.weekday()
        return d - timedelta(days_behind)

    def get_sg_poi(self):
        self.logger.info("Running get_sg_poi()")
        county_cbg = gpd.read_file(self.county_cbg)
        county_cbg = county_cbg[county_cbg.COUNTYFP == self.COUNTY]
        county_cbg.GEOID = county_cbg.GEOID.astype(str)

        safe_dfs = []
        self.logger.info(self.safe_df)
        for safe_df_name in self.safe_df:
            for path in glob.glob(safe_df_name + "*.parquet"):
                temp = pd.read_parquet(
                    path,
                    engine="pyarrow",
                    columns=["date_begin", "latitude", "longitude", "poi_cbg"],
                )
                safe_dfs.append(temp)
        safe_df = pd.concat(safe_dfs, ignore_index=True)

        safe_df["poi_cbg"] = safe_df["poi_cbg"].astype(str)
        safe_df.merge(county_cbg, left_on="poi_cbg", right_on="GEOID").to_csv(
            f"{self.data_path}/sg_poi_cbgs.csv", index=False
        )

    def by_day(row):
        week = 7
        temp = pd.DataFrame()
        start_date = row["date_begin"]
        visitor_homes = list(
            chain.from_iterable([[k] * v for k, v in row["visitor_home_cbgs"].items()])
        )
        norm_dist = Safegraph.find_norm_dist(row["bucketed_dwell_times"])

        if len(visitor_homes) > 0:
            home_cbg = random.sample(visitor_homes, 1)[0]
            visitor_homes.remove(home_cbg)

            for day in range(week):
                temp.loc[day, "date"] = start_date + timedelta(days=day)
                temp.loc[day, "visits"] = int(row["visits_by_day"][day])
                temp.loc[day, "home_cbg"] = str(home_cbg)
                temp.loc[day, "poi_cbg"] = str(row["poi_cbg"]).split(".")[0]
                while True:
                    sampled_value = norm_dist.rvs(size=1)
                    if sampled_value >= 0:
                        break
                temp.loc[day, "dwell_time"] = sampled_value

        return temp

    def get_day_of_week(self):
        self.logger.info("Running get_day_of_week()")

        random.seed(42)

        county_cbg = gpd.read_file(self.county_cbg)
        county_cbg = county_cbg[county_cbg.COUNTYFP == self.COUNTY]
        county_cbg.GEOID = county_cbg.GEOID.astype(str)

        safe_dfs = []
        safe_df_days = pd.DataFrame()

        start_prev_monday = Safegraph.prev_weekday(self.start_date, 0)
        end_prev_monday = Safegraph.prev_weekday(self.end_date, 0)

        for safe_df_name in self.safe_df:
            for path in glob.glob(safe_df_name + "*.parquet"):
                temp = pd.read_parquet(
                    path,
                    engine="pyarrow",
                    columns=[
                        "date_begin",
                        "visits_by_day",
                        "visitor_home_cbgs",
                        "poi_cbg",
                        "bucketed_dwell_times",
                    ],
                )
                temp = temp[temp["date_begin"] >= start_prev_monday]
                temp = temp[temp["date_begin"] <= end_prev_monday]
                safe_dfs.append(temp)

        safe_df = pd.concat(safe_dfs, ignore_index=True)

        safe_df["visitor_home_cbgs"] = safe_df["visitor_home_cbgs"].apply(
            lambda x: json.loads(x)
        )
        safe_df["bucketed_dwell_times"] = safe_df["bucketed_dwell_times"].apply(
            lambda x: json.loads(x)
        )
        safe_df["visits_by_day"] = safe_df["visits_by_day"].apply(
            lambda x: ast.literal_eval(x)
        )

        safe_days = []
        for idx, row in safe_df.iterrows():
            safe_days.append(Safegraph.by_day(row))
        safe_df_days = pd.concat(safe_days, ignore_index=True)

        safe_df_days = safe_df_days[
            (safe_df_days["date"] >= self.start_date)
            & (safe_df_days["date"] <= self.end_date)
        ]

        safe_df_days = safe_df_days.merge(
            county_cbg[["GEOID"]], left_on="home_cbg", right_on="GEOID"
        )
        safe_df_days = safe_df_days.merge(
            county_cbg[["GEOID"]], left_on="poi_cbg", right_on="GEOID"
        )

        safe_df_days = (
            safe_df_days.groupby(["date", "home_cbg", "poi_cbg"]).sum().reset_index()
        )

        safe_df_days.to_csv(f"{self.data_path}/sg_visits_by_day.csv", index=False)

        self.logger.info(safe_df_days.visits.sum())
