import pandas as pd
import geopandas as gpd
import os
from datetime import datetime, date, timedelta
import numpy as np


def union(path, date):
    date = date.strftime("%Y-%m-%d")
    # lodes = pd.read_csv(f"{path}/lodes_combs/lodes_{day}.csv")
    lodes = pd.read_csv(f"{path}/lodes_combs/lodes_{date}.csv")
    lodes = lodes.rename(
        columns={
            "h_geocode": "home_cbg",
            "w_geocode": "work_cbg",
            "total_jobs": "visits",
            "time_0": "go_time",
            "time_0_str": "go_time_str",
            "time_0_secs": "go_time_secs",
            "time_1": "return_time",
            "time_1_str": "return_time_str",
            "time_1_secs": "return_time_secs",
        }
    )

    lodes["home_cbg"] = lodes["home_cbg"].astype(str)
    # lodes = lodes.merge(chatta[["GEOID"]], left_on="home_cbg", right_on="GEOID")

    lodes_go = lodes[
        [
            "home_cbg",
            "work_cbg",
            "home_geom",
            "home_loc_lat",
            "home_loc_lon",
            "work_geom",
            "work_loc_lat",
            "work_loc_lon",
            "visits",
            "go_time",
            "go_time_str",
            "go_time_secs",
        ]
    ]

    lodes_go = lodes_go.rename(
        columns={
            "home_cbg": "pickup_cbg",
            "home_geom": "pickup_geom",
            "home_loc_lat": "pickup_lat",
            "home_loc_lon": "pickup_lon",
            "work_cbg": "dropoff_cbg",
            "work_geom": "dropoff_geom",
            "work_loc_lat": "dropoff_lat",
            "work_loc_lon": "dropoff_lon",
            "go_time": "pickup_time",
            "go_time_str": "pickup_time_str",
            "go_time_secs": "pickup_time_secs",
        }
    )

    lodes_return = lodes[
        [
            "home_cbg",
            "work_cbg",
            "home_geom",
            "home_loc_lat",
            "home_loc_lon",
            "work_geom",
            "work_loc_lat",
            "work_loc_lon",
            "visits",
            "return_time",
            "return_time_str",
            "return_time_secs",
        ]
    ]

    lodes_return = lodes_return.rename(
        columns={
            "work_cbg": "pickup_cbg",
            "work_geom": "pickup_geom",
            "work_loc_lat": "pickup_lat",
            "work_loc_lon": "pickup_lon",
            "home_cbg": "dropoff_cbg",
            "home_geom": "dropoff_geom",
            "home_loc_lat": "dropoff_lat",
            "home_loc_lon": "dropoff_lon",
            "return_time": "pickup_time",
            "return_time_str": "pickup_time_str",
            "return_time_secs": "pickup_time_secs",
        }
    )

    lodes_return = lodes_return[lodes_go.columns]

    # ADDED TO INDICATE GO AND RETURN TRIPS
    lodes_go["direction"] = 0
    lodes_return["direction"] = 1

    lodes_flattened = pd.concat([lodes_go, lodes_return]).sort_index()

    sg = pd.read_csv(f"{path}/safegraph_combs/sg_{date}.csv")
    sg = sg.rename(columns={"poi_cbg": "work_cbg"})

    sg["home_cbg"] = sg["home_cbg"].astype(str)
    # sg = sg.merge(chatta[["GEOID"]], left_on="home_cbg", right_on="GEOID")

    sg_go = sg[
        [
            "home_cbg",
            "work_cbg",
            "home_geom",
            "home_loc_lat",
            "home_loc_lon",
            "work_geom",
            "work_loc_lat",
            "work_loc_lon",
            "visits",
            "go_time",
            "go_time_str",
            "go_time_secs",
        ]
    ]

    sg_go = sg_go.rename(
        columns={
            "home_cbg": "pickup_cbg",
            "home_geom": "pickup_geom",
            "home_loc_lat": "pickup_lat",
            "home_loc_lon": "pickup_lon",
            "work_cbg": "dropoff_cbg",
            "work_geom": "dropoff_geom",
            "work_loc_lat": "dropoff_lat",
            "work_loc_lon": "dropoff_lon",
            "go_time": "pickup_time",
            "go_time_str": "pickup_time_str",
            "go_time_secs": "pickup_time_secs",
        }
    )

    sg_return = sg[
        [
            "home_cbg",
            "work_cbg",
            "home_geom",
            "home_loc_lat",
            "home_loc_lon",
            "work_geom",
            "work_loc_lat",
            "work_loc_lon",
            "visits",
            "return_time",
            "return_time_str",
            "return_time_secs",
        ]
    ]

    sg_return = sg_return.rename(
        columns={
            "work_cbg": "pickup_cbg",
            "work_geom": "pickup_geom",
            "work_loc_lat": "pickup_lat",
            "work_loc_lon": "pickup_lon",
            "home_cbg": "dropoff_cbg",
            "home_geom": "dropoff_geom",
            "home_loc_lat": "dropoff_lat",
            "home_loc_lon": "dropoff_lon",
            "return_time": "pickup_time",
            "return_time_str": "pickup_time_str",
            "return_time_secs": "pickup_time_secs",
        }
    )

    sg_return = sg_return[sg_go.columns]

    # ADDED TO INDICATE GO AND RETURN TRIPS
    sg_go["direction"] = 0
    sg_return["direction"] = 1

    sg_return = sg_return[sg_return["pickup_time_secs"] <= 86400]

    sg_flattened = pd.concat([sg_go, sg_return]).sort_index()

    union = pd.concat([lodes_flattened, sg_flattened]).sort_index()
    union["pickup_time"] = pd.to_datetime(union["pickup_time"])
    union["pickup_hour"] = union["pickup_time"].apply(lambda x: x.hour)
    union.to_csv(f"{path}/union_all_trips_{date}.csv", index=False)
