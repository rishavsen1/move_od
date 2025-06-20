import pandas as pd
import geopandas as gpd
import os
from datetime import datetime, date, timedelta
import numpy as np


def union(path, date, sg_enabled):
    date = date.strftime("%Y-%m-%d")
    # lodes = pd.read_csv(f"{path}/lodes_combs/lodes_{day}.csv")
    lodes = pd.read_csv(f"{path}/lodes_combs/lodes_{date}.csv")
    lodes = lodes.rename(
        columns={
            "h_geocode": "origin_cbg",
            "w_geocode": "dest_cbg",
            "total_jobs": "visits",
            "time_0": "go_time",
            "time_0_str": "go_time_str",
            "time_0_secs": "go_time_secs",
            "time_1": "return_time",
            "time_1_str": "return_time_str",
            "time_1_secs": "return_time_secs",
        }
    )

    lodes["origin_cbg"] = lodes["origin_cbg"].astype(str)
    # lodes = lodes.merge(chatta[["GEOID"]], left_on="origin_cbg", right_on="GEOID")

    lodes_go = lodes[
        [
            "origin_cbg",
            "dest_cbg",
            "origin_geom",
            "origin_loc_lat",
            "origin_loc_lon",
            "dest_geom",
            "dest_loc_lat",
            "dest_loc_lon",
            "visits",
            "go_time",
            "go_time_str",
            "go_time_secs",
        ]
    ]

    lodes_go = lodes_go.rename(
        columns={
            "origin_cbg": "pickup_cbg",
            "origin_geom": "pickup_geom",
            "origin_loc_lat": "pickup_lat",
            "origin_loc_lon": "pickup_lon",
            "dest_cbg": "dropoff_cbg",
            "dest_geom": "dropoff_geom",
            "dest_loc_lat": "dropoff_lat",
            "dest_loc_lon": "dropoff_lon",
            "go_time": "pickup_time",
            "go_time_str": "pickup_time_str",
            "go_time_secs": "pickup_time_secs",
        }
    )

    lodes_return = lodes[
        [
            "origin_cbg",
            "dest_cbg",
            "origin_geom",
            "origin_loc_lat",
            "origin_loc_lon",
            "dest_geom",
            "dest_loc_lat",
            "dest_loc_lon",
            "visits",
            "return_time",
            "return_time_str",
            "return_time_secs",
        ]
    ]

    lodes_return = lodes_return.rename(
        columns={
            "dest_cbg": "pickup_cbg",
            "dest_geom": "pickup_geom",
            "dest_loc_lat": "pickup_lat",
            "dest_loc_lon": "pickup_lon",
            "origin_cbg": "dropoff_cbg",
            "origin_geom": "dropoff_geom",
            "origin_loc_lat": "dropoff_lat",
            "origin_loc_lon": "dropoff_lon",
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

    if sg_enabled:
        sg = pd.read_csv(f"{path}/safegraph_combs/sg_{date}.csv")
        # sg = sg.rename(columns={"poi_cbg": "dest_cbg"})

        sg["origin_cbg"] = sg["origin_cbg"].astype(str)
        # sg = sg.merge(chatta[["GEOID"]], left_on="origin_cbg", right_on="GEOID")

        sg_go = sg[
            [
                "origin_cbg",
                "dest_cbg",
                "origin_geom",
                "origin_loc_lat",
                "origin_loc_lon",
                "dest_geom",
                "dest_loc_lat",
                "dest_loc_lon",
                "visits",
                "go_time",
                "go_time_str",
                "go_time_secs",
            ]
        ]

        sg_go = sg_go.rename(
            columns={
                "origin_cbg": "pickup_cbg",
                "origin_geom": "pickup_geom",
                "origin_loc_lat": "pickup_lat",
                "origin_loc_lon": "pickup_lon",
                "dest_cbg": "dropoff_cbg",
                "dest_geom": "dropoff_geom",
                "dest_loc_lat": "dropoff_lat",
                "dest_loc_lon": "dropoff_lon",
                "go_time": "pickup_time",
                "go_time_str": "pickup_time_str",
                "go_time_secs": "pickup_time_secs",
            }
        )

        sg_return = sg[
            [
                "origin_cbg",
                "dest_cbg",
                "origin_geom",
                "origin_loc_lat",
                "origin_loc_lon",
                "dest_geom",
                "dest_loc_lat",
                "dest_loc_lon",
                "visits",
                "return_time",
                "return_time_str",
                "return_time_secs",
            ]
        ]

        sg_return = sg_return.rename(
            columns={
                "dest_cbg": "pickup_cbg",
                "dest_geom": "pickup_geom",
                "dest_loc_lat": "pickup_lat",
                "dest_loc_lon": "pickup_lon",
                "origin_cbg": "dropoff_cbg",
                "origin_geom": "dropoff_geom",
                "origin_loc_lat": "dropoff_lat",
                "origin_loc_lon": "dropoff_lon",
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

    else:
        sg_flattened = pd.DataFrame()

    union = pd.concat([lodes_flattened, sg_flattened]).sort_index()
    union["pickup_time"] = pd.to_datetime(union["pickup_time"])
    union["pickup_hour"] = union["pickup_time"].apply(lambda x: x.hour)
    union.to_csv(f"{path}/union_all_trips_{date}.csv", index=False)
