import aiohttp
import asyncio
from tqdm.asyncio import tqdm
import pandas as pd
import os


async def traveltimeTransit(latHome, lonHome, latWork, lonWork, time, semaphore):
    async with semaphore:
        request_url = f"http://localhost:8080/otp/routers/default/plan?fromPlace={latHome},{lonHome}&toPlace={latWork},{lonWork}&mode=WALK,TRANSIT&date=10-09-2023&time={time}&maxWalkDistance=1000"

        async with aiohttp.ClientSession() as session:
            try:
                res_dict = {}
                res_dict.update(
                    {
                        "origin_lat": latHome,
                        "origin_lon": lonHome,
                        "destination_lat": latWork,
                        "destination_lon": lonWork,
                    }
                )
                async with session.get(request_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if "plan" in data.keys() and "itineraries" in data["plan"].keys():
                            res_dict.update({"transit_time": int(data["plan"]["itineraries"][0]["duration"])})
                            res_dict.update({"transfers": int(data["plan"]["itineraries"][0]["transfers"])})
                            legs = pd.DataFrame(data["plan"]["itineraries"][0]["legs"])
                            # df.loc[i, 'trips'] = np.array(legs['tripId'].dropna().to_list())
                            # df.loc[i, 'trips'] = df.apply(lambda r: tuple(legs['tripId'].dropna()), axis=1).apply(np.array)
                            (
                                res_dict["trip1"],
                                res_dict["trip2"],
                                res_dict["trip3"],
                                res_dict["trip4"],
                                res_dict["trip5"],
                                res_dict["trip6"],
                            ) = (None,) * 6
                            (
                                res_dict["route1"],
                                res_dict["route2"],
                                res_dict["route3"],
                                res_dict["route4"],
                                res_dict["route5"],
                                res_dict["route6"],
                            ) = (None,) * 6
                            mode_split = legs["mode"].dropna().to_list()
                            res_dict.update({"mode": mode_split})
                            if "tripId" in legs.columns:
                                trip_split = legs["tripId"].dropna().to_list()
                                route_split = legs["route"][legs["route"] != ""].to_list()

                                for i, trip in enumerate(trip_split, start=1):
                                    res_dict[f"trip{i}"] = trip.split(":")[1]

                                for i, route in enumerate(route_split, start=1):
                                    res_dict[f"route{i}"] = route

                            res_dict.update({"bus_capacity_upto_2hours": len(data["plan"]["itineraries"]) * 35})
            except Exception as e:
                print(f"Error during API call: {e}")
                return res_dict
            return res_dict


async def traveltimeDrive(latHome, lonHome, latWork, lonWork, time, semaphore):
    async with semaphore:
        request_url = f"http://localhost:8080/otp/routers/default/plan?fromPlace={latHome},{lonHome}&toPlace={latWork},{lonWork}&mode=CAR&date=10-09-2023&time={time}&maxWalkDistance=1000"

        async with aiohttp.ClientSession() as session:
            try:
                res_dict = {}
                res_dict.update(
                    {
                        "origin_lat": latHome,
                        "origin_lon": lonHome,
                        "destination_lat": latWork,
                        "destination_lon": lonWork,
                    }
                )
                async with session.get(request_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if "plan" in data.keys() and "itineraries" in data["plan"].keys():
                            res_dict.update({"transit_time": int(data["plan"]["itineraries"][0]["duration"])})
            except Exception as e:
                print(f"Error during API call: {e}")
                return res_dict
            return res_dict


async def traveltimeWalk(latHome, lonHome, latWork, lonWork, time, semaphore):
    async with semaphore:
        request_url = f"http://localhost:8080/otp/routers/default/plan?fromPlace={latHome},{lonHome}&toPlace={latWork},{lonWork}&mode=WALK&date=10-09-2023&time={time}&maxWalkDistance=1000000"

        res_dict = {}
        res_dict.update(
            {
                "origin_lat": latHome,
                "origin_lon": lonHome,
                "destination_lat": latWork,
                "destination_lon": lonWork,
            }
        )
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(request_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if "plan" in data.keys() and "itineraries" in data["plan"].keys():
                            res_dict.update({"transit_time": int(data["plan"]["itineraries"][0]["duration"])})
            except Exception as e:
                print(f"Error during API call: {e}")
                return res_dict
            return res_dict


async def process_rows_transit(dataframe, semaphore):
    loop = asyncio.get_event_loop()
    tasks = []
    for index, row in dataframe.iterrows():
        task = loop.create_task(
            traveltimeTransit(
                row["origin_loc_lat"],
                row["origin_loc_lon"],
                row["dest_loc_lat"],
                row["dest_loc_lon"],
                row["pickup_time_0_str"],
                semaphore,
            )
        )
        tasks.append(task)

    results = []
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        result = await f
        results.append(result)

    return results


async def process_rows_drive(dataframe, semaphore):
    loop = asyncio.get_event_loop()
    tasks = []
    for index, row in dataframe.iterrows():
        task = loop.create_task(
            traveltimeDrive(
                row["origin_loc_lat"],
                row["origin_loc_lon"],
                row["dest_loc_lat"],
                row["dest_loc_lon"],
                row["pickup_time_0_str"],
                semaphore,
            )
        )
        tasks.append(task)

    results = []
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        result = await f
        results.append(result)

    return results


async def process_rows_walk(dataframe, semaphore):
    loop = asyncio.get_event_loop()
    tasks = []
    for index, row in dataframe.iterrows():
        task = loop.create_task(
            traveltimeWalk(
                row["origin_loc_lat"],
                row["origin_loc_lon"],
                row["dest_loc_lat"],
                row["dest_loc_lon"],
                row["pickup_time_0_str"],
                semaphore,
            )
        )
        tasks.append(task)

    results = []
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        result = await f
        results.append(result)

    return results


max_workers = os.cpu_count() - 4


async def get_transit_time(trips_df):
    semaphore = asyncio.Semaphore(max_workers)
    results = await process_rows_transit(trips_df, semaphore)
    for res in results:
        matching_rows = trips_df[
            (trips_df["origin_loc_lat"] == res["origin_lat"])
            & (trips_df["origin_loc_lon"] == res["origin_lon"])
            & (trips_df["dest_loc_lat"] == res["destination_lat"])
            & (trips_df["dest_loc_lon"] == res["destination_lon"])
        ]

        if not matching_rows.empty:
            for index in matching_rows.index:
                if "transit_time" in res.keys():
                    trips_df.loc[index, res.keys()] = res.values()
                else:
                    trips_df.loc[
                        index,
                        [
                            "transit_time",
                            "transfers",
                            "mode",
                            "trip1",
                            "trip2",
                            "trip3",
                            "trip4",
                            "trip5",
                            "trip6",
                            "route1",
                            "route2",
                            "route3",
                            "route4",
                            "route5",
                            "route6",
                            "bus_capacity_upto_2hours",
                        ],
                    ] = [None] * 16

    return trips_df


async def get_drive_time(trips_df):
    semaphore = asyncio.Semaphore(max_workers)
    results = await process_rows_drive(trips_df, semaphore)
    for res in results:
        matching_rows = trips_df[
            (trips_df["origin_loc_lat"] == res["origin_lat"])
            & (trips_df["origin_loc_lon"] == res["origin_lon"])
            & (trips_df["dest_loc_lat"] == res["destination_lat"])
            & (trips_df["dest_loc_lon"] == res["destination_lon"])
        ]

        if not matching_rows.empty:
            for index in matching_rows.index:
                if "transit_time" in res.keys():
                    trips_df.loc[index, "drive_time"] = res["transit_time"]
                else:
                    trips_df.loc[
                        index,
                        ["drive_time"],
                    ] = [None] * 1

    return trips_df


async def get_walk_time(trips_df):
    semaphore = asyncio.Semaphore(max_workers)
    results = await process_rows_walk(trips_df, semaphore)
    for res in results:
        matching_rows = trips_df[
            (trips_df["origin_loc_lat"] == res["origin_lat"])
            & (trips_df["origin_loc_lon"] == res["origin_lon"])
            & (trips_df["dest_loc_lat"] == res["destination_lat"])
            & (trips_df["dest_loc_lon"] == res["destination_lon"])
        ]

        if not matching_rows.empty:
            for index in matching_rows.index:
                if "transit_time" in res.keys():
                    trips_df.loc[index, "walk_time"] = res["transit_time"]
                else:
                    trips_df.loc[
                        index,
                        ["walk_time"],
                    ] = [None] * 1

    return trips_df


os.makedirs("./outputs", exist_ok=True)

CITY = "Memphis"
OD_NAME = f"/home/rishav/Programs/move_od/origin_dest/{CITY}_origin_dest.csv"
trips = pd.read_csv(f"{OD_NAME}")

trips = asyncio.run(get_walk_time(trips))
trips = asyncio.run(get_drive_time(trips))
trips = asyncio.run(get_transit_time(trips))

trips.to_csv(f"./outputs/{CITY}_transit_drive_walk.csv")


# await get_transit_time(trips)  # Use this in an environment with an existing event loop (like Jupyter Notebook)
