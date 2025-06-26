import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import osmnx as ox
import networkx as nx
import numpy as np
import multiprocessing as mp
from tqdm import tqdm

from generate.config import *
from generate.utils import calculate_speed_shift, apply_mssr_to_existing_graphs


# Helper function to get travel time bin based on census categories
def get_travel_time_bin(minutes):
    """Maps travel time in minutes to census travel time bin format"""
    if minutes < 5:
        return "under_5_minutes"
    elif minutes < 10:
        return "5_to_9_minutes"
    elif minutes < 15:
        return "10_to_14_minutes"
    elif minutes < 20:
        return "15_to_19_minutes"
    elif minutes < 25:
        return "20_to_24_minutes"
    elif minutes < 30:
        return "25_to_29_minutes"
    elif minutes < 35:
        return "30_to_34_minutes"
    elif minutes < 40:
        return "35_to_39_minutes"
    elif minutes < 45:
        return "40_to_44_minutes"
    elif minutes < 60:
        return "45_to_59_minutes"
    elif minutes < 90:
        return "60_to_89_minutes"
    else:
        return "90_minutes_and_over"


def get_census_time_bin(time, dep_edges, dep_names):
    """Maps a pandas Timestamp to census time bin using the same method as calibration"""
    mins = time.hour * 60 + time.minute

    for i, edge in enumerate(dep_edges[1:]):
        if mins < edge:
            return dep_names[i]

    return dep_names[-1]


def get_census_time_bin_index(time, dep_edges, dep_names):
    """Maps a pandas Timestamp to census time bin index (0-13)"""
    mins = time.hour * 60 + time.minute

    for i, edge in enumerate(dep_edges[1:]):
        if mins < edge:
            return i

    return len(dep_names) - 1


def process_od_pair_with_geoid(od_task):
    """Process a single OD pair with CBG information and return the routing result"""
    orig_node, dest_node, dep_time, origin_geoid, dest_geoid, dep_edges, dep_names = od_task

    if isinstance(dep_time, int):
        raise Exception("int dep_time")

    # Find closest hour
    hour_to_use = dep_time.floor(TIME_INTERVAL)
    global hourly_graphs

    G_hour = hourly_graphs.get(hour_to_use)
    if G_hour is None:
        return None  # Skip if no graph is available for this time

    # Compute shortest path
    try:
        route = nx.shortest_path(G_hour, source=orig_node, target=dest_node, weight="travel_time")
        # Sum travel time
        total_travel_time_sec = sum(G_hour[u][v][0].get("travel_time", 0) for u, v in zip(route[:-1], route[1:]))

        # Calculate total distance in meters, then convert to miles
        total_distance_m = sum(G_hour[u][v][0].get("length", 0) for u, v in zip(route[:-1], route[1:]))
        total_distance_mi = total_distance_m * 0.000621371  # Convert meters to miles

    except nx.NetworkXNoPath:
        total_travel_time_sec = np.nan
        total_distance_mi = np.inf

    arrival_time = dep_time + pd.to_timedelta(total_travel_time_sec, unit="s")

    # Map departure time to census time bin format
    departure_time_bin = get_census_time_bin(dep_time, dep_edges, dep_names)
    departure_time_bin_index = get_census_time_bin_index(dep_time, dep_edges, dep_names)

    # Also get travel time bin for analysis
    travel_time_min = total_travel_time_sec / 60
    travel_time_bin = get_travel_time_bin(travel_time_min)

    return {
        "origin_geoid": origin_geoid,
        "destination_geoid": dest_geoid,
        "origin_node": orig_node,
        "destination_node": dest_node,
        "departure_time": dep_time,
        "departure_time_bin": departure_time_bin_index,  # Human readable format
        # "departure_time_bin_index": departure_time_bin_index,  # Same integer index as calibration code
        "arrival_time": arrival_time,
        "travel_time_min": travel_time_min,
        "travel_time_bin": travel_time_bin,
        "travel_distance_mi": total_distance_mi,
    }


def build_od_pairs(od_df, desired_date):
    od_pairs = []
    for _, row in od_df.iterrows():
        orig_dep = pd.Timestamp(row["departure_time"])
        dep_time = orig_dep.floor(TIME_INTERVAL)
        time_str = dep_time.strftime("%H:%M:%S")

        od_pairs.append(
            {
                "origin_geoid": row["h_geocode"],
                "destination_geoid": row["w_geocode"],
                "origin": (row["origin_loc_lat"], row["origin_loc_lon"]),
                "destination": (row["dest_loc_lat"], row["dest_loc_lon"]),
                "departure_time": pd.Timestamp(f"{desired_date} {time_str}"),
            }
        )

    return od_pairs


def build_od_pairs_post_calibration(od_df, desired_date):
    od_pairs = []
    for _, row in od_df.iterrows():
        orig_dep = pd.Timestamp(row["departure_datetime"])
        dep_time = orig_dep
        time_str = dep_time.strftime("%H:%M:%S")

        od_pairs.append(
            {
                "origin_geoid": row["origin_geoid"],
                "destination_geoid": row["destination_geoid"],
                "origin": (row["origin_lat"], row["origin_lon"]),
                "destination": (row["destination_lat"], row["destination_lon"]),
                "departure_time": pd.Timestamp(f"{desired_date} {time_str}"),
            }
        )

    return od_pairs


def get_routed(od_df, desired_date, hourly_graphs_arg, post_calibration=False, parallel=False):
    global hourly_graphs
    hourly_graphs = hourly_graphs_arg

    dep_edges = np.array(
        [
            0,
            5 * 60,
            5 * 60 + 30,
            6 * 60,
            6 * 60 + 30,
            7 * 60,
            7 * 60 + 30,
            8 * 60,
            8 * 60 + 30,
            9 * 60,
            10 * 60,
            11 * 60,
            12 * 60,
            16 * 60,
            24 * 60,
        ]
    )
    dep_labels = list(range(len(dep_edges) - 1))
    dep_names = [
        "12am_to_4:59am",
        "5am_to_5:29am",
        "5:30am_to_5:59am",
        "6am_to_6:29am",
        "6:30am_to_6:59am",
        "7am_to_7:29am",
        "7:30am_to_7:59am",
        "8am_to_8:29am",
        "8:30am_to_8:59am",
        "9am_to_9:59am",
        "10am_to_10:59am",
        "11am_to_11:59am",
        "12pm_to_3:59pm",
        "4pm_to_11:59pm",
    ]

    # print("Processing OD pairs sequentially")

    od_df["departure_time"] = pd.to_datetime(od_df["departure_time"])

    # minutes-since-midnight
    mins = od_df["departure_time"].dt.hour * 60 + od_df["departure_time"].dt.minute

    # which bin each dep falls into
    bins = np.digitize(mins, dep_edges[1:-1])

    # pick a random minute in each bin
    lo = dep_edges[bins]
    hi = dep_edges[bins + 1]
    rep_mins = np.random.randint(lo, hi)

    if not post_calibration:
        od_pairs = build_od_pairs(od_df, desired_date)
    else:
        # reconstruct rep_dep_time
        midnight = od_df["departure_datetime"].dt.normalize()
        od_df["departure_datetime"] = midnight + pd.to_timedelta(rep_mins, unit="m")
        od_pairs = build_od_pairs_post_calibration(od_df, desired_date)

    # Collect all OD information
    origin_lons = []
    origin_lats = []
    dest_lons = []
    dest_lats = []
    departs = []
    origin_geoids = []  # Add CBG tracking
    dest_geoids = []  # Add CBG tracking

    for od in od_pairs:
        dep_time = od["departure_time"]
        origin_lat, origin_lon = od["origin"]
        dest_lat, dest_lon = od["destination"]

        # Get CBG information (if available in your od_pairs)
        origin_geoid = od.get("origin_geoid", "unknown")  # Default if not available
        dest_geoid = od.get("destination_geoid", "unknown")  # Default if not available

        departs.append(dep_time)
        origin_lons.append(origin_lon)
        origin_lats.append(origin_lat)
        dest_lons.append(dest_lon)
        dest_lats.append(dest_lat)
        origin_geoids.append(origin_geoid)
        dest_geoids.append(dest_geoid)

    G_0 = list(hourly_graphs.values())[0]

    print("origin_lons", len(origin_lons))
    # Find nearest nodes
    orig_nodes = ox.distance.nearest_nodes(G_0, X=origin_lons, Y=origin_lats)
    dest_nodes = ox.distance.nearest_nodes(G_0, X=dest_lons, Y=dest_lats)

    # Enhanced task tuples with CBG information
    od_tasks = list(zip(orig_nodes, dest_nodes, departs, origin_geoids, dest_geoids))

    # Add dep_edges and dep_names to each task tuple
    od_tasks = [
        (orig, dest, dep, origin_geoid, dest_geoid, dep_edges, dep_names)
        for (orig, dest, dep, origin_geoid, dest_geoid) in od_tasks
    ]
    # Parallel processing with the updated function
    n_cpus = max(1, mp.cpu_count() - 1)
    if parallel:
        print(f"Routing OD pairs in parallel using {n_cpus} cores")
        with mp.Pool(n_cpus) as pool:
            all_results = list(
                tqdm(pool.imap(process_od_pair_with_geoid, od_tasks), total=len(od_tasks), desc="Routing OD pairs")
            )
    else:
        all_results = []
        for task in tqdm(od_tasks, total=len(od_tasks), desc="Routing OD pairs (single process)"):
            try:
                result = process_od_pair_with_geoid(task)
                all_results.append(result)
            except Exception as e:
                print(f"Error processing task: {e}")
                all_results.append(None)

    # Filter out failures
    routing_results = [r for r in all_results if r is not None]
    print(f"Done: {len(routing_results)}/{len(od_pairs)} succeeded")

    # Convert to DataFrame for easier analysis
    routing_df = pd.DataFrame(routing_results)

    # Now you can analyze by CBG
    if len(routing_df) > 0:
        # Example: average travel time by origin CBG
        # avg_travel_time_by_origin = routing_df.groupby("origin_geoid")["travel_time_min"].mean()

        # # Example: count of trips between CBG pairs
        # od_counts = routing_df.groupby(["origin_geoid", "destination_geoid"]).size().reset_index(name="trip_count")

        print(f"Unique origin CBGs: {routing_df['origin_geoid'].nunique()}")
        print(f"Unique destination CBGs: {routing_df['destination_geoid'].nunique()}")

    return routing_df


def perform_mean_speed_shift(routing_df, travel_time_to_work_by_geoid):
    mssr = calculate_speed_shift(routing_df, travel_time_to_work_by_geoid)
    print(f"Mean Speed Shift Ratio (MSSr): {mssr:.4f}")

    # Create hourly graphs with speed shift
    hourly_graphs_adjusted = apply_mssr_to_existing_graphs(hourly_graphs, mssr)

    return hourly_graphs_adjusted
