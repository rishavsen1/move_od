import pandas as pd
from shapely.geometry import LineString
import osmnx as ox
import geopandas as gpd
import multiprocessing as mp
from tqdm.notebook import tqdm
import networkx as nx
from generate.config import *


def find_origin_dest_nodes(df, G):
    origin_lons = df["origin_loc_lon"].to_list()
    origin_lats = df["origin_loc_lat"].to_list()
    dest_lons = df["dest_loc_lon"].to_list()
    dest_lats = df["dest_loc_lat"].to_list()

    origin_nodes = ox.distance.nearest_nodes(G, X=origin_lons, Y=origin_lats)
    dest_nodes = ox.distance.nearest_nodes(G, X=dest_lons, Y=dest_lats)

    df["origin_nodes"] = origin_nodes
    df["dest_nodes"] = dest_nodes

    # origin_loc_to_node = {loc: node for loc, node in zip(origin_locs, origin_nodes)}
    # dest_loc_to_node = {loc: node for loc, node in zip(dest_locs, dest_nodes)}

    origin_nodes = list(set(origin_nodes))
    dest_nodes = list(set(dest_nodes))

    return df


def process_od_pair_with_geoid(od_task, hourly_graphs):
    """Process a single OD pair with geoid information and return the routing result"""
    index, orig_node, dest_node, dep_time = od_task

    if isinstance(dep_time, int):
        raise Exception("int dep_time")

    # Find closest hour
    hour_to_use = dep_time.floor(TIME_INTERVAL)
    G_hour = hourly_graphs.get(hour_to_use)
    if G_hour is None:
        return None  # Skip if no graph is available for this time

    # Compute shortest path
    try:
        route = nx.shortest_path(G_hour, source=orig_node, target=dest_node, weight="weight")
        # Sum travel time
        total_travel_time_sec = sum(G_hour[u][v][0].get("weight", 0) for u, v in zip(route[:-1], route[1:]))

        # Calculate total distance in meters, then convert to miles
        total_distance_m = sum(G_hour[u][v][0].get("length", 0) for u, v in zip(route[:-1], route[1:]))
        total_distance_mi = total_distance_m * 0.000621371  # Convert meters to miles

        arrival_time = dep_time + pd.to_timedelta(total_travel_time_sec, unit="s")
        departure_time_bin = hour_to_use.hour

        return {
            "index": index,
            "departure_time_bin": departure_time_bin,  # Include departure_time_bin
            "arrival_time": arrival_time,
            "time_taken_min": total_travel_time_sec / 60,
            "distance_miles": total_distance_mi,
            "total_distance": total_distance_m,
            # "route_nodes": route,
        }

    except nx.NetworkXNoPath:
        return None  # Skip silently


def generate_travel_times_single_process(df, hourly_graphs):
    """
    Generate travel times for OD pairs using a single process.
    This avoids parallel processing for simplicity and safety.
    """
    df["departure_time"] = pd.to_datetime(df["departure_time"])
    indices = df.index.to_list()
    origin_nodes = df["origin_nodes"]
    dest_nodes = df["dest_nodes"]
    departs = df["departure_time"]

    od_tasks = list(zip(indices, origin_nodes, dest_nodes, departs))

    print("Routing OD pairs sequentially...")
    all_results = []
    for od_task in tqdm(od_tasks, desc="Routing OD pairs"):
        result = process_od_pair_with_geoid(od_task, hourly_graphs)
        if result is not None:
            all_results.append(result)

    # Filter out failures
    print(f"Done: {len(all_results)}/{df.shape[0]} succeeded")
    routing_df = pd.DataFrame(all_results)

    # Merge results back into the original dataframe
    df = df.merge(routing_df, on="index", how="left")
    return df


def process_inrix(state, county, inrix_df, conversion_df):
    hourly_inrix_df = (
        inrix_df.set_index("measurement_tstamp")
        .groupby("xd_id")
        .resample(TIME_INTERVAL)  # now resample every 30 minutes instead of hourly
        .agg(
            {
                "speed": "mean",
                "historical_average_speed": "mean",
                "reference_speed": "mean",
                "travel_time_minutes": "mean",
                "confidence_score": "mean",
                "cvalue": "mean",
            }
        )
        .reset_index()
    )

    # Format the result
    hourly_inrix_df = hourly_inrix_df.round(2)  # Round to 2 decimal places

    # Extract all unique hourly timestamps
    all_hours = hourly_inrix_df["measurement_tstamp"].drop_duplicates().sort_values()

    hourly_graphs = {}  # Store G_hour per timestamp

    for hour in all_hours:
        # Filter INRIX for this hour
        inrix_snapshot = hourly_inrix_df[hourly_inrix_df["measurement_tstamp"] == hour]

        # Merge with conversion
        merged_df = pd.merge(inrix_snapshot, conversion_df, left_on="xd_id", right_on="xd")

        # Create LineStrings
        geometries = [
            LineString([(x1, y1), (x2, y2)])
            for x1, y1, x2, y2 in zip(
                merged_df["start_longitude"],
                merged_df["start_latitude"],
                merged_df["end_longitude"],
                merged_df["end_latitude"],
            )
        ]
        inrix_gdf = gpd.GeoDataFrame(merged_df, geometry=geometries, crs="EPSG:4326")

        # Fetch OSM network only once (outside loop for efficiency)
        if "G_base" not in locals():

            G_base = ox.graph_from_place(f"{county} County, {state}, USA", network_type="drive")
            edges_base = ox.graph_to_gdfs(G_base, nodes=False, edges=True).to_crs(epsg=3857)

        # Project INRIX and join to edges
        inrix_proj = inrix_gdf.to_crs(epsg=3857)
        joined = gpd.sjoin_nearest(inrix_proj, edges_base, how="left", distance_col="dist")

        # Clone graph
        G_hour = G_base.copy()

        # Update edge weights in G_hour
        for idx, row in joined.iterrows():
            u, v, key = row["index_right0"], row["index_right1"], row["index_right2"]
            speed = row["speed"]
            length = row["length"]
            if pd.notnull(speed) and speed > 0:
                travel_time = (3.6 * length) / speed
                G_hour[u][v][key]["travel_time"] = travel_time
                G_hour[u][v][key]["weight"] = travel_time

        # Assign default weights to zero/None weights
        default_speed_kmh = 80
        for u, v, k, data in G_hour.edges(keys=True, data=True):
            weight = data.get("weight", None)
            if (weight == 0) or (weight is None):
                length = data.get("length", None)
                if length:
                    travel_time = (3.6 * length) / default_speed_kmh
                    data["weight"] = travel_time

        # Store graph
        hourly_graphs[hour] = G_hour

        G_0 = list(hourly_graphs.values())[0]

        return G_0, hourly_graphs