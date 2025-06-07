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


def process_inrix(state, county, inrix_df=None, conversion_df=None, desired_date=None):
    """
    Process INRIX data or fallback to OSM speed limits if INRIX is not available
    """

    # Fetch OSM network
    G_base = ox.graph_from_place(f"{county} County, {state}, USA", network_type="drive")

    if inrix_df is None or inrix_df.empty:
        print("INRIX data not available, using OSM speed limits as fallback")
        return create_graphs_from_osm_speeds(G_base, desired_date)

    # Process INRIX data as before
    hourly_inrix_df = (
        inrix_df.set_index("measurement_tstamp")
        .groupby("xd_id")
        .resample(TIME_INTERVAL)
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

    hourly_inrix_df = hourly_inrix_df.round(2)
    all_hours = hourly_inrix_df["measurement_tstamp"].drop_duplicates().sort_values()

    hourly_graphs = {}
    edges_base = ox.graph_to_gdfs(G_base, nodes=False, edges=True).to_crs(epsg=3857)

    for hour in all_hours:
        # Filter INRIX for this hour
        inrix_snapshot = hourly_inrix_df[hourly_inrix_df["measurement_tstamp"] == hour]

        # Merge with conversion
        merged_df = pd.merge(inrix_snapshot, conversion_df, left_on="xd_id", right_on="xd")

        # Create LineStrings and process as before
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
        inrix_proj = inrix_gdf.to_crs(epsg=3857)
        joined = gpd.sjoin_nearest(inrix_proj, edges_base, how="left", distance_col="dist")

        # Clone graph and update with INRIX data
        G_hour = G_base.copy()

        # Update edge weights with INRIX data
        for idx, row in joined.iterrows():
            u, v, key = row["index_right0"], row["index_right1"], row["index_right2"]
            speed = row["speed"]
            length = row["length"]
            if pd.notnull(speed) and speed > 0:
                travel_time = (3.6 * length) / speed
                G_hour[u][v][key]["travel_time"] = travel_time
                G_hour[u][v][key]["weight"] = travel_time

        # Use OSM speed limits for edges without INRIX data
        assign_osm_speeds_to_graph(G_hour)
        hourly_graphs[hour] = G_hour

    G_0 = list(hourly_graphs.values())[0]
    return G_0, hourly_graphs


def create_graphs_from_osm_speeds(G_base, desired_date):
    """
    Create hourly graphs using only OSM speed limits (fallback when no INRIX data)
    """
    print("Creating graphs based on OSM speed limits...")

    # Create a base graph with OSM speeds
    G_osm = G_base.copy()
    assign_osm_speeds_to_graph(G_osm)

    # Since we don't have time-varying data, create the same graph for multiple hours
    # You can adjust this based on your needs (e.g., create variations for peak/off-peak)
    hourly_graphs = {}

    # Create graphs for a typical day (every 30 minutes as per TIME_INTERVAL)
    base_date = pd.Timestamp(f"{desired_date}")
    for hour in range(0, 24):
        for minute in [0, 30]:  # Every 30 minutes
            timestamp = base_date.replace(hour=hour, minute=minute)

            # You could apply time-of-day speed adjustments here
            G_hour = G_osm.copy()

            # Optional: Apply peak hour speed reductions
            if is_peak_hour(hour):
                apply_peak_hour_adjustments(G_hour, reduction_factor=0.7)

            hourly_graphs[timestamp] = G_hour

    G_0 = list(hourly_graphs.values())[0]
    return G_0, hourly_graphs


def assign_osm_speeds_to_graph(G):
    """
    Assign travel times based on OSM speed limits and road types
    """
    # Default speeds by road type (km/h)
    default_speeds = {
        "motorway": 110,
        "trunk": 90,
        "primary": 70,
        "secondary": 60,
        "tertiary": 50,
        "residential": 40,
        "service": 30,
        "unclassified": 50,
        "living_street": 20,
        "track": 25,
        "path": 15,
        "footway": 5,
        "cycleway": 15,
        "steps": 5,
    }

    for u, v, k, data in G.edges(keys=True, data=True):
        # Try to get speed from OSM data first
        speed_kmh = None

        # Check for maxspeed tag
        if "maxspeed" in data:
            maxspeed = data["maxspeed"]
            if isinstance(maxspeed, str):
                try:
                    # Handle different formats: "50", "50 mph", etc.
                    if "mph" in maxspeed.lower():
                        speed_kmh = float(maxspeed.replace("mph", "").strip()) * 1.60934
                    else:
                        speed_kmh = float(maxspeed.strip())
                except (ValueError, AttributeError):
                    pass
            elif isinstance(maxspeed, (int, float)):
                speed_kmh = float(maxspeed)

        # If no speed found, use highway type
        if speed_kmh is None:
            highway = data.get("highway", "unclassified")
            if isinstance(highway, list):
                highway = highway[0]  # Take first if multiple
            speed_kmh = default_speeds.get(highway, 50)  # Default to 50 km/h

        # Calculate travel time
        length = data.get("length", 100)  # Default length if missing
        travel_time = (3.6 * length) / speed_kmh  # Convert to seconds

        # Update graph
        data["travel_time"] = travel_time
        data["weight"] = travel_time
        data["speed_kmh"] = speed_kmh


def is_peak_hour(hour):
    """
    Determine if the given hour is during peak traffic
    """
    morning_peak = 7 <= hour <= 9
    evening_peak = 17 <= hour <= 19
    return morning_peak or evening_peak


def apply_peak_hour_adjustments(G, reduction_factor=0.7):
    """
    Apply speed reductions during peak hours
    """
    for u, v, k, data in G.edges(keys=True, data=True):
        if "weight" in data:
            # Increase travel time (reduce effective speed) during peak hours
            data["weight"] = data["weight"] / reduction_factor
            if "travel_time" in data:
                data["travel_time"] = data["travel_time"] / reduction_factor


# Update your main calling code to handle missing INRIX data:
def get_hourly_graphs(state, county, inrix_df=None, conversion_df=None):
    """
    Wrapper function to get hourly graphs with fallback
    """
    try:
        if inrix_df is not None and not inrix_df.empty and conversion_df is not None:
            print("Using INRIX data for traffic conditions")
            return process_inrix(state, county, inrix_df, conversion_df)
        else:
            print("INRIX data unavailable, using OSM speed limits")
            return process_inrix(state, county, None, None)
    except Exception as e:
        print(f"Error processing INRIX data: {e}")
        print("Falling back to OSM speed limits")
        G_base = ox.graph_from_place(f"{county} County, {state}, USA", network_type="drive")
        return create_graphs_from_osm_speeds(G_base)
