import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import osmnx as ox
import networkx as nx


# Load INRIX speed data and conversion data
inrix_df = pd.read_csv("../data/Hamilton-County-INRIX.csv")
conversion_df = pd.read_csv("../data/XD_Identification.csv")

desired_date = pd.Timestamp("2025-03-10 01:00:00").date()
inrix_df["measurement_tstamp"] = pd.to_datetime(inrix_df["measurement_tstamp"])
inrix_df = inrix_df[inrix_df["measurement_tstamp"].dt.date == desired_date]

# Group by xd_id and resample by hour to get hourly means
hourly_inrix_df = (
    inrix_df.set_index("measurement_tstamp")
    .groupby("xd_id")
    .resample("H")
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

# Extract all unique hourly timestamps
all_hours = hourly_inrix_df["measurement_tstamp"].drop_duplicates().sort_values()
hourly_inrix_df = hourly_inrix_df.round(2)  # Round to 2 decimal places
import collections

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
        minx, miny, maxx, maxy = inrix_gdf.total_bounds
        G_base = ox.graph_from_bbox(north=maxy, south=miny, east=maxx, west=minx, network_type="drive")
        edges_base = ox.graph_to_gdfs(G_base, nodes=False, edges=True).to_crs(epsg=3857)
        nodes_base, _ = ox.graph_to_gdfs(G_base, nodes=True, edges=False)

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
    default_speed_kmh = 66
    for u, v, k, data in G_hour.edges(keys=True, data=True):
        weight = data.get("weight", None)
        if (weight == 0) or (weight is None):
            length = data.get("length", None)
            if length:
                travel_time = (3.6 * length) / default_speed_kmh
                data["weight"] = travel_time

    # Store graph
    hourly_graphs[hour] = G_hour


routing_results = []

for od in od_pairs:
    dep_time = od["departure_time"]
    origin_lat, origin_lon = od["origin"]
    dest_lat, dest_lon = od["destination"]
    
    # Find closest hour
    hour_to_use = dep_time.floor('H')
    G_hour = hourly_graphs.get(hour_to_use)
    if G_hour is None:
        print(f"No graph for hour {hour_to_use}, skipping...")
        continue

    # Find nearest nodes
    orig_node = ox.distance.nearest_nodes(G_hour, X=origin_lon, Y=origin_lat)
    dest_node = ox.distance.nearest_nodes(G_hour, X=dest_lon, Y=dest_lat)

    # Compute shortest path
    try:
        route = nx.shortest_path(G_hour, source=orig_node, target=dest_node, weight="weight")
        # Sum travel time
        total_travel_time_sec = sum(G_hour[u][v][0].get("weight", 0) for u, v in zip(route[:-1], route[1:]))
        arrival_time = dep_time + pd.to_timedelta(total_travel_time_sec, unit='s')

        routing_results.append({
            "departure_time": dep_time,
            "arrival_time": arrival_time,
            "travel_time_min": total_travel_time_sec / 60,
            "route_nodes": route
        })

        print(f"OD routed at {dep_time}, travel time: {total_travel_time_sec / 60:.2f} min, arrival: {arrival_time}")
    
    except nx.NetworkXNoPath:
        print(f"No route found for OD at {dep_time}")
