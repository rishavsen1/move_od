import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point


def intpt_func(row):
    return Point(row["INTPTLON"], row["INTPTLAT"])


def func_home_pt(row):
    return Point(row.home_loc_lon, row.home_loc_lat)


def func_work_pt(row):
    return Point(row.work_loc_lon, row.work_loc_lat)


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


def write_to_file(output_path, file_path, file_name, df):
    df.to_csv(f"{output_path}/{file_path}/{file_name}", index=False)


def sample_rows(grouped_df, probabilities, remaining_jobs, origin_col, dest_col):
    # Estimate a safe number of rows to sample to avoid overshooting the remaining jobs
    safe_sample_size = max(1, min(len(probabilities), int(remaining_jobs / probabilities.mean())))

    # Keep sampling until the sum of total_jobs in sampled rows is close to remaining_jobs
    sampled_rows = pd.DataFrame(columns=[origin_col, dest_col, "total_jobs"])
    while sampled_rows["total_jobs"].sum() < remaining_jobs and len(probabilities) > 0:
        # Sample one row at a time to avoid overshooting
        chosen_row = probabilities.sample(n=1, weights=probabilities)
        chosen_row_df = grouped_df.loc[chosen_row.index]
        if sampled_rows["total_jobs"].sum() + chosen_row_df["total_jobs"].values[0] <= remaining_jobs:
            sampled_rows = pd.concat([sampled_rows, chosen_row_df.reset_index()])
        probabilities.drop(chosen_row.index, inplace=True)

    return sampled_rows


def marginal_dist(df, origin_col, dest_col, sample_size):
    grouped_df = df.groupby([origin_col, dest_col]).sum()

    # Calculate probabilities
    total_jobs = grouped_df["total_jobs"].sum()
    probabilities = grouped_df["total_jobs"] / total_jobs

    subsampled_df = pd.DataFrame()
    remaining_jobs = sample_size

    while remaining_jobs > 0 and not probabilities.empty:
        sampled_rows = sample_rows(grouped_df, probabilities, remaining_jobs, origin_col, dest_col)
        subsampled_df = pd.concat([subsampled_df, sampled_rows])
        remaining_jobs = sample_size - subsampled_df["total_jobs"].sum()
        # probabilities.drop(sampled_rows.index, inplace=True)

    return subsampled_df


def read_data(data_path, lodes=False, sg_enabled=False, ms_enabled=False, sample_size=np.inf):
    print("Reading data")
    # loading geometry data
    county_cbg = pd.read_csv(f"{data_path}/county_cbg.csv")
    county_cbg["intpt"] = county_cbg[["INTPTLAT", "INTPTLON"]].apply(lambda p: intpt_func(p), axis=1)
    county_cbg = gpd.GeoDataFrame(county_cbg, geometry=gpd.GeoSeries.from_wkt(county_cbg.geometry))
    county_cbg.GEOID = county_cbg.GEOID.astype(str)
    county_cbg["location"] = county_cbg.intpt.apply(lambda p: [p.y, p.x])

    # loading residential buildings
    res_build = pd.read_csv(
        f"{data_path}/county_residential_buildings.csv",
    )
    res_build = gpd.GeoDataFrame(res_build, geometry=gpd.GeoSeries.from_wkt(res_build.geometry))
    res_build["location"] = res_build.geometry.apply(lambda p: [p.y, p.x])
    res_build.GEOID = res_build.GEOID.astype(str)

    # loading work buildings
    com_build = pd.read_csv(
        f"{data_path}/county_work_locations.csv",
    )

    com_build = gpd.GeoDataFrame(com_build, geometry=gpd.GeoSeries.from_wkt(com_build.geometry))
    com_build["location"] = com_build.geometry.apply(lambda p: [p.y, p.x])
    com_build = com_build.reset_index()
    com_build.GEOID = com_build.GEOID.astype(str)

    # loading all buildings (MS dataset)
    if ms_enabled:
        ms_build = pd.read_csv(f"{data_path}/county_buildings_MS.csv")
        ms_build = gpd.GeoDataFrame(ms_build, geometry=gpd.GeoSeries.from_wkt(ms_build.geo_centers))
        ms_build.GEOID = ms_build.GEOID.astype(str)
        ms_build["location"] = ms_build.geometry.apply(lambda p: [p.y, p.x])
    else:
        ms_build = pd.DataFrame()

    sg = pd.DataFrame()
    county_lodes = pd.DataFrame()

    if sg_enabled:
        sg = pd.read_csv(f"{data_path}/sg_visits_by_day.csv")
        sg["home_cbg"] = sg["home_cbg"].astype(str)
        sg["poi_cbg"] = sg["poi_cbg"].astype(str)
        # marginal_dist(sg, "home_cbg", "poi_cbg", sample_size)

    if lodes:
        county_lodes = pd.read_csv(
            f"{data_path}/county_lodes.csv",
            dtype={"TRACTCE20_home": "string", "TRACTCE20_work": "string"},
        )
        county_lodes["h_geocode"] = county_lodes["h_geocode"].astype(str)
        county_lodes["w_geocode"] = county_lodes["w_geocode"].astype(str)
        # marginal_dist(county_lodes, "h_geocode", "w_geocode", sample_size)

    return county_cbg, res_build, com_build, ms_build, county_lodes, sg
