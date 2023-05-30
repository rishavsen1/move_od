import pandas as pd
import geopandas as gpd
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


def read_data(data_path, lodes=False, sg_enabled=False):
    # print("Running safegraph_comb.py")

    # loading geometry data
    county_cbg = pd.read_csv(f"{data_path}/county_cbg.csv")
    county_cbg["intpt"] = county_cbg[["INTPTLAT", "INTPTLON"]].apply(
        lambda p: intpt_func(p), axis=1
    )
    county_cbg = gpd.GeoDataFrame(
        county_cbg, geometry=gpd.GeoSeries.from_wkt(county_cbg.geometry)
    )
    county_cbg.GEOID = county_cbg.GEOID.astype(str)
    county_cbg["location"] = county_cbg.intpt.apply(lambda p: [p.y, p.x])

    # loading residential buildings
    res_build = pd.read_csv(
        f"{data_path}/county_residential_buildings.csv", index_col=[0]
    )
    res_build = gpd.GeoDataFrame(
        res_build, geometry=gpd.GeoSeries.from_wkt(res_build.geometry)
    )
    res_build["location"] = res_build.geometry.apply(lambda p: [p.y, p.x])
    res_build.GEOID = res_build.GEOID.astype(str)

    # loading work buildings
    com_build = pd.read_csv(
        f"{data_path}/county_work_loc_poi_com_civ.csv", index_col=[0]
    )

    com_build = gpd.GeoDataFrame(
        com_build, geometry=gpd.GeoSeries.from_wkt(com_build.geometry)
    )
    com_build["location"] = com_build.geometry.apply(lambda p: [p.y, p.x])
    com_build = com_build.reset_index()
    com_build.GEOID = com_build.GEOID.astype(str)

    # loading all buildings (MS dataset)
    ms_build = pd.read_csv(f"{data_path}/county_buildings_MS.csv")
    ms_build = gpd.GeoDataFrame(
        ms_build, geometry=gpd.GeoSeries.from_wkt(ms_build.geo_centers)
    )
    ms_build.GEOID = ms_build.GEOID.astype(str)
    ms_build["location"] = ms_build.geometry.apply(lambda p: [p.y, p.x])

    sg = pd.DataFrame()
    county_lodes = pd.DataFrame()

    if sg_enabled:
        sg = pd.read_csv(f"{data_path}/sg_visits_by_day.csv")
        sg["home_cbg"] = sg["home_cbg"].astype(str)
        sg["poi_cbg"] = sg["poi_cbg"].astype(str)

    if lodes:
        county_lodes = pd.read_csv(
            f"{data_path}/county_lodes_2019.csv",
            dtype={"TRACTCE20_home": "string", "TRACTCE20_work": "string"},
        )
        county_lodes.w_geocode = county_lodes.w_geocode.astype(str)
        county_lodes.h_geocode = county_lodes.h_geocode.astype(str)

    return county_cbg, res_build, com_build, ms_build, county_lodes, sg
