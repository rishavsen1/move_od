import streamlit as st
import os
import datetime
import pandas as pd
import numpy as np
import geopandas as gpd
import shapely

from generate.lodes_read import LodesGen
from generate.safegraph import Safegraph
from generate.locations_OSM_SG import LocationsOSMSG
from generate.read_ms_buildings import MSBuildings
from generate.lodes_combs import LodesComb
from generate.process_inrix import process_inrix
from generate.safegraph_combs import SgCombs
from generate.union_lodes_sg import union
from generate.utils import *
import generate.union_lodes_sg

from generate.logger import Logger

# # Ensure the script runs from the base folder of the repository
# base_path = os.path.dirname(os.path.abspath(__file__))
# os.chdir(base_path)


def read_county_geoid_df(county_geoid_path, output_path, logger):
    if os.path.exists(f"{output_path}/county_geoid.geojson"):
        county_geoid_df = gpd.read_file(f"{output_path}/county_geoid.geojson")
        county_geoid_df["intpt"] = county_geoid_df[["INTPTLAT", "INTPTLON"]].apply(lambda p: intpt_func(p), axis=1)
        county_geoid_df["location"] = county_geoid_df.intpt.apply(lambda p: [p.y, p.x])
        success = True
    else:
        county_geoid_df = gpd.read_file(county_geoid_path)
        if not all(col in county_geoid_df.columns for col in ["GEOID", "COUNTYFP", "INTPTLAT", "INTPTLON"]):
            county_geoid_df = county_geoid_df.rename(
                {"GEOID20": "GEOID", "COUNTYFP20": "COUNTYFP", "INTPTLAT20": "INTPTLAT", "INTPTLON20": "INTPTLON"},
                axis=1,
            )

        county_geoid_df = county_geoid_df[["GEOID", "COUNTYFP", "INTPTLAT", "INTPTLON", "geometry"]]

        county_geoid_df.to_file(f"{output_path}/county_geoid.geojson", driver="GeoJSON")

        success = False
        if county_geoid_df.shape[0] > 0:
            success = True

    return county_geoid_df, success


def read_lodes_df(county_fips, county_lodes_paths, county_geoid_df, output_path, logger, od_option):
    success = False
    if os.path.exists(f"{output_path}/county_lodes.parquet"):
        county_lodes_df = pd.read_parquet(f"{output_path}/county_lodes.parquet")
        unique_countyfps = list(
            set(county_lodes_df["COUNTYFP_x"].astype(str).unique()).union(
                set(county_lodes_df["COUNTYFP_y"].astype(str).unique())
            )
        )
        county_lodes_df = county_lodes_df.drop(["GEOID_x", "GEOID_y", "COUNTYFP_x", "COUNTYFP_y"], axis=1)
        success = True
    else:
        lodes_read = LodesGen(county_fips, county_lodes_paths, county_geoid_df, output_path, logger, od_option)
        county_lodes_df, unique_countyfps, success = lodes_read.generate()

    return county_lodes_df, unique_countyfps, success


def read_safegraph_data(county_fips, city, county_geoid_path, safe_df, start_date, end_date, logger, output_path):
    success = False
    if os.path.exists(f"{output_path}/sg_poi_geoids.csv") and os.path.exists(f"{output_path}/sg_visits_by_day.csv"):
        sg_poi_df = pd.read_csv(f"{output_path}/sg_poi_geoids.csv")
        sg_df = pd.read_csv(f"{output_path}/sg_visits_by_day.csv")
    else:
        safegraph = Safegraph(
            county_fips,
            city,
            county_geoid_path,
            safe_df,
            output_path,
            start_date,
            end_date,
            logger,
        )
        sg_poi_df, success1 = safegraph.get_sg_poi()
        sg_df, success2 = safegraph.get_day_of_week()

        success = success1 and success2

    return sg_poi_df, sg_df, success


def read_ms_buildings_data(county_fips, county_geoid_df, logger, output_path):
    success = False
    ms_buldings_path = f"{output_path}/county_buildings_MS.geojson"
    if os.path.exists(ms_buldings_path):
        ms_buildings_df = gpd.read_file(ms_buldings_path)

        ms_buildings_df["geo_centers"] = ms_buildings_df.geometry.centroid
        ms_buildings_df["location"] = ms_buildings_df.geo_centers.apply(lambda p: [p.y, p.x])
        ms_buildings_df = ms_buildings_df[["geometry", "GEOID", "geo_centers", "location"]]

        success = True
    else:
        ms_builds = MSBuildings(county_fips, county_geoid_df, ms_path, output_path, logger)
        ms_buildings_df, success = ms_builds.buildings()

    return ms_buildings_df, success


def read_origin_dest_locations(county_fips, county, county_geoid_df, sg_enabled, output_path, logger, od_option):
    if os.path.exists(f"{output_path}/county_residential_buildings.geojson") and os.path.exists(
        f"{output_path}/county_work_locations.geojson"
    ):
        res_builds = gpd.read_file(f"{output_path}/county_residential_buildings.geojson")
        res_builds["geo_centers"] = res_builds.geometry.centroid
        res_builds["location"] = res_builds.geometry.centroid.apply(lambda p: [p.y, p.x])

        combined_work_locations = gpd.read_file(f"{output_path}/county_work_locations.geojson")
        combined_work_locations["geo_centers"] = combined_work_locations.geometry.centroid
        combined_work_locations["location"] = combined_work_locations.geometry.centroid.apply(lambda p: [p.y, p.x])

        success = True

    else:
        locations = LocationsOSMSG(county_fips, county, county_geoid_df, sg_enabled, output_path, logger, od_option)
        res_builds, combined_work_locations, success = locations.find_locations_OSM()
    return res_builds, combined_work_locations, success


def read_inrix_data(start_date, inrix_path, inrix_conversion_path):
    inrix_df = pd.read_csv(inrix_path)
    conversion_df = pd.read_csv(inrix_conversion_path)

    desired_date = start_date
    inrix_df["measurement_tstamp"] = pd.to_datetime(inrix_df["measurement_tstamp"])
    inrix_df = inrix_df[inrix_df["measurement_tstamp"].dt.date == desired_date]

    G, hourly_graphs = process_inrix(state, county, inrix_df, conversion_df)

    return G, hourly_graphs


st.set_page_config(layout="wide")


def set_dates():
    if len(st.session_state.dates) == 2:
        st.session_state.start_date, st.session_state.end_date = st.session_state.dates
    else:
        # Optionally handle cases where the date range is not valid
        st.error("Please select a valid date range.")


if "start_date" not in st.session_state or "end_date" not in st.session_state or "state_name" not in st.session_state:
    current_date = datetime.datetime.now()
    st.session_state.start_date = current_date.replace(year=2025, month=3, day=10).date()
    st.session_state.end_date = current_date.replace(year=2025, month=3, day=10).date()
    st.session_state.state_name = None

os.makedirs("log_files", exist_ok=True)

st.header("MOVE-OD")

st.write(
    "You can find FIPS codes here: [Federal Information Processing System (FIPS) Codes for States and Counties](https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt)"
)
col1, col2, col3 = st.columns(3)


states, state_fips, counties_in_state, county_fips = get_states_and_counties()
with col1:
    state = st.selectbox("State", options=list(states.keys()), index=42)
with col2:
    county = st.selectbox("County", options=sorted(counties_in_state[state]), index=32)
with col3:
    county_fips = st.text_input("County's FIPS code", value=(str(county_fips[state, county][0])[-3:]))
city = ""
state_id = states[state]

col1, col2, col3 = st.columns(3)

with col1:
    st.session_state.dates = st.date_input(
        "Enter date range",
        value=(st.session_state.start_date, st.session_state.end_date),
    )

set_dates()
start_date = st.session_state.start_date
end_date = st.session_state.end_date

# times = col3.number_input("Choose number of slots to generate for:", value=1, min_value=0, max_value=10)
# start_times = []
# end_times = []

# for time in range(int(times)):
#     col1, col2 = st.columns(2)

#     with col1:
#         start_times.append(st.time_input(f"Start time for slot {time+1}", datetime.time(6, 00)))

#     with col2:
#         end_times.append(st.time_input(f"Enter end time for slot {time+1}", datetime.time(11, 00)))

# for s, e in zip(start_times, end_times):
#     if s >= e:
#         st.error("Start time should be greater than end time")
#         break

year_range = []
if start_date.year == end_date.year:
    year_range.append(str(start_date.year))
else:
    for year in range(start_date.year, end_date.year + 1):
        year_range.append(str(year))

sg_enabled = False
lodes_enabled = False

choice = col2.multiselect("Choose type of data to generate for:", ["LODES", "Safegraph"], default=["LODES"])
lodes_year = col3.text_input("Enter LODES data year (Latest year is 2022):", value=("2022"))

col1, col2, col3 = st.columns(3)

# translation = col2.text_input("INRIX translation file", value=f"{inrix_path}/USA_Tennessee.zip")
# segments = col1.text_input(
#     "INRIX translation segments file", value=f"{inrix_path}/USA_TN_OSM_20231201_segments_shapefile.zip"
# )
inrix_folder_path = "./data/inrix"
inrix_path = col1.text_input("INRIX data path", value=f"{inrix_folder_path}/Hamilton-County-INRIX.csv")
inrix_conversion_path = col2.text_input("INRIX conversion path", value=f"{inrix_folder_path}/XD_Identification.csv")

tiger_shapefile_year = col3.text_input("Enter TIGER shpaefile year (Latest year is 2024):", value=("2024"))

output_path = f"./move_OD/{state}/{county}/{start_date}_{end_date}"
st.write(f"Output file path: {output_path}")


safe_df = []

col1, col2 = st.columns(2)
ccol1, ccol2 = st.columns(2)
if "Safegraph" in choice:
    sg_enabled = True
    for idx, year in enumerate(year_range):
        safe_df.append(
            st.text_input(
                f"Enter Safegraph parquet file path for {year}",
                value=f"./data/states/{state}/safegraph.parquet/year={year}/region={state}/city={city}/",
            )
        )
else:
    # sg_enabled = col2.checkbox("Use Safegraph data to get additional POI(workplace) locations?")
    sg_enabled = False
    if sg_enabled:
        for idx, year in enumerate(year_range):
            safe_df.append(
                ccol2.text_input(
                    f"Enter Safegraph parquet file path for {year}",
                    value=f"./data/states/{state}/safegraph.parquet/year={year}/region={state}/city={city}/",
                )
            )

ms_enabled = col1.checkbox("Use Global Buildings Footprint data", value=True)

od_option = st.radio(
    "Choose an option for OD generation:",
    (
        "Origin and Destination in same County",
        # "Only Origin in County",
        # "Only Destination in County",
    ),
)

# st.write("You selected:", od_option)

begin = st.button("Begin process")

if begin:

    with st.spinner("Downloading and gathering files"):
        invalid_path = False
        os.makedirs(output_path, exist_ok=True)

        if "LODES" in choice:
            lodes_enabled = True

        county_lodes_paths = [f"./data/states/{state}/{state_id.lower()}_od_main_JT00_{lodes_year}.csv"]
        if od_option != "Origin and Destination in same County":
            county_lodes_paths.append(f"./data/states/{state}/{state_id.lower()}_od_aux_JT00_{lodes_year}.csv")

        if not invalid_path:

            cpus = os.cpu_count() - 1
            run_lodes_sg_parallel = False
            lodes_cpu_max = 1
            days_count = (end_date - start_date).days

            if cpus > days_count:
                run_lodes_sg_parallel = True
                lodes_cpu_max = days_count
                sg_cpu_max = cpus - days_count

            datetime_ranges = get_datetime_ranges(start_date, end_date, timedelta=15)

            logger = Logger(f"{output_path}/{county}_{state}_{start_date}_{end_date}")

            state_fips_id = state_fips[state]
            # TODO: Change to block
            with st.spinner("Downloading Shapefiles"):
                county_geoid_path = f"./data/states/{state}/tl_{tiger_shapefile_year}_{state_fips_id}_bg.zip"

                if not os.path.exists(county_geoid_path):
                    compressed_path = f"./data/states/{state}/tl_{tiger_shapefile_year}_{state_fips_id}_bg.zip"
                    url = f"https://www2.census.gov/geo/tiger/TIGER{tiger_shapefile_year}/BG/tl_{tiger_shapefile_year}_{state_fips_id}_bg.zip"
                    download_shapefile(
                        logger,
                        url=url,
                        compressed_path=compressed_path,
                    )

            if ms_enabled:
                state_stripped = state.replace(" ", "")
                ms_path = f"./data/states/{state}/{state_stripped}.geojson"
                if not os.path.exists(ms_path):
                    with st.spinner("Downloading Global Buildings Footprint"):
                        download_ms_buildings(logger, state, state_stripped)

            if "LODES" in choice:
                flag = False
                for county_lodes_path in county_lodes_paths:
                    if not os.path.exists(county_lodes_path):
                        flag = True

                if flag:
                    with st.spinner("Downloading LODES files"):
                        download_lodes(
                            logger,
                            state,
                            states[state].lower(),
                            lodes_code=0,
                            year=lodes_year,
                        )

                if not os.path.exists(output_path + "/lodes_combs"):
                    os.makedirs(output_path + "/lodes_combs", exist_ok=True)

            if "Safegraph" in choice:
                if not os.path.exists(output_path + "/safegraph_combs"):
                    os.mkdir(output_path + "/safegraph_combs")

    with st.spinner("Generating ODs"):

        # Read GEOID data
        county_geoid_df, success = read_county_geoid_df(county_geoid_path, output_path, logger)
        if success:
            st.success("GEOID data stored")
        else:
            st.error("GEOID data not processesed")

        # Read LODES data
        county_lodes_df, unique_countyfps, success = read_lodes_df(
            county_fips, county_lodes_paths, county_geoid_df, output_path, logger, od_option
        )
        if success:
            st.success("LODES data filtered")
        else:
            st.error("LODES data not processesed")

        # Modifying GEOID data to keep the relevant counties
        county_geoid_df = county_geoid_df[
            county_geoid_df["COUNTYFP"].astype(str).isin([str(fp) for fp in unique_countyfps])
        ]
        logger.info(f"County has {county_geoid_df.shape[0]} census block groups")

        # Getting safegraph data if enabled
        if sg_enabled:
            sg_poi_df, sg_df, success = read_safegraph_data(
                county_fips, city, county_geoid_path, safe_df, start_date, end_date, logger, output_path
            )

            if success:
                st.success("Safegraph data filtered")
            else:
                st.error("Safegraph data not processesed")

        if ms_enabled:
            ms_buildings_df, success = read_ms_buildings_data(county_fips, county_geoid_df, logger, output_path)
            if success:
                st.success("MS Buildings data filtered")
            else:
                st.error("MS Buildings data not processesed")
        else:
            ms_buildings_df = pd.DataFrame()

        res_locations, combined_work_locations, success = read_origin_dest_locations(
            county_fips, county, county_geoid_df, sg_enabled, output_path, logger, od_option
        )
        if success:
            st.success("Origins and Destinations generated")
        else:
            st.error("Origins and Destinations not processesed")

        # county_geoid, res_locations, combined_work_locations, ms_build, county_lodes, sg = read_data(
        #     output_path=output_path,
        #     lodes=lodes_enabled,
        #     sg_enabled=sg_enabled,
        #     ms_enabled=ms_enabled,
        # )

        logger.info(f"Lodes entries: {county_lodes_df.shape[0]}")
        logger.info(f"Census block groups: {county_geoid_df.shape[0]}")
        logger.info(f"OSM Residential buildings: {res_locations.shape[0]}")
        logger.info(f"OSM Commercial buildings: {combined_work_locations.shape[0]}")

        if ms_enabled:
            logger.info(f"Microsoft Building Footprints buildings: {ms_buildings_df.shape[0]}")

        G, hourly_graphs = read_inrix_data(start_date, inrix_path, inrix_conversion_path)

        if "LODES" in choice:
            lodes_combs = LodesComb(
                county_geoid_df,
                output_path,
                ms_enabled,
                datetime_ranges,
                logger,
            )
            lodes_combs.main(
                county_geoid_df,
                res_locations,
                combined_work_locations,
                ms_buildings_df,
                county_lodes_df,
                state_fips_id,
                county_fips,
                G,
                hourly_graphs,
                block_groups="*",
            )
            st.success("Custom OD generated (LODES)")

        if "Safegraph" in choice and "LODES" in choice:
            days = pd.date_range(start_date, end_date, freq="d").to_list()
            for day in days:
                union(output_path, day, sg_enabled)
