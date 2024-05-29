import streamlit as st
import os
import datetime
import pandas as pd
import numpy as np

import lodes_read
import safegraph
import locations_OSM_SG
import read_ms_buildings
import lodes_combs
import safegraph_combs as sg_combs
from utils import (
    read_data,
    get_datetime_ranges,
    get_states_and_counties,
    download_lodes,
    download_shapefile,
    download_ms_buildings,
    get_travel_time,
)
import union_lodes_sg

from logger import Logger

st.set_page_config(layout="wide")


def set_dates():
    if len(st.session_state.dates) == 2:
        st.session_state.start_date, st.session_state.end_date = st.session_state.dates
    else:
        # Optionally handle cases where the date range is not valid
        st.error("Please select a valid date range.")


if "start_date" not in st.session_state or "end_date" not in st.session_state or "state_name" not in st.session_state:
    current_date = datetime.datetime.now()
    st.session_state.start_date = current_date.replace(year=2024).date()
    st.session_state.end_date = current_date.replace(year=2024).date()
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

col1, col2, col3 = st.columns(3)

with col1:
    st.session_state.dates = st.date_input(
        "Enter date range", value=(st.session_state.start_date, st.session_state.end_date)
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

inrix_folder_path = "/home/rishav/Programs/move_od/data/INRIX"

inrix_path = col2.text_input("INRIX data path", value=f"{inrix_folder_path}/Hamilton-2021-Inrix-Data.csv")

choice = col3.multiselect("Choose type of data to generate for:", ["LODES", "Safegraph"], default=["LODES"])

if "LODES" in choice:
    lodes_enabled = True

county_lodes_paths = [
    f"../data/states/{state}/{states[state].lower()}_od_main_JT00_2021.csv",
    f"../data/states/{state}/{states[state].lower()}_od_aux_JT00_2021.csv",
]

col1, col2, col3 = st.columns(3)

translation = col2.text_input("INRIX translation file", value=f"{inrix_path}/USA_Tennessee.zip")
segments = col1.text_input(
    "INRIX translation segments file", value=f"{inrix_path}/USA_TN_OSM_20231201_segments_shapefile.zip"
)
inrix_segments = col2.text_input("INRIX segments file", value=f"{inrix_path}/XD_Identification.zip")


output_path = f"../move_OD/{county}_{state}_{start_date}_{end_date}"
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
                value=f"../data/states/{state}/safegraph.parquet/year={year}/region={state}/city={city}/",
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
                    value=f"../data/states/{state}/safegraph.parquet/year={year}/region={state}/city={city}/",
                )
            )

ms_enabled = col1.checkbox("Use Global Buildings Footprint data", value=True)

od_option = st.radio(
    "Choose an option for OD generation:",
    (
        "Origin and Destination in same County",
        "Only Origin in County",
        "Only Destination in County",
    ),
)

reruns = st.number_input("No. of subsets to generate")
# st.write("You selected:", od_option)

begin = st.button("Begin process")

if begin:

    invalid_path = False
    os.makedirs(output_path, exist_ok=True)

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

        with st.spinner("Downloading Shapefiles"):
            year = "2023"
            county_cbg = f"../data/states/{state}/tl_2023_{state_fips[state]}_bg.zip"
            compressed_path = f"../data/states/{state}/tl_{year}_{state_fips}_bg.zip"
            url = f"https://www2.census.gov/geo/tiger/TIGER2023/BG/tl_{year}_{state_fips}_bg.zip"

            if not os.path.exists(county_cbg):
                download_shapefile(logger, state, state_fips=state_fips[state], year="2023", url=url)

        if ms_enabled:
            state_stripped = state.replace(" ", "")
            ms_path = f"../data/states/{state}/{state_stripped}.geojson"
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
                        year="2021",
                    )

            if not os.path.exists(output_path + "/lodes_combs"):
                os.mkdir(output_path + "/lodes_combs")

        if "Safegraph" in choice:
            if not os.path.exists(output_path + "/safegraph_combs"):
                os.mkdir(output_path + "/safegraph_combs")

        with st.spinner("In Progress..."):

            if os.path.exists(f"{output_path}/county_lodes.csv") and os.path.exists(f"{output_path}/county_cbg.csv"):
                st.success("LODES filtered data already present")
            else:
                lodes_read = lodes_read.LodesGen(
                    county_fips, county_lodes_paths, county_cbg, output_path, logger, od_option
                )
                lodes_read.generate()
                st.success("LODES data filtered")

            if sg_enabled:
                if os.path.exists(f"{output_path}/sg_poi_cbgs.csv") and os.path.exists(
                    f"{output_path}/sg_visits_by_day.csv"
                ):
                    st.success("Safegraph filtered data already present")
                else:
                    safegraph = safegraph.Safegraph(
                        county_fips,
                        city,
                        county_cbg,
                        safe_df,
                        output_path,
                        start_date,
                        end_date,
                        logger,
                    )
                    safegraph.get_sg_poi()
                    safegraph.get_day_of_week()
                    st.success("Safegraph data filtered")

            if ms_enabled:
                if os.path.exists(f"{output_path}/county_buildings_MS.csv"):
                    st.success("MS Buildings filtered already present")
                else:
                    ms_builds = read_ms_buildings.MSBuildings(county_fips, county_cbg, ms_path, output_path, logger)
                    ms_builds.buildings()
                    st.success("MS Buildings data filtered")

            if os.path.exists(f"{output_path}/county_residential_buildings.csv") and os.path.exists(
                f"{output_path}/county_work_locations.csv"
            ):
                st.success("Locations already present")

            else:
                locations = locations_OSM_SG.LocationsOSMSG(
                    county_fips, county, county_cbg, sg_enabled, output_path, logger, od_option
                )
                locations.find_locations_OSM()
                st.success("Locations generated")

            county_cbg, res_build, com_build, ms_build, county_lodes, sg = read_data(
                output_path=output_path,
                lodes=lodes_enabled,
                sg_enabled=sg_enabled,
                ms_enabled=ms_enabled,
            )

            logger.info(f"Lodes entries: {county_lodes.shape[0]}")
            logger.info(f"Census block groups: {county_cbg.shape[0]}")
            logger.info(f"OSM Residential buildings: {res_build.shape[0]}")
            logger.info(f"OSM Commercial buildings: {com_build.shape[0]}")
            logger.info(f"Microsoft Building Footprints buildings: {ms_build.shape[0]}")

            if "LODES" in choice:
                lodes_combs = lodes_combs.LodesComb(
                    county_cbg,
                    output_path,
                    ms_enabled,
                    datetime_ranges,
                    logger,
                )
                lodes_combs.main(
                    county_cbg,
                    res_build,
                    com_build,
                    ms_build,
                    county_lodes,
                    state,
                    county,
                    state_fips[state],
                    county_fips,
                    block_groups="*",
                )
                st.success("Custom OD generated (LODES)")
            # if "Safegraph" in choice:
            #     sg_combs = sg_combs.SgCombs(
            #         county_cbg,
            #         output_path,
            #         ms_enabled,
            #         timedelta,
            #         start_times,
            #         end_times,
            #         start_date,
            #         end_date,
            #         logger,
            #     )
            #     sg_combs.main(county_cbg, res_build, com_build, ms_build, sg, sg_cpu_max)
            #     st.success("Custom OD generated (Safegraph)")

            if "Safegraph" in choice and "LODES" in choice:
                days = pd.date_range(start_date, end_date, freq="d").to_list()
                for day in days:
                    union_lodes_sg.union(output_path, day, sg_enabled)
