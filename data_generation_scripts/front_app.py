import streamlit as st
import os
import datetime
import streamlit as st

import lodes_read
import safegraph
import locations_OSM_SG
import read_ms_buildings
import lodes_combs
import safegraph_combs as sg_combs

st.header('MOVE-OD')
st.subheader(f'Current working directory:')
st.write(os.getcwd())

st.write("You can find FIPS codes here: [Federal Information Processing System (FIPS) Codes for States and Counties](https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt)")
county = st.text_input("Enter County's FIPS code", value="037")
area = st.text_input("Enter County's name", value="Davidson")
city = st.text_input("Enter City's name", value="Nashville")
data_path = st.text_input("Enter output data path", value="../out")

st.write("You can download necessary Shapefiles here: [Federal Information Processing System (FIPS) Codes for States and Counties](https://www.census.gov/cgi-bin/geo/shapefiles/index.php)")

county_cbg = st.text_input("Enter Block group shapefile path", value='../data/Tennessee Census Block Groups/tl_2020_47_bg.shp')
number_lodes = st.number_input('Enter number of LODES file paths available', value = 6, min_value=0, max_value=20)

county_lodes_paths = []
for lodes in range(number_lodes):
    county_lodes_paths.append(st.text_input(f"Enter LODES path {lodes+1}", value=f'../data/lodes/tn_od_main_JT0{lodes}_2019.csv'))

choice = st.multiselect('Choose type of data to generate for:', ['LODES', 'Safegraph'])

timedelta = st.number_input('Select a value of Timedelta (in seconds)', value=15)

times = st.number_input('Choose number of slots to generate for:',  value = 2, min_value=0, max_value=10)
time_start = []
time_end = []
for time in range(times):
    time_start.append(st.time_input(f"Enter start time for slot {time+1}", datetime.time(7, 00)))
    time_end.append(st.time_input(f"Enter end time for slot {time+1}", datetime.time(9, 00)))

safe_df = ''
sg_enabled = False
if 'Safegraph' in choice:
    sg_enabled = True
    safe_df = st.text_input("Enter Safegraph parquet file path", value=f"../data/safegraph.parquet/year=2021/region=TN/city={city}/")
    
else:
    sg_enabled = st.checkbox("Use Safegraph data")
    if sg_enabled:
        safe_df = st.text_input("Enter Safegraph parquet file path", value=f"../data/safegraph.parquet/year=2021/region=TN/city={city}/")

builds = ''
ms_enabled = st.checkbox("Use MS Buildings data")
if ms_enabled:
    builds = st.text_input("Enter MS buildings file path", value="../data/Tennessee.geojson")

if not os.path.exists(data_path):
    os.mkdir(data_path)


begin = st.button('Begin process')

if begin:

    with st.spinner('In Progress...'):
        lodes_read = lodes_read.Lodes_gen(county, county_lodes_paths, county_cbg, data_path)
        locations = locations_OSM_SG.locations_OSM_SG(county, area, county_cbg, sg_enabled, data_path)
        lodes_combs = lodes_combs.Lodes_comb(county_cbg, data_path, ms_enabled, timedelta, time_start, time_end)
        sg_combs = sg_combs.Sg_combs(county_cbg, data_path, ms_enabled, timedelta, time_start, time_end)

        # lodes_read.generate()
        # st.success('LODES data filtered')

        # if sg_enabled:
        #     safegraph = safegraph.Safegraph(county, city, county_cbg, safe_df, data_path)
        #     safegraph.filter_SG()
        #     st.success('Safegraph data filtered')

        # if ms_enabled:
        #     ms_builds = read_ms_buildings.MS_Buildings(county, county_cbg, builds, data_path)
        #     ms_builds.buildings()
        #     st.success('MS Buildings data filtered')

        # locations.find_locations_OSM()  
        # st.success('Locations generated')   

        if 'LODES' in choice:
            lodes_combs.generate_combs()
            st.success('Custom OD generated (LODES)')
        if 'Safegraph' in choice:
            sg_combs.generate_combs()
            st.success('Custom OD generated (Safegraph)')
