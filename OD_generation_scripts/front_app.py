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
fips = st.text_input("Enter County's FIPS code", value="037")
county = st.text_input("Enter County's name", value="Davidson")
city = st.text_input("Enter City's name", value="Nashville")
state = st.text_input("Enter State", value="TN")

st.write("You can download necessary Shapefiles here: [Federal Information Processing System (FIPS) Codes for States and Counties](https://www.census.gov/cgi-bin/geo/shapefiles/index.php)")

st.write("The program expects you to gather all the required data under a common 'data' folder")
data_path = st.text_input('Enter path to common data folder where Block Group, LODES, Safegraph(optional), MS Buildings (optional) are stored', value='../data')

county_cbg = st.text_input("Enter Block group shapefile path", value=f'{data_path}/tl_2022_47_bg/tl_2022_47_bg.shp')
number_lodes = st.number_input('Enter number of LODES file paths available', value = 6, min_value=0, max_value=20)

county_lodes_paths = []
for lodes in range(number_lodes):
    county_lodes_paths.append(st.text_input(f"Enter LODES path {lodes+1}", value=f'{data_path}/lodes/tn_od_main_JT0{lodes}_2019.csv'))

choice = st.multiselect('Choose type of data to generate for:', ['LODES', 'Safegraph'], default=['LODES'])

# day_count = int(st.number_input('Enter no. of days to generate for', value=1, step=1 ))
start_date = st.date_input('Enter start date', datetime.date(2021, 1, 4))
end_date = st.date_input('Enter end date', start_date, min_value=start_date)
if end_date < start_date:
    st.error('End date should be greater than or equal to start date')
        

timedelta = st.number_input('Select a value of Timedelta (in seconds)', value=15)

times = st.number_input('Choose number of slots to generate for:',  value = 2, min_value=0, max_value=10)
time_start = []
time_end = []
for time in range(times):
    time_start.append(st.time_input(f"Enter start time for slot {time+1}", datetime.time(7, 00)))
    time_end.append(st.time_input(f"Enter end time for slot {time+1}", datetime.time(9, 00)))

year_range = []
if start_date.year == end_date.year:
    year_range.append(str(start_date.year))
else:
    for year in range(start_date.year, end_date.year+1):
        year_range.append(str(year))

safe_df = []

sg_enabled = False
if 'Safegraph' in choice:
    sg_enabled = True
    for idx, year in enumerate(year_range):
        safe_df.append(st.text_input(f"Enter Safegraph parquet file path {idx + 1}", value=f"{data_path}/safegraph.parquet/year={year}/region={state}/city={city}/"))
    
else:
    sg_enabled = st.checkbox("Use Safegraph data")
    if sg_enabled:
        for idx, year in enumerate(year_range):
            safe_df.append(st.text_input(f"Enter Safegraph parquet file path {idx + 1}", value=f"{data_path}/safegraph.parquet/year={year}/region={state}/city={city}/"))

builds = ''
ms_enabled = st.checkbox("Use MS Buildings data")
if ms_enabled:
    st.write('Microsoft Buildings footprint can be downloaded from [Global ML Buildings Footprint by Bing Maps](https://github.com/microsoft/GlobalMLBuildingFootprints)')
    builds = st.text_input("Enter MS buildings file path", value=f"{data_path}/Tennessee.geojson")

output_path = st.text_input('Enter output file path', value=f'../generated_OD/{county}_{state}_{start_date}_{end_date}')

begin = st.button('Begin process')


if begin:


    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
    if not os.path.exists(output_path+'/lodes_combs'):
        os.mkdir(output_path+'/lodes_combs')
    if not os.path.exists(output_path+'/safegraph_combs'):
        os.mkdir(output_path+'/safegraph_combs')

    with st.spinner('In Progress...'):
        lodes_read = lodes_read.Lodes_gen(fips, county_lodes_paths, county_cbg, output_path)
        locations = locations_OSM_SG.locations_OSM_SG(fips, county, county_cbg, sg_enabled, output_path)
        lodes_combs = lodes_combs.Lodes_comb(county_cbg, output_path, ms_enabled, timedelta, time_start, time_end, start_date, end_date)
        sg_combs = sg_combs.Sg_combs(county_cbg, output_path, ms_enabled, timedelta, time_start, time_end, start_date, end_date)
        # sg_combs_p = sg_combs_p.Sg_combs(county_cbg, output_path, ms_enabled, timedelta, time_start, time_end, )
        # sg_combs_pp = sg_combs_pp.Sg_combs(county_cbg, output_path, ms_enabled, timedelta, time_start, time_end, )

        if os.path.exists(f'{output_path}/county_lodes_2019.csv') and os.path.exists(f'{output_path}/county_cbg.csv'):
            st.success('LODES filtered data already present')
        else:
            lodes_read.generate()
            st.success('LODES data filtered')

        if sg_enabled:
            if os.path.exists(f'{output_path}/sg_poi_cbgs.csv') and os.path.exists(f'{output_path}/sg_visits_by_day.csv'):
                st.success('Safegraph filtered data already present')
            else:    
                safegraph = safegraph.Safegraph(fips, city, county_cbg, safe_df, output_path, start_date, end_date)
                safegraph.get_sg_poi()
                safegraph.get_day_of_week()
                st.success('Safegraph data filtered')

        if ms_enabled:
            if os.path.exists(f'{output_path}/county_buildings_MS.csv'):
                st.success('MS Buildings filtered already present')
            else:
                ms_builds = read_ms_buildings.MS_Buildings(fips, county_cbg, builds, output_path)
                ms_builds.buildings()
                st.success('MS Buildings data filtered')

        if os.path.exists(f'{output_path}/county_residential_buildings.csv') and os.path.exists(f'{output_path}/county_work_loc_poi_com_civ.csv'):
            st.success('Locations already present')           
        else:
            locations.find_locations_OSM()  
            st.success('Locations generated')   

        if 'LODES' in choice:
            lodes_combs.main()
            st.success('Custom OD generated (LODES)')
        if 'Safegraph' in choice:
            sg_combs.main()
            st.success('Custom OD generated (Safegraph)')