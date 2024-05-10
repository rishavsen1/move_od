import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
import random
import datetime
import requests
import gzip
import os
import shutil
import zipfile
import subprocess
from io import StringIO
from multiprocessing import cpu_count, Pool
from dask import delayed, compute
from tqdm.auto import tqdm

from config import CENSUS_API_KEY

KM_TO_MILES = 0.621371


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


def sample_rows(grouped_df, sample_size):
    """Efficiently sample rows based on the 'total_jobs' distribution."""
    # Calculate cumulative sum of jobs and total jobs
    total_jobs = grouped_df["total_jobs"].sum()
    probabilities = grouped_df["total_jobs"] / total_jobs

    # Sample the data
    chosen_indices = np.random.choice(grouped_df.index, size=sample_size, replace=True, p=probabilities)
    sampled_df = grouped_df.loc[chosen_indices].copy()
    sampled_df["sampled_jobs"] = 1  # Each row contributes one job in the sample

    # Aggregate sampled data
    sampled_df = sampled_df.groupby(level=[0, 1]).sum().reset_index()
    return sampled_df


def marginal_dist(df, origin_col, dest_col, sample_size):
    """Create a subsampled DataFrame based on marginal distributions."""
    # Group by origin and destination, aggregating total_jobs
    grouped_df = df.groupby([origin_col, dest_col])["total_jobs"].sum().reset_index()
    grouped_df.set_index([origin_col, dest_col], inplace=True)

    # Handle case where sample_size exceeds the total available jobs
    if sample_size > grouped_df["total_jobs"].sum():
        sample_size = int(grouped_df["total_jobs"].sum())

    # Sample the rows according to the total_jobs distribution
    sampled_df = sample_rows(grouped_df, sample_size)

    return sampled_df


def combine_date_time(date, time):
    return datetime.datetime.combine(date, time)


def get_datetime_ranges(start_date, end_date, start_times, end_times, timedelta):
    # Creating the list of lists of datetime objects
    datetime_ranges = []
    current_date = start_date
    while current_date <= end_date:
        for start_time, end_time in zip(start_times, end_times):
            start_datetime = combine_date_time(current_date, start_time)
            end_datetime = combine_date_time(current_date, end_time)

            # Adjust if end_datetime is before start_datetime (crossing midnight)
            if end_datetime < start_datetime:
                end_datetime += datetime.timedelta(days=1)

            datetime_range = [
                start_datetime + datetime.timedelta(seconds=x * timedelta)
                for x in range(0, int((end_datetime - start_datetime).total_seconds() / timedelta))
            ]
            datetime_ranges.append(datetime_range)
        current_date = current_date + datetime.timedelta(days=1)

    return datetime_ranges


def fetch_and_parse_fips():
    # URL of the FIPS code file
    url = "https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt"

    # Fetch the data
    response = requests.get(url)
    if response.status_code == 200:
        data = response.text
    else:
        raise Exception(f"Failed to retrieve data: Status code {response.status_code}")

    data = response.text.split("\n")[13:]  # Adjust the number based on the actual header line count
    data = "\n".join(data)
    # Define column specifications according to the fixed-width format of the file
    col_specification = [
        (0, 2),  # State FIPS (this seems to be slightly off based on your output)
        (4, 90),  # Rest of the line, which seems to include state name and possibly county dataz
    ]

    # Read the fixed-width file
    fips_df = pd.read_fwf(StringIO(data), colspecs=col_specification, header=None)
    fips_df.columns = ["State FIPS", "Data"]

    return fips_df


def get_fips_codes(state_abbreviation, county_name):
    # Fetch and parse the FIPS data
    fips_df = fetch_and_parse_fips()

    # Query the DataFrame for state FIPS code
    state_fips = fips_df[fips_df["State Abbreviation"] == state_abbreviation]["State FIPS"].unique()

    if not state_fips:
        return None, None

    # Query the DataFrame for county FIPS code
    county_fips = fips_df[
        (fips_df["State Abbreviation"] == state_abbreviation)
        & (fips_df["County Name"].str.startswith(county_name, na=False))
    ]["County FIPS"].values

    if county_fips.size > 0:
        return state_fips[0], county_fips[0]
    else:
        return state_fips[0], None


def read_data(output_path, lodes=False, sg_enabled=False, ms_enabled=False, sample_size=np.inf):
    print("Reading data")
    # loading geometry data
    county_cbg = pd.read_csv(f"{output_path}/county_cbg.csv")
    county_cbg = gpd.GeoDataFrame(county_cbg, geometry=gpd.GeoSeries.from_wkt(county_cbg.geometry))
    county_cbg.GEOID = county_cbg.GEOID.astype(str)

    # loading residential buildings
    res_build = pd.read_csv(
        f"{output_path}/county_residential_buildings.csv",
    )
    res_build = gpd.GeoDataFrame(res_build, geometry=gpd.GeoSeries.from_wkt(res_build.geometry))
    res_build["location"] = res_build.geometry.apply(lambda p: [p.y, p.x])
    res_build.GEOID = res_build.GEOID.astype(str)

    # loading work buildings
    com_build = pd.read_csv(
        f"{output_path}/county_work_locations.csv",
    )

    com_build = gpd.GeoDataFrame(com_build, geometry=gpd.GeoSeries.from_wkt(com_build.geometry))
    com_build["location"] = com_build.geometry.apply(lambda p: [p.y, p.x])
    com_build = com_build.reset_index()
    com_build.GEOID = com_build.GEOID.astype(str)

    # loading all buildings (MS dataset)
    if ms_enabled:
        ms_build = pd.read_csv(f"{output_path}/county_buildings_MS.csv")
        ms_build = gpd.GeoDataFrame(ms_build, geometry=gpd.GeoSeries.from_wkt(ms_build.geo_centers))
        ms_build.GEOID = ms_build.GEOID.astype(str)
        ms_build["location"] = ms_build.geometry.apply(lambda p: [p.y, p.x])
    else:
        ms_build = pd.DataFrame()

    sg = pd.DataFrame()
    county_lodes = pd.DataFrame()

    if sg_enabled:
        sg = pd.read_csv(f"{output_path}/sg_visits_by_day.csv")
        sg["home_cbg"] = sg["home_cbg"].astype(str)
        sg["poi_cbg"] = sg["poi_cbg"].astype(str)
        # marginal_dist(sg, "home_cbg", "poi_cbg", sample_size)

    if lodes:
        county_lodes = pd.read_csv(
            f"{output_path}/county_lodes.csv",
            dtype={"TRACTCE20_home": "string", "TRACTCE20_work": "string"},
        )
        county_lodes["h_geocode"] = county_lodes["h_geocode"].astype(str)
        county_lodes["w_geocode"] = county_lodes["w_geocode"].astype(str)
        # marginal_dist(county_lodes, "h_geocode", "w_geocode", sample_size)

    return county_cbg, res_build, com_build, ms_build, county_lodes, sg


# def state_abbreviation_to_full(name):
#     state_name = None
#     if name:
#         name_to_abbreviation = {value: key for key, value in states.items()}
#         print(name_to_abbreviation)
#         state_name = name_to_abbreviation.get(name, "Invalid state abbreviation")
#     return state_name


def get_states_and_counties():
    df = pd.read_csv("../data/uscounties.csv", dtype={"county_fips": "str"})
    states = df.groupby("state_name").first()["state_id"].to_dict()
    state_to_county = df.groupby("state_name")["county"].apply(list).to_dict()
    state_fips = df.groupby("state_name").first()["county_fips"].apply(lambda x: str(x)[:2]).to_dict()
    state_to_county_fips = df.groupby(["state_name", "county"])["county_fips"].apply(list).to_dict()
    return states, state_fips, state_to_county, state_to_county_fips


def download_shapefile(logger, state, state_fips, year):
    url = f"https://www2.census.gov/geo/tiger/TIGER2023/BG/tl_{year}_{state_fips}_bg.zip"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        compressed_path = f"../data/states/{state}/tl_{year}_{state_fips}_bg.zip"
        os.makedirs(os.path.dirname(compressed_path), exist_ok=True)
        with open(compressed_path, "wb") as f:
            f.write(response.content)
        logger.info(f"File downloaded and saved as: {compressed_path}")

    else:
        logger.error(f"Failed to download the file: Status code {response.status_code}")


def download_lodes(logger, state, state_abbr, lodes_code, year):
    type = "gzip"

    url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state_abbr}/od/{state_abbr}_od_main_JT0{lodes_code}_{year}.csv.gz"
    compressed_path = f"../data/states/{state}/{state_abbr}_od_main_JT0{lodes_code}_{year}.csv.gz"
    decompressed_path = f"../data/states/{state}/{state_abbr}_od_main_JT0{lodes_code}_{year}.csv"
    download_and_decompress(type, logger, url, compressed_path, decompressed_path)

    url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state_abbr}/od/{state_abbr}_od_aux_JT0{lodes_code}_{year}.csv.gz"
    compressed_path = f"../data/states/{state}/{state_abbr}_od_aux_JT0{lodes_code}_{year}.csv.gz"
    decompressed_path = f"../data/states/{state}/{state_abbr}_od_aux_JT0{lodes_code}_{year}.csv"
    download_and_decompress(type, logger, url, compressed_path, decompressed_path)


def download_ms_buildings(logger, state, state_stripped):
    type = "zip"
    url = f"https://usbuildingdata.blob.core.windows.net/usbuildings-v2/{state_stripped}.geojson.zip"
    compressed_path = f"../data/states/{state}/{state_stripped}.geojson.zip"
    decompressed_path = f"../data/states/{state}/"  # Path to the directory to extract files
    download_and_decompress(type, logger, url, compressed_path, decompressed_path)


def download_and_decompress(type, logger, url, compressed_path, decompressed_path):
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        total_size_in_bytes = int(response.headers.get("content-length", 0))
        block_size = 1024

        os.makedirs(os.path.dirname(compressed_path), exist_ok=True)
        progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
        with open(compressed_path, "wb") as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(response.content)
        progress_bar.close()

        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            logger.error(f"Something went wrong!")
        logger.info(f"File downloaded and saved as: {compressed_path}")

        if type == "gzip":
            with gzip.open(compressed_path, "rb") as f_in:
                with open(decompressed_path.rstrip("/"), "wb") as f_out:  # Assuming the path adjustment for gzip
                    shutil.copyfileobj(f_in, f_out)
            logger.info(f"File decompressed and saved as: {decompressed_path.rstrip('/')}")

        elif type == "zip":
            with zipfile.ZipFile(compressed_path, "r") as zip_ref:
                zip_ref.extractall(decompressed_path)
            logger.info(f"Files decompressed and saved in directory: {decompressed_path}")

        os.remove(compressed_path)
        logger.info(f"Removed the compressed file: {compressed_path}")

    else:
        logger.error(f"Failed to download the file: Status code {response.status_code}")


def run_script(script_path, logger):
    with subprocess.Popen([script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
        # Read the output line by line as it is produced
        for line in proc.stdout:
            print(line, end="")  # Print to console in real-time
            logger.info(line.strip())  # Log each line

        # Check for errors and log them
        for line in proc.stderr:
            logger.error(line.strip())

    # Wait for the process to finish and get the exit code
    proc.wait()
    return proc.returncode


def traveltimeDrive(row):
    latStart, lonStart, latDest, lonDest, time, mode_type = (
        row["origin_loc_lat"],
        row["origin_loc_lon"],
        row["dest_loc_lat"],
        row["dest_loc_lon"],
        str(row["pickup_time_str"]),
        row["mode_type"],
    )
    modes = {"drive": "CAR", "transit": "WALK,TRANSIT", "walk": "WALK"}
    mode = modes[mode_type]
    request_url = f"http://localhost:8080/otp/routers/default/plan?fromPlace={latStart},{lonStart}&toPlace={latDest},{lonDest}&mode={mode}&date=01-26-2024&time={time}&maxWalkDistance=100000"

    res_dict = {
        "origin_loc_lat": latStart,
        "origin_loc_lon": lonStart,
        "dest_loc_lat": latDest,
        "dest_loc_lon": lonDest,
        "error": "",  # include an error field in your result dictionary
    }

    try:
        response = requests.get(request_url)
        if response.status_code == 200:
            data = response.json()
            try:
                if "plan" in data and "itineraries" in data["plan"]:
                    distance = sum(leg["distance"] for leg in data["plan"]["itineraries"][0]["legs"])
                    res_dict.update(
                        {"transit_time": int(data["plan"]["itineraries"][0]["duration"]), "distance_m": distance}
                    )
            except (KeyError, TypeError) as e:
                res_dict["error"] = "Error parsing response data: {}".format(e)
        else:
            res_dict["error"] = f"HTTP error {response.status_code}"

    except requests.exceptions.RequestException as e:
        res_dict["error"] = f"Request failed: {e}"

    return res_dict


def process_rows_drive(mode_type, dataframe):
    data_list = dataframe
    if not isinstance(dataframe, dict):
        data_list = dataframe.to_dict("records")
        for data in data_list:
            data.update({"mode_type": mode_type})
        delayed_tasks = [delayed(traveltimeDrive)(data) for data in data_list]

        results = []
        for result in tqdm(compute(*delayed_tasks, scheduler="threads"), total=len(data_list)):
            results.append(result)
    else:
        data_list.update({"mode_type": mode_type})
        results = [traveltimeDrive(data_list)]

    return results


def get_travel_time(mode_type, trips_df):
    results = process_rows_drive(mode_type, trips_df)
    for res in results:
        matching_rows = trips_df[
            (trips_df["origin_loc_lat"] == res["origin_loc_lat"])
            & (trips_df["origin_loc_lon"] == res["origin_loc_lon"])
            & (trips_df["dest_loc_lat"] == res["dest_loc_lat"])
            & (trips_df["dest_loc_lon"] == res["dest_loc_lon"])
        ]

        if not matching_rows.empty:
            for index in matching_rows.index:
                if "transit_time" in res:
                    trips_df.loc[index, f"{mode_type}_time"] = res["transit_time"]
                    trips_df.loc[index, "total_distance"] = res["distance_m"]
                    trips_df.loc[index, "distance_miles"] = res["distance_m"] / 1000 * KM_TO_MILES
                else:
                    trips_df.loc[index, f"{mode_type}_time"] = None
                    trips_df.loc[index, "total_distance"] = None
                    trips_df.loc[index, "distance_miles"] = None

    return trips_df


def get_travel_time_dict(mode_type, trips_dict):
    results = process_rows_drive(mode_type, trips_dict)
    result_dict = {}
    for res in results:
        if "transit_time" in res:
            result_dict[f"{mode_type}_time"] = res["transit_time"]
            result_dict["total_distance"] = res["distance_m"]
            result_dict["distance_miles"] = res["distance_m"] / 1000 * KM_TO_MILES
        else:
            result_dict[f"{mode_type}_time"] = None
            result_dict["total_distance"] = None
            result_dict["distance_miles"] = None

    return result_dict


def get_census_data(api_key, api_url, table_name, state_fips, county_fips, block_groups):
    url = api_url

    params = {
        "get": f"NAME,group({table_name})",
        # "for": f"county:{county_fips}",
        # "in": f"state:{state_fips}",
        "for": f"block group:{block_groups}",
        "in": f"state:{state_fips} county:{county_fips}",
        "key": api_key,
    }

    response = requests.get(url, params=params)
    # print(response.url)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    else:
        print("Failed to retrieve data:", response.status_code)


def get_census_depart_time(
    table="B08302",
    api_url="https://api.census.gov/data/2022/acs/acs5",
    state_fips="47",
    county_fips="065",
    block_groups="*",
):
    # tables = 'B08134'
    # table = "B08302"
    # api_url = "https://api.census.gov/data/2022/acs/acs5"

    # tables = 'P034'
    # api_url = "https://api.census.gov/data/2000/dec/sf3"

    df = get_census_data(CENSUS_API_KEY, api_url, table, state_fips, county_fips, block_groups)

    columns_to_be_renamed = {
        f"{table}_001E": "total_estimate",
        f"{table}_001M": "total_margin",
        f"{table}_002E": "12am_to_4:59am_estimate",
        f"{table}_002M": "12am_to_4:59am_margin",
        f"{table}_003E": "5am_to_5:29am_estimate",
        f"{table}_003M": "5am_to_5:29am_margin",
        f"{table}_004E": "5:30am_to_5:59am_estimate",
        f"{table}_004M": "5:30am_to_5:59am_margin",
        f"{table}_005E": "6am_to_6:29am_estimate",
        f"{table}_005M": "6am_to_6:29am_margin",
        f"{table}_006E": "6:30am_to_6:59am_estimate",
        f"{table}_006M": "6:30am_to_6:59am_margin",
        f"{table}_007E": "7am_to_7:29am_estimate",
        f"{table}_007M": "7am_to_7:29am_margin",
        f"{table}_008E": "7:30am_to_7:59am_estimate",
        f"{table}_008M": "7:30am_to_7:59am_margin",
        f"{table}_009E": "8am_to_8:29am_estimate",
        f"{table}_009M": "8am_to_8:29am_margin",
        f"{table}_010E": "8:30am_to_8:59am_estimate",
        f"{table}_010M": "8:30am_to_8:59am_margin",
        f"{table}_011E": "9am_to_9:59am_estimate",
        f"{table}_011M": "9am_to_9:59am_margin",
        f"{table}_012E": "10am_to_10:59am_estimate",
        f"{table}_012M": "10am_to_10:59am_margin",
        f"{table}_013E": "11am_to_11:59am_estimate",
        f"{table}_013M": "11am_to_11:59am_margin",
        f"{table}_014E": "12pm_to_3:59pm_estimate",
        f"{table}_014M": "12pm_to_3:59pm_margin",
        f"{table}_015E": "4pm_to_11:59pm_estimate",
        f"{table}_015M": "4pm_to_11:59pm_margin",
    }

    df_renamed = df[list(columns_to_be_renamed.keys()) + ["GEO_ID", "state", "county", "tract", "block group"]].rename(
        columns_to_be_renamed, axis=1
    )
    df_renamed["GEO_ID"] = df_renamed["GEO_ID"].apply(lambda x: x.split("US")[1].lstrip("0"))
    df_renamed["total_estimate"] = df_renamed["total_estimate"].astype(int)

    for column in df_renamed.columns:
        if "estimate" in column or "margin" in column:
            df_renamed[column] = df_renamed[column].astype("Int64")

    return df_renamed
