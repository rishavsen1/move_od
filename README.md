# Move-OD

We can generate origin-destination (OD) pairs for a given county or city, from the releveant datasets about the region. The geographic areas are divided into census tracts(larger, poor resolution) and census block groups (CBGs) (smaller, better resolution). We find the movement matrix of people travelling for jobs (LODES), or general purposes (Safegraph).

## The data

1. Geographic data

The geographic data of Hamilton county defines the area and boundaries of the census tracts and CBGs. It also provides unique GEOIDs for each zone under consideration, which we can further use to find relevant regions in other datasets.

2. People movement`<br>`
   a. [LODES dataset](https://lehd.ces.census.gov/data/)`<br>`
   b. Safegraph dataset (our data is for Jan 2021-Mar2021)

These provide the information about the movement of people (the number of people moving, and their origin and destination CBG).

3. Residential and Work locations

   The locations of residential and commercial(work) areas are obtained from OpenStreetMaps(OSM). They are obtained by using the [OSM Accomodation tags](https://wiki.openstreetmap.org/wiki/Key:building#Accommodation) and the python library OSMNX.

   The work locations are obtained as a combination of the tags [commercial](https://wiki.openstreetmap.org/wiki/Key:building#Commercial), [civic/amenity](https://wiki.openstreetmap.org/wiki/Key:building#Civic/amenity), and the Safegraph POI (point of interest) locations.

4. Microsoft Buildings dataset (complements to OSM locations)

   These are the locations of all buildings in Hamilton county, obtained from [Microsoft Building Footprints](https://github.com/Microsoft/USBuildingFootprints), and are **not labelled** as home/work places. These locations are used in lieu of OSM labelled locations only in case the concerned CBG has no home/work locations from OSM.

   The data extraction is done as in [read_ms_buildings.py](OD_generation_scripts/read_ms_buildings.py).

## Deriving the OD matrix

We intend to find the Origin Destination (OD) matrix for Hamilton county. The home location acts as the **origin**, while the work location acts as the **destination**. It is obtained as a combination of the home location, work location, travel start time, and their return time. It is found by uniform sampling across the home and commercial locations.

**Note**: A random seed of 42 is used for all the random samples.

There are a few assumptions made to help in the process of sampleing the data:

1. For the LODES and Safegraph data, we find the number of people travelling between each OD pair, and randomly sample the home and work locations from the OSM locations (or Microsoft Buildings if needed).
2. The travel start time(go_time) is sampled randomly from 7AM to 9AM (at 15min intervals)
3. The return time is sampled randomly from 4PM to 6PM (at 15min intervals)

For example if an OD pair has 50 people travelling among them, then we sample 50 home and 50 work locations and randomly choose 50 start and return times in the manner described.

## Running the pipeline

(This was run on python 3.9 on Ubuntu machine)
Install the requirements using `pip install -r requirements.txt`.

After installing Streamlit, run the command:

```
$ cd OD_generation_scripts
$ streamlit run front_app.py --browser.gatherUsageStats False
```

Now you can enter the necessary data and selct between **LODES** and **Safegraph** OD generation. The result will be produced in the chosen output folder.

## Explanation of the generated OD data

The generated data has been converted to parquet and is available in the folder you selected to be the output folder. ( by deafult: `generated_OD`)

Each row in either dataset represents a single trip by one person. In the case of lodes dataset, the trip represent movement to the job location and then back to home. Here are the key columns.

The columns(with their datatypes) are:

- h_geocode(string): The GEOID of the person's home CBG
- w_geocode(string): The GEOID of the person's work CBG
- total_jobs(float): the total number of people moving for jobs between the h_geocode and w_geocode (its sum gives us the total number of people moving; can be ignored for simplicity purposes).
- Note in case of the safegraph data the corresponding column is frequency. It shows the cumulative movement between the two block groups.
- home_loc_lat(float): latitude of chosen home location
- home_loc_lon(float): longitude of chosen home location
- work_loc_lat(float): latitude of chosen work location
- work_loc_lon(float): longitude of chosen work location
- go_time/time_0(datetime.time): time the person leaves the home - in 24 hour format
- return_time/time_1(datetime.time): time the person leaves the workplace - in 24 hour format
- go_time_str/time_0_str(string): time the person leaves the home - in 24 hour format (as a string)
- return_time_str/time_1_str(string): time the person leaves the workplace - in 24 hour format (as a string)
- home_geom(Point): shapely point of home location
- work_geom(Point): shapely point of work location

For LODES, the times are written as time_0, time_1 ... because there may be additional time windows that we may want to add in the OD generation.

LODES OD dataset example:
![LODES example](plots/LODES_cols.png)

Safegraph OD dataset example:
![Safegraph example](plots/Sg_cols.png)

Here's a time distribution of the people moving from LODES data (Hamilton county, TN, USA). The example data has a time step of 1 min. The start time is chosen from 7am to 9am , and return times are chosen as 4pm to 6pm.
![lodes poeple movement going time](plots/ham_lodes_time_0.png)
![lodes poeple movement return time](plots/ham_lodes_time_1.png)

Time distribution of the people moving from Safegaprh data (Chattanooga, TN, USA). The times are chosen to be from 7am to 9pm, to model the flow of people across an entire day.

![Safegraph poeple movement going time](plots/ham_sg_go.png)
![Safegraph poeple movement return time](plots/ham_sg_return.png)
