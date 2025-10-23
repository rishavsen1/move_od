#!/usr/bin/env python
# coding: utf-8
import os
import math
import osmnx as ox
import geopandas as gpd
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm


def process_section(miny, maxy, minx, maxx, tags, logger):
    retries = 0
    while retries < 5:
        try:
            logger.info(f"Processing section: {miny}, {maxy}, {minx}, {maxx}")
            geoms = ox.features_from_bbox(miny, maxy, minx, maxx, tags).reset_index()
            geoms = geoms[["geometry", "building"]]
            logger.info(f"Successfully processed section: {miny}, {maxy}, {minx}, {maxx}")
            return geoms
        except Exception as e:
            logger.error(f"Exception in section {miny}, {maxy}, {minx}, {maxx}: {e}")
        retries += 1
    logger.error(f"Failed to process section after 5 retries: {miny}, {maxy}, {minx}, {maxx}")
    return None


class LocationsOSMSG:
    def __init__(self, county, area, county_geoid_df, sg_enabled, output_path, logger, od_option):
        self.COUNTY = county
        self.AREA = area
        self.county_geoid_df = county_geoid_df[["GEOID", "COUNTYFP", "geometry", "INTPTLAT", "INTPTLON"]]
        # self.sg = pd.read_csv(sg)
        self.sg_enabled = sg_enabled
        self.output_path = output_path
        self.logger = logger
        self.od_option = od_option
        self.logger.info("Initliazing locations_OSM_SG.py")

    def split_bbox(self, miny, maxy, minx, maxx, num_splits):
        width = maxx - minx
        # height = maxy - miny
        slice_width = width / num_splits
        # slice_height = height / num_splits
        splits = [
            (
                miny,
                maxy,
                minx + i * slice_width,
                minx + (i + 1) * slice_width,
            )
            for i in range(num_splits)
        ]
        return splits

    # def func(row):
    #     str(Point(gpd.points_from_xy(row.INTPTLAT, row.INTPTLON)[0]))

    def find_locations_OSM(self):
        self.logger.info("Running locations_OSM_SG.py func")

        # Filter county data based on the OD option
        if self.od_option == "Origin and Destination in same County":
            county_geoid_df = self.county_geoid_df[self.county_geoid_df.COUNTYFP == self.COUNTY]
        else:
            county_geoid_df = self.county_geoid_df

        # Transform CRS once
        county_geoid_df = county_geoid_df.to_crs("epsg:4326")
        minx, miny, maxx, maxy = county_geoid_df.geometry.total_bounds

        # Check if buildings file already exists
        buildings_file = f"{self.output_path}/county_all_buildings.geojson"
        if os.path.exists(buildings_file):
            buildings = gpd.read_file(buildings_file)
        else:
            num_workers = min(multiprocessing.cpu_count(), 12)
            splits = self.split_bbox(miny, maxy, minx, maxx, num_workers)
            func_args = [(s[0], s[1], s[2], s[3], {"building": True}, self.logger) for s in splits]

            results = []
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                future_to_split = {executor.submit(process_section, *args): args for args in func_args}

                # Use tqdm for progress tracking
                for future in tqdm(as_completed(future_to_split), total=len(func_args), desc="Processing sections"):
                    try:
                        result = future.result()
                        if result is not None:
                            results.append(result)
                    except Exception as e:
                        self.logger.error(f"Error processing section {future_to_split[future]}: {e}")

            # Combine results and save to file
            if results:
                buildings = pd.concat(results, ignore_index=True)
                buildings.to_file(buildings_file, driver="GeoJSON")
            else:
                self.logger.error("No results were obtained from multiprocessing.")
                return None

        # Determine UTM zone
        mean_lon = minx + (maxx - minx) / 2
        mean_lat = miny + (maxy - miny) / 2
        zone_number = math.floor((mean_lon + 180) / 6) + 1
        utm_epsg = f"326{zone_number}" if mean_lat >= 0 else f"327{zone_number}"

        residential_types = [
            "yes",
            "residential",
            "bungalow",
            "cabin",
            "dormitory",
            "hotel",
            "house",
            "semidetached_house",
            "barracks",
            "farm",
            "ger",
            "houseboat",
            "static_caravan",
            "terrace",
        ]
        res_build = buildings[buildings.building.isin(residential_types)]
        res_build = res_build.sjoin(county_geoid_df[["GEOID", "geometry", "INTPTLAT", "INTPTLON"]])

        # Convert polygons to centroids
        mask = res_build.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
        res_build = res_build.to_crs(f"epsg:{utm_epsg}")
        res_build.loc[mask, "geometry"] = res_build.loc[mask, "geometry"].centroid
        res_build = res_build.to_crs("epsg:4326")

        # Save residential buildings
        res_build = res_build.drop(columns=["index_right"], errors="ignore")
        res_build.to_file(f"{self.output_path}/county_residential_buildings.geojson", driver="GeoJSON")

        points = gpd.points_from_xy(res_build["INTPTLON"], res_build["INTPTLAT"])

        res_build["geo_centers"] = res_build.geometry.centroid
        res_build["location"] = res_build.geometry.centroid.apply(lambda p: [p.y, p.x])

        success = False
        if isinstance(res_build, gpd.GeoDataFrame) and res_build.shape[0] > 0:
            success = True

        # work tags

        # Filter commercial and civic buildings
        commercial_types = ["commercial", "industrial", "kiosk", "office", "retail", "supermarket", "warehouse"]
        civic_types = [
            "bakehouse",
            "civic",
            "college",
            "fire_station",
            "government",
            "hospital",
            "kindergarten",
            "public",
            "school",
            "train_station",
            "transportation",
            "university",
        ]

        com_build = buildings[buildings.building.isin(commercial_types)]
        com_build = com_build.sjoin(county_geoid_df[["GEOID", "geometry", "INTPTLAT", "INTPTLON"]])

        civ_build = buildings[buildings.building.isin(civic_types)]
        civ_build = civ_build.sjoin(county_geoid_df[["GEOID", "geometry", "INTPTLAT", "INTPTLON"]])

        # Process commercial and civic buildings
        for build_df, name in [(com_build, "commercial"), (civ_build, "civic")]:
            mask = build_df.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
            build_df = build_df.to_crs(f"epsg:{utm_epsg}")
            build_df.loc[mask, "geometry"] = build_df.loc[mask, "geometry"].centroid
            build_df = build_df.to_crs("epsg:4326")
            build_df["location"] = list(zip(build_df.geometry.y, build_df.geometry.x))

        # Combine work locations
        combined_work_locations = pd.concat(
            [com_build[["GEOID", "geometry"]], civ_build[["GEOID", "geometry"]]], ignore_index=True
        )

        # Save work locations

        self.logger.info("Finished processing locations_OSM_SG.py")

        if self.sg_enabled:
            self.find_locations_SG(self, combined_work_locations)

        else:
            combined_work_locations.GEOID = combined_work_locations.GEOID.astype(str).apply(lambda x: x.split(".")[0])
            combined_work_locations.to_file(f"{self.output_path}/county_work_locations.geojson", driver="GeoJSON")

        combined_work_locations["geo_centers"] = combined_work_locations.geometry.centroid
        combined_work_locations["location"] = combined_work_locations.geometry.centroid.apply(lambda p: [p.y, p.x])

        if isinstance(combined_work_locations, gpd.GeoDataFrame) and not combined_work_locations.empty:
            success = success and True

        return res_build, combined_work_locations, success

    # # ## adding safegraph poi locations
    # def find_locations_SG(self, combined_locations):
    #     # sg = gpd.read_file('path to safegraph file') # path removed due to privacy concerns
    #     self.sg = pd.read_csv(f"{self.output_path}/sg_poi_geoids.csv")
    #     self.sg.longitude = self.sg.longitude.astype(float)
    #     self.sg.latitude = self.sg.latitude.astype(float)
    #     geom = [Point(xy) for xy in zip(self.sg.longitude, self.sg.latitude)]
    #     self.sg = gpd.GeoDataFrame(self.sg, geometry=geom, crs="epsg:4326")

    #     # adding in safegraph POI locations to OSM work locations

    #     combined_locations_sg = pd.concat(
    #         [
    #             self.sg[["poi_geoid", "geometry"]].rename({"poi_geoid": "GEOID"}, axis=1),
    #             combined_locations[["GEOID", "geometry"]],
    #         ]
    #     )
    #     # combined_locations = com_build[['GEOID', 'geometry']].append(civ_build[['GEOID', 'geometry']])
    #     combined_locations_sg["GEOID"] = combined_locations["GEOID"].astype(str).str.split(".").str[0]

    #     # saving work buildings to file
    #     combined_locations_sg.to_csv(f"{self.output_path}/county_work_locations.csv", index=False)
    #     self.logger.info("Finished locations_OSM_Sg")
    #     return


# def preproc(x, attr:str, attr_name:str, ret_attr:str) -> any:
#     if getattr(x, attr) == attr_name:
#         return getattr(x, ret_attr)
#     return x

# Mixin
