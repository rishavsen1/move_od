#!/usr/bin/env python
# coding: utf-8
import os
import math
import osmnx as ox
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, Point
import geopandas as gpd
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed


# COUNTY = '037'
# AREA = 'Davidson'

# print(COUNTY)

# input Hamilton county geo file
# county_cbg = gpd.read_file('../data/Tennessee Census Block Groups/tl_2020_47_bg.shp')

# sg_enabled = False
# sg = pd.read_csv('../data/sg_poi_cbgs.csv') # path removed due to privacy concerns


def process_section(miny, maxy, minx, maxx, tags):
    retries = 0
    while retries < 5:
        try:
            geoms = ox.features_from_bbox(miny, maxy, minx, maxx, tags).reset_index()
            # print(geoms.columns)
            geoms = geoms[["geometry", "building"]]
            print(f"Got geometries for {miny, maxy, minx, maxx, tags}")
            return geoms
        except Exception as e:
            print("Exception:", e)

        retries += 1


class LocationsOSMSG:
    def __init__(self, county, area, county_cbg, sg_enabled, output_path, logger, od_option):
        self.COUNTY = county
        self.AREA = area
        self.county_cbg = gpd.read_file(county_cbg)[["GEOID", "COUNTYFP", "geometry", "INTPTLAT", "INTPTLON"]]
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
        if self.od_option == "Origin and Destination in same County":
            self.county_cbg = self.county_cbg[self.county_cbg.COUNTYFP == self.COUNTY]
        elif (self.od_option == "Only Origin in County") or (self.od_option == "Only Destination in County"):
            self.county_cbg = self.county_cbg

        # self.county_cbg = self.county_cbg[self.county_cbg.COUNTYFP == self.COUNTY]
        self.county_cbg = self.county_cbg.to_crs("epsg:4326")

        minx, miny, maxx, maxy = self.county_cbg.geometry.total_bounds

        if os.path.exists(f"{self.output_path}/county_all_buildings.geojson"):
            buildings = gpd.read_file(f"{self.output_path}/county_all_buildings.geojson")
        else:
            num_workers = multiprocessing.cpu_count()
            splits = self.split_bbox(miny, maxy, minx, maxx, num_workers)
            func_args = [(s[0], s[1], s[2], s[3], {"building": True}) for s in splits]

            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(process_section, *args) for args in func_args]

                buildings = [future.result() for future in futures]

                buildings = pd.concat(buildings)
                buildings.to_file(f"{self.output_path}/county_all_buildings.geojson", driver="GeoJSON")
                # finding all buildings
                # tags = {"building": True}
                # buildings = ox.geometries_from_bbox(miny, maxy, maxx, minx, tags)
                # buildings = ox.geometries_from_bbox(34.854382885097905, 35.935532323321, -84.19759521484375, -85.553161621093756, tags)

        mean_lon = minx + (maxx - minx) / 2
        mean_lat = miny + (maxy - miny) / 2
        zone_number = math.floor((mean_lon + 180) / 6) + 1
        if mean_lat >= 0:
            utm_epsg = f"326{str(zone_number)}"
        else:
            utm_epsg = f"327{str(zone_number)}"

        # aggregating all residential tags
        res_build = (
            buildings[
                (buildings.building == "yes")
                | (buildings.building == "residential")
                | (buildings.building == "bungalow")
                | (buildings.building == "cabin")
                | (buildings.building == "dormitory")
                | (buildings.building == "hotel")
                | (buildings.building == "house")
                | (buildings.building == "semidetached_house")
                | (buildings.building == "barracks")
                | (buildings.building == "farm")
                | (buildings.building == "ger")
                | (buildings.building == "houseboat")
                | (buildings.building == "static_caravan")
                | (buildings.building == "terrace")
            ][["geometry", "building"]].sjoin(self.county_cbg[["GEOID", "geometry", "INTPTLAT", "INTPTLON"]])
            # .reset_index()[["geometry", "building"]]
        )
        mask = res_build.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
        res_build = res_build.to_crs(f"epsg:{utm_epsg}")
        res_build.loc[mask, "geometry"] = res_build.loc[mask, "geometry"].centroid
        res_build = res_build.to_crs("epsg:4326")

        # converting the default internal point of each cbg to a shapely Point
        points = gpd.points_from_xy(res_build["INTPTLON"], res_build["INTPTLAT"])
        res_build["intpt"] = points.astype(str)
        res_build["location"] = list(zip(res_build.geometry.y, res_build.geometry.x))
        res_build = res_build.drop({"index_right"}, axis=1)

        # saving residential buildings
        # TODO: Error Handling
        try:
            res_build = res_build[["geometry", "GEOID", "intpt", "location"]]
            res_build.to_csv(f"{self.output_path}/county_residential_buildings.csv", index=False)
        except FileNotFoundError:
            self.logger.info(f"File not found: {self.output_path}/county_residential_buildings.csv")
        except:
            self.logger.info("General exception")

        # work tags

        com_build = (
            buildings[
                (buildings.building == "commercial")
                | (buildings.building == "industrial")
                | (buildings.building == "kiosk")
                | (buildings.building == "office")
                | (buildings.building == "retail")
                | (buildings.building == "supermarket")
                | (buildings.building == "warehouse")
            ][["geometry", "building"]].sjoin(self.county_cbg[["GEOID", "geometry", "INTPTLAT", "INTPTLON"]])
            # .reset_index()[["geometry", "building"]]
        )
        civ_build = (
            buildings[
                (buildings.building == "bakehouse")
                | (buildings.building == "civic")
                | (buildings.building == "college")
                | (buildings.building == "fire_station")
                | (buildings.building == "government")
                | (buildings.building == "hospital")
                | (buildings.building == "kindergarten")
                | (buildings.building == "public")
                | (buildings.building == "school")
                | (buildings.building == "train_station")
                | (buildings.building == "transportation")
                | (buildings.building == "university")
            ][["geometry", "building"]].sjoin(self.county_cbg[["GEOID", "geometry", "INTPTLAT", "INTPTLON"]])
            # .reset_index()[["geometry", "building"]]
        )

        mask = com_build.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
        com_build = com_build.to_crs(f"epsg:{utm_epsg}")
        com_build.loc[mask, "geometry"] = com_build.loc[mask, "geometry"].centroid
        com_build = com_build.to_crs("epsg:4326")

        mask = civ_build.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
        civ_build = civ_build.to_crs(f"epsg:{utm_epsg}")
        civ_build.loc[mask, "geometry"] = civ_build.loc[mask, "geometry"].centroid
        civ_build = civ_build.to_crs("epsg:4326")

        # res_build["intpt"] = locations_OSM_SG.apply(lambda row: self.func(row))

        civ_build["location"] = list(zip(civ_build.geometry.y, civ_build.geometry.x))
        com_build["location"] = list(zip(com_build.geometry.y, com_build.geometry.x))

        points = gpd.points_from_xy(civ_build["INTPTLON"], civ_build["INTPTLAT"])
        civ_build["intpt"] = points.astype(str)
        points = gpd.points_from_xy(com_build["INTPTLON"], com_build["INTPTLAT"])
        com_build["intpt"] = points.astype(str)

        combined_locations = pd.concat(
            [
                com_build[["GEOID", "geometry", "intpt"]],
                civ_build[["GEOID", "geometry", "intpt"]],
            ]
        )

        if self.sg_enabled:
            self.find_locations_SG(self, combined_locations)

        else:
            combined_locations.GEOID = combined_locations.GEOID.astype(str).apply(lambda x: x.split(".")[0])
            
            combined_locations.to_csv(f"{self.output_path}/county_work_locations.csv", index=False)

    # ## adding safegraph poi locations
    def find_locations_SG(self, combined_locations):
        # sg = gpd.read_file('path to safegraph file') # path removed due to privacy concerns
        self.sg = pd.read_csv(f"{self.output_path}/sg_poi_cbgs.csv")
        self.sg.longitude = self.sg.longitude.astype(float)
        self.sg.latitude = self.sg.latitude.astype(float)
        geom = [Point(xy) for xy in zip(self.sg.longitude, self.sg.latitude)]
        self.sg = gpd.GeoDataFrame(self.sg, geometry=geom, crs="epsg:4326")

        # adding in safegraph POI locations to OSM work locations

        combined_locations_sg = pd.concat(
            [
                self.sg[["poi_cbg", "geometry"]].rename({"poi_cbg": "GEOID"}, axis=1),
                combined_locations[["GEOID", "geometry"]],
            ]
        )
        # combined_locations = com_build[['GEOID', 'geometry']].append(civ_build[['GEOID', 'geometry']])
        combined_locations_sg["GEOID"] = combined_locations["GEOID"].astype(str).str.split(".").str[0]

        # saving work buildings to file
        combined_locations_sg.to_csv(f"{self.output_path}/county_work_locations.csv", index=False)
        self.logger.info("Finished locations_OSM_Sg")
        return


# def preproc(x, attr:str, attr_name:str, ret_attr:str) -> any:
#     if getattr(x, attr) == attr_name:
#         return getattr(x, ret_attr)
#     return x

# Mixin
