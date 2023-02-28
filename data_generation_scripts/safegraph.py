#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import datetime as dt
import ast
import geopandas as gpd
import numpy as np
import math
pd.options.display.float_format = "{:.2f}".format


# COUNTY = '037'
# CITY = 'Nashville'
# county_cbg  = gpd.read_file('../data/Tennessee Census Block Groups/tl_2020_47_bg.shp')
# safe_df =  pd.read_parquet(f'../data/safegraph.parquet/year=2021/region=TN/city={CITY}/', engine='pyarrow')

class Safegraph:


    def __init__(self, county, city, county_cbg, safe_df, data_path):
        print('Initliazing safegraph.py')
        self.COUNTY = county
        self.county_cbg = gpd.read_file(county_cbg)
        self.CITY = city
        self.safe_df = pd.read_parquet(safe_df, engine='pyarrow')
        self.data_path = data_path

    def func(x):
        return pd.DataFrame(x.items(), columns = ['home_cbg','frequency'])


    def filter_SG(self):
        print('Running safegraph.py')
        self.county_cbg = self.county_cbg[self.county_cbg.COUNTYFP == self.COUNTY]
        self.county_cbg.GEOID = self.county_cbg.GEOID.astype(str)

        county_cbgs = self.county_cbg.GEOID.unique().tolist()


        pd.set_option('display.max_columns', None)
        self.safe_df.head()

        # reference : # https://docs.safegraph.com/docs/places

        # l= ['category_tags', 'naics_code', 'date_begin','distance_from_home', 'includes_parking_lot','location_name', 'poi_cbg','open_hours','raw_visit_counts', 'related_same_day_brand','related_same_week_brand','sub_category', 'top_category','visits_by_day','visits_friday','visits_saturday', 'visits_sunday', 'visits_monday', 'visits_tuesday', 'visits_wednesday', 'visits_thursday', 'polygon_wkt', 'latitude', 'longitude']
        l= ['category_tags', 'naics_code', 'date_begin','distance_from_home', 'includes_parking_lot','location_name', 'median_dwell', 'poi_cbg','open_hours','raw_visit_counts', 'sub_category', 'top_category','visits_by_day','visits_friday','visits_saturday', 'visits_sunday', 'visits_monday', 'visits_tuesday', 'visits_wednesday', 'visits_thursday', 'latitude', 'longitude']

        df = self.safe_df.drop(self.safe_df.columns.difference(['visitor_home_aggregation', 'visitor_home_cbgs']+ l), 1)
        df['poi_cbg'] = df['poi_cbg'].apply(lambda x: str(x).split('.')[0])
        df = df[df['poi_cbg'].isin(county_cbgs)].reset_index(drop = True)
        df['visitor_home_cbgs'] = pd.DataFrame(df['visitor_home_cbgs'].apply(ast.literal_eval))
        df['visitor_info'] = df.apply(lambda x: Safegraph.  func(x['visitor_home_cbgs']), axis =1)
        df = df.sort_values(by = ['date_begin']).reset_index(drop = True)

        chunks = np.array_split(df, 5)

        t = [chunks[i].groupby('date_begin') for i in range(5)]

        # x = t[0].get_group(dt.date(2021, 1, 4))
        # x
        # x.columns

        # for keys in t[0].groups:
        #     print (keys)

        res = pd.DataFrame()

        for key in t[0].groups:
            x = t[0].get_group(key)
            x = x.reset_index(drop = True)
            y = pd.DataFrame()
            for i in range(len(x[:10])):
                temp = x['visitor_info'][i]
                alpha = pd.DataFrame(data=df.loc[i,l]).T
                temp = pd.concat([temp,alpha], axis = 1) 
                temp['date_begin'] = x['date_begin'][i]
                temp['poi_cbg'] = x['poi_cbg'][i]
                temp['category_tags'] = x['category_tags'][i]
                temp['naics_code'] = x['naics_code'][i]
                temp['distance_from_home'] = x['distance_from_home'][i]
                temp['includes_parking_lot'] = x['includes_parking_lot'][i]
                temp['location_name'] = x['location_name'][i]
                temp['median_dwell'] = x['median_dwell'][i]
                temp['open_hours'] = x['open_hours'][i]
                temp['raw_visit_counts'] = x['raw_visit_counts'][i]
                # temp['related_same_day_brand'] = x['related_same_day_brand'][i]
                # temp['related_same_week_brand'] = x['related_same_week_brand'][i]
                temp['sub_category'] = x['sub_category'][i]
                temp['top_category'] = x['top_category'][i]
                temp['visits_by_day'] = x['visits_by_day'][i]
                temp['visits_monday'] = x['visits_monday'][i]
                temp['visits_tuesday'] = x['visits_tuesday'][i]
                temp['visits_wednesday'] = x['visits_wednesday'][i]
                temp['visits_thursday'] = x['visits_thursday'][i]
                temp['visits_friday'] = x['visits_friday'][i]
                temp['visits_saturday'] = x['visits_saturday'][i]
                temp['visits_sunday'] = x['visits_sunday'][i]
                # temp['polygon_wkt'] = x['polygon_wkt'][i]
                temp['latitude'] = x['latitude'][i]
                temp['longitude'] = x['longitude'][i]
                # print(temp)
                y = y.append(temp)
            y['home_cbg'] = pd.to_numeric(y['home_cbg'], errors='coerce')
            y = y.dropna(subset=['home_cbg'])
            y['home_cbg'] =y['home_cbg'].apply(lambda x: str(x).split('.')[0])
            y = y[y['home_cbg'].isin(county_cbgs)].reset_index(drop = True)

            # y = y.groupby(['date_begin', 'category_tags', 'naics_code', 'distance_from_home', 'includes_parking_lot', 'location_name', 'open_hours', 'raw_visit_counts', 'related_same_day_brand', 'related_same_week_brand', 'sub_category', 'top_category', 'visits_by_day', 'visits_friday','visits_monday', 'visits_saturday', 'visits_sunday', 'visits_thursday', 'visits_tuesday', 'visits_wednesday', 'polygon_wkt', 'home_cbg', 'poi_cbg', 'latitude', 'longitude']).agg({'frequency': sum}).reset_index()
            y = y.groupby(['date_begin', 'category_tags', 'naics_code', 'distance_from_home', 'includes_parking_lot', 'location_name', 'median_dwell', 'open_hours', 'raw_visit_counts', 'sub_category', 'top_category', 'visits_by_day', 'visits_friday','visits_monday', 'visits_saturday', 'visits_sunday', 'visits_thursday', 'visits_tuesday', 'visits_wednesday', 'home_cbg', 'poi_cbg', 'latitude', 'longitude']).agg({'frequency': sum}).reset_index()
            res = res.append(y)
            res = res.reset_index(drop = True)

        t = res.groupby('home_cbg').first().reset_index()
        t.home_cbg = t.home_cbg
        # .apply(lambda x: str(math.trunc(x)))
        t.merge(self.county_cbg, left_on='home_cbg', right_on='GEOID').to_csv(f'{self.data_path}/sg_home_cbgs.csv', index=False)

        t = res.groupby('poi_cbg').first().reset_index()
        t.poi_cbg = t.poi_cbg
        # .apply(lambda x: str(math.trunc(x)))
        t.merge(self.county_cbg, left_on='poi_cbg', right_on='GEOID').to_csv(f'{self.data_path}/sg_poi_cbgs.csv', index=False)


        from datetime import datetime
        # start_date = datetime.strptime('Jan 11 2021  11:00PM', '%b %d %Y %I:%M%p').date()
        # end_date = datetime.strptime('Mar 16 2021  12:00AM', '%b %d %Y %I:%M%p').date()
        # # end_date
        exact_date = datetime.strptime('Jan 11 2021  12:00AM', '%b %d %Y %I:%M%p').date()
        res2 = res
        res2.home_cbg = res2.home_cbg.astype(str)
        self.county_cbg.GEOID = self.county_cbg.GEOID.astype(str)
        # res2.home_cbg = res2.home_cbg.astype('double') 
        # res2.home_cbg = res2.home_cbg.astype('int64') 
        # res2.home_cbg = res2.home_cbg.astype('str') 

        # res2[(res2.date_begin>=start_date) & (res2.date_begin<=end_date)].reset_index(drop=True)
        # res2[res2.date_begin==exact_date].reset_index(drop=True)
        res3 = res2[res2.date_begin==exact_date].merge(self.county_cbg[['TRACTCE', 'GEOID', 'NAMELSAD']], left_on='home_cbg', right_on='GEOID').drop('GEOID', axis=1)


        res2.to_csv(f'{self.data_path}/county_sg_first_chunk_of_five.csv')
        res3.to_csv(f'{self.data_path}/county_sg_week2_jan21_reduced_cols.csv')


        # for key in t[1].groups:
        #     x = t[1].get_group(key)
        #     x = x.reset_index(drop = True)
        #     y = pd.DataFrame()
        #     for i in range(len(x)):
        #         temp = x['visitor_info'][i]
        #         temp['date_begin'] = x['date_begin'][i]
        #         temp['poi_cbg'] = x['poi_cbg'][i]
        #         temp['category_tags'] = x['category_tags'][i]
        #         temp['distance_from_home'] = x['distance_from_home'][i]
        #         temp['includes_parking_lot'] = x['includes_parking_lot'][i]
        #         temp['location_name'] = x['location_name'][i]
        #         temp['open_hours'] = x['open_hours'][i]
        #         temp['raw_visit_counts'] = x['raw_visit_counts'][i]
        #         temp['related_same_day_brand'] = x['related_same_day_brand'][i]
        #         temp['related_same_week_brand'] = x['related_same_week_brand'][i]
        #         temp['sub_category'] = x['sub_category'][i]
        #         temp['top_category'] = x['top_category'][i]
        #         temp['visits_by_day'] = x['visits_by_day'][i]
        #         temp['visits_monday'] = x['visits_monday'][i]
        #         temp['visits_tuesday'] = x['visits_tuesday'][i]
        #         temp['visits_wednesday'] = x['visits_wednesday'][i]
        #         temp['visits_thursday'] = x['visits_thursday'][i]
        #         temp['visits_friday'] = x['visits_friday'][i]
        #         temp['visits_saturday'] = x['visits_saturday'][i]
        #         temp['visits_sunday'] = x['visits_sunday'][i]
        #         temp['polygon_wkt'] = x['polygon_wkt'][i]
        #         y = y.append(temp)
        #     y['home_cbg'] = pd.to_numeric(y['home_cbg'], errors='coerce')
        #     y = y.dropna(subset=['home_cbg'])
        #     y['home_cbg'] =y['home_cbg'].astype(float)
        #     y = y[y['home_cbg'].isin(county_cbgs)].reset_index(drop = True)
        #     y = y.groupby(['date_begin', 'category_tags', 'home_cbg', 'poi_cbg']).agg({'frequency': sum}).reset_index()
        #     res = res.append(y)
        #     res = res.reset_index(drop = True)


        # In[66]:


        # for key in t[2].groups:
        #     x = t[2].get_group(key)
        #     x = x.reset_index(drop = True)
        #     y = pd.DataFrame()
        #     for i in range(len(x)):
        #         temp = x['visitor_info'][i]
        #         temp['date_begin'] = x['date_begin'][i]
        #         temp['poi_cbg'] = x['poi_cbg'][i]
        #         # temp['brands'] = x['brands'][i]
        #         temp['category_tags'] = x['category_tags'][i]
        #         temp['distance_from_home'] = x['distance_from_home'][i]
        #         temp['includes_parking_lot'] = x['includes_parking_lot'][i]
        #         temp['location_name'] = x['location_name'][i]
        #         temp['open_hours'] = x['open_hours'][i]
        #         temp['raw_visit_counts'] = x['raw_visit_counts'][i]
        #         temp['related_same_day_brand'] = x['related_same_day_brand'][i]
        #         temp['related_same_week_brand'] = x['related_same_week_brand'][i]
        #         temp['sub_category'] = x['sub_category'][i]
        #         temp['top_category'] = x['top_category'][i]
        #         temp['visits_by_day'] = x['visits_by_day'][i]
        #         temp['visits_monday'] = x['visits_monday'][i]
        #         temp['visits_tuesday'] = x['visits_tuesday'][i]
        #         temp['visits_wednesday'] = x['visits_wednesday'][i]
        #         temp['visits_thursday'] = x['visits_thursday'][i]
        #         temp['visits_friday'] = x['visits_friday'][i]
        #         temp['visits_saturday'] = x['visits_saturday'][i]
        #         temp['visits_sunday'] = x['visits_sunday'][i]
        #         temp['polygon_wkt'] = x['polygon_wkt'][i]
        #         y = y.append(temp)
        #     y['home_cbg'] = pd.to_numeric(y['home_cbg'], errors='coerce')
        #     y = y.dropna(subset=['home_cbg'])
        #     y['home_cbg'] =y['home_cbg'].astype(float)
        #     y = y[y['home_cbg'].isin(county_cbgs)].reset_index(drop = True)
        #     y = y.groupby(['date_begin', 'category_tags', 'distance_from_home', 'includes_parking_lot', 'location_name', 'open_hours', 'raw_visit_counts', 'related_same_day_brand', 'related_same_week_brand', 'sub_category', 'top_category', 'visits_by_day', 'visits_friday','visits_monday', 'visits_saturday', 'visits_sunday', 'visits_thursday', 'visits_tuesday', 'visits_wednesday', 'polygon_wkt', 'home_cbg', 'poi_cbg']).agg({'frequency': sum}).reset_index()
        #     res = res.append(y)
        #     res = res.reset_index(drop = True)


        # In[68]:


        # for key in t[3].groups:
        #     x = t[3].get_group(key)
        #     x = x.reset_index(drop = True)
        #     y = pd.DataFrame()
        #     for i in range(len(x)):
        #         temp = x['visitor_info'][i]
        #         temp['date_begin'] = x['date_begin'][i]
        #         temp['poi_cbg'] = x['poi_cbg'][i]
        #         temp['category_tags'] = x['category_tags'][i]
        #         temp['distance_from_home'] = x['distance_from_home'][i]
        #         temp['includes_parking_lot'] = x['includes_parking_lot'][i]
        #         temp['location_name'] = x['location_name'][i]
        #         temp['open_hours'] = x['open_hours'][i]
        #         temp['raw_visit_counts'] = x['raw_visit_counts'][i]
        #         temp['related_same_day_brand'] = x['related_same_day_brand'][i]
        #         temp['related_same_week_brand'] = x['related_same_week_brand'][i]
        #         temp['sub_category'] = x['sub_category'][i]
        #         temp['top_category'] = x['top_category'][i]
        #         temp['visits_by_day'] = x['visits_by_day'][i]
        #         temp['visits_monday'] = x['visits_monday'][i]
        #         temp['visits_tuesday'] = x['visits_tuesday'][i]
        #         temp['visits_wednesday'] = x['visits_wednesday'][i]
        #         temp['visits_thursday'] = x['visits_thursday'][i]
        #         temp['visits_friday'] = x['visits_friday'][i]
        #         temp['visits_saturday'] = x['visits_saturday'][i]
        #         temp['visits_sunday'] = x['visits_sunday'][i]
        #         temp['polygon_wkt'] = x['polygon_wkt'][i]
        #         y = y.append(temp)
        #     y['home_cbg'] = pd.to_numeric(y['home_cbg'], errors='coerce')
        #     y = y.dropna(subset=['home_cbg'])
        #     y['home_cbg'] =y['home_cbg'].astype(float)
        #     y = y[y['home_cbg'].isin(county_cbgs)].reset_index(drop = True)
        #     y = y.groupby(['date_begin', 'category_tags', 'distance_from_home', 'includes_parking_lot', 'location_name', 'open_hours', 'raw_visit_counts', 'related_same_day_brand', 'related_same_week_brand', 'sub_category', 'top_category', 'visits_by_day', 'visits_friday','visits_monday', 'visits_saturday', 'visits_sunday', 'visits_thursday', 'visits_tuesday', 'visits_wednesday', 'polygon_wkt', 'home_cbg', 'poi_cbg']).agg({'frequency': sum}).reset_index()
        #     res = res.append(y)
        #     res = res.reset_index(drop = True)


        # In[69]:


        # for key in t[4].groups:
        #     x = t[4].get_group(key)
        #     x = x.reset_index(drop = True)
        #     y = pd.DataFrame()
        #     for i in range(len(x)):
        #         temp = x['visitor_info'][i]
        #         temp['date_begin'] = x['date_begin'][i]
        #         temp['poi_cbg'] = x['poi_cbg'][i]
        #         # temp['brands'] = x['brands'][i]
        #         temp['category_tags'] = x['category_tags'][i]
        #         temp['distance_from_home'] = x['distance_from_home'][i]
        #         temp['includes_parking_lot'] = x['includes_parking_lot'][i]
        #         temp['location_name'] = x['location_name'][i]
        #         temp['open_hours'] = x['open_hours'][i]
        #         temp['raw_visit_counts'] = x['raw_visit_counts'][i]
        #         temp['related_same_day_brand'] = x['related_same_day_brand'][i]
        #         temp['related_same_week_brand'] = x['related_same_week_brand'][i]
        #         temp['sub_category'] = x['sub_category'][i]
        #         temp['top_category'] = x['top_category'][i]
        #         temp['visits_by_day'] = x['visits_by_day'][i]
        #         temp['visits_monday'] = x['visits_monday'][i]
        #         temp['visits_tuesday'] = x['visits_tuesday'][i]
        #         temp['visits_wednesday'] = x['visits_wednesday'][i]
        #         temp['visits_thursday'] = x['visits_thursday'][i]
        #         temp['visits_friday'] = x['visits_friday'][i]
        #         temp['visits_saturday'] = x['visits_saturday'][i]
        #         temp['visits_sunday'] = x['visits_sunday'][i]
        #         temp['polygon_wkt'] = x['polygon_wkt'][i]
        #         y = y.append(temp)
        #     y['home_cbg'] = pd.to_numeric(y['home_cbg'], errors='coerce')
        #     y = y.dropna(subset=['home_cbg'])
        #     y['home_cbg'] =y['home_cbg'].astype(float)
        #     y = y[y['home_cbg'].isin(county_cbgs)].reset_index(drop = True)
        #     y = y.groupby(['date_begin', 'category_tags', 'distance_from_home', 'includes_parking_lot', 'location_name', 'open_hours', 'raw_visit_counts', 'related_same_day_brand', 'related_same_week_brand', 'sub_category', 'top_category', 'visits_by_day', 'visits_friday','visits_monday', 'visits_saturday', 'visits_sunday', 'visits_thursday', 'visits_tuesday', 'visits_wednesday', 'polygon_wkt', 'home_cbg', 'poi_cbg']).agg({'frequency': sum}).reset_index()
        #     res = res.append(y)
        #     res = res.reset_index(drop = True)


        # In[71]:


        # res.to_csv("county_allweeks2021_new.csv")


        # In[105]:


        # res2 = res
        # res2.home_cbg = res2.home_cbg.astype('double') 
        # res2.home_cbg = res2.home_cbg.astype('int64') 
        # res2.home_cbg = res2.home_cbg.astype('str') 


        # In[130]:


        # res3 = res2.merge(county_cbg[['TRACTCE', 'GEOID', 'NAMELSAD', 'geometry']], left_on='home_cbg', right_on='GEOID').drop('GEOID', axis=1)


        # In[132]:


        # res3.to_csv('county_allweeks2021_cbg_names.csv', index=False)


        # In[136]:


        # res3[res3.date_begin<'2021-02-01']


        # In[127]:


        # pd.set_option('display.max_columns', None)
        # location_df = pd.DataFrame(res3.location_name.unique(), columns=['locations']).sort_values('locations')
        # # .to_csv('locations.csv', index=False)
        # location_df.merge(res3[['location_name', 'category_tags']], left_on='locations', right_on='location_name', how='left')


        # In[33]:


        # res['category_tags'].value_counts().nlargest(20)


        # In[45]:


        # sample = self.safe_df.sample(n = 50)
        # sample.to_csv("sample.csv")

