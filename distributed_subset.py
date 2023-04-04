import pandas as pd
import geopandas as gpd
import os
from datetime import datetime, date, timedelta

chatta = gpd.read_file('data/chattanooga_city_lim.geojson').to_crs('epsg:4326')
ham = gpd.read_file('data/tl_2022_47_bg/tl_2022_47_bg.shp').to_crs('epsg:4326')
ham['GEOID'] = ham['GEOID'].astype(str)

chatta = ham.sjoin(chatta)

total_reqs =  200

def form_subset(df, freq, home, work):
    df_grouped = pd.DataFrame()
    if not df.empty:
        df[home] = df[home].astype(str)
        df[work] = df[work].astype(str)
        df_within_chatta = df.merge(chatta[['GEOID']], left_on=home, right_on='GEOID').drop(['GEOID'], axis=1)\
                            .merge(chatta[['GEOID']], left_on=work, right_on='GEOID').drop(['GEOID'], axis=1)\
                            # .sample(n=total_reqs, weights=freq, replace = False, random_state=42)
        
        cbg_count_df = len(df_within_chatta[home].unique())
        multiplier = 0

        while len(df_grouped) <= total_reqs :
            temp = df_within_chatta.groupby(home).nth([multiplier]).reset_index()
            df_grouped = pd.concat([df_grouped, temp], ignore_index=True)
            multiplier += 1

    return df_grouped[:total_reqs]
        

def input():
    base = date(year=2021, month=2, day=1)
    numdays = 7
    input_days = [base + timedelta(days=x) for x in range(numdays)]
    
    path = 'generated_OD/Hamilton_TN_2021-01-31_2021-02-24'
    os.makedirs(f'{path}/subsets/', exist_ok=True)

    for day in input_days:
        print(f'Sampling LODES for {day}')
        lodes = pd.read_csv(f'{path}/lodes_combs/lodes_{day}.csv')
        if day.weekday()>=5:
            lodes = pd.DataFrame()
        form_subset(lodes, 'total_jobs', home='h_geocode', work='w_geocode').to_csv(f'{path}/subsets/lodes_{day}.csv', index=False)
    
        print(f'Sampling SG for {day}')
        sg = pd.read_csv(f'{path}/safegraph_combs/sg_{day}.csv')    
        form_subset(sg, 'visits', home='home_cbg', work='poi_cbg').to_csv(f'{path}/subsets/sg_{day}.csv', index=False)


input()