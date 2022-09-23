import os 
import sys
import pandas as pd
import datetime as dt
import numpy as np
import requests
import geopandas
from geopandas import GeoDataFrame
from shapely import wkt


def get_file_path(current_dir, file_type, file_name):

    return os.path.abspath(os.path.join(current_dir, os.pardir, file_type, file_name))




def extract_csv_to_pandas(file_path):

    
    return pd.read_csv(file_path, index_col=False)

def load_df_to_csv(output_df, output_file):
    '''
    This function takes an output dataframe and an output file, and it
    writes the dataframe to that output file
    '''
    #write df to csv file with quotes around each field
    output_df.to_csv(output_file, index=False,header=True)

def transform_crop_data(data):
    '''
    This function takes a dataframe the crop csv dataset, filters to only year 2021
    and returns a dataframe with only 'field_id', 'field_geometry', 'crop_type'
    '''
    return data[data["year"] == 2021][['field_id', 'field_geometry', 'crop_type']]


def transform_spectral_data(data):
    '''
    This function takes a dataframe the crop csv dataset, filters to only year 2021
    and returns a dataframe with only 'field_id', 'field_geometry', 'crop_type'
    '''
    #filters to only 2021
    data["date"] = pd.to_datetime(data["date"])
    data = data[data["date"].dt.year == 2021]
    
    #calculated the ndvi
    data["ndvi"] = (data["nir"] - data["red"]) / (data["nir"] + data["red"])

    #this function recreates a SQL window function where the max_ndvi per tile for the year 
    #2021 can be filtered to by data["ndvi_row_num"] == 1
    data["ndvi_row_num"] = data.sort_values(['ndvi'], \
             ascending=[False])\
             .groupby(["tile_id", "tile_geometry"])\
             .cumcount() + 1
    #filter to only top POS
    data = data[data["ndvi_row_num"] == 1]
    data = data.rename(columns={"ndvi": "pos", "date": "pos_date"})

    return data[["tile_id", "tile_geometry", "pos", "pos_date"]]



def transform_soil_data(data):
    '''
    This function takes a soil dataframe and does the following:
    1. removes duplicates
    2. computes the horizontal layer weights
    3. computes the weighted average of horizontal layers for each component
    4. computes the weighted average of components for each map unit
    5. returns the weighted averages from 5 along with mukey, mukey_geometry
    '''
    def w_avg(df, weights):

        w_avg_dict = {}
        w_avg_dict["om"] = (df["om"] * df[weights]).sum() / df[weights].sum()
        w_avg_dict["cec"] = (df["cec"] * df[weights]).sum() / df[weights].sum()
        w_avg_dict["ph"] = (df["ph"] * df[weights]).sum() / df[weights].sum()


        return pd.Series(w_avg_dict, index=["om", "cec", "ph"])    


    data["hz_weights"] = abs(data["hzdept"] - data["hzdepb"]) / data["hzdepb"]
    #get weighted average for each component
    data = data.groupby(["mukey","mukey_geometry","cokey","comppct"])\
        .apply(w_avg, "hz_weights").reset_index()
    #get weighted average for each map unit
    return data.groupby(["mukey","mukey_geometry"])\
       .apply(w_avg, "comppct").reset_index()


def transform_weather_csv(data):
    data = data[data["year"] == 2021]
    def get_calc(df):

        calc_dict = {}
        calc_dict["precip"] = df["precip"].sum()
        calc_dict["min_temp"] = df["temp"].min()
        calc_dict["max_temp"] = df["temp"].max()
        calc_dict["mean_temp"] = df["temp"].mean()


        return pd.Series(calc_dict, index=["precip", "min_temp", "max_temp", "mean_temp"]) 

    return data.groupby(["fips_code"]).apply(get_calc).reset_index()
    
def get_state_county(lat, lon):
    url = 'https://geo.fcc.gov/api/census/block/find?latitude=%s&longitude=%s&format=json' % (lat, lon)
    response = requests.get(url)
    data = response.json()
    
    return {"state": data['State']['FIPS'], "county": data['County']['FIPS'][2:]}


def transform_crop_data_with_county_and_state(data):
 
    gdf = geopandas.GeoDataFrame(data)
    gdf['field_geometry'] = gdf['field_geometry'].apply(wkt.loads)
    gdf.set_geometry("field_geometry", inplace=True)
    gdf["centroid"] = gdf["field_geometry"].centroid
    
    #get the state and county codes
    for index, row in gdf.iterrows():
     
        state_county_dict = get_state_county(str(row["centroid"].y), str(row["centroid"].x))
        gdf.loc[index, "county_code"] = state_county_dict["county"]
        gdf.loc[index, "state_code"] = state_county_dict["state"]

    return gdf
    
def join_weather_and_crop_data(weather_df, crop_df):
    
    weather_df["fips_code"] = weather_df["fips_code"].apply(str)
    weather_df["state"] = weather_df["fips_code"].str[:2]
    weather_df["county"] = weather_df["fips_code"].str[2:]

    joined_data = pd.merge(weather_df, crop_df,  how='inner', left_on=['state','county'], right_on = ["state_code","county_code"])

    return joined_data[["field_id", "precip", "min_temp","max_temp", "mean_temp"]]



def main():
    #get the folder path for the input files
    path = os.getcwd()

    #(1) crop.csv ETL
    input_file_path = get_file_path(path, "inputs", "crop.csv")
    output_file_path = get_file_path(path, "outputs", "crop_2021.csv")
    
    crop_df = extract_csv_to_pandas(input_file_path)
    
    filtered_crop_df = transform_crop_data(crop_df)

    load_df_to_csv(filtered_crop_df, output_file_path)
    

    #(2) spectral.csv ETL
    input_file_path = get_file_path(path, "inputs", "spectral.csv")
    output_file_path = get_file_path(path, "outputs", "spectral_with_peak_NDVI.csv")

    spectral_df = extract_csv_to_pandas(input_file_path)
    transformed_spectral_data = transform_spectral_data(spectral_df)
    load_df_to_csv(transformed_spectral_data, output_file_path)
    
    #(3) soil.csv ETL
    input_file_path = get_file_path(path, "inputs", "soil.csv")
    output_file_path = get_file_path(path, "outputs", "soil_with_horizontal_weighted_avg.csv")

    soil_df = extract_csv_to_pandas(input_file_path)
    transformed_soil_data = transform_soil_data(soil_df)
    load_df_to_csv(transformed_soil_data, output_file_path)
  
    #(4) weather.csv ETL
    input_file_path = get_file_path(path, "inputs", "weather.csv")
    output_file_path = get_file_path(path, "outputs", "field_id_with_weather_data.csv")

    weather_df = extract_csv_to_pandas(input_file_path)
    #get calculations by field_id
    weather_2021_df = transform_weather_csv(weather_df)

    #using crop_df with only 2021 data, get state and county for the field geometries
    crop_data_with_codes = transform_crop_data_with_county_and_state(filtered_crop_df)
    #join weather and crop data
    weather_and_crop_data = join_weather_and_crop_data(weather_2021_df, crop_data_with_codes)
    load_df_to_csv(weather_and_crop_data, output_file_path)


if __name__ == "__main__":
    main()