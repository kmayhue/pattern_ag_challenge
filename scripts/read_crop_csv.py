import os 
import sys
import pandas as pd


def get_file_path(current_dir, file_type, file_name):

    return os.path.abspath(os.path.join(current_dir, os.pardir, file_type, file_name))




def extract_csv_to_pandas(file_path):

    return pd.read_csv(file_path)


def transform_crop_data(data):
    '''
    This function takes a dataframe the crop csv dataset, filters to only year 2021
    and returns a dataframe with only 'field_id', 'field_geometry', 'crop_type'
    '''
    return data[data["year"] == 2021][['field_id', 'field_geometry', 'crop_type']]
    


def load_df_to_csv(output_df, output_file):
    '''
    This function takes an output dataframe and an output file, and it
    writes the dataframe to that output file
    '''
    #write df to csv file with quotes around each field
    output_df.to_csv(output_file, index=False,header=True)

def main():
    #get the folder path for the input files
    path = os.getcwd()
    input_file_path = get_file_path(path, "inputs", "crop.csv")
    output_file_path = get_file_path(path, "outputs", "crop_2021.csv")
    

    crop_df = extract_csv_to_pandas(input_file_path)
    filtered_crop_df = transform_crop_data(crop_df)
    load_df_to_csv(filtered_crop_df, output_file_path)

if __name__ == "__main__":
    main()