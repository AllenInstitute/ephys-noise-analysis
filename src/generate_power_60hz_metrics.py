"""
-----------------------------------------------------------------------
File name: generate_power_60hz_metrics.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Developer: Ramkumar Rajanbabu
Date/time created: 3/9/2023
Description: Template for generating power_60hz_metrics_2023.csv
-----------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import h5py
import numpy as np
import os
import pandas as pd
import pg8000
from datetime import datetime, date, timedelta
# File imports
from ipfx.dataset.create import create_ephys_data_set, get_nwb_version
# Test imports
import time # To measure program execution time


# Functions
def generate_cell_path(cellname):
    """
    Generates a file path given a cell name (ex. "Vip-IRES-Cre;Ai14-366688.04.01.01").
    
    Parameters:
        cellname (string): a string specifying the cell name.
    
    Returns:
        result (list): a list with a single string specifying the file path.
    """
    
    conn = pg8000.connect(user="limsreader", host="limsdb2", database="lims2", password="limsro", port=5432)
    cur = conn.cursor()
    cur.execute(
    """SELECT err.storage_directory AS path 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    WHERE cell.name LIKE '{}'
    """.format(cellname))

    result = cur.fetchone()
    cur.close()
    conn.close()
    
    if result != None:
        return result


def find_nwb_v2(path):
    nwb2_file = None
    for root, dirs, files in os.walk(path):
        for fil in files:
            if fil.endswith(".nwb"):
                test_nwb_name = os.path.join(path, fil)
                try:
                    test_nwb_version = get_nwb_version(test_nwb_name)
                    nwb_version = test_nwb_version['major']
                    if nwb_version == 2:
                        nwb2_file = path + fil
                except OSError as e:
                    pass
        return nwb2_file

# Dates
dt_today = datetime.today() # datetime.datetime(2022, 10, 21, 8, 22, 9, 517314)
date_today = dt_today.date() # datetime.date(2022, 10, 21)
date_prev_day = date_today - timedelta(days=1) # datetime.date(2022, 10, 20)

# Directories
json_data_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc-data-warehouse/data-sources/jem_lims_metadata.csv"
power_60hz_data_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc-data-warehouse/data-sources/power_60hz_metrics_2023.csv"

# Lists
jem_fields = ["jem-date_patch", "jem-date_patch_y", "jem-date_patch_m", "jem-date_patch_d", "jem-id_cell_specimen", "jem-id_patched_cell_container", "jem-status_success_failure"]
sweep_cols= ["cell_name", "inbath_long_rms", "inbath_short_rms", "cellatt_long_rms", "cellatt_short_rms", "breakin_long_rms", "breakin_short_rms"]
noise_cols= ["jem-date_patch", "cell_name", "inbath_long_rms", "inbath_short_rms", "cellatt_long_rms", "cellatt_short_rms", "breakin_long_rms", "breakin_short_rms"]

# Read data source as a pandas dataframe
jem_df = pd.read_csv(json_data_dir, usecols=jem_fields, low_memory=False)
# Filters
jem_df = jem_df.loc[jem_df["jem-status_success_failure"] == "SUCCESS"]
#jem_df = jem_df.loc[jem_df["jem-status_patch_tube"] == "Patch Tube"]
jem_df = jem_df.loc[jem_df["jem-date_patch_y"] == 2023]
# Clean column of duplicates and NAs
jem_df.drop_duplicates(subset=["jem-id_cell_specimen"], inplace=True)
jem_df.dropna(subset=["jem-id_cell_specimen"], inplace=True)
# Sort values by date
jem_df.sort_values(by=["jem-date_patch"], ascending=True, inplace=True)

# Filters dataframe to user specified date
#jem_df = jem_df[jem_df["jem-date_patch"].str.contains(date_prev_day.strftime("%m/%d/%Y"))]

# Gather list of experiments based on the filtered pandas dataframe
cell_list = jem_df["jem-id_cell_specimen"].tolist()
len(cell_list)

power_60hz_dict = {}
last_nwb_layer = 8

num = 1
start = time.time()
for cell_name in cell_list:
    cell_values_dict = {}
    cell_values_list = []
    
    print(f"***Loop ({num})***")
    path = generate_cell_path(cell_name)
    if path:
        # ['/allen'] to '/allen' by using path[0]
        path = '/' + path[0] # '/' + '/allen...' = '//allen...'
        nwb2_filepath = find_nwb_v2(path)
        # Terminal print statements
        print(f"Cell name: {cell_name}")
        print(f"File path: {nwb2_filepath}")
        
        # Use nwb2_filepath to find hdf5 file
        h5f5_file = h5py.File(nwb2_filepath, "r")
        
        # Use hdf5_file to find the TextualResultsKeys
        textual_results_keys = h5f5_file["general/results/textualResultsKeys"]
        
        keys_list = []
        search_str = "Sweep Formula store [power60HzRatio]"
        for k in textual_results_keys[0]:
            keys_list.append(k)
        
        if search_str in keys_list:
            power60idx = keys_list.index(search_str)
            print(cell_name)
            print(search_str)
            print(power60idx)
        
            # Use hdf5_file to find the TextualResultsValues
            textual_results_values = h5f5_file["general/results/textualResultsValues"]

            for k in range(len(textual_results_values)):
                # k = ?
                if len(textual_results_values[k][power60idx][last_nwb_layer]) > 0:
                    cell_values_list.append(textual_results_values[k][power60idx][last_nwb_layer])

            # Remove ";" from each string in the cell_values_list
            cell_values_list = [sub[: -1] for sub in cell_values_list]
            # Convert all items in list from string to float
            cell_values_list = [float(x) for x in cell_values_list]

            # Create mean, mediuan and standard deviation for cell_values_list
            mean = np.mean(cell_values_list)
            med = np.median(cell_values_list)
            std = np.std(cell_values_list)

            # Create a dictionary with mean, meadian and standard deviation
            cell_values_dict["power_60hz_mean"] = mean
            cell_values_dict['power_60hz_median'] = med
            cell_values_dict["power_60hz_std"] = std

            # Create a main dictionary (power_60hz_dict) with an inner dictionary (cell_values_dict)
            power_60hz_dict[cell_name] = cell_values_dict
        else:
            pass
        num += 1

print("\nThe for loop was executed in", round(((time.time()-start)/60), 2), "minutes.")

# .transpose() switches the rows and columns
df = pd.DataFrame(power_60hz_dict).transpose()
# Reset the index to name the cell_name column
df.reset_index(inplace=True)
df.rename(columns={"index": "cell_name"}, inplace=True)

# Merge dataframes to get the date column
merged_cols = ["jem-date_patch", "cell_name", "power_60hz_mean", "power_60hz_median", "power_60hz_std"]
merged_df = pd.merge(left=df, right=jem_df, how="left", left_on="cell_name", right_on="jem-id_cell_specimen")
merged_df = merged_df[merged_cols]

# Convert dataframe to csv
merged_df.to_csv(path_or_buf=power_60hz_data_dir, index=False)
