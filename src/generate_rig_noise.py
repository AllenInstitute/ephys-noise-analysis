"""
-----------------------------------------------------------------------
File name: generate_rig_noise.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Developer: Ramkumar Rajanbabu
Date/time created: 10/14/2022
Description: Template for generating noise_metrics_2022.csv
-----------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import csv
import numpy as np
import os
import pandas as pd
import pg8000
from datetime import datetime, date, timedelta
# File imports
from functions.general_functions import generate_cell_path, find_nwb_v2, make_dataset, calculate_std_vs
# Test imports
import time # To measure program execution time


# Dates
dt_today = datetime.today() # datetime.datetime(2022, 10, 21, 8, 22, 9, 517314)
date_today = dt_today.date() # datetime.date(2022, 10, 21)
date_prev_day = date_today - timedelta(days=1) # datetime.date(2022, 10, 20)

# Directories
json_data_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc-data-warehouse/data-sources/jem_lims_metadata.csv"
noise_data_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc-data-warehouse/data-sources/noise_metrics_2023.csv"

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
#len(cell_list)

if os.path.exists(noise_data_dir):
    noise_df = pd.read_csv(noise_data_dir)
else:
    noise_df = pd.DataFrame(columns=noise_cols)
    noise_df.to_csv(noise_data_dir, mode="a", index=False, header=noise_cols)

# Use the 3 voltage sweep names and use startswith (column=stimulus_code)
vs_inbath_stim_names = ["EXTPINBATH141203", "EXTPINBATH180424"]
vs_cellatt_stim_names = ["EXTPCllATT141203", "EXTPCllATT180424"]
vs_breakin_stim_names = ["EXTPBREAKN141203", "EXTPBREAKN180424"]

num = 1
start = time.time()
for cell_name in cell_list:
    if cell_name not in list(noise_df["cell_name"]):
        print(f"***Loop ({num})***")
        path = generate_cell_path(cell_name)
        if path:
            # ['/allen'] to '/allen' by using path[0]
            path = '/' + path[0] # '/' + '/allen...' = '//allen...'
            nwb2_filepath = find_nwb_v2(path)
            # Terminal print statements
            print(f"Cell name: {cell_name}")
            print(f"File path: {nwb2_filepath}")
            if nwb2_filepath:
                dataset = make_dataset(cell_name, nwb2_filepath)

        # Returns the sweep number (column=sweep_number)
        try:
            vs_inbath_sweep_nums = dataset.get_sweep_numbers(stimuli=vs_inbath_stim_names)
            vs_cellatt_sweep_nums = dataset.get_sweep_numbers(stimuli=vs_cellatt_stim_names)
            vs_breakin_sweep_nums = dataset.get_sweep_numbers(stimuli=vs_breakin_stim_names)
        except IndexError:
            print(f"{cell_name} does not contain a voltage sweep.")

        try: 
            (inbath_long_rms, inbath_short_rms) = calculate_std_vs(vs_inbath_sweep_nums)
            (cellatt_long_rms, cellatt_short_rms) = calculate_std_vs(vs_cellatt_sweep_nums)
            (breakin_long_rms, breakin_short_rms) = calculate_std_vs(vs_breakin_sweep_nums)
        
            sweep_df = pd.DataFrame(columns=sweep_cols)
            row_list = [cell_name, inbath_long_rms, inbath_short_rms, cellatt_long_rms, cellatt_short_rms, breakin_long_rms, breakin_short_rms]
            row = pd.Series(row_list, index=sweep_df.columns)
            sweep_df = sweep_df.append(row, ignore_index=True)
            
            df = pd.merge(left=sweep_df, right=jem_df, how="left", left_on="cell_name", right_on="jem-id_cell_specimen")
            df = df[noise_cols]
            
            new_data_df = pd.DataFrame(columns=noise_cols)
            new_data_df = new_data_df.append(df, ignore_index=True)
            new_data_df.to_csv(noise_data_dir, mode="a", index=False, header=False)
            print()
        
        except (NameError, TypeError) as e:
            print("Missing Voltage Sweep")
            print()
        num += 1
    else:
    	print("The for loop did not run because there is already a cell name in the csv.")

print("\nThe for loop was executed in", round(((time.time()-start)/60), 2), "minutes.")
