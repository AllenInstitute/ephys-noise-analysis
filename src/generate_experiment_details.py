"""
-----------------------------------------------------------------------
File name: generate_experiment_details.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Developer: Ramkumar Rajanbabu
Date/time created: 10/14/2022
Description: Template for generating experiment-detail.csv
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
from functions.general_functions import generate_cell_path, find_nwb_v2, make_dataset
# Test imports
import time # To measure program execution time


# Dates
dt_today = datetime.today() # datetime.datetime(2022, 10, 21, 8, 22, 9, 517314)
date_today = dt_today.date() # datetime.date(2022, 10, 21)
date_prev_day = date_today - timedelta(days=1) # datetime.date(2022, 10, 20)

# Directories
json_data_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/formatted_data/master_jem.csv"
exp_data_dir = "C:/Users/ramr/Documents/Github/personal_repos/allen_institute_projects/experiment-details.csv"

# Lists
jem_fields = ["jem-date_patch", "jem-date_patch_y", "jem-date_patch_m", "jem-date_patch_d",
              "jem-id_cell_specimen", "jem-id_patched_cell_container", "jem-status_success_failure"]
cols = ["cell_name", "sweep_number", "stimulus_units", "bridge_balance_mohm", "leak_pa",
        "stimulus_scale_factor", "stimulus_code", "stimulus_code_ext",
        "clamp_mode", "stimulus_name"]

# Read data source as a pandas dataframe
jem_df = pd.read_csv(json_data_dir, usecols=jem_fields, low_memory=False)
# Filters
jem_df = jem_df.loc[jem_df["jem-status_success_failure"] == "SUCCESS"]
#jem_df = jem_df.loc[jem_df["jem-status_patch_tube"] == "Patch Tube"]
jem_df = jem_df.loc[jem_df["jem-date_patch_y"] == 2022]
# Clean column of duplicates and NAs
jem_df.drop_duplicates(subset=["jem-id_cell_specimen"], inplace=True)
jem_df.dropna(subset=["jem-id_cell_specimen"], inplace=True)

# Filters dataframe to user specified date
jem_df = jem_df[jem_df["jem-date_patch"].str.contains(date_prev_day.strftime("%m/%d/%Y"))]

# Gather list of experiments based on the filtered pandas dataframe
cell_list = jem_df["jem-id_cell_specimen"].tolist()
len(cell_list)


exp_details_df = pd.read_csv(exp_data_dir)

num = 1
start = time.time()

for cell_name in cell_list:
    if cell_name not in list(exp_details_df["cell_name"]):
        print(f"***Loop ({num})***")
        path = generate_cell_path(cell_name)
        if path:
            path = '/' + path[0] # '/' + '/allen...' = '//allen...'
            nwb2_filepath = find_nwb_v2(path)
            # Terminal print statements
            #print(f"Cell name: {cell_name}")
            #print(f"File path: {nwb2_filepath}")
            if nwb2_filepath:
                dataset = make_dataset(cell_name, nwb2_filepath)

        df = dataset.sweep_table
        df["cell_name"] = cell_name
        df = df[cols]

        new_data_df = pd.DataFrame(columns=cols)
        new_data_df = new_data_df.append(df, ignore_index=True)
        new_data_df.to_csv("experiment-details.csv", mode="a", index=False, header=False)
        print()

        num += 1

if cell_name in list(exp_details_df["cell_name"]):
    print("The program did not run because there is already a cell name in the csv.")
else:
    print(f"The loop ran {num} experiments.")
    print("\nThe for loop was executed in", round(((time.time()-start)/60), 2), "minutes.")
