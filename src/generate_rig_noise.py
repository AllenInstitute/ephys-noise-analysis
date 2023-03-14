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


def make_dataset(cellname, nwb_path):
    try:
        dataset = create_ephys_data_set(nwb_file=nwb_path)
    except (ValueError, OSError, TypeError):
        print("can't make dataset for ", cellname)
        dataset = None

    return dataset

def calculate_std_vs(vs_swp_num_lst):
    """
    vs_swp_num_lst (list): a list of sweep numbers
    """
    
    try:
        # -1 calls the last sweep in the list
        sweepnum = vs_swp_num_lst[-1]
        # Create a sweep table dataframe
        swp_df = dataset.sweep_table 
        # Select row with the specified sweepnum
        swp_row_df = swp_df.loc[swp_df["sweep_number"] == sweepnum]
        # Runs only if the sweep is in voltage clamp
        if "VoltageClamp" in list(swp_row_df['clamp_mode']):
            sweep = dataset.sweep(sweepnum)
            epochs = sweep.epochs
            samp_rate = sweep.sampling_rate

            test_epoch = epochs["test"]
            recording_epoch = epochs["recording"]
            stim_epoch = epochs["stim"]
            experiment_epoch = epochs["experiment"]

            #bl_long_duration = 0.500 # 500 ms baseline duration
            bl_short_duration = 0.0015 # 1.5 ms short baseline duration
            bl_end = int(sweep.epochs['stim'][0]) # same baseline end can be used for both long and short baselines
            #bl_long_start = int(((bl_end / sweep.sampling_rate) - bl_long_duration) * sweep.sampling_rate)
            bl_short_start = int(((bl_end / samp_rate) - bl_short_duration) * samp_rate)
            
            # New method which doesn't work for older stim sets
            #bl_long = sweep.i[bl_long_start : bl_end]
            #long_rms = np.std(bl_long).round(3)
            
            # Old method works for older stim sets
            buffer2 = samp_rate * 0.015
            bl_long = sweep.i[(int(test_epoch[1]+buffer2)):stim_epoch[0]]
            long_rms = np.std(bl_long).round(3)
            
            # New method that works for old and new stim sets
            bl_short = sweep.i[bl_short_start : bl_end]
            short_rms = np.std(bl_short).round(3)
            
            #print(bl_long_start, bl_end)
            return long_rms, short_rms

    except (NameError, TypeError, AttributeError) as e:
        print("NameError")
        return None    


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
jem_df = jem_df.loc[jem_df["jem-date_patch_y"] == 2023]
# Clean column of duplicates and NAs
jem_df.drop_duplicates(subset=["jem-id_cell_specimen"], inplace=True)
jem_df.dropna(subset=["jem-id_cell_specimen"], inplace=True)

# Filters dataframe to user specified date
jem_df = jem_df[jem_df["jem-date_patch"].str.contains(date_prev_day.strftime("%m/%d/%Y"))]
# Sort values by date
jem_df.sort_values(by=["jem-date_patch"], ascending=True, inplace=True)

# Gather list of experiments based on the filtered pandas dataframe
cell_list = jem_df["jem-id_cell_specimen"].tolist()

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
        except (IndexError) as e:
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
