"""
-----------------------------------------------------------------------
File name: generate_functions.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Developer: Ramkumar Rajanbabu
Date/time created: 10/22/2022
Description: Template for functions
-----------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import os
import pg8000
# File imports
from ipfx.dataset.create import create_ephys_data_set, get_nwb_version


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
    """
    """
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
    """
    """
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

    except (NameError, TypeError) as e:
        print("NameError")
        return None    
