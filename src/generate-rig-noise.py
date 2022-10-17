"""
-----------------------------------------------------------------------
File name: generate-rig-noise.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Developer: Ramkumar Rajanbabu
Date/time created: 10/14/2022
Description: Template for generating rig-noise.csv
-----------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import csv
import numpy as np
import os
import pandas as pd
import pg8000
# File imports
from ipfx.dataset.create import create_ephys_data_set, get_nwb_version
# Test imports
import time # To measure program execution time
