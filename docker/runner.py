# Copyright (c) 2021, University of Washington
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the University of Washington nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF WASHINGTON AND CONTRIBUTORS
# “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE UNIVERSITY OF WASHINGTON OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys
import json
from h5coro import h5coro, s3driver
import numpy as np
import pandas as pd

# ########################################################
# Constants
# ########################################################

BEAMS = [ "gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r" ]

MONTH_TO_SEASON = { #[is_north][month] --> 0: winter, 1: spring, 2: summer, 3: fall
    True: { 1: 0, 2: 0, 3: 0, 4: 1, 5: 1, 6: 1, 7: 2, 8: 2, 9: 2, 10: 3, 11: 3, 12: 3 },
    False: { 1: 2, 2: 2, 3: 2, 4: 3, 5: 3, 6: 3, 7: 0, 8: 0, 9: 0, 10: 1, 11: 1, 12: 1 }
}

# ########################################################
# Metadata
# ########################################################

# constant - statistics to generate for granule
METADATA_STATS = [
    ('granule',''),
    ('beam',''),
    ('region',''),
    ('season',''),
    ('total_photons',''),
    ('subaqueous_photons',''),
    ('sea_surface_photons',''),
    ('sea_surface_std','.3f'),
    ('bathy_photons',''),
    ('bathy_mean_depth','.3f'),
    ('bathy_min_depth','.3f'),
    ('bathy_max_depth','.3f'),
    ('bathy_std_depth','.3f'),
    ('bathy_above_sea_surface', ''),
    ('bathy_below_sensor_depth', ''),
    ('histogram',''),
    ('polygon', ''),
    ('begin_time', ''),
    ('end_time', '')
]

# constant - variables to read from granule
METADATA_VARIABLES = [
    "ortho_h",
    "surface_h",
    "class_ph"
]

# function - processing granule
def metadata(parms):

    # initialize arguments
    granule = parms["granule"]
    path = parms["url"].split("s3://")[-1] + "/" + granule
    csv_lines = ','.join([f'{entry[0]}' for entry in METADATA_STATS]) + "\n"

    try:
        # open granule
        h5obj = h5coro.H5Coro(path, s3driver.S3Driver, errorChecking=True, verbose=False, credentials={"role": True, "role":"iam"}, multiProcess=False)

        # read datasets
        datasets = [f'{beam}/{var}' for beam in BEAMS for var in METADATA_VARIABLES]
        promise = h5obj.readDatasets(datasets + ['metadata/extent'], block=True, enableAttributes=False)

        # pull out metadata
        extent = json.loads(promise["metadata/extent"])

        # process each beam in the granule
        for beam in BEAMS:
            try:
                # get info
                month = int(granule[10:12])
                region = int(granule[27:29])

                # read columns
                columns = {var: promise[f'{beam}/{var}'][:] for var in METADATA_VARIABLES}

                # build dataframes
                df = pd.DataFrame(columns)
                df["depth"] = df["surface_h"] - df["ortho_h"]
                df_sea_surface = df[df["class_ph"] == 41]
                df_subaqueous = df[df["class_ph"].isin([0, 40]) & df["depth"] >= 0.0] # treating depth == 0.0 as subaqueous to be consistent with histogram below
                df_bathy = df[df["class_ph"] == 40]

                # build histogram
                values = df_bathy["depth"].to_numpy()
                hist, bin_edges = np.histogram(values, bins=np.arange(0, 51, 1)) # [0.0, 1.0), [1.0, 2.0), ..., [49.0, 50.0], note last range is fully closed
                hist_str = ' '.join([f"{int(value)}" for value in hist])

                # build stats
                row = {
                    "granule": granule,
                    "beam": beam,
                    "region": region,
                    "season": MONTH_TO_SEASON[region<8][month],
                    "total_photons": len(df),
                    "subaqueous_photons": len(df_subaqueous),
                    "sea_surface_photons": len(df_sea_surface),
                    "sea_surface_std": df_sea_surface["ortho_h"].std(),
                    "bathy_photons": len(df_bathy),
                    "bathy_mean_depth": df_bathy["depth"].mean(),
                    "bathy_min_depth": df_bathy["depth"].min(),
                    "bathy_max_depth": df_bathy["depth"].max(),
                    "bathy_std_depth": df_bathy["depth"].std(),
                    "bathy_above_sea_surface": len(df_bathy[df_bathy["depth"] < 0.0]),
                    "bathy_below_sensor_depth": len(df_bathy[df_bathy["depth"] > 50.0]),
                    "histogram": hist_str,
                    "polygon": extent["polygon"],
                    "begin_time": extent["begin_time"],
                    "end_time": extent["end_time"]
                }

                # build csv line
                csv_lines += ','.join([f'{row[entry[0]]:{entry[1]}}' for entry in METADATA_STATS]) + "\n"

            except TypeError:
                pass # missing beam

        # return csv lines
        return csv_lines, True

    except Exception as e:
        return f'{granule} - {e}', False

# ########################################################
# Main
# ########################################################

# get command line parameters
if len(sys.argv) <= 2:
    print("Not enough parameters: python runner.py <input json file> <output json file>")
    sys.exit()
else:
    input_file = sys.argv[1]
    output_file = sys.argv[2]

try:
    # read input json
    parms_json = input_file
    with open(parms_json, 'r') as json_file:
        parms = json.load(json_file)

    # call function
    if parms["function"] == "metadata":
        result, status = metadata(parms)
    else:
        result = f'unrecognized function: {parms["function"]}'
        status = False

# handle exceptions
except Exception as e:
    result = f'exception: {e}'
    status = False

# write output
with open(output_file, "w") as file:
    file.write(json.dumps({"status": status, "result": result}))
