import argparse
import tempfile
import os
import sys
import h5py
import pandas as pd
import numpy as np
from sliderule import sliderule

# -------------------------------------------
# command line arguments
# -------------------------------------------
parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--domain',             type=str,               default="slideruleearth.io")
parser.add_argument('--organization',       type=str,               default="sliderule")
parser.add_argument('--desired_nodes',      type=int,               default=None)
parser.add_argument('--time_to_live',       type=int,               default=600) # 10 hours
parser.add_argument('--before_dir',         type=str,               default="/data/ATL24/test_cleanup/before")
parser.add_argument('--after_dir',          type=str,               default="/data/ATL24/test_cleanup/after")
parser.add_argument('--test_run',           action='store_true',    default=False) # exit after first test
args,_ = parser.parse_known_args()

if args.organization == "None":
    args.organization = None

# -------------------------------------------
# initialize sliderule client
# -------------------------------------------
sliderule.init(args.domain, organization=args.organization, desired_nodes=args.desired_nodes, time_to_live=args.time_to_live, verbose=False)

# -------------------------------------------
# get list of tests to run
# -------------------------------------------
tests_to_run = [] # (resource, beams)
files_in_after_dir = os.listdir(args.after_dir)
for file in files_in_after_dir:
    if file.endswith(".csv"):
        resource = file[:-9] + ".h5"
        beam = file[-8:-4]
        tests_to_run.append((resource, beam))

# -------------------------------------------
# run tests
# -------------------------------------------
number_of_passed_tests = 0
for test in tests_to_run:

    # pull out parameters of test
    resource, beam = test
    print(f'Testing {resource} beam {beam} ', end='')

    # read expected results
    expected_results_file = f'{args.after_dir}/{resource[:-3]}_{beam}.csv'
    expected_df = pd.read_csv(expected_results_file).rename(columns={"X_ATC": "x_atc"})
    print(f'{len(expected_df)} photons... ', end='')

    # read expected summary file
    expected_summary_file = f'{args.after_dir}/{resource[:-3]}_{beam}.txt'
    expected_relabeled = 0
    with open(expected_summary_file, "r") as file:
        for line in file.readlines():
            line = line.strip()
            if line.endswith("relabeled"):
                tokens = line.split(' ')
                expected_relabeled = int(tokens[0])

    # build parameters of request
    parms = {
        "resource":resource,
        "output": {
            "format": "h5",
            "path": tempfile.mktemp()
        },
        "beams": [beam]
    }

    # make request
    rsps = sliderule.source("atl24g2", {"parms": parms}, stream=True)
    resultfile = sliderule.procoutputfile(parms, rsps)
    h5f = h5py.File(resultfile)
    relabeled = int(h5f[beam]["class_ph"].attrs["relabeled"])
    actual_results = {
        "x_atc": h5f[beam]["x_atc"][:],
        "ortho_h": h5f[beam]["ortho_h"][:],
        "class_ph": h5f[beam]["class_ph"][:]
    }
    actual_df = pd.DataFrame(actual_results)

    # compare actual to expected results
    diff_df = actual_df == expected_df
    x_atc_mask = np.isclose(actual_df["x_atc"], expected_df["x_atc"], rtol=1e-5, atol=1e-8)
    ortho_h_mask = np.isclose(actual_df["ortho_h"], expected_df["ortho_h"], rtol=1e-5, atol=1e-8)

    # display results
    if len(actual_df) != len(expected_df):
        print(f'FAIL (incorrect number of rows in results, {len(actual_df)} != {len(expected_df)})')
    elif False in diff_df["class_ph"].value_counts():
        print(f'FAIL (there were {diff_df["class_ph"].value_counts()[False]} miscompares in the class_ph column)')
    elif False in x_atc_mask:
        print(f'FAIL (there were {len(x_atc_mask) - np.count_nonzero(x_atc_mask)} meaningful x_atc differences)')
    elif False in ortho_h_mask:
        print(f'FAIL (there were {len(ortho_h_mask) - np.count_nonzero(ortho_h_mask)} meaningful ortho_h differences)')
    elif relabeled != expected_relabeled:
        print(f'FAIL (unexpected number of photons relabled, {relabeled} != {expected_relabeled})')
    else:
        number_of_passed_tests += 1
        print(f'pass ({relabeled} relabeled)')

    # clean up
    os.remove(parms["output"]["path"])

    # exit if a test run
    if args.test_run:
        break

# -------------------------------------------
# final summary
# -------------------------------------------
print(f'Passed {number_of_passed_tests} out of {len(tests_to_run)} tests')
if number_of_passed_tests == len(tests_to_run):
    sys.exit(0)
else:
    sys.exit(1)
