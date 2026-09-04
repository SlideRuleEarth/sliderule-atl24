import importlib
import sys
import random
import string
import argparse
from sliderule import sliderule
from atl24r3_database import Database, Status

try:
    tqdm = importlib.import_module("tqdm").tqdm
except Exception:
    print(f"tqdm unavailable, progress of operations will not be reported")
    def tqdm(iterable, **kwargs):
        return iterable

#########################################
# command line arguments
#########################################
parser = argparse.ArgumentParser(description="""ATL24 Platinum Run""")
parser.add_argument('--cycle',      type=int,               default=None) # 1, 2, 3, etc.
parser.add_argument('--rerun',      type=str,               default=None) # name of run (e.g. rerun_32GB)
parser.add_argument('--vcpus',      type=int,               default=4)
parser.add_argument('--memory',     type=int,               default=16000)
parser.add_argument('--batch_size', type=int,               default=10000)
parser.add_argument('--script',     type=str,               default="scripts/gen_atl24r3.lua")
parser.add_argument('--database',   type=str,               default="data/atl24r3_database.json")
parser.add_argument('--vset',       type=str,               default="data/atl24r3_validation_set.txt")
parser.add_argument('--vrun',       type=str,               default=None) # name of validation run (e.g. vset_run2)
args = parser.parse_args()

#########################################
# open database
#########################################
database = Database(args.database)

#########################################
# create runner sessions and authentiate
#########################################
session = sliderule.create_session(verbose=True)
session.authenticate() # gives privileges to access SlideRule Runner

#########################################
# function: submit job
#########################################
def submit_job(name, granules):

    # process job in batches
    for i in range(0, len(granules), args.batch_size):

        # build and check name
        job_name = f"{name}_{i}"
        if job_name in database.submissions:
            unique = ''.join(random.choices(string.ascii_lowercase, k=3))
            job_name = f"{name}_{unique}_{i}"

        # submit job
        args_list = granules[i:i+args.batch_size]
        lua_script = open(args.script, "r").read()
        rsps = session.runner.submit(name=job_name, script=lua_script, args=args_list, optional_args={"vcpus":args.vcpus, "memory":args.memory, "image": "sliderule:atl24"})
        print(f"Submitted job {job_name} using script {args.script} with {len(args_list)} entries")

        # save job
        database.submissions[job_name] = rsps | {"complete": False}
        print(f"Saved job submission", rsps)

        # save granules
        for granule in args_list:
            database.granules[granule] = {"name": job_name, "status": Status.PENDING}

#########################################
# process granules for verification
#########################################
if args.vrun:

    # check argument
    if not isinstance(args.vrun, str) or len(args.vrun) == 0:
        print("Must supply name for the validation job")
        sys.exit(1)

    # get granules from verification list
    with open(args.vset, "r") as file:
        lines = file.readlines()
        granules = [line.strip() for line in lines if len(line) > 30]

    # submit job
    submit_job(args.vrun, granules)

#########################################
# rerun granules that have failed
#########################################
if args.rerun:

    # check argument
    if not isinstance(args.rerun, str) or len(args.rerun) == 0:
        print("Must supply name for the rerun job")
        sys.exit(1)

    # get granules from database set
    granules = []
    for granule,result in database.granules.items():
        if result["status"] == Status.ERROR:
            granules.append(granule)

    # submit job
    submit_job(args.rerun, granules)

#########################################
# process granules for cycle
#########################################
if args.cycle:

    # get list of granules to process
    print(f"Requesting ATL03 granules from CMR for cycle {args.cycle}")
    atl03_granules = sliderule.source("earthdata", {
        "asset": "icesat2",
        "cycle": args.cycle,
        "max_resources": 100000
    })
    print(f"Retrieved list of {len(atl03_granules)} granules to process")
    granules = [f"{granule}" for granule in atl03_granules]

    # submit job
    submit_job(f"atl24r3_{args.cycle}", granules)

#########################################
# save database
#########################################
database.write()
