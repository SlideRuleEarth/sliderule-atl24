# {
#     "submissions": {
#         "<name>": {
#             "run_url": <run url>,
#             "job_id": <job id>,
#             "status": {
#                 "SUBMITTED": <x>,
#                 "PENDING": <x>,
#                 "RUNNABLE": <x>,
#                 "STARTING": <x>,
#                 "RUNNING": <x>,
#                 "SUCCEEDED": <x>,
#                 "FAILED": <x>
#             },
#             "complete": <true|false>
#         },
#         ...
#     },
#     "granules": {
#         "<ATL03 granule>": {
#             "name": <job>, -- of job responsible for processing granule
#             "status": <status>,
#             "rsps": <response from runner>,
#             "duration": <seconds it took to complete>,
#             "attributes": <custom dictionary>
#         }
#     }
# }

import json
import os
from enum import Enum

# ###############################
# Status
# ###############################

class Status(str, Enum):
    """Valid values of a granule's "status" field."""

    # set by gen_atl24r3.py
    PENDING         = "pending"     # granule has not been processed yet
    OUTPUT          = "output"      # granule processed and produced an ATL24 output
    EMPTY           = "empty"       # granule processed but produced no output
    ERROR           = "error"       # granule failed to process

    # set by transfer_atl24r3.py
    TX_READY        = "tx_ready"        # output located in s3 and attributes collected
    TX_INITIATED    = "tx_initiated"    # notification posted to the NSIDC stream
    TX_FAILED       = "tx_failed"       # notification could not be posted
    MISSING         = "missing"         # output could not be located in s3

    def __str__(self):
        return self.value

# ###############################
# Database
# ###############################

class Database:

    def __init__(self, filename):
        self.filename = filename
        try:
            # read database
            with open(filename, "r") as file:
                self.database = json.load(file)
        except FileNotFoundError:
            # create database
            with open(filename, "w") as file:
                self.database = {"submissions": {}, "granules": {}}
                json.dump(self.database, file)

    @property
    def granules(self):
        return self.database["granules"]

    @property
    def submissions(self):
        return self.database["submissions"]

    # Update granule status in database
    def update_status(self, granule, status):
        self.granules[granule]["status"] = Status(status).value

    # Update granule attributes in database
    def update_attributes(self, granule, attributes):
        self.granules[granule]["attributes"] = attributes
        self.update_status(granule, Status.TX_READY)

    # Write database out to file
    def write(self, filename=None):
        filename = filename or self.filename
        # written via a temporary file so that an interrupt cannot truncate the database
        tmp_filename = f"{filename}.tmp"
        with open(tmp_filename, "w") as file:
            json.dump(self.database, file, indent=2)
        os.replace(tmp_filename, filename)
