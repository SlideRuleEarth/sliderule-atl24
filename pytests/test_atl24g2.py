"""Tests for sliderule gedi plugin."""

import pytest
from sliderule import sliderule
import h5py
import os

class TestAtl24g2:
    def test_nominal(self, init):
        resource = "ATL24_20241107234251_08052501_006_01_001_01.h5"
        output_path = "/tmp/atl24.h5"
        parms = {
            "resource": resource,
            "output": {
                "format": "h5",
                "path": output_path
            }
        }
        rsps = sliderule.source("atl24g2", {"parms": parms}, stream=True)
        filename = sliderule.procoutputfile(parms, rsps)
        h5f = h5py.File(filename)
        assert init
        assert filename == output_path
        assert len(h5f.keys()) == 9
        for key in ['ancillary_data', 'gt1l', 'gt1r', 'gt2l', 'gt2r', 'gt3l', 'gt3r', 'metadata', 'orbit_info']:
            assert key in h5f
        assert len(h5f["gt1l"].keys()) == 19
        for key in ['class_ph', 'confidence', 'ellipse_h', 'index_ph', 'index_seg', 'invalid_kd', 'invalid_wind_speed', 'lat_ph', 'lon_ph', 'low_confidence_flag', 'night_flag', 'ortho_h', 'sensor_depth_exceeded', 'sigma_thu', 'sigma_tvu', 'surface_h', 'time_ns', 'x_atc', 'y_atc']:
            assert key in h5f["gt1l"]
        assert sum(h5f["gt1l"]["night_flag"]) == 188588
        assert sum(h5f["gt1l"]["low_confidence_flag"]) == 12
        bathy_cnt = 0
        for c in h5f["gt1l"]["class_ph"]:
            if c == 40:
                bathy_cnt += 1
        assert bathy_cnt == 90
        os.remove(filename)