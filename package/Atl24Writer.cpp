/*
 * Copyright (c) 2021, University of Washington
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the University of Washington nor the names of its
 *    contributors may be used to endorse or promote products derived from this
 *    software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF WASHINGTON AND CONTRIBUTORS
 * “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE UNIVERSITY OF WASHINGTON OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/******************************************************************************
 * INCLUDES
 ******************************************************************************/

#include "Atl24Writer.h"
#include "OsApi.h"
#include "EventLib.h"
#include "List.h"
#include "GeoDataFrame.h"
#include "FieldColumn.h"
#include "TimeLib.h"
#include "icesat2/Atl24DataFrame.h"

/******************************************************************************
 * STATIC DATA
 ******************************************************************************/

const char* Atl24Writer::OBJECT_TYPE = "Atl24Writer";
const char* Atl24Writer::LUA_META_NAME = "Atl24Writer";
const struct luaL_Reg Atl24Writer::LUA_META_TABLE[] = {
    {"write",       luaWriteFile},
    {NULL,          NULL}
};

const char* Atl24Writer::BEAMS[NUM_BEAMS] = {"gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"};

/******************************************************************************
 * LOCAL FUNCTIONS
 ******************************************************************************/

static void add_group(List<HdfLib::dataset_t>& datasets, const char* name)
{
    HdfLib::dataset_t group = {name, HdfLib::GROUP, RecordObject::INVALID_FIELD, NULL, 0} ;
    datasets.add(group);
}

static void add_variable(List<HdfLib::dataset_t>& datasets, const char* name, Field* field)
{
    long size = field->length() * field->getTypeSize();
    uint8_t* buffer = new uint8_t[size];
    field->serialize(buffer, size);
    HdfLib::dataset_t variable = {name, HdfLib::VARIABLE, static_cast<RecordObject::fieldType_t>(field->getEncodedType()), buffer, size};
    datasets.add(variable);
}

static void add_attribute(List<HdfLib::dataset_t>& datasets, const char* name, const char* value)
{
    long size = StringLib::size(value) + 1;
    char* buffer = StringLib::duplicate(value);
    HdfLib::dataset_t attribute = {name, HdfLib::ATTRIBUTE, RecordObject::STRING, reinterpret_cast<uint8_t*>(buffer), size};
    datasets.add(attribute);
}

static void add_attribute_double(List<HdfLib::dataset_t>& datasets, const char* name, const double value)
{
    double* buffer = new double[1];
    buffer[0] = value;
    HdfLib::dataset_t attribute = {name, HdfLib::ATTRIBUTE, RecordObject::DOUBLE, reinterpret_cast<uint8_t*>(buffer), 8};
    datasets.add(attribute);
}

/******************************************************************************
 * METHODS
 ******************************************************************************/

/*----------------------------------------------------------------------------
 * init
 *----------------------------------------------------------------------------*/
void Atl24Writer::init (void)
{
    // do nothing
}

/*----------------------------------------------------------------------------
 * luaCreate - create(<table of beams>)
 *----------------------------------------------------------------------------*/
int Atl24Writer::luaCreate (lua_State* L)
{
    Atl24DataFrame* _dataframes[NUM_BEAMS] = {NULL, NULL, NULL, NULL, NULL, NULL};

    try
    {
        /* Get Parmeters */
        int dataframe_table_index = 1;
        if(lua_istable(L, dataframe_table_index))
        {
            for(int i = 0; i < NUM_BEAMS; i++)
            {
                lua_getfield(L, dataframe_table_index, BEAMS[i]);
                if(!lua_isnil(L, -1))
                {
                    _dataframes[i] = dynamic_cast<Atl24DataFrame*>(getLuaObject(L, -1, GeoDataFrame::OBJECT_TYPE));
                }
                lua_pop(L, 1);
            }
        }

        /* Return Dispatch Object */
        return createLuaObject(L, new Atl24Writer(L, _dataframes));
    }
    catch(const RunTimeException& e)
    {
        mlog(e.level(), "Error creating %s: %s", LUA_META_NAME, e.what());
        return returnLuaStatus(L, false);
    }
}

/*----------------------------------------------------------------------------
 * Constructor
 *----------------------------------------------------------------------------*/
Atl24Writer::Atl24Writer(lua_State* L, Atl24DataFrame** _dataframes):
    LuaObject(L, OBJECT_TYPE, LUA_META_NAME, LUA_META_TABLE)
{
    for(int i = 0; i < NUM_BEAMS; i++)
    {
        dataframes[i] = _dataframes[i];
    }
}

/*----------------------------------------------------------------------------
 * Destructor
 *----------------------------------------------------------------------------*/
Atl24Writer::~Atl24Writer(void)
{
    for(int i = 0; i < NUM_BEAMS; i++)
    {
        if(dataframes[i])
        {
            dataframes[i]->releaseLuaObject();
        }
    }
}

/*----------------------------------------------------------------------------
 * writeFile
 *----------------------------------------------------------------------------*/
int Atl24Writer::luaWriteFile(lua_State* L)
{
    bool status;
    List<HdfLib::dataset_t> datasets;

    try
    {
        Atl24Writer* lua_obj = dynamic_cast<Atl24Writer*>(getLuaSelf(L, 1));
        const char* filename = getLuaString(L, 2);

        /* Add Beams */
        for(int i = 0; i < NUM_BEAMS; i++)
        {
            /* Get and Check DataFrame for Beam */
            Atl24DataFrame* df = lua_obj->dataframes[i];
            if(!df) continue;

            /* Create Beam Group */
            add_group(datasets, BEAMS[i]);

            /* Create Variable - class_ph */
            add_variable(datasets, "class_ph", &df->class_ph);
            add_attribute(datasets, "contentType", "modelResults");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "0 - unclassified, 1 - other, 40 - bathymetry, 41 - sea surface");
            add_attribute(datasets, "long_name", "Photon classification");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "scalar");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - confidence */
            add_variable(datasets, "confidence", &df->confidence);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "ensemble confidence score from 0.0 to 1.0 where larger numbers represent higher confidence in classification");
            add_attribute(datasets, "long_name", "Ensemble confidence");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "scalar");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - time_ns */
            FieldColumn<int64_t> delta_time;
            for(long i = 0; i < df->time_ns.length(); i++)
            {
                static const int64_t ATLAS_LEAP_SECONDS = 18; // optimization based on the time period of ATLAS data at the time of ATL24 generation (2025)
                int64_t value = (df->time_ns[i].nanoseconds / 1000000000.0) - (Icesat2Fields::ATLAS_SDP_EPOCH_GPS + TimeLib::GPS_EPOCH_START - ATLAS_LEAP_SECONDS);
                delta_time.append(value);
            }
            add_variable(datasets, "time_ns", &delta_time);
            add_attribute(datasets, "contentType", "physicalMeasurement");
            add_attribute(datasets, "coordinates", "lat_ph lon_ph");
            add_attribute(datasets, "description", "The transmit time of a given photon, measured in seconds from the ATLAS Standard Data Product Epoch. Note that multiple received photons associated with a single transmit pulse will have the same delta_time. The ATLAS Standard Data Products (SDP) epoch offset is defined within /ancillary_data/atlas_sdp_gps_epoch as the number of GPS seconds between the GPS epoch (1980-01-06T00:00:00.000000Z UTC) and the ATLAS SDP epoch. By adding the offset contained within atlas_sdp_gps_epoch to delta time parameters, the time in gps_seconds relative to the GPS epoch can be computed.");
            add_attribute(datasets, "long_name", "Elapsed GPS seconds");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "seconds since 2018-01-01");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - ellipse_h */
            add_variable(datasets, "ellipse_h", &df->ellipse_h);
            add_attribute(datasets, "contentType", "physicalMeasurement");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "Height of each received photon, relative to the WGS-84 ellipsoid including refraction correction. Note neither the geoid, ocean tide nor the dynamic atmosphere (DAC) corrections are applied to the ellipsoidal heights.");
            add_attribute(datasets, "long_name", "Photon WGS84 height");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "meters");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - index_ph */
            FieldColumn<int32_t>* index_ph = dynamic_cast<FieldColumn<int32_t>*>(df->getColumn("index_ph"));
            add_variable(datasets, "index_ph", index_ph);
            add_attribute(datasets, "contentType", "physicalMeasurement");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "0-based index of the photon in the ATL03 heights group");
            add_attribute(datasets, "long_name", "Photon index");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "scalar");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - index_seg */
            FieldColumn<int32_t>* index_seg = dynamic_cast<FieldColumn<int32_t>*>(df->getColumn("index_seg"));
            add_variable(datasets, "index_seg", index_seg);
            add_attribute(datasets, "contentType", "physicalMeasurement");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "0-based index of the photon in the ATL03 geolocation group");
            add_attribute(datasets, "long_name", "Segment index");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "scalar");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - invalid_kd */
            add_variable(datasets, "invalid_kd", &df->invalid_kd);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "Indicates that no data was available in the VIIRS Kd490 8-day cycle dataset at the time and location of the photon");
            add_attribute(datasets, "long_name", "Invalid Kd");
            add_attribute(datasets, "source", "VIIRS Kd490");
            add_attribute(datasets, "units", "boolean");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - invalid_wind_speed */
            add_variable(datasets, "invalid_wind_speed", &df->invalid_wind_speed);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "Indicates that ATL09 data was not able to be read to determine wind speed");
            add_attribute(datasets, "long_name", "Invalid wind speed");
            add_attribute(datasets, "source", "ATL09");
            add_attribute(datasets, "units", "boolean");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - lat_ph */
            add_variable(datasets, "lat_ph", &df->lat_ph);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lon_ph");
            add_attribute(datasets, "description", "Latitude of each received photon. Computed from the ECF Cartesian coordinates of the bounce point.");
            add_attribute(datasets, "long_name", "Latitude");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "degrees_north");
            add_attribute(datasets, "standard_name", "latitude");
            add_attribute_double(datasets, "valid_max", 90.0);
            add_attribute_double(datasets, "valid_min", -90.0);
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - lon_ph */
            add_variable(datasets, "lon_ph", &df->lon_ph);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph");
            add_attribute(datasets, "description", "Longitude of each received photon. Computed from the ECF Cartesian coordinates of the bounce point.");
            add_attribute(datasets, "long_name", "Longitude");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "degrees_east");
            add_attribute(datasets, "standard_name", "longitude");
            add_attribute_double(datasets, "valid_max", 180.0);
            add_attribute_double(datasets, "valid_min", -180.0);
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - low_confidence_flag */
            add_variable(datasets, "low_confidence_flag", &df->low_confidence_flag);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "There is low confidence that the photon classified as bathymetry is actually bathymetry");
            add_attribute(datasets, "long_name", "Low confidence bathymetry flag");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "boolean");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - night_flag */
            add_variable(datasets, "night_flag", &df->night_flag);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "The solar elevation was less than 5 degrees at the time and location of the photon");
            add_attribute(datasets, "long_name", "Night flag");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "boolean");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - ortho_h */
            add_variable(datasets, "ortho_h", &df->ortho_h);
            add_attribute(datasets, "contentType", "physicalMeasurement");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "Height of each received photon, relative to the geoid.");
            add_attribute(datasets, "long_name", "Orthometric height");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "meters");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - sensor_depth_exceeded */
            add_variable(datasets, "sensor_depth_exceeded", &df->sensor_depth_exceeded);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "The subaqueous photon is below the maximum depth detectable by the ATLAS sensor given the Kd of the water column");
            add_attribute(datasets, "long_name", "Sensor depth exceeded");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "boolean");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - sigma_thu */
            add_variable(datasets, "sigma_thu", &df->sigma_thu);
            add_attribute(datasets, "contentType", "physicalMeasurement");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "The combination of the aerial and subaqueous horizontal uncertainty for each received photon");
            add_attribute(datasets, "long_name", "Total horizontal uncertainty");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "meters");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - sigma_tvu */
            add_variable(datasets, "sigma_tvu", &df->sigma_tvu);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "The combination of the aerial and subaqueous vertical uncertainty for each received photon");
            add_attribute(datasets, "long_name", "Total vertical uncertainty");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "meters");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - surface_h */
            add_variable(datasets, "surface_h", &df->surface_h);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "The geoid corrected height of the sea surface at the detected photon");
            add_attribute(datasets, "long_name", "Sea surface orthometric height");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "meters");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - x_atc */
            add_variable(datasets, "x_atc", &df->x_atc);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "Along-track distance in a segment projected to the ellipsoid of the received photon, based on the Along-Track Segment algorithm.  Total along track distance can be found by adding this value to the sum of segment lengths measured from the start of the most recent reference groundtrack.");
            add_attribute(datasets, "long_name", "Distance from equator crossing");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "meters");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Create Variable - y_atc */
            add_variable(datasets, "y_atc", &df->y_atc);
            add_attribute(datasets, "contentType", "modelResult");
            add_attribute(datasets, "coordinates", "delta_time lat_ph lon_ph");
            add_attribute(datasets, "description", "Across-track distance projected to the ellipsoid of the received photon from the reference ground track.  This is based on the Along-Track Segment algorithm described in Section 3.1 of the ATBD.");
            add_attribute(datasets, "long_name", "Distance off RGT");
            add_attribute(datasets, "source", "ATL03");
            add_attribute(datasets, "units", "meters");
            datasets.add(HdfLib::PARENT_DATASET);

            /* Go Back to Parent Group */
            datasets.add(HdfLib::PARENT_DATASET);
        }

        /* Write HDF5 File */
        status = HdfLib::write(filename, datasets);
    }
    catch(const RunTimeException& e)
    {
        mlog(e.level(), "Error writing file: %s", e.what());
        status = false;
    }

    /* Clean Up */
    for(int i = 0; i < datasets.length(); i++)
    {
        delete [] datasets[i].data;
    }

    /* Return */
    return returnLuaStatus(L, status);
}
