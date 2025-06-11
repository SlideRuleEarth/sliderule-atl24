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
#include "icesat2/Icesat2Fields.h"

/******************************************************************************
 * STATIC DATA
 ******************************************************************************/

const char* Atl24Writer::OBJECT_TYPE = "Atl24Writer";
const char* Atl24Writer::LUA_META_NAME = "Atl24Writer";
const struct luaL_Reg Atl24Writer::LUA_META_TABLE[] = {
    {"write",       luaWriteFile},
    {NULL,          NULL}
};

const char* Atl24Writer::RELEASE = "02";

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

static void add_scalar(List<HdfLib::dataset_t>& datasets, const char* name, Field* field)
{
    long size = field->length() * field->getTypeSize();
    uint8_t* buffer = new uint8_t[size];
    field->serialize(buffer, size);
    HdfLib::dataset_t scalar = {name, HdfLib::SCALAR, static_cast<RecordObject::fieldType_t>(field->getEncodedType()), buffer, size};
    datasets.add(scalar);
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
    HdfLib::dataset_t attribute = {name, HdfLib::ATTRIBUTE, RecordObject::DOUBLE, reinterpret_cast<uint8_t*>(buffer), sizeof(value)};
    datasets.add(attribute);
}

static void add_attribute_int32(List<HdfLib::dataset_t>& datasets, const char* name, const int32_t value)
{
    int32_t* buffer = new int32_t[1];
    buffer[0] = value;
    HdfLib::dataset_t attribute = {name, HdfLib::ATTRIBUTE, RecordObject::INT32, reinterpret_cast<uint8_t*>(buffer), sizeof(value)};
    datasets.add(attribute);
}

static void add_attribute_int8(List<HdfLib::dataset_t>& datasets, const char* name, const int8_t value)
{
    int8_t* buffer = new int8_t[1];
    buffer[0] = value;
    HdfLib::dataset_t attribute = {name, HdfLib::ATTRIBUTE, RecordObject::INT32, reinterpret_cast<uint8_t*>(buffer), sizeof(value)};
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
    Icesat2Fields* _parms = NULL;
    Atl24DataFrame* _dataframes[NUM_BEAMS] = {NULL, NULL, NULL, NULL, NULL, NULL};
    Atl24Granule* _granule = NULL;

    try
    {
        int parms_index = 1;
        int dataframe_table_index = 2;
        int granule_index = 3;

        /* Get Parameters */
        _parms = dynamic_cast<Icesat2Fields*>(getLuaObject(L, parms_index, Icesat2Fields::OBJECT_TYPE));

        /* Get DataFrames */
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

        /* Get Granule */
        _granule = dynamic_cast<Atl24Granule*>(getLuaObject(L, granule_index, Atl24Granule::OBJECT_TYPE));

        /* Return Dispatch Object */
        return createLuaObject(L, new Atl24Writer(L, _parms, _dataframes, _granule));
    }
    catch(const RunTimeException& e)
    {
        mlog(e.level(), "Error creating %s: %s", LUA_META_NAME, e.what());
        for(int i = 0; i < NUM_BEAMS; i++)
        {
            if(_dataframes[i]) _dataframes[i]->releaseLuaObject();
        }
        if(_granule) _granule->releaseLuaObject();
        if(_parms) _parms->releaseLuaObject();
        return returnLuaStatus(L, false);
    }
}

/*----------------------------------------------------------------------------
 * Constructor
 *----------------------------------------------------------------------------*/
Atl24Writer::Atl24Writer(lua_State* L, Icesat2Fields* _parms, Atl24DataFrame** _dataframes, Atl24Granule* _granule):
    LuaObject(L, OBJECT_TYPE, LUA_META_NAME, LUA_META_TABLE),
    release(RELEASE),
    parms(_parms),
    granule(_granule)
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
    if(parms)
    {
        parms->releaseLuaObject();
    }

    for(int i = 0; i < NUM_BEAMS; i++)
    {
        if(dataframes[i])
        {
            dataframes[i]->releaseLuaObject();
        }
    }

    if(granule)
    {
        granule->releaseLuaObject();
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
        /* Get Self */
        Atl24Writer* lua_obj = dynamic_cast<Atl24Writer*>(getLuaSelf(L, 1));
        Icesat2Fields* parms = lua_obj->parms;
        Atl24Granule& granule = *lua_obj->granule;

        /* Get Filename */
        const char* filename = getLuaString(L, 2);

        /**********************/
        /* Create Beam Groups */
        /**********************/
        Atl24DataFrame* last_df = NULL;
        for(int i = 0; i < NUM_BEAMS; i++)
        {
            /* Get and Check DataFrame for Beam */
            Atl24DataFrame* df = lua_obj->dataframes[i];
            if(!df) continue;
            last_df = df;

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

        /* Check For At Least One Beam */
        if(last_df == NULL) throw RunTimeException(CRITICAL, RTE_FAILURE, "Attempted to write ATL24 file with no beams");

        /*******************************/
        /* Create Ancillary Data Group */
        /*******************************/
        add_group(datasets, "ancillary_data");

        /* Create Variable - atlas_sdp_gps_epoch */
        add_scalar(datasets, "atlas_sdp_gps_epoch", &granule["atlas_sdp_gps_epoch"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Number of GPS seconds between the GPS epoch (1980-01-06T00:00:00.000000Z UTC) and the ATLAS Standard Data Product (SDP) epoch (2018-01-01:T00.00.00.000000 UTC). Add this value to delta time parameters to compute full gps_seconds (relative to the GPS epoch) for each data point.");
        add_attribute(datasets, "long_name", "ATLAS Epoch Offset");
        add_attribute(datasets, "source", "Operations");
        add_attribute(datasets, "units", "seconds since 1980-01-06T00:00:00.000000Z");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - data_end_utc */
        add_scalar(datasets, "data_end_utc", &granule["data_start_utc"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "UTC (in CCSDS-A format) of the last data point within the granule.");
        add_attribute(datasets, "long_name", "End UTC Time of Granule (CCSDS-A, Actual)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - data_start_utc */
        add_scalar(datasets, "data_start_utc", &granule["data_start_utc"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "UTC (in CCSDS-A format) of the first data point within the granule.");
        add_attribute(datasets, "long_name", "Start UTC Time of Granule (CCSDS-A, Actual)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - end_cycle */
        add_scalar(datasets, "end_cycle", last_df->getMetaData("cycle"));
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The ending cycle number associated with the data contained within this granule. The cycle number is the counter of the number of 91-day repeat cycles completed by the mission.");
        add_attribute(datasets, "long_name", "Ending Cycle");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - end_delta_time */
        add_scalar(datasets, "end_delta_time", &granule["end_delta_time"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Number of GPS seconds since the ATLAS SDP epoch at the last data point in the file. The ATLAS Standard Data Products (SDP) epoch offset is defined within /ancillary_data/atlas_sdp_gps_epoch as the number of GPS seconds between the GPS epoch (1980-01-06T00:00:00.000000Z UTC) and the ATLAS SDP epoch. By adding the offset contained within atlas_sdp_gps_epoch to delta time parameters, the time in gps_seconds relative to the GPS epoch can be computed.");
        add_attribute(datasets, "long_name", "ATLAS End Time (Actual)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "seconds since 2018-01-01");
        add_attribute(datasets, "standard_name", "time");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - end_geoseg */
        add_scalar(datasets, "end_geoseg", &granule["end_geoseg"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The ending geolocation segment number associated with the data contained within this granule. ICESat granule geographic regions are further refined by geolocation segments. During the geolocation process, a geolocation segment is created approximately every 20m from the start of the orbit to the end.  The geolocation segments help align the ATLAS strong a weak beams and provide a common segment length for the L2 and higher products. The geolocation segment indices differ slightly from orbit-to-orbit because of the irregular shape of the Earth. The geolocation segment indices on ATL01 and ATL02 are only approximate because beams have not been aligned at the time of their creation.");
        add_attribute(datasets, "long_name", "Ending Geolocation Segment");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - end_gpssow */
        add_scalar(datasets, "end_gpssow", &granule["end_gpssow"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "GPS seconds-of-week of the last data point in the granule.");
        add_attribute(datasets, "long_name", "Ending GPS SOW of Granule (Actual)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "seconds");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - end_gpsweek */
        add_scalar(datasets, "end_gpsweek", &granule["end_gpsweek"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "GPS week number of the last data point in the granule.");
        add_attribute(datasets, "long_name", "Ending GPSWeek of Granule (Actual)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "weeks from 1980-01-06");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - end_orbit */
        add_scalar(datasets, "end_orbit", &granule["orbit_number"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The ending orbit number associated with the data contained within this granule. The orbit number increments each time the spacecraft completes a full orbit of the Earth.");
        add_attribute(datasets, "long_name", "Ending Orbit Number");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - end_region */
        add_scalar(datasets, "end_region", last_df->getMetaData("region"));
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The ending product-specific region number associated with the data contained within this granule. ICESat-2 data products are separated by geographic regions. The data contained within a specific region are the same for ATL01 and ATL02. ATL03 regions differ slightly because of different geolocation segment locations caused by the irregular shape of the Earth. The region indices for other products are completely independent.");
        add_attribute(datasets, "long_name", "Ending Region");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - end_rgt */
        add_scalar(datasets, "end_rgt", last_df->getMetaData("rgt"));
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The ending reference groundtrack (RGT) number associated with the data contained within this granule. There are 1387 reference groundtrack in the ICESat-2 repeat orbit. The reference groundtrack increments each time the spacecraft completes a full orbit of the Earth and resets to 1 each time the spacecraft completes a full cycle.");
        add_attribute(datasets, "long_name", "Ending Reference Groundtrack");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - granule_end_utc */
        add_scalar(datasets, "granule_end_utc", &granule["granule_end_utc"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Requested end time (in UTC CCSDS-A) of this granule.");
        add_attribute(datasets, "long_name", "End UTC Time of Granule (CCSDS-A, Requested)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - granule_start_utc */
        add_scalar(datasets, "granule_start_utc", &granule["granule_start_utc"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Requested start time (in UTC CCSDS-A) of this granule.");
        add_attribute(datasets, "long_name", "Start UTC Time of Granule (CCSDS-A, Requested)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - release */
        add_scalar(datasets, "release", &lua_obj->release);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Release number of the granule. The release number is incremented when the software or ancillary data used to create the granule has been changed.");
        add_attribute(datasets, "long_name", "Release Number");
        add_attribute(datasets, "source", "Operations");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - resource */
        add_scalar(datasets, "resource", last_df->getMetaData("granule"));
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "ATL03 granule used to produce this granule");
        add_attribute(datasets, "long_name", "ATL03 Resource");
        add_attribute(datasets, "source", "Operations");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - sliderule_version */
        add_scalar(datasets, "sliderule_version", &parms->slideruleVersion);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Version of SlideRule software used to generate this granule");
        add_attribute(datasets, "long_name", "SlideRule Version");
        add_attribute(datasets, "source", "Operations");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - sliderule_commit */
        add_scalar(datasets, "sliderule_commit", &parms->buildInformation);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Git commit ID (https://github.com/SlideRuleEarth/sliderule.git) of SlideRule software used to generate this granule");
        add_attribute(datasets, "long_name", "SlideRule Commit");
        add_attribute(datasets, "source", "Operations");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - sliderule_environment */
        add_scalar(datasets, "sliderule_environment", &parms->environmentVersion);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Git commit ID (https://github.com/SlideRuleEarth/sliderule.git) of SlideRule environment used to generate this granule");
        add_attribute(datasets, "long_name", "SlideRule Environment");
        add_attribute(datasets, "source", "Operations");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - start_cycle */
        add_scalar(datasets, "start_cycle", last_df->getMetaData("cycle"));
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The starting cycle number associated with the data contained within this granule. The cycle number is the counter of the number of 91-day repeat cycles completed by the mission.");
        add_attribute(datasets, "long_name", "Starting Cycle");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - start_delta_time */
        add_scalar(datasets, "start_delta_time", &granule["start_delta_time"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Number of GPS seconds since the ATLAS SDP epoch at the first data point in the file. The ATLAS Standard Data Products (SDP) epoch offset is defined within /ancillary_data/atlas_sdp_gps_epoch as the number of GPS seconds between the GPS epoch (1980-01-06T00:00:00.000000Z UTC) and the ATLAS SDP epoch. By adding the offset contained within atlas_sdp_gps_epoch to delta time parameters, the time in gps_seconds relative to the GPS epoch can be computed.");
        add_attribute(datasets, "long_name", "ATLAS Start Time (Actual)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "seconds since 2018-01-01");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - start_geoseg */
        add_scalar(datasets, "start_geoseg", &granule["start_geoseg"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The starting geolocation segment number associated with the data contained within this granule. ICESat granule geographic regions are further refined by geolocation segments. During the geolocation process, a geolocation segment is created approximately every 20m from the start of the orbit to the end.  The geolocation segments help align the ATLAS strong a weak beams and provide a common segment length for the L2 and higher products. The geolocation segment indices differ slightly from orbit-to-orbit because of the irregular shape of the Earth. The geolocation segment indices on ATL01 and ATL02 are only approximate because beams have not been aligned at the time of their creation.");
        add_attribute(datasets, "long_name", "Starting Geolocation Segment");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - start_gpssow */
        add_scalar(datasets, "start_gpssow", &granule["start_gpssow"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "GPS seconds-of-week of the first data point in the granule.");
        add_attribute(datasets, "long_name", "Start GPS SOW of Granule (Actual)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "seconds");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - start_gpsweek */
        add_scalar(datasets, "start_gpsweek", &granule["start_gpsweek"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "GPS week number of the first data point in the granule.");
        add_attribute(datasets, "long_name", "Start GPSWeek of Granule (Actual)");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "weeks from 1980-01-06");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - start_orbit */
        add_scalar(datasets, "start_orbit", &granule["orbit_number"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The starting orbit number associated with the data contained within this granule. The orbit number increments each time the spacecraft completes a full orbit of the Earth.");
        add_attribute(datasets, "long_name", "Starting Orbit Number");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - start_region */
        add_scalar(datasets, "start_region", last_df->getMetaData("region"));
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The starting product-specific region number associated with the data contained within this granule. ICESat-2 data products are separated by geographic regions. The data contained within a specific region are the same for ATL01 and ATL02. ATL03 regions differ slightly because of different geolocation segment locations caused by the irregular shape of the Earth. The region indices for other products are completely independent.");
        add_attribute(datasets, "long_name", "Starting Region");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - start_rgt */
        add_scalar(datasets, "start_rgt", last_df->getMetaData("rgt"));
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "The starting reference groundtrack (RGT) number associated with the data contained within this granule. There are 1387 reference groundtrack in the ICESat-2 repeat orbit. The reference groundtrack increments each time the spacecraft completes a full orbit of the Earth and resets to 1 each time the spacecraft completes a full cycle.");
        add_attribute(datasets, "long_name", "Starting Reference Groundtrack");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - version */
        add_scalar(datasets, "version", &granule["version"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "Version number of this granule within the release. It is a sequential number corresponding to the number of times the granule has been reprocessed for the current release.");
        add_attribute(datasets, "long_name", "Version");
        add_attribute(datasets, "source", "Operations");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Go Back to Parent Group */
        datasets.add(HdfLib::PARENT_DATASET);

        /***************************/
        /* Create Orbit Info Group */
        /***************************/
        add_group(datasets, "orbit_info");

        /* Create Variable - crossing_time */
        add_scalar(datasets, "crossing_time", &granule["crossing_time"]);
        add_attribute(datasets, "contentType", "referenceInformation");
        add_attribute(datasets, "description", "The time, in seconds since the ATLAS SDP GPS Epoch, at which the ascending node crosses the equator. The ATLAS Standard Data Products (SDP) epoch offset is defined within /ancillary_data/atlas_sdp_gps_epoch as the number of GPS seconds between the GPS epoch (1980-01-06T00:00:00.000000Z UTC) and the ATLAS SDP epoch. By adding the offset contained within atlas_sdp_gps_epoch to delta time parameters, the time in gps_seconds relative to the GPS epoch can be computed.");
        add_attribute(datasets, "long_name", "Ascending Node Crossing Time");
        add_attribute(datasets, "source", "POD/PPD");
        add_attribute(datasets, "units", "seconds since 2018-01-01");
        add_attribute(datasets, "standard_name", "time");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - cycle_number */
        add_scalar(datasets, "cycle_number", last_df->getMetaData("cycle"));
        add_attribute(datasets, "contentType", "referenceInformation");
        add_attribute(datasets, "description", "Tracks the number of 91-day cycles in the mission, beginning with 01.  A unique orbit number can be determined by subtracting 1 from the cycle_number, multiplying by 1387 and adding the rgt value.");
        add_attribute(datasets, "long_name", "Cycle Number");
        add_attribute(datasets, "source", "POD/PPD");
        add_attribute(datasets, "units", "counts");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - lan */
        add_scalar(datasets, "lan", &granule["lan"]);
        add_attribute(datasets, "contentType", "referenceInformation");
        add_attribute(datasets, "description", "Longitude at the ascending node crossing.");
        add_attribute(datasets, "long_name", "Ascending Node Longitude");
        add_attribute(datasets, "source", "POD/PPD");
        add_attribute(datasets, "units", "degrees_east");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - orbit_number */
        add_scalar(datasets, "orbit_number", &granule["orbit_number"]);
        add_attribute(datasets, "contentType", "referenceInformation");
        add_attribute(datasets, "description", "Unique identifying number for each planned ICESat-2 orbit.");
        add_attribute(datasets, "long_name", "Orbit Number");
        add_attribute(datasets, "source", "Operations");
        add_attribute(datasets, "units", "1");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - rgt */
        add_scalar(datasets, "rgt", last_df->getMetaData("rgt"));
        add_attribute(datasets, "contentType", "referenceInformation");
        add_attribute(datasets, "description", "The reference ground track (RGT) is the track on the earth at which a specified unit vector within the observatory is pointed. Under nominal operating conditions, there will be no data collected along the RGT, as the RGT is spanned by GT2L and GT2R.  During slews or off-pointing, it is possible that ground tracks may intersect the RGT. The ICESat-2 mission has 1387 RGTs.");
        add_attribute(datasets, "long_name", "Reference Ground Track");
        add_attribute(datasets, "source", "POD/PPD");
        add_attribute(datasets, "units", "counts");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - sc_orient */
        add_scalar(datasets, "sc_orient", &granule["sc_orient"]);
        add_attribute(datasets, "contentType", "referenceInformation");
        add_attribute(datasets, "description", "This parameter tracks the spacecraft orientation between forward, backward and transitional flight modes. ICESat-2 is considered to be flying forward when the weak beams are leading the strong beams; and backward when the strong beams are leading the weak beams. ICESat-2 is considered to be in transition while it is maneuvering between the two orientations. Science quality is potentially degraded while in transition mode.");
        add_attribute(datasets, "long_name", "Spacecraft Orientation");
        add_attribute(datasets, "source", "POD/PPD");
        add_attribute(datasets, "units", "1");
        add_attribute(datasets, "flag_meanings", "backward forward transition");
        add_attribute(datasets, "flag_values", "0, 1, 2");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - sc_orient_time */
        add_scalar(datasets, "sc_orient_time", &granule["sc_orient_time"]);
        add_attribute(datasets, "contentType", "referenceInformation");
        add_attribute(datasets, "description", "The time of the last spacecraft orientation change between forward, backward and transitional flight modes, expressed in seconds since the ATLAS SDP GPS Epoch. ICESat-2 is considered to be flying forward when the weak beams are leading the strong beams; and backward when the strong beams are leading the weak beams. ICESat-2 is considered to be in transition while it is maneuvering between the two orientations. Science quality is potentially degraded while in transition mode. The ATLAS Standard Data Products (SDP) epoch offset is defined within /ancillary_data/atlas_sdp_gps_epoch as the number of GPS seconds between the GPS epoch (1980-01-06T00:00:00.000000Z UTC) and the ATLAS SDP epoch. By adding the offset contained within atlas_sdp_gps_epoch to delta time parameters, the time in gps_seconds relative to the GPS epoch can be computed.");
        add_attribute(datasets, "long_name", "Time of Last Spacecraft Orientation Change");
        add_attribute(datasets, "source", "POD/PPD");
        add_attribute(datasets, "units", "seconds since 2018-01-01");
        add_attribute(datasets, "standard_name", "time");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Go Back to Parent Group */
        datasets.add(HdfLib::PARENT_DATASET);

        /*************************/
        /* Create Metadata Group */
        /*************************/
        add_group(datasets, "metadata");

        /* Create Variable - sliderule */
        add_scalar(datasets, "sliderule", &granule["sliderule"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "sliderule server and request information");
        add_attribute(datasets, "long_name", "SlideRule MetaData");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "json");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - sliderule */
        add_scalar(datasets, "profile", &granule["profile"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "runtimes of the various algorithms");
        add_attribute(datasets, "long_name", "Algorithm RunTimes");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "json");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - sliderule */
        add_scalar(datasets, "stats", &granule["stats"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "granule level statistics");
        add_attribute(datasets, "long_name", "Granule Metrics");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "json");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Create Variable - sliderule */
        add_scalar(datasets, "extent", &granule["extent"]);
        add_attribute(datasets, "contentType", "auxiliaryInformation");
        add_attribute(datasets, "description", "geospatial and temporal extents");
        add_attribute(datasets, "long_name", "Query MetaData");
        add_attribute(datasets, "source", "Derived");
        add_attribute(datasets, "units", "json");
        datasets.add(HdfLib::PARENT_DATASET);

        /* Go Back to Parent Group */
        datasets.add(HdfLib::PARENT_DATASET);

        /*******************/
        /* Write HDF5 File */
        /*******************/
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
