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

            add_variable(datasets, "confidence", &df->confidence);
            add_variable(datasets, "time_ns", &df->time_ns); // TODO: this will need to be converted into delta_time
            add_variable(datasets, "ellipse_h", &df->ellipse_h);
//            add_variable(datasets, "index_ph", &df->index_ph);
//            add_variable(datasets, "index_seg", &df->index_seg);
            add_variable(datasets, "invalid_kd", &df->invalid_kd);
            add_variable(datasets, "invalid_wind_speed", &df->invalid_wind_speed);
            add_variable(datasets, "lat_ph", &df->lat_ph);
            add_variable(datasets, "lon_ph", &df->lon_ph);
            add_variable(datasets, "low_confidence_flag", &df->low_confidence_flag);
            add_variable(datasets, "night_flag", &df->night_flag);
            add_variable(datasets, "ortho_h", &df->ortho_h);
            add_variable(datasets, "sensor_depth_exceeded", &df->sensor_depth_exceeded);
            add_variable(datasets, "sigma_thu", &df->sigma_thu);
            add_variable(datasets, "sigma_tvu", &df->sigma_tvu);
            add_variable(datasets, "surface_h", &df->surface_h);
            add_variable(datasets, "x_atc", &df->x_atc);
            add_variable(datasets, "y_atc", &df->y_atc);

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
