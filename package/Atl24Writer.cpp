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
                _dataframes[i] = dynamic_cast<Atl24DataFrame*>(getLuaObject(L, -1, Atl24DataFrame::OBJECT_TYPE));
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

    try
    {
        Atl24Writer* lua_obj = dynamic_cast<Atl24Writer*>(getLuaSelf(L, 1));
        const char* filename = getLuaString(L, 2);

        HdfLib::dataset_t gt1l_group = {"gt1l", HdfLib::GROUP, RecordObject::INVALID_FIELD, NULL, 0};
        HdfLib::dataset_t gt1r_group = {"gt1r", HdfLib::GROUP, RecordObject::INVALID_FIELD, NULL, 0};
        HdfLib::dataset_t gt2l_group = {"gt2l", HdfLib::GROUP, RecordObject::INVALID_FIELD, NULL, 0};
        HdfLib::dataset_t gt2r_group = {"gt2r", HdfLib::GROUP, RecordObject::INVALID_FIELD, NULL, 0};
        HdfLib::dataset_t gt3l_group = {"gt3l", HdfLib::GROUP, RecordObject::INVALID_FIELD, NULL, 0};
        HdfLib::dataset_t gt3r_group = {"gt3r", HdfLib::GROUP, RecordObject::INVALID_FIELD, NULL, 0};

        List<HdfLib::dataset_t> datasets({
            gt1l_group,
            HdfLib::PARENT_DATASET,
            gt1r_group,
            HdfLib::PARENT_DATASET,
            gt2l_group,
            HdfLib::PARENT_DATASET,
            gt2r_group,
            HdfLib::PARENT_DATASET,
            gt3l_group,
            HdfLib::PARENT_DATASET,
            gt3r_group,
            HdfLib::PARENT_DATASET
        });

        status = HdfLib::write(filename, datasets);
    }
    catch(const RunTimeException& e)
    {
        mlog(e.level(), "Error writing file: %s", e.what());
        status = false;
    }

    return returnLuaStatus(L, status);
}
