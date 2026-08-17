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

#include <math.h>
#include <float.h>

#include "atl24.h"
#include "ensemble.h"
#include "xgboost.h"
#include "photon.h"

#include "OsApi.h"
#include "TimeLib.h"
#include "FieldElement.h"
#include "Icesat2Parameters.h"
#include "BathyDataFrame.h"
#include "Atl24Runner.h"

using namespace ATL24::main_pipeline;

/******************************************************************************
 * DATA
 ******************************************************************************/

const char* Atl24Runner::LUA_META_NAME = "Atl24Runner";
const struct luaL_Reg Atl24Runner::LUA_META_TABLE[] = {
    {NULL,          NULL}
};

/******************************************************************************
 * METHODS
 ******************************************************************************/

 /*----------------------------------------------------------------------------
 * luaCreate - create(<parms>)
 *----------------------------------------------------------------------------*/
int Atl24Runner::luaCreate (lua_State* L)
{
    Icesat2Parameters* _parms = NULL;

    try
    {
        _parms = dynamic_cast<Icesat2Parameters*>(getLuaObject(L, 1, Icesat2Parameters::OBJECT_TYPE));
        const long _serialize_threshold = getLuaInteger(L, 2, true, DEFAULT_SERIALIZE_THRESHOLD);
        return createLuaObject(L, new Atl24Runner(L, _parms, _serialize_threshold));
    }
    catch(const RunTimeException& e)
    {
        if(_parms) _parms->releaseLuaObject();
        mlog(e.level(), "Error creating %s: %s", OBJECT_TYPE, e.what());
        return returnLuaStatus(L, false);
    }
}

/*----------------------------------------------------------------------------
 * Constructor
 *----------------------------------------------------------------------------*/
Atl24Runner::Atl24Runner (lua_State* L, Icesat2Parameters* _parms, long _serialize_threshold):
    GeoDataFrame::FrameRunner(L, LUA_META_NAME, LUA_META_TABLE),
    parms(_parms),
    serializeThreshold(_serialize_threshold)
{
}

/*----------------------------------------------------------------------------
 * Destructor
 *----------------------------------------------------------------------------*/
Atl24Runner::~Atl24Runner (void)
{
    if(parms) parms->releaseLuaObject();
}

/*----------------------------------------------------------------------------
 * run
 *----------------------------------------------------------------------------*/
bool Atl24Runner::run (GeoDataFrame* dataframe)
{
    bool status = true;
    ATL24::ensemble::Params ensemble_params;
    FString model_filename("%s/atl24.tgz", CONFDIR);

    // cast dataframe to ATL24 specific dataframe
    BathyDataFrame& df = *dynamic_cast<BathyDataFrame*>(dataframe);

    // create new columns
    FieldColumn<int>* class_ph = new FieldColumn<int>;

    // determine serialization
    const bool serialize = df.length() > serializeThreshold;
    mlog(INFO, "Running classifier on spot %d in %s mode", df.spot.value, serialize ? "serial" : "parallel");

    try
    {
        // convert dataframe to algorithm input structure
        vector<ATL24::photon::Photon> p(df.length());
        for(size_t i = 0; i < static_cast<size_t>(df.length()); ++i)
        {
            // only the below members of the structure are used
            p[i].gps_seconds    = TimeLib::sysex2gpstime(df.time_ns[i]);
            p[i].lat_ph         = df.lat_ph[i];
            p[i].lon_ph         = df.lon_ph[i];
            p[i].x_atc          = df.x_atc[i];
            p[i].h_ph           = df.ellipse_h[i];
            p[i].geoid          = df.ellipse_h[i] - df.geoid_corr_h[i];
            p[i].quality_ph     = df.quality_ph[i];
            p[i].spot           = df.spot.value;
        }

        // execute classifier
        if(serialize) experiment.lock();
        const ATL24::xgboost::ClassificationResult results = ATL24::main_pipeline::classify (p, model_filename.c_str(), true, ensemble_params);
        if(serialize) experiment.unlock();
        for(const int& label: results.labels)
        {
            // add class_ph
            class_ph->append(label);
        }
    }
    catch(const std::exception& e)
    {
        status = false;
        mlog(CRITICAL, "Failed to run classifier on %s spot %d: %s", df.granule.value.c_str(), df.spot.value, e.what());
    }


    // add columns to dataframe
    df.addExistingColumn("class_ph", class_ph, "photon classification");

    // return success
    return status;
}
