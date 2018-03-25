/**************************************************************************
   Copyright (c) 2017 sewenew

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 *************************************************************************/

#include "command.h"
#include <cassert>

namespace sw {

namespace redis {

namespace cmd {

// STRING commands.

void set(Connection &connection,
            const StringView &key,
            const StringView &val,
            const std::chrono::milliseconds &ttl,
            UpdateType type) {
    Connection::CmdArgs args;
    args << "SET" << key << val;

    std::string ttl_options;
    if (ttl > std::chrono::milliseconds(0)) {
        ttl_options = std::to_string(ttl.count());
        args << "PX" << ttl_options;
    }

    detail::set_update_type(args, type);

    connection.send(args);
}

// LIST commands.

void linsert(Connection &connection,
                const StringView &key,
                InsertPosition position,
                const StringView &pivot,
                const StringView &val) {
    std::string pos;
    switch (position) {
    case InsertPosition::BEFORE:
        pos = "BEFORE";
        break;

    case InsertPosition::AFTER:
        pos = "AFTER";
        break;

    default:
        assert(false);
    }

    connection.send("LINSERT %b %s %b %b",
                    key.data(), key.size(),
                    pos.c_str(),
                    pivot.data(), pivot.size(),
                    val.data(), val.size());
}

void geodist(Connection &connection,
                const StringView &key,
                const StringView &member1,
                const StringView &member2,
                GeoUnit unit) {
    Connection::CmdArgs args;
    args << "GEODIST" << key << member1 << member2;

    detail::set_geo_unit(args, unit);

    connection.send(args);
}

namespace detail {

void set_update_type(Connection::CmdArgs &args, UpdateType type) {
    switch (type) {
    case UpdateType::EXIST:
        args << "XX";
        break;

    case UpdateType::NOT_EXIST:
        args << "NX";
        break;

    case UpdateType::ALWAYS:
        // Do nothing.
        break;

    default:
        throw RException("Unknown update type.");
    }
}

void set_aggregation_type(Connection::CmdArgs &args, Aggregation aggr) {
    args << "AGGREGATE";

    switch (aggr) {
    case Aggregation::SUM:
        args << "SUM";
        break;

    case Aggregation::MIN:
        args << "MIN";
        break;

    case Aggregation::MAX:
        args << "MAX";
        break;

    default:
        throw RException("Unknown aggregation type.");
    }
}

void set_geo_unit(Connection::CmdArgs &args, GeoUnit unit) {
    switch (unit) {
    case GeoUnit::M:
        args << "M";
        break;

    case GeoUnit::KM:
        args << "KM";
        break;

    case GeoUnit::MI:
        args << "MI";
        break;

    case GeoUnit::FT:
        args << "FT";
        break;

    default:
        throw RException("Unknown geo unit type");
        break;
    }
}

}

}

}

}
