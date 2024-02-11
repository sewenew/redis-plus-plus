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

#include "sw/redis++/pipeline.h"
#include "shards.h"

namespace sw {

namespace redis {

std::vector<ReplyUPtr> PipelineImpl::exec(Connection &connection, std::size_t cmd_num) {
    std::vector<ReplyUPtr> replies;
    size_t total_cmd = cmd_num;
    while (cmd_num > 0) {
        try {
            replies.push_back(connection.recv(false));
        } catch (const MovedError & e){
            throw PipelineMovedError(e, total_cmd - cmd_num);
        } catch (const AskError & e){
            throw PipelineAskError(e, total_cmd - cmd_num);
        } catch (const SlotUncoveredError & e){
            throw PipelineSlotUncoveredError(e, total_cmd - cmd_num);
        }
        --cmd_num;
    }

    return replies;
}

}

}
