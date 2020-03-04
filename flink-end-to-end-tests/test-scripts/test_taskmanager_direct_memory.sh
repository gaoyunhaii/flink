#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(dirname "$0")"/common.sh

MORE_TMS=$1
TOTAL_RECORDS=$2

echo "MORE_TMS = ${MORE_TMS}, max count = ${TOTAL_RECORDS}"

TEST=flink-taskmanager-direct-memory-test
TEST_PROGRAM_NAME=TaskManagerDirectMemoryTestProgram
TEST_PROGRAM_JAR=${END_TO_END_DIR}/$TEST/target/$TEST_PROGRAM_NAME.jar

set_config_key "akka.ask.timeout" "60 s"
set_config_key "web.timeout" "60000"

set_config_key "taskmanager.memory.process.size" "1024m" # 1024Mb x 5TMs = 5Gb total heap

set_config_key "taskmanager.memory.managed.size" "8" # 8Mb
set_config_key "taskmanager.memory.network.min" "192mb"
set_config_key "taskmanager.memory.network.max" "192mb"
set_config_key "taskmanager.memory.jvm-metaspace.size" "64m"

set_config_key "taskmanager.numberOfTaskSlots" "20" # 20 slots per TM
set_config_key "taskmanager.network.netty.num-arenas" "1"
set_config_key "taskmanager.memory.framework.off-heap.size" "20m"

start_cluster # this also starts 1TM
start_taskmanagers ${MORE_TMS} # 1TM + 4TM = 5TM a 20 slots = 100 slots

function check() {
  while true;do
    jps | grep TaskManagerRunner | while read a b;do echo $a $b;jmap -histo:live $a | egrep -i "DirectArena|Chunk";jstack $a | grep -i "Flink Netty";done;sleep 5;printf "\n\n";
  done
}

check &
PID=$!

echo "pid is $PID"
# This call will result in a deployment with state meta data of 100 x 100 x 40 union states x each 40 entries.
# We can scale up the numbers to make the test even heavier.
$FLINK_DIR/bin/flink run ${TEST_PROGRAM_JAR} \
--test.map_parallelism $(echo "${MORE_TMS} * 20" | bc) \
--test.reduce_parallelism 20 \
--test.rate 10000000 \
--test.record_length 2048 \
--test.total_records ${TOTAL_RECORDS}
kill -9 $PID
