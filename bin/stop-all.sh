#!/usr/bin/env bash
#
# (C) Copyright IBM Corp. 2015, 2016
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# using environment variable to find tiara home directory
if [ -z "$TIARA_HOME" ]; then echo "TIARA_HOME is NOT set"; else echo "TIARA_HOME defined as '$TIARA_HOME'"; fi


echo "========= Stopping Decahose =========="

$TIARA_HOME/bin/stop-pollDecahose.sh

echo "========= Stopping Word2Vec model Generation =========="

$TIARA_HOME/bin/stop-word2vec.sh

echo "========= Stopping Decahose =========="

$TIARA_HOME/bin/stop-decahose.sh

echo "========= Stopping REST API =========="

$TIARA_HOME/bin/stop-restapi.sh
