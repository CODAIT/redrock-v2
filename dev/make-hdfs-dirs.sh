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

TARGETHOST="localhost:9000" # change to point to your namenode
ARRAY=( 
	'/data/rawTweets/' 
	'/tiara/decahose/streaming' 
	'/tiara/en' 
	'/tiara/toks' 
	'/tiara/debug' 
	'/tiara/models/daily'
)
ELEMENTS=${#ARRAY[@]}

# Make paths on hdfs
for (( i=0;i<$ELEMENTS;i++)); do
	FULLPATH=hdfs://$TARGETHOST${ARRAY[${i}]}
	hdfs dfs -mkdir -p $FULLPATH
    echo Created folder: $FULLPATH
done 

