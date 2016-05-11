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

# using environment variable to find Spark home directory
if [ -z "$SPARK_HOME" ]; then echo "SPARK_HOME is NOT set"; else echo "SPARK_HOME defined as '$SPARK_HOME'"; fi
# using environment variable to find tiara home directory
if [ -z "$TIARA_HOME" ]; then echo "TIARA_HOME is NOT set"; else echo "TIARA_HOME defined as '$TIARA_HOME'"; fi


# generates the new .jar considering new configurations.
# Run this command separated on cluster, before push code to all nodes. Comment it out on cluster
echo " ==========  Compiling code and generating .jar ============"
sbt 'project tiara-word2vec' assembly

# Changes the location where spark is being launched so it creates its own metastore_db
cd $TIARA_HOME/word2vec-models

echo "============ Running Word2vec =============="
#run program on cluster
HOSTNAME="$(/bin/hostname -f)"
nohup $SPARK_HOME/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file://$TIARA_HOME/conf/log4j.properties" --driver-memory 3g --num-executors 4 --master spark://$HOSTNAME:7077 --class com.tiara.word2vec.Application $TIARA_HOME/word2vec-models/target/scala-2.10/tiara-word2vec-model.jar > $TIARA_HOME/word2vec-models/nohup-word2vec.out&

echo "======== Word2vec started. Check nohup-Word2vec.out =============="
