/**
 * (C) Copyright IBM Corp. 2015, 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.tiara.word2vec

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.Logging

/**
 * Created by barbaragomes on 4/15/16.
 */
object Application extends App with Logging {

  // Compute historical model first
  if (Config.word2vecConf.getBoolean("historical.enabled")) {
    Word2VecModelComputation.computeHistoricalWord2Vec()
  }

  implicit val system = ActorSystem("Word2Vec-Actor")
  val updateWord2VecDaily = system.actorOf(W2vScheduler.props)
  updateWord2VecDaily ! W2vScheduler.StartW2VModelGeneration
}

object Config {
  // Global Application configuration
  val appConf: Config = ConfigFactory.load("tiara-app").getConfig("tiara")
  val word2vecConf = appConf.getConfig("word-2-vec")
}
