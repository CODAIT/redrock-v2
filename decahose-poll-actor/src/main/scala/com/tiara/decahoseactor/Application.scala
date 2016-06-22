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
package com.tiara.decahoseactor

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}

/**
 * Created by barbaragomes on 4/21/16.
 */
object Application extends App{

  /* Sends message to decahose actor so it can start downloading files */
  implicit val system = ActorSystem("Decahose-Producer")
  val pollingDecahoseData = system.actorOf(PollDecahoseData.props)
  pollingDecahoseData ! PollDecahoseData.StartDownloadingData

}

object Config {
  // Global Application configuration
  val appConf: Config = ConfigFactory.load("tiara-app").getConfig("tiara")
  val processorConf = appConf.getConfig("decahose-processor")
}
