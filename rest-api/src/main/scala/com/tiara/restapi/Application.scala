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

package com.tiara.restapi

import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.{ActorSystem, Props}
import akka.io.IO
import org.apache.spark.Logging
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

/**
 * Created by barbaragomes on 4/1/16.
 */
object Application extends App with Logging {

  implicit val system = ActorSystem("restapi-actor")
  val monitorModels = system.actorOf(UpdateWord2VecModel.props)
  monitorModels ! UpdateWord2VecModel.StartMonitoringWord2VecModels

  val service = system.actorOf(Props[TiaraServiceActor], "tiara-restapi")

  implicit val timeout = Timeout(800.seconds)
  val bindIP = Config.restapi.getString("bind-ip")
  val bindPort = Config.restapi.getInt("bind-port")
  IO(Http) ? Http.Bind(service, interface = bindIP, port = bindPort)
  logInfo(s"REST API started. Binding IP: $bindIP --> Binding Port: $bindPort")

}

object Config {
  // Global Application configuration
  val appConf: Config = ConfigFactory.load("tiara-app").getConfig("tiara")
  val restapi = appConf.getConfig("rest-api")
}

