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
