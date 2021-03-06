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

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import akka.actor.{Props, Terminated, Actor}
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.Logging
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by barbaragomes on 4/15/16.
 */
class W2vScheduler extends Actor with Logging {

  import W2vScheduler._
  val delay = calculateDelay()
  val interval = Config.word2vecConf.getInt("generate-model-timeinterval").seconds

  // TODO: create a scheduler to be executed at a specific time instead of intervals
  context.system.scheduler.schedule(delay, interval) {
    generateModel()
  }

  override def receive: Receive = {
    case StartW2VModelGeneration => {
      logInfo("Word2Vec model generation started.")
      generateModel()
    }
    case Terminated => {
      logInfo("Word2vec model stopped.")
    }
  }

  def generateModel(): Unit = {
    val folderName: SimpleDateFormat =
      new SimpleDateFormat(Config.word2vecConf.getString("date-format"))
    // Get day before today
    val datenow = DateUtils.addDays(Calendar.getInstance().getTime(), -1)
    val tokensFolder = s"$folderPrefix${folderName.format(datenow)}"
    logInfo(s"Generating model for: ${folderName.format(datenow)}")
    try {
      val w2v = new Word2VecModelComputation(tokensFolder)
      w2v.generateModel()
    } catch {
      case e: Exception => logError(s"Could not generate model for ${tokensFolder}", e)
    }
  }
}

object W2vScheduler {
  case object StartW2VModelGeneration

  val folderPrefix = Config.word2vecConf.getString("prefix-tokens-folder-daily")

  def props: Props = {
    Props(new W2vScheduler)
  }

  def calculateDelay(): FiniteDuration = {
    val datenow = Calendar.getInstance().getTime()
    val nextTime = DateUtils.setMinutes(DateUtils.setHours(DateUtils.addDays(datenow, 1),
      Config.word2vecConf.getInt("run-new-model-at")), 0)
    ((nextTime.getTime()-datenow.getTime())/1000).seconds
  }
}
