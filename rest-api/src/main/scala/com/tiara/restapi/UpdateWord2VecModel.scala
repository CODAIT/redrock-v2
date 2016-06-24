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

import java.io.{BufferedReader, InputStreamReader}
import java.text.SimpleDateFormat
import org.apache.spark.Logging
import org.apache.spark.mllib.feature.Word2VecModel
import akka.actor.{Props, Actor}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.functions._
import Utils._
/**
 * Created by barbaragomes on 4/19/16.
 */
class UpdateWord2VecModel extends Actor with Logging {

  import UpdateWord2VecModel._

  // Check every 10min for a new model
  context.system.scheduler.schedule(delay, interval) {
    try {
      val newModelName: String = checkForTokenFile()

      if(!newModelName.isEmpty()) {
        updateModelInMemory(newModelName)

        // Delete token file
        deleteTokenFileAfterProcessed
      }
    } catch {
      case e: Exception => logError("Something went wrong while checking for new model", e)
    }
  }

  override def receive: Receive = {
    case StartMonitoringWord2VecModels => {
      logInfo("Word2Vec models monitoring started.")
    }
  }

  private def deleteTokenFileAfterProcessed() = {
    try {
      ApplicationContext.hadoopFS.delete(new Path(tokenFile), true)
    } catch {
      case e: Exception => logError("Could not remove token file", e)
    }
  }

  private def checkForTokenFile(): String = {
    if (ApplicationContext.hadoopFS.exists(new Path(tokenFile))) {
      val file = ApplicationContext.hadoopFS.open(new Path(tokenFile))
      val name = (new BufferedReader(new InputStreamReader(file))).readLine()
      file.close()
      name
    } else {
      ""
    }
  }
}

object UpdateWord2VecModel extends Logging {

  val delay = Config.restapi.getInt("start-scheduler-after").seconds
  val interval = Config.restapi.getInt("check-for-new-model-interval").seconds

  val modelsPath = Config.restapi.getString("path-to-daily-models")
  val tokenFile = s"${modelsPath}/${Config.restapi.getString("token-file-name")}"
  val modelFolder = Config.restapi.getString("folder-name-model")
  val freqFolder = Config.restapi.getString("folder-name-word-count")
  val englishPath = Config.restapi.getString("daily-en-tweets-dir")
  val datePrefix = Config.restapi.getString("prefix-tokens-folder-daily")

  val dateFormat = new SimpleDateFormat(Config.restapi.getString("date-format"))


  case object StartMonitoringWord2VecModels

  def props: Props = {
    Props(new UpdateWord2VecModel)
  }

  def updateModelInMemory(newModelName: String): Unit = {
    logInfo(s"New model generated: $newModelName")
    val modelPath = s"$modelsPath/$newModelName"

    try {
      InMemoryData.word2VecModel =
        Word2VecModel.load(ApplicationContext.sparkContext, s"$modelPath/$modelFolder")
      // Using counters from Redis
      // InMemoryData.frequency =
      //   ApplicationContext.sqlContext.read.parquet(s"$modelPath/$freqFolder")
      val newRTDF = ApplicationContext.sqlContext.read.parquet(s"$englishPath/$newModelName")
        .filter("verb = 'share'")
        .withColumn("stringtoks", stringTokens(col(COL_TOKENS)))
        .select(col("actor.preferredUsername").as("uid"),
          col("object.actor.preferredUsername").as("ouid"),
          /* Since the community graph will be filtered by the results
           * from the word2vec models, and the word2vec model is computed
           * using only the tokens, we don't need to use the tweet body,
           * we can use a string built from the tokens (less data in memory)
           */
          col("stringtoks").as(COL_TOKENS),
          col(COL_SENTIMENT))
      // Change in memory DF
      if (InMemoryData.retweetsENDF != null) InMemoryData.retweetsENDF.unpersist(true)
      InMemoryData.retweetsENDF = newRTDF
      InMemoryData.retweetsENDF.persist()

      // Get string reference to the date for the model and RT DF
      InMemoryData.date = newModelName.replace(datePrefix, "")

    } catch {
      case e: Exception => logError("Model could not be updated", e)
    }
  }

  def updateModel(date: String): String = {
    try {
      val tempDate = dateFormat.parse(date);
      if (date.equals(dateFormat.format(tempDate))) {
        val newModelName = s"$datePrefix$date"
        updateModelInMemory(newModelName)
      }
      "success"
    } catch {
      case e: Exception => logError("Could not validate date", e)
        "failure: correct format: YYYY-MM-DD"
    }
  }

  def updateModel1(w2vPath: String, rtStartDate: String, rtEndDate: String): String = {
    logInfo(s" updateModel1: ${w2vPath}, ${rtStartDate}, ${rtEndDate}")
    try {
      InMemoryData.word2VecModel = Word2VecModel.load(ApplicationContext.sparkContext,
        Config.appConf.getString("hadoop-default-fs") + w2vPath)
      // Using counters from Redis
      // InMemoryData.frequency =
      //   ApplicationContext.sqlContext.read.parquet(s"$modelPath/$freqFolder")
      val newRTDF = ApplicationContext.sqlContext.read.parquet(s"$englishPath")
        .filter(col("postedDate") >= lit(rtStartDate))
        .filter(col("postedDate") < lit(rtEndDate))
        .filter("verb = 'share'")
        .withColumn("stringtoks", stringTokens(col(COL_TOKENS)))
        .select(col("actor.preferredUsername").as("uid"),
          col("object.actor.preferredUsername").as("ouid"),
          /* Since the community graph will be filtered by the results
           * from the word2vec models, and the word2vec model is computed
           * using only the tokens, we don't need to use the tweet body,
           * we can use a string built from the tokens (less data in memory)
           */
          col("stringtoks").as(COL_TOKENS),
          col(COL_SENTIMENT))
      // Change in memory DF
      if (InMemoryData.retweetsENDF != null) InMemoryData.retweetsENDF.unpersist(true)
      InMemoryData.retweetsENDF = newRTDF
      InMemoryData.retweetsENDF.persist()
      logInfo(s"retweet count for [ ${rtStartDate}, ${rtEndDate} ]: " ++
        s"${InMemoryData.retweetsENDF.count}")

      // Get string reference to the date for the model and RT DF
      InMemoryData.date = rtStartDate

      "success"
    } catch {
      case e: Exception => logError("Could not validate date", e)
        "failure: correct format: YYYY-MM-DD"
    }
  }
}

object InMemoryData{
  // Reference to the current model
  @volatile var word2VecModel: Word2VecModel = null
  // Reference to the current frequency analysis
  // Use counters from redis
  // @volatile var frequency: DataFrame = null
  // Reference to the EN RT for the same date as the word2vec
  @volatile var retweetsENDF: DataFrame = null
  // Date
  @volatile var date: String = ""
}

