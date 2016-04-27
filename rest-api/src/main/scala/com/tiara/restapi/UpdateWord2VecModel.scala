package com.tiara.restapi

import java.io.{BufferedReader, InputStreamReader}
import org.apache.spark.Logging
import org.apache.spark.mllib.feature.Word2VecModel
import akka.actor.{Props, Actor}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.functions._
/**
 * Created by barbaragomes on 4/19/16.
 */
class UpdateWord2VecModel extends Actor with Logging{

  import UpdateWord2VecModel._

  val delay = Config.restapi.getInt("start-scheduler-after").seconds
  val interval = Config.restapi.getInt("check-for-new-model-interval").seconds

  //Check every 10min for a new model
  context.system.scheduler.schedule(delay, interval) {
    try {
      updateModel()
    } catch {
      case e: Exception => logError("Something went wrong while checking for new model", e)
    }
  }

  override def receive: Receive = {
    case StartMonitoringWord2VecModels =>{
      logInfo("Word2Vec models monitoring started.")
    }
  }

  private def updateModel() = {
    val newModelName: String = checkForTokenFile()
    if(!newModelName.isEmpty()) {
      logInfo(s"New model generated: $newModelName")
      val modelPath = s"$modelsPath/$newModelName"

      try {
        InMemoryData.word2VecModel = Word2VecModel.load(ApplicationContext.sparkContext,s"$modelPath/$modelFolder")
        // Using counters from Redis
        // InMemoryData.frequency = ApplicationContext.sqlContext.read.parquet(s"$modelPath/$freqFolder")
        val newRTDF = ApplicationContext.sqlContext.read.parquet(s"$englishPath/$newModelName")
                                  .filter("verb = 'share'")
                                  .select(col("actor.preferredUsername").as("uid"),
                                          col("object.actor.preferredUsername").as("ouid"),
                                          col("actor.followersCount"),
                                          col("gnip.klout_score"),
                                          col("object.body"))
        // Change in memory DF
        if (InMemoryData.retweetsENDF != null) InMemoryData.retweetsENDF.unpersist(true)
        InMemoryData.retweetsENDF = newRTDF
        InMemoryData.retweetsENDF.persist()

        // Get string reference to the date for the model and RT DF
        InMemoryData.date = newModelName.replace(removePrefix, "")

          // Delete token file
        deleteTokenFileAfterProcessed

      }catch {
        case e: Exception => logError("Model could not be updated", e)
      }
    }
  }

  private def deleteTokenFileAfterProcessed() = {
    try {
      ApplicationContext.hadoopFS.delete(new Path(tokenFile), true)
    }catch {
      case e: Exception => logError("Could not remove token file",e)
    }
  }

  private def checkForTokenFile(): String = {
    if(ApplicationContext.hadoopFS.exists(new Path(tokenFile))){
      val file = ApplicationContext.hadoopFS.open(new Path(tokenFile))
      val name = (new BufferedReader(new InputStreamReader(file))).readLine()
      file.close()
      name
    }else{
      ""
    }
  }
}

object UpdateWord2VecModel {

  val modelsPath = Config.restapi.getString("path-to-daily-models")
  val tokenFile = s"${modelsPath}/${Config.restapi.getString("token-file-name")}"
  val modelFolder = Config.restapi.getString("folder-name-model")
  val freqFolder = Config.restapi.getString("folder-name-word-count")
  val englishPath = Config.restapi.getString("daily-en-tweets-dir")
  val removePrefix = Config.restapi.getString("prefix-tokens-folder-daily")

  case object StartMonitoringWord2VecModels

  def props: Props = {
    Props(new UpdateWord2VecModel)
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

