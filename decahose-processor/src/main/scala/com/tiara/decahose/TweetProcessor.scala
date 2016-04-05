package com.tiara.decahose

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}

/**
 * Created by barbaragomes on 4/4/16.
 */
object TweetProcessor extends Logging{

  private val extractFileName = "\\b(hdfs:|file:)\\S+".r

  def startProcessingStreamingData(): Unit = {
    logInfo("Creating Spark Streaming Context")

    val ssc = createStreamingContext()

    logInfo(s"Starting Spark Streaming")
    ssc.start()
    ssc.awaitTermination()
  }

  /* Checkpoint */
  private def createStreamingContext(): StreamingContext = {

    val ssc = new StreamingContext(ApplicationContext.sparkContext, Seconds(Config.processorConf.getInt("spark.streaming-batch-time")))

    val tweetsStreaming = ssc.fileStream[LongWritable, Text, TextInputFormat](Config.processorConf.getString("decahose-dir"),
      (p: Path) => {
        /* Do not process file in WRITING mode */
        if (p.getName().endsWith(Config.processorConf.getString("writing-mode-string"))) false
        else true
      }, true).map(_._2.toString)

    tweetsStreaming.foreachRDD { (rdd: RDD[String], time: Time) =>
      logInfo(s"Streaming Batch Time: ${Utils.transformSparkTime(time)}")
      if (!rdd.partitions.isEmpty) {
        logInfo("Processing File(s):")
        extractFileName.findAllMatchIn(rdd.toDebugString).foreach((name) => logInfo(name.toString))
        loadJSONToDataFrame(rdd)
      }
    }

    ssc
  }

  private def loadJSONToDataFrame(rdd: RDD[String]) = {
    try{

      val tweetsDF = ApplicationContext.sqlContext.read.json(rdd)

      //Testing count - Tmp code
      logInfo(s"============ ${tweetsDF.count()}")

      //TODO: Filter by English tweets and Add Sentiment field

      //TODO: Save filtered DF as parquet to HDFS

      //TODO: Tokenize and create batch data to update HBase (Maps)

      //TODO: Send updates to HBase

      // Delete file just if it was processed
      if(Config.processorConf.getBoolean("spark.delete-file-after-processed")){
        logInfo("Deleting File(s):")
        extractFileName.findAllMatchIn(rdd.toDebugString).foreach((name) => Utils.deleteFile(name.toString))
      }


    }catch {
      case e: Exception => logError("Could not process file(s)", e)
    }
  }

}
