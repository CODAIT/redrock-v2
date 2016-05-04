package com.tiara.decahose

import scala.collection.mutable.WrappedArray

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import SqlUtils._

/**
 * Created by barbaragomes on 4/4/16.
 */
object TweetProcessor extends Logging{

  private val debugFlag = false;

  val extractFileName = "\\b(hdfs:|file:)\\S+".r
  val extractDateTime = "(\\d\\d\\d\\d_\\d\\d_\\d\\d_\\d\\d_\\d\\d)".r

  // where to save the english tweets in parquet format
  val enDir = Config.processorConf.getString("daily-en-tweets-dir")
  val toksDir = Config.processorConf.getString("tokens-dir")
  val debugDir = Config.processorConf.getString("debug-dir")
  val shouldUpdateCounters = Config.processorConf.getBoolean("update-redis-counters")

  def processHistoricalData(): Unit = {
    val historicalPath = Config.processorConf.getString("historical.data-path").split(" ")
    processBatchData(historicalPath)
  }

  def processBatchData(paths: Array[String]): Unit = {
    try {
      logInfo(s"Processing batch data. Directory(s): " + paths.mkString(","))
      val df = ApplicationContext.sqlContext.read.schema(ApplicationContext.schema).json(paths:_*)
      processedTweetsDataFrame(df)
    } catch {
      case e: Exception => logError("Something went wrong while processing historical batch", e)
    }
  }

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
        val tweetsDF = ApplicationContext.sqlContext.read.schema(ApplicationContext.schema).json(rdd)
        processedTweetsDataFrame(tweetsDF, rdd.toDebugString)
      }
    }

    ssc
  }

  def processedTweetsDataFrame(tweetsDF: DataFrame, debugString: String = "") = {
    try{

      //TODO: in case of multiple files, use the file size information
      //TODO: to estimate/inform how long the subsequent processing will take

      // Filter by English tweets
      // Save filtered DF as parquet to HDFS, partitioned by date
      val enDF = tweetsDF
        .filter(SQL_EN_FILTER)
        .withColumn(COL_POSTED_DATE, postedDate(col("postedTime")))

      enDF.repartition(30)
      enDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      val dfWriter = enDF
        .write
        .partitionBy(COL_POSTED_DATE)
        .mode(org.apache.spark.sql.SaveMode.Append)

      dfWriter.format("parquet").save(enDir)

      // read it back from parquet files, this is faster
//      val enDF = tweetsDF.sqlContext.read.parquet(enDir + "/" + timeWindow)
//      val enDF = tweetsDF.sqlContext.read.parquet("hdfs://spark-dense-01:8020/daily/en/2016_03_01")
      //TODO: and Add Sentiment field

      // extract all unique lower case string tokens, from both original body and the retweet
      val dateToksDF = enDF
        .filter(not(col("body").rlike(excludeRegex)))
        .select(
          col(COL_POSTED_DATE),
          col("actor.preferredUsername").as(COL_TWITTER_AUTHOR),
          flattenDistinct(array(
            lowerTwokensNoHttpNoStopNoApostropheNoNumbers(col("body")),
            lowerTwokensNoHttpNoStopNoApostropheNoNumbers(col("object.body"))
          )).as(COL_TOKEN_SET))
        .repartition(90)

      dateToksDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      enDF.unpersist(false)

      val dfwr = dateToksDF
        .write
        .partitionBy(COL_POSTED_DATE)
        .mode(org.apache.spark.sql.SaveMode.Append)

      dfwr.format("parquet").save(toksDir)

      if (shouldUpdateCounters) {
        // counter update of hashtags and user mentions and other random strings
        if (true) {
          val gDF = dateToksDF.select(
            col(COL_POSTED_DATE),
            explode(col(COL_TOKEN_SET)).as(COL_TWITTER_ENTITY)
          ).groupBy(COL_POSTED_DATE, COL_TWITTER_ENTITY).count.repartition(70)

          gDF.foreachPartition(
            (rows: Iterator[Row]) => groupedBulkUpdateCounters(COL_POSTED_DATE, COL_TWITTER_ENTITY, rows)
          )
        }

        // counter update of author posts
        // Barbara: Do not need to compute it for now
        if (false) {
          val gDF = dateToksDF.select(
            col(COL_POSTED_DATE),
            col(COL_TWITTER_AUTHOR)
          ).groupBy(COL_POSTED_DATE, COL_TWITTER_AUTHOR).count.repartition(70)

          gDF.foreachPartition(
            (rows: Iterator[Row]) => groupedBulkUpdateCounters(COL_POSTED_DATE, COL_TWITTER_AUTHOR, rows)
          )
        }

        // counter update for the tuple2 of (hashtag: author)
        // this will provide top-k posters of a given hashtag
        if (false) {
          val gDF = dateToksDF.filter("verb = 'post'")
            .select(
              col(COL_POSTED_DATE),
              explode(tagToText(col("hashtags"))).as(COL_TOKEN_1),
              col(COL_TWITTER_AUTHOR).as(COL_TOKEN_2)
            ).groupBy(COL_POSTED_DATE, COL_TOKEN_1, COL_TOKEN_2).count.repartition(70)

          gDF.foreachPartition(
            (rows: Iterator[Row]) => groupedBulkUpdateTuples(COL_POSTED_DATE, rows)
          )
        }

        // counter update of all pairs
        // Barbara: Do not need to compute it for now
        if (false) {
          val gDF = dateToksDF.explode(COL_TOKEN_SET, COL_PAIR) {
            (toks: WrappedArray[String]) =>
              toks.toSeq.distinct
                // C(n,k) where k=2
                .combinations(2).toList
                // sort the 2-tuple so we can compare the pairs in subsequent groupBy
                .map(_.sorted)
                .map((x: Seq[String]) => Tuple2(x(0), x(1)))
          }
            .select(col(COL_POSTED_DATE), col(COL_PAIR + "._1").as(COL_TOKEN_1), col(COL_PAIR + "._2").as(COL_TOKEN_2))
            .groupBy(COL_POSTED_DATE, COL_TOKEN_1, COL_TOKEN_2).count
            .repartition(90)

          gDF.foreachPartition(
            (rows: Iterator[Row]) => groupedBulkUpdatePairs(COL_POSTED_DATE, rows)
          )
        }
      }

      //TODO: debug code, should move to test module
      if (debugFlag) {

        val hourToksDF = enDF
          .filter(not(col("body").rlike(excludeRegex)))
          .select(
            postedHour(col("postedTime")).as(COL_POSTED_HOUR),
            flattenDistinct(array(
              lowerTwokensNoHttpNoStop(col("body")),
              lowerTwokensNoHttpNoStop(col("object.body"))
            )).as(COL_TOKEN_SET))

        if (false) {
          hourToksDF
            .explode(COL_TOKEN_SET, COL_PAIR) {
              (toks: WrappedArray[String]) =>
                toks.toSeq.distinct
                  // C(n,k) where k=2
                  .combinations(2).toList
                  // sort the 2-tuple so we can compare the pairs in subsequent groupBy
                  .map(_.sorted)
                  .map((x: Seq[String]) => Tuple2(x(0), x(1)))
            }
            .select(col(COL_POSTED_HOUR), col(COL_PAIR))
            .groupBy(COL_POSTED_HOUR, COL_PAIR).count
            .orderBy(col(COL_COUNT).desc)
            .write
            .partitionBy(COL_POSTED_HOUR)
            .mode(org.apache.spark.sql.SaveMode.Overwrite)
            .format("json")
            .save(debugDir + "/hour-rel-counts/")
        }

        if (false) {
          hourToksDF
            .explode(COL_TOKEN_SET, COL_PAIR) {
              (toks: WrappedArray[String]) =>
                toks.toSeq.distinct
                  // C(n,k) where k=2
                  .combinations(2).toList
                  // sort the 2-tuple so we can compare the pairs in subsequent groupBy
                  .map(_.sorted)
                  .map((x: Seq[String]) => Tuple2(x(0), x(1)))
            }
            .select(col(COL_PAIR))
            .groupBy(COL_PAIR).count
            .orderBy(col(COL_COUNT).desc)
            .write
            .mode(org.apache.spark.sql.SaveMode.Overwrite)
            .format("json")
            .save(debugDir + "/tok-rel-counts/")
        }

        if (false) {
          hourToksDF
            .select(explode(col(COL_TOKEN_SET)).as(COL_TOKEN))
            .groupBy(COL_TOKEN).count
            .orderBy(col(COL_COUNT).desc)
            .write
            .mode(org.apache.spark.sql.SaveMode.Overwrite)
            .format("json")
            .save(debugDir + "/tok-counts/")
        }

        if (false) {
          // get the hashtags and user mentions in a flat array
          val tagAndUserDF = enDF.select(
            col("twitter_entities.hashtags"),
            col("object.twitter_entities.hashtags").as("ohashtags"),
            col("twitter_entities.user_mentions").as("mentions"),
            col("object.twitter_entities.user_mentions").as("omentions"))

          // save the counts of pair combinations to a json file
          tagAndUserDF.select(flattenDistinct(array(
            tagToText(col("hashtags")),
            tagToText(col("ohashtags")),
            mentionToText(col("mentions")),
            mentionToText(col("omentions")))
          ).as("flat_tokens"))
            .explode("flat_tokens", COL_PAIR) {
              (toks: WrappedArray[String]) =>
                toks.toSeq.distinct
                  // C(n,k) where k=2
                  .combinations(2).toList
                  // sort the 2-tuple so we can compare the pairs in subsequent groupBy
                  .map(_.sorted)
                  .map((x: Seq[String]) => Tuple2(x(0), x(1)))
            }.select("pair._1", "pair._2")
            .groupBy("_1", "_2").count
            .orderBy(col(COL_COUNT).desc)
            .write
            .mode(org.apache.spark.sql.SaveMode.Overwrite)
            .format("json")
            .save(debugDir + "/rel-counts/")
        }

      }

      // Unpersist once is done
      enDF.unpersist()
      dateToksDF.unpersist()

      // Delete file just if it was processed
      if(!debugString.isEmpty && Config.processorConf.getBoolean("spark.delete-file-after-processed")){
        logInfo("Deleting File(s):")
        extractFileName.findAllMatchIn(debugString).foreach((name) => Utils.deleteFile(name.toString))
      }


    }catch {
      case e: Exception => logError("Could not process file(s)", e)
    }
  }

}
