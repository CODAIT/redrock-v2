package com.tiara.decahose

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import redis.clients.jedis._

/**
 * Created by barbaragomes on 4/4/16.
 */
object TweetProcessor extends Logging{

  private val debugFlag = false;

  val extractFileName = "\\b(hdfs:|file:)\\S+".r
  val extractDateTime = "(\\d\\d\\d\\d_\\d\\d_\\d\\d_\\d\\d_\\d\\d)".r

  // where to save the english tweets in parquet format
  val enDir = Config.processorConf.getString("daily-en-tweets-dir")
  val debugDir = Config.processorConf.getString("debug-dir")

  // field names we generate in sql stmts
  val COL_POSTED_DATE = "postedDate"
  val COL_POSTED_HOUR = "postedHour"
  val COL_TWITTER_ENTITY = "ES"
  val COL_TWITTER_AUTHOR = "AU"
  val COL_TOKEN = "tok"
  val COL_TOKEN_1 = "tok1"
  val COL_TOKEN_2 = "tok2"
  val COL_TOKEN_SET = "toks"
  val COL_PAIR = "pair"
  val COL_COUNT = "count"

  // SQL strings
  val SQL_EN_FILTER = "twitter_lang = 'en'"

  // stop words, from the assembly jar, packaged from conf dir
  val stopWords: Set[String] = scala.io.Source.fromInputStream(
    getClass.getResourceAsStream("/stop-words.1")
  ).getLines().toSet

  // regex from exclusion list (hashtags mostly)
  // we use this to exclude low coherence tweets (like those with #sex)
  val excludeRegex: String = scala.io.Source.fromInputStream(
    getClass.getResourceAsStream("/exclude.0")
  ).getLines().mkString("|")

  // UDF for date/time
  val postedHour = org.apache.spark.sql.functions.udf(
    (text: String) => text.substring(0,13)
  )
  val postedDate = org.apache.spark.sql.functions.udf(
    (text: String) => text.substring(0,10)
  )

  // UDFs for processing tweets into tokens and entities
  val lowerTwokens = org.apache.spark.sql.functions.udf(
    (text: String) => if (text != null)
      com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
      .map((x: String) => x.toLowerCase)
    else null
  )
  val lowerTwokensNoHttp = org.apache.spark.sql.functions.udf(
    (text: String) => if (text != null)
      com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
      .map((x: String) => x.toLowerCase)
      .filter((x: String) => ! x.startsWith("http"))
    else null
  )
  val lowerTwokensNoHttpNoStop = org.apache.spark.sql.functions.udf(
    (text: String) => if (text != null)
      com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
      .map((x: String) => x.toLowerCase)
      .filter((x: String) => ! stopWords.contains(x) && ! x.startsWith("http"))
    else null
  )

  val tagToText = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[Row]) => if (it!=null) it.map(
      (tag:Row) => "#"+tag.getAs[String]("text").toLowerCase
    ) else null
  )
  val mentionToText = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[Row]) => if (it!=null) it.map(
      (tag:Row) => "@"+tag.getAs[String]("screen_name").toLowerCase
    ) else null
  )
  val flatten = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[WrappedArray[String]]) => it.filter(_ != null).flatten
  )
  val flattenDistinct = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[WrappedArray[String]]) => it.filter(_ != null).flatten.distinct
  )

  //TODO: find a way to close pools/connections when the Spark job gets killed.
  // right now the TCP conns remains open in the Executor JVM
  val pool: JedisPool = new JedisPool(new JedisPoolConfig(), Config.processorConf.getString("redis-server"))
  val MAX_REDIS_PIPELINE = 10000

  def groupedBulkUpdatePairs (tsFieldName: String, rows: Iterator[Row]): Unit = {
    val jedis = pool.getResource
    var pipe = jedis.pipelined()
    var i: Int = 0
    rows.foreach(
      (row: Row) => {
        val date: String = row.getAs[String](tsFieldName)
        val tok1: String = row.getAs[String](COL_TOKEN_1)
        val tok2: String = row.getAs[String](COL_TOKEN_2)
        val count = row.getAs[Long](COL_COUNT)
        pipe.zincrby(date + ":" + tok1, count, tok2)
        pipe.zincrby(date + ":" + tok2, count, tok1)
        pipe.expire(date + ":" + tok1, 86400*7)
        pipe.expire(date + ":" + tok2, 86400*7)

        i+=1
        if (i>MAX_REDIS_PIPELINE) {
          pipe.sync()
          pipe = jedis.pipelined()
          i = 0
        }
      }
    )
    pipe.sync()
    jedis.close()
  }

  def groupedBulkUpdateCounters (tsFieldName: String, tokenFieldName: String, rows: Iterator[Row]): Unit = {
    val jedis = pool.getResource
    var pipe = jedis.pipelined()
    var i: Int = 0
    rows.foreach(
      (row: Row) => {
        val date: String = row.getAs[String](tsFieldName)
        val tok: String = row.getAs[String](tokenFieldName)
        val tok0 = tok.substring(0,1)
        val count = row.getAs[Long](COL_COUNT)
        var typeTag: String = tokenFieldName
        if (tokenFieldName == COL_TWITTER_ENTITY) {
          // 3 types of twitter "entity": hashtag, user mention, and plain string
          if (tok0 == "#" || tok0 == "@") {typeTag += tok0} else {typeTag += "S"}
        }
        pipe.zincrby(date + ":" + typeTag, count, tok)
        pipe.expire(date + ":" + typeTag, 86400*7)

        i+=1
        if (i>MAX_REDIS_PIPELINE) {
          pipe.sync()
          pipe = jedis.pipelined()
          i = 0
        }
      }
    )
    pipe.sync()
    jedis.close()
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
        loadJSONToDataFrame(rdd)
      }
    }

    ssc
  }

  private def loadJSONToDataFrame(rdd: RDD[String]) = {
    try{

      // Use predefined schema to speed up JSON parsing by more than 2x !!!
      val tweetsDF = ApplicationContext.sqlContext.read.schema(ApplicationContext.schema).json(rdd)

      val times: List[String]  = extractDateTime.findAllIn(rdd.toDebugString).toList
      val timeWindow = times.min + "," + times.max
      val dateTimes = times.sorted.mkString(",")
      logInfo(s"============  ${timeWindow} ${dateTimes}")

      //TODO: in case of multiple files, use the file size information
      //TODO: to estimate/inform how long the subsequent processing will take

//  Aborted attempt to find out the time window of the tweets and use it to name the parquet dir
//  This is bad idea as we go through the json data twice, wasting CPU. Comment out for now.
//      val timeWindow = tweetsDF.select(max("postedTime"), min("postedTime")).first.toString()
//        .replaceAll(".000Z", "").replaceAll("[:-]", "").replaceAll("[\\[\\]]", "").replaceAll(",", "-")
//      logInfo(s"============  ${timeWindow} ${tweetsDF.count()}  ${timeWindow}")

      // Filter by English tweets
      // Save filtered DF as parquet to HDFS
      if (true) {
        val enDF0 = tweetsDF.filter(SQL_EN_FILTER).repartition(90)
        enDF0.write.format("parquet").save(enDir + "/" + timeWindow)
      }

      // read it back from parquet files, this is faster
      val enDF = tweetsDF.sqlContext.read.parquet(enDir + "/" + timeWindow)
//      val enDF = tweetsDF.sqlContext.read.parquet("hdfs://spark-dense-01:8020/daily/en/2016_03_01")
      //TODO: and Add Sentiment field

      // extract all unique lower case string tokens, from both original body and the retweet
      val dateToksDF = enDF
        .filter(not(col("body").rlike(excludeRegex)))
        .select(
          postedDate(col("postedTime")).as(COL_POSTED_DATE),
          col("actor.preferredUsername").as(COL_TWITTER_AUTHOR),
          flattenDistinct(array(
            lowerTwokensNoHttpNoStop(col("body")),
            lowerTwokensNoHttpNoStop(col("object.body"))
          )).as(COL_TOKEN_SET))
        .repartition(90)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

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
      if (true) {
        val gDF = dateToksDF.select(
          col(COL_POSTED_DATE),
          col(COL_TWITTER_AUTHOR)
        ).groupBy(COL_POSTED_DATE, COL_TWITTER_AUTHOR).count.repartition(70)

        gDF.foreachPartition(
          (rows: Iterator[Row]) => groupedBulkUpdateCounters(COL_POSTED_DATE, COL_TWITTER_AUTHOR, rows)
        )
      }

      // counter update of all pairs
      if (true) {
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
            .write.format("json")
            .save(debugDir + "/hour-rel-counts/" + timeWindow)
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
            .write.format("json")
            .save(debugDir + "/tok-rel-counts/" + timeWindow)
        }

        if (false) {
          hourToksDF
            .select(explode(col(COL_TOKEN_SET)).as(COL_TOKEN))
            .groupBy(COL_TOKEN).count
            .orderBy(col(COL_COUNT).desc)
            .write.format("json")
            .save(debugDir + "/tok-counts/" + timeWindow)
        }

        if (false) {
          // get the hashtags and user mentions in a flat array
          val tagAndUserDF = enDF.select(
            col("twitter_entities.hashtags"),
            col("object.twitter_entities.hashtags").as("ohashtags"),
            col("twitter_entities.user_mentions").as("mentions"),
            col("object.twitter_entities.user_mentions").as("omentions"))

          // save the counts of pair combinations to a json file
          tagAndUserDF.select(flatten(array(
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
            .write.format("json")
            .save(debugDir + "/rel-counts/" + timeWindow)
        }

      }

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
