package com.tiara.decahose

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import redis.clients.jedis._

object SqlUtils {

  // field names we generate in sql stmts
  val COL_POSTED_DATE = Config.processorConf.getString("post-date-col-name")
  val COL_TOKEN_SET = Config.processorConf.getString("tokens-column")
  val COL_POSTED_HOUR = "postedHour"
  val COL_TWITTER_ENTITY = "ES"
  val COL_TWITTER_AUTHOR = "AU"
  val COL_TOKEN = "tok"
  val COL_TOKEN_1 = "tok1"
  val COL_TOKEN_2 = "tok2"
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
    (text: String) => text.substring(0, 13)
  )
  val postedDate = org.apache.spark.sql.functions.udf(
    (text: String) => text.substring(0, 10)
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
        .filter((x: String) => !x.startsWith("http"))
    else null
  )
  val lowerTwokensNoHttpNoStop = org.apache.spark.sql.functions.udf(
    (text: String) => if (text != null)
      com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
        .map((x: String) => x.toLowerCase)
        .filter((x: String) => !stopWords.contains(x) && !x.startsWith("http"))
    else null
  )

  val tagToText = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[Row]) => if (it != null) it.map(
      (tag: Row) => "#" + tag.getAs[String]("text").toLowerCase
    )
    else null
  )
  val mentionToText = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[Row]) => if (it != null) it.map(
      (tag: Row) => "@" + tag.getAs[String]("screen_name").toLowerCase
    )
    else null
  )
  val flattenDistinct = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[WrappedArray[String]]) => it.filter(_ != null).flatten.distinct.filter(_.length>=2)
  )

  //TODO: find a way to close pools/connections when the Spark job gets killed.
  // right now the TCP conns remains open in the Executor JVM
  val pool: JedisPool = new JedisPool(new JedisPoolConfig(), Config.processorConf.getString("redis-server"))
  val MAX_REDIS_PIPELINE = 10000

  def groupedBulkUpdatePairs(tsFieldName: String, rows: Iterator[Row]): Unit = {
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
        pipe.expire(date + ":" + tok1, 86400 * 7)
        pipe.expire(date + ":" + tok2, 86400 * 7)

        i += 1
        if (i > MAX_REDIS_PIPELINE) {
          pipe.sync()
          pipe = jedis.pipelined()
          i = 0
        }
      }
    )
    pipe.sync()
    jedis.close()
  }

  def groupedBulkUpdateCounters(tsFieldName: String, tokenFieldName: String, rows: Iterator[Row]): Unit = {
    val jedis = pool.getResource
    var pipe = jedis.pipelined()
    var i: Int = 0
    rows.foreach(
      (row: Row) => {
        val date: String = row.getAs[String](tsFieldName)
        val tok: String = row.getAs[String](tokenFieldName)
        val tok0 = tok.substring(0, 1)
        val count = row.getAs[Long](COL_COUNT)
        var typeTag: String = tokenFieldName
        if (tokenFieldName == COL_TWITTER_ENTITY) {
          // 3 types of twitter "entity": hashtag, user mention, and plain string
          if (tok0 == "#" || tok0 == "@") {
            typeTag += tok0
          } else {
            typeTag += "S"
          }
        }
        pipe.zincrby(date + ":" + typeTag, count, tok)
        pipe.expire(date + ":" + typeTag, 86400 * 7)

        i += 1
        if (i > MAX_REDIS_PIPELINE) {
          pipe.sync()
          pipe = jedis.pipelined()
          i = 0
        }
      }
    )
    pipe.sync()
    jedis.close()
  }

}