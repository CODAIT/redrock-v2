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
package com.tiara.decahose

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import redis.clients.jedis._

object SqlUtils extends Logging {

  // field names we generate in sql stmts
  val COL_POSTED_DATE = Config.processorConf.getString("post-date-col-name")
  val COL_TOKEN_SET = Config.processorConf.getString("tokens-column")
  val COL_TWITTER_ENTITY = Config.processorConf.getString("redis-tweet-entity-token-count")
  val COL_SENTIMENT = Config.processorConf.getString("sentiment-column")
  val COL_POSTED_HOUR = "postedHour"
  val COL_TWITTER_AUTHOR = "AU"
  val COL_HASHTAG = "hashtag"
  val COL_TOKEN = "tok"
  val COL_TOKEN_1 = "tok1"
  val COL_TOKEN_2 = "tok2"
  val COL_PAIR = "pair"
  val COL_COUNT = "count"
  val SENTIMENT_CUTOFF = 0.1
  val WORD_LIST_SCORE = SentiWordList.list

  // SQL strings
  val SQL_EN_FILTER = "twitter_lang = 'en'"

  val regexSuffixes: String = "[a-z0-9]*" + scala.io.Source.fromInputStream(
    getClass.getResourceAsStream("/suffixes.2")
  ).getLines().mkString("(", "|", ")") + """($|\s)"""

  val regexNumber: String = "[0-9]*(:|.|/|-)?[0-9]*(:|.|/|-)?[0-9]*$"

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
    (text: String) =>
      if (text != null) {
        com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
          .map((x: String) => x.toLowerCase)
      } else {
        null
      }
  )
  val lowerTwokensNoHttp = org.apache.spark.sql.functions.udf(
    (text: String) =>
      if (text != null) {
        com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
          .map((x: String) => x.toLowerCase)
          .filter((x: String) => !x.startsWith("http"))
      } else {
        null
      }
  )
  val lowerTwokensNoHttpNoStop = org.apache.spark.sql.functions.udf(
    (text: String) =>
      if (text != null) {
        com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
          .map((x: String) => x.toLowerCase)
          .filter((x: String) => !stopWords.contains(x) && !x.startsWith("http"))
      } else {
        null
      }
  )

  val lowerTwokensNoHttpNoStopNoApostrophe = org.apache.spark.sql.functions.udf(
    (text: String) =>
      if (text != null) {
        com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
          .map((x: String) => {
            val lower = x.toLowerCase
            if (lower.matches(regexSuffixes)) {
              // scalastyle:off non.ascii.character.disallowed
              // we need to check for the case with a non-ascii apostrophe
              val index =
                if (lower.lastIndexOf("'") == -1) lower.lastIndexOf("’") else lower.lastIndexOf("'")
              // scalastyle:on non.ascii.character.disallowed
              lower.substring(0, index)
            } else {
              lower
            }
          })
          .filter((x: String) => !stopWords.contains(x) && !x.startsWith("http"))
      } else {
        null
      }
  )

  val lowerTwokensNoHttpNoStopNoApostropheNoNumbers = org.apache.spark.sql.functions.udf(
    (text: String) =>
      if (text != null) {
        com.tiara.decahose.Twokenize.tokenizeRawTweetText(text).asScala
          .map((x: String) => {
            val lower = x.toLowerCase
            if (lower.matches(regexSuffixes)) {
              // scalastyle:off non.ascii.character.disallowed
              // we need to check for the case with a non-ascii apostrophe
              val index =
                if (lower.lastIndexOf("'") == -1) lower.lastIndexOf("’") else lower.lastIndexOf("'")
              // scalastyle:on non.ascii.character.disallowed
              lower.substring(0, index)
            } else {
              lower
            }
          })
          .filter((x: String) =>
            !stopWords.contains(x) && !x.startsWith("http") && !x.matches(regexNumber))
      } else {
        null
      }
  )

  val hashtagsFromToks = org.apache.spark.sql.functions.udf(
    (toks: WrappedArray[String]) => toks.filter(_.startsWith("#"))
  )

  val tagToText = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[Row]) =>
      if (it != null) {
        it.map(
          (tag: Row) => "#" + tag.getAs[String]("text").toLowerCase
        )
      } else {
        null
      }
  )
  val mentionToText = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[Row]) =>
      if (it != null) {
        it.map(
          (tag: Row) => "@" + tag.getAs[String]("screen_name").toLowerCase
        )
      } else {
        null
      }
  )
  val flattenDistinct = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[WrappedArray[String]]) =>
      it.filter(_ != null).flatten.distinct.filter(_.length >= 2)
  )

  val extractSentiment = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[String]) => {
      var sentScore = 0.0
      var count = 0
      it.foreach(word => {
        WORD_LIST_SCORE.get(word) match {
          case Some(score) =>
            sentScore += score.positive
            sentScore -= score.negative
            count += 1
          case _ =>
        }
      })
      val normalizedScore = sentScore/count
      if (normalizedScore > SENTIMENT_CUTOFF) {
        1 // positive
      } else if ( normalizedScore < -SENTIMENT_CUTOFF) {
        -1 // negative
      } else {
        0 // neutral
      }
    }
  )

  def toksToHashtagCounts(toks: DataFrame): DataFrame = {
    toks.select(
      col(COL_POSTED_DATE),
      explode(
        hashtagsFromToks(col("toks"))
      ).as(COL_HASHTAG)
    )
      .groupBy(COL_POSTED_DATE, COL_HASHTAG)
      .count
  }

  def toksToHashtagByAuthorCounts(toks: DataFrame): DataFrame = {
    toks.select(
      col(COL_POSTED_DATE),
      col(COL_TWITTER_AUTHOR),
      explode(
        hashtagsFromToks(col("toks"))
      ).as(COL_HASHTAG)
    )
      .distinct
      .groupBy(COL_POSTED_DATE, COL_HASHTAG)
      .count
  }

  def toksToHashtagPairCounts(toks: DataFrame): DataFrame = {
    toks.select(
      hashtagsFromToks(col("toks")).as("hashtags")
    )
      .explode("hashtags", "pair") {
        (toks: scala.collection.mutable.WrappedArray[String]) =>
          toks.toSeq.distinct
            .combinations(2).toList
            .map(_.sorted)
            .map((x: Seq[String]) => Tuple2(x(0), x(1)))
      }
      .select("pair._1", "pair._2")
      .groupBy("_1", "_2").count
  }

  val pool: JedisPool = new JedisPool(
    new JedisPoolConfig(),
    Config.processorConf.getString("redis-server"),
    Config.processorConf.getInt("redis-port")
  )
  val MAX_REDIS_PIPELINE = 10000

  // update count of unordered pairs (A, B) in redis
  // we maintain both date#A => (B, count)  and date#B => (A, count) so we can lookup the count from
  // either direction
  def groupedBulkUpdatePairs(tsFieldName: String, rows: Iterator[Row]): Unit = {
    val jedis = pool.getResource
    var pipe = jedis.pipelined()
    var i: Int = 0
    try {
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
    } catch {
      case e: Exception => logError("Counter update exception:", e)
    } finally {
      pipe.sync()
      jedis.close()
    }
  }

  // update count of 2-tuple (A, B) in redis
  // we maintain only date#A => (B, count)
  def groupedBulkUpdateTuples(tsFieldName: String, rows: Iterator[Row]): Unit = {
    val jedis = pool.getResource
    var pipe = jedis.pipelined()
    var i: Int = 0
    try {
      rows.foreach(
        (row: Row) => {
          val date: String = row.getAs[String](tsFieldName)
          val tok1: String = row.getAs[String](COL_TOKEN_1)
          val tok2: String = row.getAs[String](COL_TOKEN_2)
          val count = row.getAs[Long](COL_COUNT)
          pipe.zincrby(date + ":" + tok1, count, tok2)
          pipe.expire(date + ":" + tok1, 86400 * 7)

          i += 1
          if (i > MAX_REDIS_PIPELINE) {
            pipe.sync()
            pipe = jedis.pipelined()
            i = 0
          }
        }
      )
    } catch {
      case e: Exception => logError("Counter update exception:", e)
    } finally {
      pipe.sync()
      jedis.close()
    }
  }

  // update frequency count of single entity in redis
  // we maintain date#entity => (token, count)
  def groupedBulkUpdateCounters(tsFieldName: String,
                                tokenFieldName: String,
                                rows: Iterator[Row]): Unit = {
    val jedis = pool.getResource
    var pipe = jedis.pipelined()
    var i: Int = 0
    try {
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
    } catch {
      case e: Exception => logError("Counter update exception:", e)
    } finally {
      pipe.sync()
      jedis.close()
    }
  }


  // Update time series data points in redis
  def groupedUpdateTimeSeries(redisTSTag: String,
                              tsFieldName: String,
                              tokenFieldName: String,
                              rows: Iterator[Row]): Unit = {
    val jedis = pool.getResource
    var pipe = jedis.pipelined()
    var i: Int = 0
    try {
      rows.foreach(
        (row: Row) => {
          val date: String = row.getAs[String](tsFieldName)
          val tok: String = row.getAs[String](tokenFieldName)
          val count = row.getAs[Long](COL_COUNT)

          // do not expire this key
          pipe.hset(redisTSTag + ":" + tok, date, count.toString)
          pipe.sync

          i += 1
          if (i > MAX_REDIS_PIPELINE) {
            pipe.sync()
            pipe = jedis.pipelined()
            i = 0
          }
        }
      )
    } catch {
      case e: Exception => logError("Counter update exception:", e)
    } finally {
      pipe.sync()
      jedis.close()
    }
  }

}
