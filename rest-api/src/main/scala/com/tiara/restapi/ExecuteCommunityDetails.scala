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

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json._
import redis.clients.jedis.Jedis
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import Utils._
import scala.concurrent._

/**
 * Created by barbaragomes on 5/5/16.
 */
object ExecuteCommunityDetails extends Logging {

  val wordcloud = "wordcloud"
  val sentiment = "sentiment"
  val sentimentDescription =
    Json.obj(sentiment -> Json.obj("communityID" -> Json.arr("positive", "negative", "neutral")))
  val worldcloud = Json.obj(wordcloud -> Json.obj("communityID" -> Json.arr("word", "count")))

  def getDetails(searchTerms: String, count: Int): Future[String] = {

    logInfo(s"Get community graph details with search: $searchTerms.")
    val md5 = getMD5forSearchTerms(searchTerms)

    val result = future {computeDetails(searchTerms.toLowerCase, md5, count)}

    result.recover {
      case e: Exception =>
        logError("Could not execute request.", e); Json.stringify(buildResponse(false, null, null))
    }
  }

  private def computeDetails(searchTerms: String, md5: String, count: Int): String = {
    try {
      getDetailsForCommunities(md5, searchTerms, count)
    } catch {
      case e: Exception =>
        logError("Could not get Details for community", e)
        Json.stringify(buildResponse(false, null, null))
    }
  }

  private def getDetailsForCommunities(md5: String, searchTerms: String, count: Int): String = {
    // get Resource
    val jedis = ApplicationContext.jedisPool.getResource

    import ApplicationContext.sqlContext.implicits._

    try {
      logInfo("Loading membership from redis")
      // Get communities from redis
      val communities = jedis.smembers(md5)
      if (communities != null && !communities.isEmpty) {
        val membership: ArrayBuffer[(String, Array[String])] = ArrayBuffer.empty
        val it_com = communities.iterator()

        // Populating membership array
        while (it_com.hasNext) {
          val communityID = it_com.next()
          val users = getUsersForCommunity(md5, communityID, jedis)
          populateMembership(membership, communityID, users)
        }

        // Get membership as DF
        val membershipDF = ApplicationContext.sparkContext
          .parallelize(membership).toDF
          .withColumnRenamed("_1", "community")
          .withColumnRenamed("_2", "uids")
          .select(col("community"), explode(col("uids")).as("uid"))

        val filteredRT = getFilteredRT(searchTerms)
        logInfo("Joining membership DF to Retweets DF")
        // OBS: I think the join should be looking also to the OUID column of the filtered tweets
        // Somenthing like:
        // (filteredRT("uid") === membershipDF("uid") || filteredRT("ouid") === membershipDF("uid"))
        // Because the nodes list from the community graph includes users that was retweeted
        val membershipRT_DF =
          filteredRT.join(membershipDF, filteredRT("uid") === membershipDF("uid")).cache()

        // Get sentiment
        val sentimentResponse: JsObject = extractSentiment(membershipRT_DF)
        val wordcloudResponse: JsObject = extractWordCloud(membershipRT_DF, count)

        membershipRT_DF.unpersist(false)
        Json.stringify(buildResponse(true, sentimentResponse, wordcloudResponse))

      } else {
        logInfo(s"No cached data for search: $searchTerms == MD5 $md5")
        Json.stringify(buildResponse(false, Json.obj(), Json.obj()))
      }
    } finally {
      jedis.close()
    }
  }

  private def extractSentiment(membershipDF: DataFrame): JsObject = {

    try {
      val startTime = System.nanoTime()
      logInfo("Extracting sentiment")
      val sentiment = membershipDF.groupBy("community", "sentiment")
        .agg(count("sentiment").as("count"))
        .map(row => (row.getString(0), (row.getInt(1), row.getLong(2).toInt)))
        .groupByKey

      // Json.ojb(CommunityID -> Json.Array[count of positive, count of negative, count of neutral])
      val mapToJson: Array[JsObject] = sentiment.map(community_sentiment => {
        Json.obj(community_sentiment._1 ->
          Json.arr(
            community_sentiment._2.find {
              case (sent: Int, count: Int) => (sent, count) == (1, count)
            }.getOrElse((0, 0))._2,
            community_sentiment._2.find {
              case (sent: Int, count: Int) => (sent, count) == (-1, count)
            }.getOrElse((0, 0))._2,
            community_sentiment._2.find {
              case (sent: Int, count: Int) => (sent, count) == (0, count)
            }.getOrElse((0, 0))._2
          ))
      }).collect

      val elapsed = (System.nanoTime() - startTime) / 1e9
      logInfo(s"Extract sentiment finished. Execution time: $elapsed")

      mapToJson.fold(Json.obj())((obj1, obj2) => obj1 ++ obj2)
    }
    catch {
      case e: Exception => logError("Could not extract sentiment", e); null
    }

  }

  private def extractWordCloud(membershipDF: DataFrame, top: Int): JsObject = {
    try {
      // override ordering to sort in descending order
      implicit object ReverseLongOrdering extends Ordering[Int] {
        def compare(x: Int, y: Int) = -1 * x.compareTo(y)
      }

      logInfo("Extracting word cloud")
      val startTime = System.nanoTime()
      // If the column toks is also stored as a array
      // we can skip the call to the tokensFromString UDF
      val explodeByTokens =
        membershipDF.withColumn("tokensArray", tokensFromString(col(COL_TOKENS)))
          .select(col("community"), explode(col("tokensArray")).as("word"))
          .filter(excludeHandles(col("word")))

      val aggToJson = explodeByTokens.groupBy("community", "word").agg(count("word").as("count"))
        .map(row => (row.getString(0), (row.getString(1), row.getLong(2).toInt)))
        .groupByKey
        .map {
          case (id: String, wordcount: Iterable[(String, Int)]) => {
            val sorted =
              (wordcount.toArray.sortBy { case (word, count) => count } (ReverseLongOrdering))
                .take(top)
                .map { case (word, count) => Json.arr(word, count) }
            Json.obj(id -> sorted)
          }
        }.collect()

      val elapsed = (System.nanoTime() - startTime) / 1e9
      logInfo(s"Extract wordcloud finished. Execution time: $elapsed")

      aggToJson.fold(Json.obj())((obj1, obj2) => obj1 ++ obj2)

    } catch {
      case e: Exception => logError("Could not extratc word cloud", e); null
    }
  }

  private def getFilteredRT(searchTerms: String): DataFrame = {
    logInfo("Filtering Retweets DF")
    val filterRegex = s"(${searchTerms.trim.replaceAll(",", " | ")} )"
    InMemoryData.retweetsENDF.filter(col(COL_TOKENS).rlike(filterRegex))
  }

  private def getUsersForCommunity(md5: String, communityID: String, jResource: Jedis):
  Array[String] = {
    jResource.lrange(s"$md5:$communityID", 0, -1).toArray.map(_.toString)
  }

  private def populateMembership(membership: ArrayBuffer[(String, Array[String])],
                                 communityID: String,
                                 users: Array[String]) = {
    if (users != null && !users.isEmpty) {
      membership.append((communityID, users))
    }
  }

  private def buildResponse(success: Boolean, sentimentJson: JsObject, wordcloudJson: JsObject):
  JsObject = {
    Json.obj("success" -> success) ++
      sentimentDescription ++ worldcloud ++
      Json.obj("communitydetails" -> (
        Json.obj(sentiment -> (if (sentimentJson == null) JsNull else sentimentJson)) ++
          Json.obj(wordcloud -> (if (wordcloudJson == null) JsNull else wordcloudJson))
        ))
  }
}
