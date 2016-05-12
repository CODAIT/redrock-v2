package com.tiara.restapi

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json._
import Utils._
import redis.clients.jedis.Jedis
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent._

/**
 * Created by barbaragomes on 5/5/16.
 */
object ExecuteCommunityDetails extends Logging{

  val wordcloud = "wordcloud"
  val sentiment = "sentiment"
  val sentimentDescription = Json.obj(sentiment -> Json.obj("communityID" -> Json.arr("positive", "negative", "neutral")))
  val worldcloud = Json.obj(wordcloud -> Json.obj("communityID" -> Json.arr("word", "count")))

  def getDetails(searchTerms: String): Future[String] = {

    logInfo(s"Get community graph details with search: $searchTerms.")
    val md5 = getMD5forSearchTerms(searchTerms)

    val result = future {computeDetails(searchTerms.toLowerCase(),md5)}

    result.recover{
      case e: Exception => logError("Could not execute request.", e); Json.stringify(buildResponse(false,JsNull,JsNull))
    }
  }

  private def computeDetails(searchTerms: String, md5: String):String = {
    try{
      detailsFromJoin(md5, searchTerms)
    }catch {
      case e: Exception => logError("Could not get Details for community", e); Json.stringify(buildResponse(false,JsNull,JsNull))
    }
  }

  private def detailsFromJoin(md5: String, searchTerms: String): String = {
    //get Resource
    val jedis = ApplicationContext.jedisPool.getResource

    logInfo("Loading membership from redis")
    // Get communities from redis
    val communities = jedis.smembers(md5)
    if(communities != null && !communities.isEmpty) {
      val membership: ArrayBuffer[(String, Array[String])] = ArrayBuffer.empty
      val it_com = communities.iterator()

      // Populating membership array
      while(it_com.hasNext){
        val communityID = it_com.next()
        val users = getUsersForCommunity(md5, communityID, jedis)
        populateMembership(membership, communityID, users)
      }

      import ApplicationContext.sqlContext.implicits._
      //Get membership as DF
      val membershipDF = ApplicationContext.sparkContext
                          .parallelize(membership).toDF
                          .withColumnRenamed("_1", "community")
                          .withColumnRenamed("_2", "uids")
                          .select(col("community"), explode(col("uids")).as("uid"))

      val filteredRT = getFilteredRT(searchTerms)
      logInfo("Joining membership DF to Retweets DF")
      val membershipRT_DF = filteredRT.join(membershipDF, filteredRT("uid") === membershipDF("uid")).cache()

      // Get sentiment
      val sentimentResponse:JsValue = extractSentiment(membershipRT_DF)

      Json.stringify(buildResponse(true,sentimentResponse,JsNull))
    }else{
      logInfo(s"No cached data for search: $searchTerms == MD5 $md5")
      Json.stringify(buildResponse(false,Json.obj(),Json.obj()))
    }
  }

  private def extractSentiment(membershipDF: DataFrame): JsValue = {

    try {
      val startTime = System.nanoTime()

      logInfo("Extracting sentiment")
      val sentiment = membershipDF.groupBy("community", "sentiment")
        .agg(count("sentiment").as("count"))
        .map(row => (row.getString(0), (row.getInt(1), row.getLong(2).toInt)))
        .groupByKey

      // Json.ojb(CommunityID -> Json.Array[count of positive, count of negative, count of neutral])
      val mapToJson = sentiment.map(community_sentiment => {
        Json.obj(community_sentiment._1 ->
          Json.arr(community_sentiment._2.find { case (sent: Int, count: Int) => (sent, count) ==(1, count) }.getOrElse((0, 0))._2,
            community_sentiment._2.find { case (sent: Int, count: Int) => (sent, count) ==(-1, count) }.getOrElse((0, 0))._2,
            community_sentiment._2.find { case (sent: Int, count: Int) => (sent, count) ==(0, count) }.getOrElse((0, 0))._2))
      }).collect

      var jsonResponse = Json.obj()
      mapToJson.foreach(communityData => jsonResponse = jsonResponse ++ communityData)

      val elapsed = (System.nanoTime() - startTime) / 1e9
      logInfo(s"Extract sentiment finished. Execution time: $elapsed")

      jsonResponse
    }
    catch {
      case e: Exception => logError("Could not extract sentiment", e); JsNull
    }

  }

  private def getFilteredRT(searchTerms: String): DataFrame = {
    logInfo("Filtering Retweets DF")
    val filterRegex = s"(${searchTerms.trim.replaceAll(","," | ")} )"
    InMemoryData.retweetsENDF.filter(col(COL_TOKENS).rlike(filterRegex))
  }

  private def getUsersForCommunity(md5: String, communityID: String, jResource: Jedis): Array[String] = {
    jResource.lrange(s"$md5:$communityID", 0, -1).toArray.map(_.toString)
  }

  private def populateMembership(membership:ArrayBuffer[(String, Array[String])], communityID: String, users: Array[String]) = {
    if(users != null && !users.isEmpty)
      membership.append((communityID,users))
  }

  private def buildResponse(success: Boolean, sentimentJson: JsValue, wordcloudJson: JsValue):JsObject = {
    Json.obj("success" -> success) ++
      sentimentDescription ++ worldcloud ++
      Json.obj("communitydetails" -> (Json.obj(sentiment -> sentimentJson)
        ++ Json.obj(wordcloud -> wordcloudJson)))
  }
}
