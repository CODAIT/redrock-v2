package com.tiara.restapi

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import play.api.libs.json._
import Utils._
import redis.clients.jedis.Jedis
import org.apache.spark.sql.functions._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent._

/**
 * Created by barbaragomes on 5/5/16.
 */
object ExecuteCommunityDetails extends Logging{

  val wordcloud = "wordcloud"
  val sentiment = "sentiment"

  def getDetails(searchTerms: String): Future[String] = {

    logInfo(s"Get community graph details with search: $searchTerms.")
    val md5 = getMD5forSearchTerms(searchTerms)

    val result = future {computeDetails(searchTerms.toLowerCase(),md5)}

    result.recover{
      case e: Exception => logError("Could not execute request.", e); Json.stringify(buildResponse(false,null))
    }
  }

  private def computeDetails(searchTerms: String, md5: String):String = {
    val jedis = ApplicationContext.jedisPool.getResource
    val communities = jedis.smembers(md5)
    if(communities != null && !communities.isEmpty){
      val it = communities.iterator()
      var response = Json.obj()
      while(it.hasNext){
        val community = it.next()
        response = response ++ getDetailsForCommunity(md5,community,jedis)
      }
      jedis.close()
      Json.stringify(buildResponse(true,response))
    }else{
      logInfo("Cache for graph expired or is not complete yet")
      jedis.close()
      Json.stringify(buildResponse(true,Json.obj()))
    }
  }

  private def getDetailsForCommunity(md5: String, communityID: String, jResource: Jedis): JsObject = {
    val userList:Array[String] = jResource.lrange(s"$md5:$communityID", 0, -1).toArray.map(_.toString)
    if(userList != null && !userList.isEmpty){
      val tweePerUserDF = InMemoryData.retweetsENDF.filter(col("uid").isin(userList:_*))
      Json.obj(communityID -> Json.obj(wordcloud -> Json.arr(tweePerUserDF.count()), sentiment -> Json.arr(tweePerUserDF.count())))
    }else{
      Json.obj(communityID -> Json.obj(wordcloud -> Json.arr(), sentiment -> Json.arr()))
    }
  }

  private def buildResponse(success: Boolean, response: JsObject):JsObject = {
    Json.obj("success" -> success) ++ Json.obj("communitydetails" -> response)
  }
}
