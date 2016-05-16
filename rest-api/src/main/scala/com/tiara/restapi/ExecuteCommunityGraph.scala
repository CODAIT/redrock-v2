package com.tiara.restapi

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, future}
import org.apache.spark.sql.functions._
import Utils._
import scala.util.{Success, Failure}
import redis.clients.jedis._
import scala.concurrent._

/**
 * Created by barbaragomes on 4/25/16.
 */
object ExecuteCommunityGraph extends Logging{

  val objcName = "communities"
  val nodeDescription = Json.obj("node" -> Json.arr("label","id","degree","community","x","y","z"))
  val edgeDescription = Json.obj("edge" -> Json.arr("id", "source", "target", "weight"))
  val cacheMembership = Config.restapi.getBoolean("cache-graph-membership")
  val cacheExpiration = Config.restapi.getInt("membership-graph-expiration")

  def getResults(searchTerms: String, get3Dresults: Boolean = false): Future[String] = {

    logInfo(s"Get community graph with search: $searchTerms.")
    logInfo(s"Add 3D result: $get3Dresults")

    val result = future {getCommunityGraphForVisualization(searchTerms.toLowerCase(),get3Dresults)}

    result.recover{
      case e: Exception => logError("Could not execute request.", e); Json.stringify(buildResponse(false))
    }

  }

  private def getCommunityGraphForVisualization(searchTerms: String, get3Dresults: Boolean): String = {
    val startTime = System.nanoTime()
    logInfo("Filtering RT dataframe")
    /* Using white space between the terms in order to avoid matches like: #go => #going
     * Match should be a complete match: #go only matches #go
     */
    val filterRegex = s"(${searchTerms.trim.replaceAll(","," | ")} )"
    val filteredDF = InMemoryData.retweetsENDF.filter(col(COL_TOKENS).rlike(filterRegex))

    var results: JsObject = null
    var success:Boolean = false;
    try {
      results = getCommunity(filteredDF, get3Dresults)
      success = true
    }catch {
      case e: Exception => logInfo("Error while generating community graph", e)
    }

    val response:JsObject = buildResponse(get3Dresults, success, results)
    val elapsed = (System.nanoTime() - startTime) / 1e9
    logInfo(s"Community Graph finished. Execution time: $elapsed")

    if(cacheMembership) {
      // Cache graph membership in background thread
      Future {
        cacheCommunityMembership(searchTerms, results)
      }.onComplete {
        case Success(md5) => logInfo(s"Graph membership computed for search term MD5 $md5")
        case Failure(t) => logError("Could not cache graph membership", t)
      }
    }

    Json.stringify(response)
  }

  private def getCommunity(filteredDF: DataFrame, get3Dresults: Boolean): JsObject = {
    val edgeList = filteredDF.select(col("uid"), col("ouid")).collect.map((r: Row) => (r.getString(0), r.getString(1)))

    GraphUtils.edgeListToFinalJson(edgeList, zeroZ = !get3Dresults)
  }

  private def buildResponse(get3Dresults:Boolean, success: Boolean = true, result: JsObject = null):JsObject = {
    val response = Json.obj("success" -> success) ++
      Json.obj("status" -> 0) ++
      nodeDescription ++ edgeDescription

    if(!success)
      response ++ Json.obj(objcName -> JsNull)
    else if(result == null){
      response ++ Json.obj(objcName -> Json.arr())
    }else{
      response ++ Json.obj(objcName -> result)
    }
  }

  private def cacheCommunityMembership(searchTerms: String, graph: JsObject):String = {
    logInfo("Caching graph membership")
    val md5 = getMD5forSearchTerms(searchTerms)
    logInfo(s"Search terms MD5: $md5")

    val jedis = ApplicationContext.jedisPool.getResource
    var pipe = jedis.pipelined()

    if(jedis.exists(md5)){
      logInfo("Dropping old cache")
      val keys:Array[String] = jedis.keys(s"$md5*").toArray.map(_.toString)
      jedis.del(keys:_*)
    }

    var count = 0
    val nodes = (graph \ GraphUtils.nodesLabel).as[List[List[JsValue]]]
    nodes.foreach(node => {
      val community:String = node(3).as[String]
      pipe.sadd(md5,community)
      pipe.lpush(s"$md5:$community", node(0).as[String])
      // Timeline when the cache will be avaiable
      pipe.expire(md5,cacheExpiration)
      pipe.expire(s"$md5:$community",cacheExpiration)
      count += 2;

      // Commit to redis every 10k instructions
      if(count == 10000){
        pipe.sync()
        pipe.close()
        pipe = jedis.pipelined()
        count = 0
      }
    })

    pipe.sync()
    pipe.close()
    jedis.close()
    md5
  }

}
