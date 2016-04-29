package com.tiara.restapi

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.{JsArray, JsNull, JsObject, Json}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, future}
import org.apache.spark.sql.functions._

import scala.concurrent._

/**
 * Created by barbaragomes on 4/25/16.
 */
object ExecuteCommunityGraph extends Logging{

  val objcName = "communities"
  val nodeDescription = Json.obj("node" -> Json.arr("label","id","degree","community","x","y","z"))
  val edgeDescription = Json.obj("edge" -> Json.arr("id", "source", "target", "weight"))

  def getResults(searchTerms: String, get3Dresults: Boolean = false): Future[String] = {

    logInfo(s"Get community graph with search: $searchTerms.")
    logInfo(s"Add 3D result: $get3Dresults")

    val result = future {getCommunityGraphForVisualization(searchTerms.toLowerCase(),get3Dresults)}

    result.recover{
      case e: Exception => logError("Could not execute request.", e); Json.stringify(buildResponse(false))
    }

  }

  private def getCommunityGraphForVisualization(searchTerms: String, get3Dresults: Boolean): String = {
    //Get filtered retweets
    val filterRegex = s"(${searchTerms.trim.replaceAll(",","|")})"
    val filteredDF = InMemoryData.retweetsENDF.filter(col("body").rlike(filterRegex))

    var results: JsObject = null
    if(get3Dresults) {
      results = getCommunity3D(filteredDF)
    }else {
      results = getCommunity2D(filteredDF)
    }

    val response:JsObject = buildResponse(get3Dresults, true, results)

    Json.stringify(response)
  }

  private def getCommunity2D(filteredDF: DataFrame): JsObject = {
    val edgeList = filteredDF.select(col("uid"), col("ouid")).collect.map((r: Row) => (r.getString(0), r.getString(1)))

    GraphUtils.edgeListToFinalJson(edgeList, zeroZ = true)
  }

  private def getCommunity3D(filteredDF: DataFrame): JsObject = {
    val edgeList = filteredDF.select(col("uid"), col("ouid")).collect.map((r: Row) => (r.getString(0), r.getString(1)))

    GraphUtils.edgeListToFinalJson(edgeList, zeroZ=false)
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

}
