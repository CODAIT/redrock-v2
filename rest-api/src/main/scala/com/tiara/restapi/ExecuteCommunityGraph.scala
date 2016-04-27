package com.tiara.restapi

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import play.api.libs.json.{JsNull, JsObject, JsArray, Json}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, future}
import org.apache.spark.sql.functions._
import scala.concurrent._

/**
 * Created by barbaragomes on 4/25/16.
 */
object ExecuteCommunityGraph extends Logging{

  val objcName = "communities"
  val objDescription2D = Json.obj("content" -> Json.arr("label","id","degree","community","x","y"))
  val objDescription3D = Json.obj("content" -> Json.arr("source","target","weight","label","x","y"))

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

    var results: List[JsArray] = null
    if(get3Dresults) {
      results = getCommunity3D(filteredDF)
    }else {
      results = getCommunity2D(filteredDF)
    }

    val response:JsObject = buildResponse(get3Dresults, true, results)

    Json.stringify(response)
  }

  private def getCommunity2D(filteredDF: DataFrame):List[JsArray] = {
    //TODO: Implement graph generation from FILTERED RT DF
    List[JsArray](Json.arr("teste","for","2D","api", "count", filteredDF.count()))
  }

  private def getCommunity3D(filteredDF: DataFrame):List[JsArray] = {
    //TODO: Implement graph generation from FILTERED RT DF
    List[JsArray](Json.arr("teste","for","3D","api", "count", filteredDF.count()))
  }

  private def buildResponse(get3Dresults:Boolean, success: Boolean = true, result: List[JsArray] = null):JsObject = {
    val response = Json.obj("success" -> success) ++
      Json.obj("status" -> 0) ++
      (if(get3Dresults) objDescription3D else objDescription2D)
    if(!success)
      response ++ Json.obj(objcName -> JsNull)
    else if(result == null){
      response ++ Json.obj(objcName -> Json.arr())
    }else{
      response ++ Json.obj(objcName -> result)
    }
  }

}
