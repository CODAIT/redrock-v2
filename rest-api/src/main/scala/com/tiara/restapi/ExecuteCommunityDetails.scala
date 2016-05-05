package com.tiara.restapi

import org.apache.spark.Logging
import play.api.libs.json.{JsValue, JsObject, Json}
import Utils._

import scala.concurrent._

/**
 * Created by barbaragomes on 5/5/16.
 */
object ExecuteCommunityDetails extends Logging{

  def getDetails(searchTerms: String): Future[String] = {

    logInfo(s"Get community graph with search: $searchTerms.")
    val md5 = getMD5forSearchTerms(searchTerms)

    val result = future {computeDetails(searchTerms.toLowerCase(),md5)}

    result.recover{
      case e: Exception => logError("Could not execute request.", e); Json.stringify(buildResponse(false))
    }
  }

  private def computeDetails(searchTerms: String, md5: String):String = {
    ""
  }

  private def buildResponse(success: Boolean = true):JsObject = {
    Json.obj("success" -> success)
  }
}
