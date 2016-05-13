package com.tiara.restapi

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by barbaragomes on 4/20/16.
 */
class TiaraServiceActor extends Actor with TiaraService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(tiaraRoute)

}

trait TiaraService extends HttpService {

  val home = pathPrefix("tiara")
  val forceNodeGraph = path("getsynonyms") & parameters('searchterm, 'count.as[Int])
  val communityGraph = path("getcommunities") & parameters('searchterms, 'get3d.as[Boolean])
  val topTerms = path("gettopterms") & parameters('count.as[Int])
  val communityDetails = path("getcommunitiesdetails") & parameters('searchterms)
  val gephiLayoutService = path("gephi-layout") & parameters('graph)

  val tiaraRoute =
    home {

      /* End point to get force node graph data */
      forceNodeGraph { (searchTerm, count) =>
        get {
          respondWithMediaType(`application/json`) {
            complete {
              ExecuteWord2VecAndFrequencyAnalysis.getResults(searchTerm, count)
            }
          }
        }
      } ~
      communityGraph { (searchTerms, get3D) =>
        get{
          respondWithMediaType(`application/json`) {
            complete {
              ExecuteCommunityGraph.getResults(searchTerms,get3D)
            }
          }
        }
      } ~
      topTerms { (count) =>
        get{
          respondWithMediaType(`application/json`) {
            complete {
              ExecuteMetricAnalysis.getTopTerms(count)
            }
          }
        }
      } ~
      communityDetails { (searchTerms) =>
        get{
          respondWithMediaType(`application/json`) {
            complete {
              ExecuteCommunityDetails.getDetails(searchTerms)
            }
          }
        }
      }
      gephiLayoutService { (graph) =>
        get{
          respondWithMediaType(`application/json`) {
            complete {
              GraphUtils.getLayoutJsonForFile(graph)
            }
          }
        }
//        post{
//          respondWithMediaType(`application/json`) {
//            complete {
//            }
//          }
//        }
      }
    }

}
