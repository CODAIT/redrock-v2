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

import akka.actor.{ActorContext, Actor}
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
  def actorRefFactory: ActorContext = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive: Receive = runRoute(tiaraRoute)

}

trait TiaraService extends HttpService {

  val home = pathPrefix("tiara")
  val forceNodeGraph = path("getsynonyms") & parameters('searchterm, 'count.as[Int])
  val communitySetParams =
    path("community-set-params") & parameters('resolution.as[Double], 'top.as[Double])
  val communityGraph = path("getcommunities") & parameters('searchterms, 'get3d.as[Boolean])
  val topTerms = path("gettopterms") & parameters('count.as[Int])
  val communityDetails = path("getcommunitiesdetails") & parameters('searchterms, 'count.as[Int])
  val updateModel = path("updateModel") & parameters('w2vpath, 'rtstartdate, 'rtenddate)
  val gephiLayoutService = path("gephi-layout") & parameters('graph, 'threshold.as[Double])

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
      communitySetParams { (resolution, top) =>
        get{
          respondWithMediaType(`application/json`) {
            complete {
              ExecuteCommunityGraph.setParams(resolution, top)
            }
          }
        }
      } ~
        communityGraph { (searchTerms, get3D) =>
          get{
            respondWithMediaType(`application/json`) {
              complete {
                ExecuteCommunityGraph.getResults(searchTerms, get3D)
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
      communityDetails { (searchTerms, count) =>
        get{
          respondWithMediaType(`application/json`) {
            complete {
              ExecuteCommunityDetails.getDetails(searchTerms, count)
            }
          }
        }
      } ~
      updateModel { (w2vPath, rtStartDate, rtEndDate) =>
        get{
          respondWithMediaType(`application/json`) {
            complete {
              UpdateWord2VecModel.updateModel1(w2vPath, rtStartDate, rtEndDate)
            }
          }
        }
      } ~
      gephiLayoutService { (graph, threshold) =>
        get{
          respondWithMediaType(`application/json`) {
            complete {
              GraphUtils.getLayoutJsonForFile(graph, speedDescentThreshold = threshold)
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
