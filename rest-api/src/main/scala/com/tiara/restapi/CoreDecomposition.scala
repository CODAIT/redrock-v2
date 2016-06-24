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

import org.gephi.graph.api.{GraphModel, Graph, Node}
import org.gephi.graph.impl.{GraphModelImpl, NodeImpl}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import scala.collection._
import scala.collection.convert.decorateAsScala._
import java.util.concurrent.ConcurrentHashMap

/**
  * Created by zchen on 5/8/16.
  */
class CoreDecomposition {

  def run() {}

  // XXX this is a port of the networkx core decomposition logic,
  // this currently produces incorrect result
  def coreNumber0(graphModel: GraphModel) = {
    val graph = graphModel.getGraph
    val degrees = new mutable.HashMap[Node, Int]
    graph.getNodes.toArray.foreach{
      (n: Node) =>
        degrees(n) = graph.getDegree(n)
    }

    val nodes = degrees.toSeq.sortBy(_._2).map(_._1).toArray

    val bin_boundaries = new ArrayBuffer[Int](10000)
    //    bin_boundaries.add(0)
    bin_boundaries.append(0)
    //    bin_boundaries :+ 0
    var curr_degree = 0

    for ((v, i) <- nodes.view.zipWithIndex) {
      if (degrees(v) > curr_degree) {
        val delta = degrees(v) - curr_degree
        //          bin_boundaries.addAll(List.fill(i)(delta))
        var j = 0
        for (j <- 1 to i) { bin_boundaries.append(delta) }
        //          bin_boundaries :+ (List.fill(i)(degrees(v) - curr_degree))
        curr_degree = degrees(v)
      }
    }

    val node_pos = new mutable.HashMap[Node, Int]
    nodes.zipWithIndex.foreach {
      case (n, i) => node_pos(n) = i
    }

    val core = degrees.clone
    val nbrs = new mutable.HashMap[Node, mutable.Set[Node]]
    graph.getNodes.toArray.foreach{
      (n: Node) =>
        nbrs(n) = mutable.Set[Node]()
        nbrs(n) ++= graph.getNeighbors(n).toArray
    }
    nodes.foreach{
      (v: Node) =>
        nbrs(v).foreach{
          (u: Node) =>
            if (core(u) > core(v)) {
              nbrs(u).remove(v)
              val pos = node_pos(u)
              val bin_start = bin_boundaries( core(u) )
              node_pos(u) = bin_start
              node_pos(nodes(bin_start)) = pos

              //            nodes(bin_start), nodes(pos) = nodes(pos), nodes(bin_start)
              val tmp = nodes(bin_start)
              nodes(bin_start) = nodes(pos)
              nodes(pos) = tmp

              bin_boundaries(core(u)) += 1
              core(u) -= 1
            }
        }
    }
    core
  }


  val core = new scala.collection.mutable.HashMap[Node, Int]
  //  val core = new ConcurrentHashMap[Node, Int]().asScala
  var change: Boolean = true
  val scheduled = new scala.collection.mutable.HashMap[Node, Boolean]
  //  val scheduled = new ConcurrentHashMap[Node, Boolean].asScala
  var iteration = 0

  def kCoreComputeUpperBound(v: NodeImpl, d_v: Int, N_v: Array[Node]): Int = {
    //    int[] c = new int[core[v]+1];
    //
    //    for(int i=0; i<d_v; i++) {
    //      int u = N_v[i];
    //      int j = Math.min(core[v], core[u]);
    //      c[j]++;
    //    }

    val c = new Array[Int](core(v) + 1)
    N_v.foreach{
      (u: Node) =>
        val j = Math.min(core(v), core(u))
        c(j) += 1
    }

    //    int cumul = 0;
    //    for(int i=core[v]; i>=2; i--) {
    //      cumul = cumul + c[i];
    //      if (cumul >= i)
    //        return i;
    //    }
    //
    //    return d_v;

    var cumul: Int = 0
    var i: Int = core(v)
    while (i>=2) {
      cumul = cumul + c(i)
      if (cumul >= i) {
        return i
      }
      i -= 1
    }

    d_v
  }

  def kCoreUpdate(g: Graph, v: NodeImpl): Unit = {
    if (iteration == 0) {
      //      core(v) = v.getOutDegree
      //      core(v) = v.getDegree
      scheduled(v) = true
      change = true
    } else {

      //      val d_v = v.getOutDegree
      val d_v = v.getDegree
      val n_v = g.getNeighbors(v).toArray
      val localEstimate = kCoreComputeUpperBound(v, d_v, n_v)

      if (localEstimate < core(v)) {
        core(v) = localEstimate
        change = true

        //      for(int i=0; i<d_v; i++) {
        //        int u = N_v[i];
        //        if(core[v]<=core[u])
        //          scheduled[u] = true;
        //      }
        n_v.foreach {
          (u: Node) =>
            if (core(v) <= core(u)) {
              scheduled(u) = true
            }
        }
      }
    }

  }

  // this is ported from java to scala and gephi
  // from https://github.com/athomo/kcore/blob/master/src/KCoreWG_M.java
  def kCoreDecomp(graphModel: GraphModel): Int = {
    val graph = graphModel.getDirectedGraph
    val n = graph.getNodeCount
    //    val n = graph.getNodes.iterator().next
    //    val ni = n.asInstanceOf[NodeImpl]
    //    val nV = graph.getNeighbors(n)

    val nodesSeq = graph.getNodes.toCollection
    val nodesPar = nodesSeq.par.toSet
    // scala.collection.parallel.immutable.ParSet[Node]() ++ graph.getNodes.toCollection
    //    nodesPar.tasksupport =
    //      new scala.collection.parallel.ForkJoinTaskSupport(
    //        new scala.concurrent.forkjoin.ForkJoinPool(1)
    //      )

    //    for (v <- graph.getNodes) {scheduled(v) = true;}
    nodesSeq.foreach{
      (v: Node) =>
        //        core(v) = graph.getOutDegree(v)
        core(v) = graph.getDegree(v)
        scheduled(v) = true
    }

    nodesPar.tasksupport =
      new scala.collection.parallel.ForkJoinTaskSupport(
        new scala.concurrent.forkjoin.ForkJoinPool(1)
      )

    while(true) {
      println("Iteration " + iteration)

      var num_scheduled = 0
      val scheduledNow = scheduled.clone()
      //      for(int v=0; v<n; v++)
      //      for (v <- graph.getNodes) {scheduled(v) = false;}
      //      nodes.foreach{ (v: Node) => scheduled(v) = false }

      //      for(int v=0; v<n; v++) {
      //      for (v <- graph.getNodes) {
      nodesSeq.foreach {
        (v: Node) =>
          scheduled(v) = false
          if(scheduledNow(v) == true) {
            num_scheduled += 1
            kCoreUpdate(graph, v.asInstanceOf[NodeImpl])
          }
      }
      println( "\t\t" + ((100.0*num_scheduled)/n) + "%\t of nodes were scheduled this iteration.")
      iteration += 1
      if (change == false) {
        return 0
      } else {
        change = false
      }
    }
    0
  }

}
