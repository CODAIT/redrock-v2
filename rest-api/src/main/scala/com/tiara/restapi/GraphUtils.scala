package com.tiara.restapi

import org.gephi.statistics.plugin.Modularity

import scala.collection.JavaConversions._
import org.gephi.graph.api._
import org.gephi.graph.api.Configuration
import org.gephi.graph.api.GraphModel
import org.gephi.graph.impl.{GraphModelImpl, NodeImpl}
import org.gephi.layout.plugin.forceAtlas3D.ForceAtlas3DLayout
import org.gephi.layout.plugin.forceAtlas2.ForceAtlas2
import play.api.libs.json.{JsArray, JsNull, JsObject, Json}


/**
  * Created by zchen on 4/28/16.
  */
object GraphUtils {

  val nodesLabel = "nodes"

  def randomizeLayout(graph: GraphModel, range: Float, zeroZ: Boolean = true): Unit = {
    for (node <- graph.getGraph.getNodes) {
      node.setX((((0.01 + Math.random()) * range * 2) - range).toFloat)
      node.setY((((0.01 + Math.random()) * range * 2) - range).toFloat)
      if (zeroZ) {node.setZ(0)} else {
        node.setZ((((0.01 + Math.random()) * range * 2) - range).toFloat)
      }
    }
  }

  def printLayout(graph: GraphModel): Unit = {
    for (node <- graph.getGraph.getNodes) {
      println(s"n=${node.getId}, x=${node.x}, y=${node.y}, z=${node.z}")
    }
  }

  def edgeListToGephiModel(graph: Array[(String, String)]): GraphModelImpl = {
    val config = new Configuration
    val graphModel = GraphModel.Factory.newInstance(config)
    val mod = graphModel.asInstanceOf[org.gephi.graph.impl.GraphModelImpl]
    val factory = graphModel.factory
    val store = mod.getStore

    val directedGraph: DirectedGraph = graphModel.getDirectedGraph

    for (edge <- graph) {
      val src = edge._1
      val dst = edge._2

      var n0: Node = null
      if (!store.hasNode(src)) {
        n0 = factory.newNode(src)
        n0.setLabel(src)
        store.addNode(n0)
      } else {
        n0 = store.getNode(src)
      }

      var n1: Node = null
      if (!store.hasNode(dst)) {
        n1 = factory.newNode(dst)
        n1.setLabel(dst)
        store.addNode(n1)
      } else {
        n1 = store.getNode(dst)
      }

      val e1 = factory.newEdge(n0, n1, 0, 1.0, true)
      store.addEdge(e1)
    }

    mod
  }

  def gephiLayout(graphModel: GraphModel, zeroZ: Boolean = true) = {
    randomizeLayout(graphModel, 100, zeroZ)

    val layout: ForceAtlas3DLayout = new ForceAtlas3DLayout(null)
    layout.setThreadCount(30)
    layout.setGraphModel(graphModel)
    layout.resetPropertiesValues()
    layout.initAlgo()
    var i: Int = 0
    while (i < 150 && layout.canAlgo) {
      layout.goAlgo()
      i += 1
    }
    layout.endAlgo()
  }

  def modelToJson(graphModel: GraphModel): JsObject = {
    val modCol = graphModel.getNodeTable.getColumn(Modularity.MODULARITY_CLASS)
    val directedGraph = graphModel.getDirectedGraph

    val nodes = Json.obj(nodesLabel ->
      JsArray(graphModel.getGraph.getNodes.map(
        (n: Node) =>
          Json.arr(n.getId.toString, n.getStoreId.toString, directedGraph.getDegree(n),
              n.getAttribute(modCol).toString, n.x, n.y, n.z)
      ).toSeq)
    )

    val edges = Json.obj("edges" ->
      JsArray(graphModel.getGraph.getEdges.map(
        (e: Edge) =>
          Json.arr(e.getTarget.getId.toString, e.getSource.getStoreId.toString,
            e.getTarget.getStoreId.toString, e.getWeight.toString)
      ).toSeq)
    )

    nodes ++ edges
  }

  def edgeListToFinalJson(edges: Array[(String,String)], zeroZ: Boolean = true): JsObject = {
    val mod = edgeListToGephiModel(edges)
    println(s"node count: ${mod.getGraph.getNodeCount}, edge count: ${mod.getGraph.getEdgeCount}")
    gephiLayout(mod, zeroZ)
    val modularity: Modularity = new Modularity
    modularity.execute(mod)
    modelToJson(mod)
  }

  def TestMain(args: Array[String]) = {

    val G = scala.io.Source.fromFile(
      //      "/tmp/small100.csv"
      args(0)
    ).getLines().toArray.map{
      (line: String) =>
        val toks = line.split(" ")
        new Tuple2(toks(0), toks(1))
    }
    val mod = edgeListToGephiModel(G)
    gephiLayout(mod, true)
    val modularity: Modularity = new Modularity
    modularity.execute(mod)

    println(Json.prettyPrint(modelToJson(mod)))
  }

}
