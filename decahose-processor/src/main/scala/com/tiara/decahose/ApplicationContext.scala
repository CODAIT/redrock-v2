package com.tiara.decahose

/**
 * Created by barbaragomes on 4/4/16.
 */

import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive._

object ApplicationContext {

  val sparkConf = new SparkConf()
  sparkConf.setAppName(Config.appConf.getString("app-name") + " - Decahose")
  sparkConf.set("spark.scheduler.mode", "FAIR")

  // Spark master resources
  sparkConf.set("spark.executor.memory",Config.processorConf.getString("spark.executor-memory"))
  sparkConf.set("spark.executor.cores", s"${Config.processorConf.getInt("spark.executor-cores")}")
  sparkConf.set("spark.ui.port",Config.processorConf.getString("spark.UI-port"))
  sparkConf.set("spark.cores.max",s"${Config.processorConf.getInt("spark.cores-max")}")

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sparkContext)

  // config sqlContext
  sqlContext.setConf("spark.sql.shuffle.partitions", s"""${Config.processorConf.getInt("spark.sql-shuffle-partitions")}""")
  sqlContext.setConf("spark.sql.tungsten.enabled", "true")

  // generic class to access and manage HDFS files/directories located in distributed environment.
  val hadoopFS: FileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

}
