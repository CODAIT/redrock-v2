package com.tiara.restapi

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by barbaragomes on 4/19/16.
 */
object ApplicationContext {
  val sparkConf = new SparkConf()
  sparkConf.setAppName(Config.appConf.getString("app-name") + " - Rest-API")
  sparkConf.set("spark.scheduler.mode", "FAIR")

  // Spark master resources
  sparkConf.set("spark.executor.memory",Config.restapi.getString("spark.executor-memory"))
  sparkConf.set("spark.executor.cores", s"${Config.restapi.getInt("spark.executor-cores")}")
  sparkConf.set("spark.ui.port",Config.restapi.getString("spark.UI-port"))
  sparkConf.set("spark.cores.max",s"${Config.restapi.getInt("spark.cores-max")}")

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sparkContext)

  // config sqlContext
  sqlContext.setConf("spark.sql.shuffle.partitions", s"""${Config.restapi.getInt("spark.sql-shuffle-partitions")}""")
  sqlContext.setConf("spark.sql.tungsten.enabled", "true")

  // generic class to access and manage HDFS files/directories located in distributed environment.
  sparkContext.hadoopConfiguration.set("fs.defaultFS", Config.appConf.getString("hadoop-default-fs"))
  val hadoopFS: FileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
}
