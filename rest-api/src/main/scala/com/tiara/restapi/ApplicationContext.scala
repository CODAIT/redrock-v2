package com.tiara.restapi

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.{JedisPoolConfig, JedisPool}

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

  /* You shouldn't use the same instance from different threads because you'll have strange errors.
   * And sometimes creating lots of Jedis instances is not good enough because it means lots of sockets and connections.
   * To avoid these problems, you should use JedisPool, which is a threadsafe pool of network connections.
   */
  val jedisPool: JedisPool = new JedisPool(new JedisPoolConfig(), Config.restapi.getString("redis-server"))

}
