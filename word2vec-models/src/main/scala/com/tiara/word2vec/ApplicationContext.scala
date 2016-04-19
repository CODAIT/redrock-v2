package com.tiara.word2vec

/**
 * Created by barbaragomes on 4/15/16.
 */

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive._

object ApplicationContext {

  val sparkConf = new SparkConf()
  sparkConf.setAppName(Config.appConf.getString("app-name") + " - Word2Vec")
  sparkConf.set("spark.scheduler.mode", "FAIR")

  // Spark master resources
  sparkConf.set("spark.executor.memory",Config.word2vecConf.getString("spark.executor-memory"))
  sparkConf.set("spark.executor.cores", s"${Config.word2vecConf.getInt("spark.executor-cores")}")
  sparkConf.set("spark.ui.port",Config.word2vecConf.getString("spark.UI-port"))
  sparkConf.set("spark.cores.max",s"${Config.word2vecConf.getInt("spark.cores-max")}")

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sparkContext)

  // config sqlContext
  sqlContext.setConf("spark.sql.shuffle.partitions", s"""${Config.word2vecConf.getInt("spark.sql-shuffle-partitions")}""")
  sqlContext.setConf("spark.sql.tungsten.enabled", "true")

  // generic class to access and manage HDFS files/directories located in distributed environment.
  val hadoopFS: FileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

  RegisterTwokenizeUDF.register

}

object RegisterTwokenizeUDF{

  def register = ApplicationContext.sqlContext.udf.register("twokenize", (text:String) => twokenize(text))

  private def twokenize(text:String): Array[String] = {
    cmu.arktweetnlp.Twokenize.tokenizeRawTweetText(text).toArray.map(_.toString.toLowerCase())
  }
}
