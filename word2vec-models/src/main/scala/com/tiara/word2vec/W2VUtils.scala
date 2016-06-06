package com.tiara.word2vec

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray
import SqlUtils._
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zchen on 6/1/16.
  */
object W2VUtils extends Logging {

//  val sparkContext = new SparkContext(new SparkConf)
//  val sqlContext = new HiveContext( sparkContext )

  def computeW2VForWindow(sqlContext: SQLContext, hashtagOnly: Boolean, prefix: String, start: String, count: Int): Unit = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val startDate = formatter.parse(start)
    val startCal = Calendar.getInstance()
    startCal.setTime(startDate)
    startCal.add(java.util.Calendar.DAY_OF_MONTH, count)
    val end = formatter.format(startCal.getTime)

    val tweetsTokens = sqlContext
      .read.parquet(prefix + "/toks")
      .filter(col("postedDate") >= lit(start))
      .filter(col("postedDate") < lit(end))
      .select("toks")

    val tweetsTokensRDD = if (hashtagOnly)
      tweetsTokens.select(getHashtagsAndHandles(col("toks")).as("toks"))
        .filter(!isEmpty(col("toks")))
        .rdd
    else
      tweetsTokens.rdd

    val rddToWord2Vec = tweetsTokensRDD.map((r:Row)=> r.getAs[WrappedArray[String]](0))

    //    val folder = s"""$modelFolder/$start"""

    //Computing and storing frequency analysis
    // Use counters from redis
    //saveWordCount(tweetsTokensRDD, folder)

    // Create word2Vec
    val w2v = new Word2Vec()
      .setNumIterations(6)
      .setMinCount(5*count)
      .setNumPartitions(12)
      .setVectorSize(100)
      .setSeed(42L)
      .setWindowSize(5)

    logInfo(s"Computing word2vec model for $count / $start")
    val w2v_model = w2v.fit(rddToWord2Vec)
    logInfo(s"Word2vec model computed for $count / $start")
    logInfo(s"Word2vec Size == ${w2v_model.getVectors.size}")
    w2v_model.save(sqlContext.sparkContext, s"${prefix}/models/days=${count}/${start}/w2v")
    //      s"$folder/${Config.word2vecConf.getString("folder-name-model")}")
    logInfo(s"Word2vec model stored for $count / $start")
  }

  def batchComputeW2V(sqlContext: SQLContext, hashtagOnly: Boolean = true, prefix: String, startStr: String, endStr: String, count: Int): Unit = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")

//    val startStr = "2016-01-03"
//    val endStr = "2016-05-14"

    val startDate = formatter.parse(startStr)
    val startCal = Calendar.getInstance()
    startCal.setTime(startDate)
    var nowCal = startCal

    val endDate = formatter.parse(endStr)
    val endCal = Calendar.getInstance()
    endCal.setTime(endDate)

    while (nowCal.before(endCal)) {
      computeW2VForWindow(sqlContext, hashtagOnly, prefix, formatter.format(nowCal.getTime), count)
      nowCal.add(java.util.Calendar.DAY_OF_MONTH, count)
    }

  }

}
