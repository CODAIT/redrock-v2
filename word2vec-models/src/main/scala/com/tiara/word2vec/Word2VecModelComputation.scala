package com.tiara.word2vec

import java.text.SimpleDateFormat

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.commons.lang3.time.DateUtils
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row

/**
 * Created by barbaragomes on 4/15/16.
 */
class Word2VecModelComputation(val date:String) extends Logging{

  import Word2VecModelComputation._

  def generateModel(): Unit = {
    val tokensPath = s"""${Config.word2vecConf.getString("path-to-daily-tweets")}/$date"""
    if(ApplicationContext.hadoopFS.exists(new Path(tokensPath))) {
      /* Read all tweets tokens inside the directory - Contain just EN */
      val tweetsTokensRDD = ApplicationContext.sqlContext
        .read.parquet(tokensPath)
        .select(tokens_col)
        .rdd

      val rddToWord2Vec = tweetsTokensRDD.map((r:Row)=> r.getAs[WrappedArray[String]](0))

      val folder = s"""$modelFolder/$date"""

      //Computing and storing frequency analysis
      // Use counters from redis
      //saveWordCount(tweetsTokensRDD, folder)

      // Create word2Vec
      val w2v = new Word2Vec()
        .setNumIterations(Config.word2vecConf.getInt("parameters.iterations-number"))
        .setMinCount(Config.word2vecConf.getInt("parameters.min-word-count"))
        .setNumPartitions(Config.word2vecConf.getInt("parameters.partition-number"))
        .setVectorSize(Config.word2vecConf.getInt("parameters.vector-size"))
        .setSeed(42L)
        .setWindowSize(Config.word2vecConf.getInt("parameters.window-size"))

      logInfo(s"Computing word2vec model for day == $date")
      val w2v_model = w2v.fit(rddToWord2Vec)
      logInfo(s"Word2vec model computed for day == $date")
      logInfo(s"Word2vec Size == ${w2v_model.getVectors.size}")
      w2v_model.save(ApplicationContext.sparkContext,s"$folder/${Config.word2vecConf.getString("folder-name-model")}")
      logInfo(s"Word2vec model stored for day == $date")

      // Write token file to let the rest-api know when there is a new model
      writeNewFileToken(date)
    }else{
      logInfo(s"No tokens found for file: $tokensPath")
    }
  }

  private def writeNewFileToken(date: String) ={
    try {
      val hdfs_file = ApplicationContext.hadoopFS.create(new Path(s"""${modelFolder}/$tokenFileName"""))
      hdfs_file.writeBytes(date)
      hdfs_file.close()
    }catch{
      case e: Exception => logError(s"Token for day $date not generated.", e)
    }
  }

  private def saveWordCount(filtered: RDD[Row], folderPath: String) = {
    import ApplicationContext.sqlContext.implicits._

    logInfo("Computing Frequency analysis")
    val freq = (filtered.flatMap(line => line.getSeq[String](0).map((_,1)))
               .reduceByKey((a, b) => a + b)
                .filter(_._2 >= Config.word2vecConf.getInt("parameters.min-word-count"))).toDF("word","freq")
    logInfo(s"Frequency Analysis ended. Total words: ${freq.count()}")
    freq.write.parquet(s"$folderPath/${Config.word2vecConf.getString("folder-name-word-count")}")
    logInfo(s"Frequency Analysis Stored")
  }

}

object Word2VecModelComputation extends Logging{

  // Column name that contains tweet txt
  val tokens_col = Config.word2vecConf.getString("col-name-tweet-txt");

  val modelFolder = Config.word2vecConf.getString("path-to-daily-models")
  val tokenFileName = Config.word2vecConf.getString("token-file-name")

  def computeHistoricalWord2Vec():Unit = {
    val folderDateFormat:SimpleDateFormat = new SimpleDateFormat(Config.word2vecConf.getString("date-format"))
    val folderPrefix = Config.word2vecConf.getString("prefix-tokens-folder-daily")

    val startDate = folderDateFormat.parse(Config.word2vecConf.getString("historical.start-date"))
    val endDate = folderDateFormat.parse(Config.word2vecConf.getString("historical.end-date"))

    // Date iterator
    var currDate = startDate
    while(endDate.getTime - currDate.getTime >= 0){
      val dayFolder = s"$folderPrefix${folderDateFormat.format(currDate)}"
      logInfo(s"Historical model for: $dayFolder")

      try{
        val w2v = new Word2VecModelComputation(dayFolder)
        w2v.generateModel()
      }catch{
        case e: Exception => logError(s"Could not generate historical model for $dayFolder", e)
      }

      currDate = DateUtils.addDays(currDate, 1)
    }
  }

}

