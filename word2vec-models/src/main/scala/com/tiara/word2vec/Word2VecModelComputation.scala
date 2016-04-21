package com.tiara.word2vec

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.ml.feature.{Word2Vec, StopWordsRemover}
import org.apache.spark.sql.DataFrame

/**
 * Created by barbaragomes on 4/15/16.
 */
class Word2VecModelComputation(val date:String) extends Logging{

  import Word2VecModelComputation._

  def generateModel(): Unit = {

    /* Read all tweets inside the directory - Contain just EN */
    val tweetsDF = ApplicationContext.sqlContext
                    .read.parquet(s"""${Config.word2vecConf.getString("path-to-daily-tweets")}/$date""")
                    // No need to filter. Zees function is saving only 'share' or 'post' actions
                    //.filter(s"verb = 'post' OR verb = 'share'")
                    // Select only the text of the tweets and twokenize it
                    .selectExpr(s"twokenize($txt_col) AS tmp")

    logInfo("Removing stopwords")
    // Remove stop words and drop column that contains the stop words
    val filteredTweetsTokens = stopWords.transform(tweetsDF).drop("tmp")

    val folder = s"""$modelFolder/$date"""

    //Computing and storing frequency analysis
    saveWordCount(filteredTweetsTokens, folder)

    // Create word2Vec
    val w2v = new Word2Vec()
              .setInputCol(txt_col)
              .setMaxIter(Config.word2vecConf.getInt("parameters.iterations-number"))
              .setMinCount(Config.word2vecConf.getInt("parameters.min-word-count"))
              .setNumPartitions(Config.word2vecConf.getInt("parameters.partition-number"))
              .setVectorSize(Config.word2vecConf.getInt("parameters.vector-size"))

    logInfo(s"Computing word2vec model for day == $date")
    val w2v_model = w2v.fit(filteredTweetsTokens)
    logInfo(s"Word2vec model computed for day == $date")
    logInfo(s"Word2vec Size == ${w2v_model.getVectors.count()}")
    w2v_model.write.save(s"$folder/${Config.word2vecConf.getString("folder-name-model")}")
    logInfo(s"Word2vec model stored for day == $date")

    // Write token file to let the rest-api know when there is a new model
    writeNewFileToken(date)

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

  private def saveWordCount(filtered: DataFrame, folderPath: String) = {
    import ApplicationContext.sqlContext.implicits._

    logInfo("Computing Frequency analysis")
    val freq = (filtered.rdd.flatMap(line => line.getSeq[String](0).map((_,1)))
               .reduceByKey((a, b) => a + b)
                .filter(_._2 >= Config.word2vecConf.getInt("parameters.min-word-count"))).toDF("word","freq")
    logInfo(s"Frequency Analysis ended. Total words: ${freq.count()}")
    freq.write.parquet(s"$folderPath/${Config.word2vecConf.getString("folder-name-word-count")}")
    logInfo(s"Frequency Analysis Stored")
  }

}

object Word2VecModelComputation{

  // Column name that contains tweet txt
  val txt_col = Config.word2vecConf.getString("col-name-tweet-txt");

  // Add punctuation to stop words
  val punctuation: Array[String] = Array("?","!",".",";",",","-",":","(",")","[", "]","{","}","*","\"","'")

  // Remove stop words
  val stopWords = new StopWordsRemover()
    .setCaseSensitive(false)
    .setInputCol("tmp")
    .setOutputCol(txt_col)
  stopWords.setStopWords(stopWords.getStopWords ++ punctuation)

  val modelFolder = Config.word2vecConf.getString("path-to-daily-models")
  val tokenFileName = Config.word2vecConf.getString("token-file-name")

}

