package com.tiara.word2vec

import scala.collection.mutable.WrappedArray

/**
 * Created by barbaragomes on 5/2/16.
 */
object SqlUtils {

  // Use only tokens that are hashtags or handles
  val getHashtagsAndHandles = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[String]) => it.filter((token:String) => (token.startsWith("#") || token.startsWith("@")))
  )

  val isEmpty = org.apache.spark.sql.functions.udf(
    (it: WrappedArray[String]) => it.isEmpty
  )

}
