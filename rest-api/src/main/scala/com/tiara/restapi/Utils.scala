package com.tiara.restapi

import java.security.MessageDigest

import scala.collection.mutable.WrappedArray

/**
 * Created by barbaragomes on 5/4/16.
 */
object Utils {

  val COL_SENTIMENT = Config.restapi.getString("sentiment-column-name")
  val COL_TOKENS = Config.restapi.getString("tokens-column-name")

  def md5(bytes: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.reset()
    digest.update(bytes)
    digest.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  }

  def getMD5forSearchTerms(searchTerms: String): String = {
    val terms = searchTerms.split(",");
    // Search terms you be sorted in order to always generate
    // the same MD5 no matter the order of the terms.
    scala.util.Sorting.quickSort(terms)
    md5(terms.mkString(",").getBytes)
  }

  val stringTokens = org.apache.spark.sql.functions.udf(
    (tokens: WrappedArray[String]) => {
        tokens.mkString(" ")
    })

}
