/**
 * (C) Copyright IBM Corp. 2015, 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.tiara.restapi

import java.security.MessageDigest

import scala.collection.mutable.WrappedArray

/**
 * Created by barbaragomes on 5/4/16.
 */
object Utils {

  val COL_SENTIMENT = Config.restapi.getString("sentiment-column-name")
  val COL_TOKENS = Config.restapi.getString("tokens-column-name")
  val TOKENS_SEPARATOR = " "

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
        tokens.mkString(TOKENS_SEPARATOR)
    })


  val tokensFromString = org.apache.spark.sql.functions.udf(
    (tokens: String) => {
      tokens.split(TOKENS_SEPARATOR)
    })


  val excludeHandles = org.apache.spark.sql.functions.udf(
    (word: String) => {
      !word.startsWith("@")
    })

}
