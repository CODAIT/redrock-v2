package com.tiara.restapi

import java.security.MessageDigest
/**
 * Created by barbaragomes on 5/4/16.
 */
object Utils {

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
}
