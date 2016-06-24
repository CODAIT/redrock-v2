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
package com.tiara.decahose

import org.apache.spark.sql.{DataFrame, Row}
import SqlUtils._
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray

/**
  * Created by zchen on 5/26/16.
  */
object BatchUtils {

  def hashtagCountTSUpdate(toks: DataFrame): Unit = {
    toksToHashtagCounts(toks).foreachPartition(
      (rows: Iterator[Row]) => groupedUpdateTimeSeries("TS:#", COL_POSTED_DATE, COL_HASHTAG, rows)
    )
  }

  def hashtagByAuthorCountTSUpdate(toks: DataFrame): Unit = {
    toksToHashtagByAuthorCounts(toks).foreachPartition(
      (rows: Iterator[Row]) => groupedUpdateTimeSeries("TS:#@", COL_POSTED_DATE, COL_HASHTAG, rows)
    )
  }

  def toksCounterUpdate1(toks: DataFrame): Unit = {
    val gDF = toks.select(
      col(COL_POSTED_DATE),
      explode(col(COL_TOKEN_SET)).as(COL_TWITTER_ENTITY)
    ).groupBy(COL_POSTED_DATE, COL_TWITTER_ENTITY).count.repartition(70)

    gDF.foreachPartition(
      (rows: Iterator[Row]) => groupedBulkUpdateCounters(COL_POSTED_DATE, COL_TWITTER_ENTITY, rows)
    )
  }

  def toksCounterUpdate2(toks: DataFrame): Unit = {
    val gDF = toks.filter("verb = 'post'")
      .select(
        col(COL_POSTED_DATE),
        explode(hashtagsFromToks(col(COL_TOKEN_SET))).as(COL_TOKEN_1),
        col(COL_TWITTER_AUTHOR).as(COL_TOKEN_2)
      ).groupBy(COL_POSTED_DATE, COL_TOKEN_1, COL_TOKEN_2).count.repartition(70)

    gDF.foreachPartition(
      (rows: Iterator[Row]) => groupedBulkUpdateTuples(COL_POSTED_DATE, rows)
    )
  }

  def toksCounterUpdate3(toks: DataFrame): Unit = {
    val gDF = toks.explode(COL_TOKEN_SET, COL_PAIR) {
      (toks: WrappedArray[String]) =>
        toks.toSeq.distinct
          // C(n,k) where k=2
          .combinations(2).toList
          // sort the 2-tuple so we can compare the pairs in subsequent groupBy
          .map(_.sorted)
          .map((x: Seq[String]) => Tuple2(x(0), x(1)))
    }
      .select(col(COL_POSTED_DATE), col(COL_PAIR + "._1").as(COL_TOKEN_1),
        col(COL_PAIR + "._2").as(COL_TOKEN_2))
      .groupBy(COL_POSTED_DATE, COL_TOKEN_1, COL_TOKEN_2).count
      .repartition(90)

    gDF.foreachPartition(
      (rows: Iterator[Row]) => groupedBulkUpdatePairs(COL_POSTED_DATE, rows)
    )
  }
}
