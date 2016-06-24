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

/**
 * Created by barbaragomes on 5/9/16.
 * Implemented using;
 * # SentiWordNet v3.0.0 (1 June 2010)
 * Copyright 2010 ISTI-CNR.
 * All right reserved.
 *
 * SentiWordNet is distributed under the Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0) license.
 * http://creativecommons.org/licenses/by-sa/3.0/
 *
 * For any information about SentiWordNet:
 * Web: http://sentiwordnet.isti.cnr.it
 *
 * Reference article:
 * http://www.academia.edu/1336655/Reviews_Classification_Using_SentiWordNet_Lexicon
 */
import scala.collection.mutable.HashMap

object SentiWordList {

  private val sentWordNetPath = "/SentiWordNet_3.0.0_20130122.txt"

  val list: HashMap[String, WordScore] = processWordListFromSentiWordNet()

  private def processWordListFromSentiWordNet(): HashMap[String, WordScore] = {

    val wordList: HashMap[String, WordScore] = new HashMap[String, WordScore]()

    scala.io.Source.fromInputStream(
      getClass.getResourceAsStream(sentWordNetPath)).getLines().foreach(auxLine => {
        // discard comments
        val line = auxLine.trim
        if (line != null && line != "" && !line.startsWith("#")) {
          val analysis = line.split("\t")
          // discard nouns
          if (analysis(0) != "n") {
            // get scores
            val postitiveScore: Double = analysis(2).toDouble
            val negativeScore: Double = analysis(3).toDouble

            // get list of words
            val words = analysis(4).split(" ")
            words.foreach(wordSense => {
              // discard sense
              val word = wordSense.substring(0, wordSense.indexOf("#")).toLowerCase()
              var tmpScore = new WordScore(postitiveScore, negativeScore, 1)
              wordList.get(word) match {
                case Some(score) =>
                  // add sum and increase count
                  tmpScore =
                    new WordScore(score.positive + postitiveScore,
                      score.negative + negativeScore, score.count + 1)
                case _ =>
              }
              wordList.put(word, tmpScore)
            })
          }
        }
      })

    // Calculate Average
    wordList.keySet.foreach(key => {
      val score = wordList.get(key).get
      val averagePositive = score.positive / score.count
      val averageNegative = score.negative / score.count
      wordList.put(key, new WordScore(averagePositive, averageNegative, score.count))
    })

    wordList
  }
}

case class WordScore(positive: Double, negative: Double, count: Int)
