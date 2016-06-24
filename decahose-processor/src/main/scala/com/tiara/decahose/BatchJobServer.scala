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

import org.apache.spark.Logging

/**
  * Created by zchen on 4/21/16.
  */
class BatchJobServer extends Runnable with Logging {

  def run(): Unit = {
    import java.net._
    import java.io._
    import scala.io._

    val server = new ServerSocket(Config.processorConf.getInt("batch-server-listen-port"))
    while (true) {
      val s = server.accept()
      val in = new BufferedSource(s.getInputStream()).getLines()
      val out = new PrintStream(s.getOutputStream())

      in.foreach( processLine(_) )
      // scalastyle:off println
      // we are turning off scalastyle println warn here
      // because the println is sending data to a socket
      out.println(in.mkString("\n"))
      // scalastyle:on println
      out.flush()
      s.close()
    }
  }

  def processLine(line: String): Unit = {
    val paths: Array[String] = line.split(" ")
    logInfo(s"batch processing paths: ${line}")
    val df = ApplicationContext.sqlContext.read.schema(ApplicationContext.schema).json(paths: _*)
    TweetProcessor.processedTweetsDataFrame(df, "", saveMode = org.apache.spark.sql.SaveMode.Append)
  }
}
