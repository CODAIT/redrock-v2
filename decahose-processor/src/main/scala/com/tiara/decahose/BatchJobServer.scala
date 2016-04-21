package com.tiara.decahose

/**
  * Created by zchen on 4/21/16.
  */
class BatchJobServer extends Runnable {

  def run() = {
    import java.net._
    import java.io._
    import scala.io._

    val server = new ServerSocket(Config.processorConf.getInt("batch-server-listen-port"))
    while (true) {
      val s = server.accept()
      val in = new BufferedSource(s.getInputStream()).getLines()
      val out = new PrintStream(s.getOutputStream())

      in.foreach( processLine(_) )
      out.println(in.mkString("\n"))
      out.flush()
      s.close()
    }
  }

  def processLine(line: String) = {
    val paths: Array[String] = line.split(" ")
    val df = ApplicationContext.sqlContext.read.schema(ApplicationContext.schema).json(paths:_*)
    TweetProcessor.processedTweetsDataFrame(df, "")
  }
}
