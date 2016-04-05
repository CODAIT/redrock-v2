package com.tiara.decahose

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.streaming.Time

/**
 * Created by barbaragomes on 4/4/16.
 */
object Utils extends Logging {

  def transformSparkTime(time: Time): String = {
    val date = new Date(time.milliseconds)
    val sdf:SimpleDateFormat = new SimpleDateFormat(Config.appConf.getString("date-time-format-to-display"))
    sdf.format(date)
  }

  /* Delete File on HDFS */
  def deleteFile(fileName: String): Unit = {
    try {
      val filePath = new Path(fileName)

      /* If is a directory */
      if (ApplicationContext.hadoopFS.isDirectory(filePath)) {
        ApplicationContext.hadoopFS.listStatus(filePath).foreach((status) => {
          logInfo(status.getPath().toString)
          ApplicationContext.hadoopFS.delete(status.getPath(), true)
        })
      }
      else {
        logInfo(fileName)
        ApplicationContext.hadoopFS.delete(filePath, true)
      }
    }catch {
      case e: Exception => logError("Could not delete file(s)", e)
    }
  }

}
