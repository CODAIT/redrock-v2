package com.tiara.decahoseactor

import java.net.URL
import java.text.SimpleDateFormat

import akka.actor.{Terminated, Props, Actor}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.{IOUtils}
import org.apache.commons.lang3.time.DateUtils
import java.util.{TimeZone, Calendar, Date}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by barbaragomes on 4/1/16.
 */
//TODO: Use log4j instead of println
class PollDecahoseData extends Actor{

  import PollDecahoseData._

  val delay = Config.processorConf.getInt("poll-decahose-actor.startDownloadingAfter").seconds
  val interval = Config.processorConf.getInt("poll-decahose-actor.timeInterval").seconds

  val conf = new Configuration()
  conf.set("fs.defaultFS", Config.appConf.getString("hadoop-default-fs"))
  val hadoopFS = FileSystem.get(conf)

  context.system.scheduler.schedule(delay, interval) {
    downloadNewFile()
  }

  //Set timezone
  TimeZone.setDefault(TimeZone.getTimeZone(Config.processorConf.getString("poll-decahose-actor.timezone")))

  val header = getHeader(Config.processorConf.getString("poll-decahose-actor.user"), Config.processorConf.getString("poll-decahose-actor.password"))
  val hostname = Config.processorConf.getString("poll-decahose-actor.hostname")

  // Bypassing handshacking validation. The certificate CN (Common Name) should be the same as host name in the URL.
  // For bluemix archive that is not true, the DN on the certificate is localhost. Talk to Viktor and seems like they will not
  // fix it for the archive server
  javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
    new javax.net.ssl.HostnameVerifier(){
      def verify(hostname:String , sslSession:javax.net.ssl.SSLSession):Boolean  = {
        if(hostname == "cde-archive.services.dal.bluemix.net")
          return true
        else
          return false
      }
    })

  override def receive: Receive = {
    case StartDownloadingData => {
      println("Decahose actor received message. Download of Decahose data started.")
    }
    case Terminated => {
      println("Download of Decahose data stopped. Last downloaded file: ")
    }
  }

  def encodeCredentials(username: String, password: String): String = {
    new String(Base64.encodeBase64String((username + ":" + password).getBytes))
  }

  def getHeader(username: String, password: String): String =
    "Basic " + encodeCredentials(username, password)

  def downloadNewFile() = {
    val (fileName, url) = getNextFileNameAndURL()
    try {
      println(s"Downloading: $url")
      val connection = new URL(url).openConnection
      connection.setRequestProperty("Authorization", header)
      //writes on hadoop file system - Same hadoop begin used by Spark
      val hdfs_path = s"${Config.processorConf.getString("decahose-dir")}/${fileName}"
      val hdfs_file = hadoopFS.create(new Path(s"${hdfs_path}${Config.processorConf.getString("writing-mode-string")}"))
      hdfs_file.write(IOUtils.toByteArray(connection.getInputStream))
      hdfs_file.close()
      //Rename File To indicate that the file is ready to be processed by Spark
      hadoopFS.rename(new Path(s"${hdfs_path}${Config.processorConf.getString("writing-mode-string")}"), new Path(s"${hdfs_path}"))
      //To write on local file system
      //FileUtils.copyInputStreamToFile(connection.getInputStream, new File(s"${Config.appConf.getString("paths.decahose-dir")}/$fileName"))
      println(s"File $fileName downloaded.")
    }catch {
      case e: Exception => println(s"Could not download file: $fileName => URL: $url"); println(e.toString)
    }
  }


  def getNextFileNameAndURL(): (String, String) ={

    val fileName = StoreDateTimeAndConfig.fileNameFormat.format(StoreDateTimeAndConfig.currDatetime)
    var response:(String, String) = (s"${fileName}_${StoreDateTimeAndConfig.fileNameExtensionJson}",
      s"$hostname${StoreDateTimeAndConfig.URLFormat.format(StoreDateTimeAndConfig.currDatetime)}_${StoreDateTimeAndConfig.fileNameExtensionJson}")

    // Use GSON extension
    if(StoreDateTimeAndConfig.currDatetime.getYear() <= (1900 - 2015) && StoreDateTimeAndConfig.currDatetime.getMonth() <= 5){
      response = (s"${fileName}_${StoreDateTimeAndConfig.fileNameExtensionGson}",
        s"$hostname${StoreDateTimeAndConfig.URLFormat.format(StoreDateTimeAndConfig.currDatetime)}_${StoreDateTimeAndConfig.fileNameExtensionGson}")
    }

    // Next datetime file name to be downloaded
    StoreDateTimeAndConfig.currDatetime = DateUtils.addMinutes(StoreDateTimeAndConfig.currDatetime, 10)
    response
  }

}

object PollDecahoseData {
  case object StartDownloadingData

  def props: Props = {
    Props(new PollDecahoseData)
  }
}

object StoreDateTimeAndConfig {

  // Datetime format for filename on bluemix
  val URLFormat:SimpleDateFormat = new SimpleDateFormat(Config.processorConf.getString("poll-decahose-actor.fileNameFormatForURL"))
  val fileNameFormat:SimpleDateFormat = new SimpleDateFormat(Config.processorConf.getString("poll-decahose-actor.fileNameFormat"))
  val fileNameExtensionJson:String = Config.processorConf.getString("poll-decahose-actor.fileNameExtensionJson")
  val fileNameExtensionGson:String = Config.processorConf.getString("poll-decahose-actor.fileNameExtensionGson")


  var currDatetime: Date = {
    /* Get the latest 10min date */
    val datenow = Calendar.getInstance().getTime()
    if(datenow.getMinutes() < 10){
      datenow.setMinutes(0)
    }else{
      datenow.setMinutes((datenow.getMinutes/10)*10)
    }
    datenow
  }
}
