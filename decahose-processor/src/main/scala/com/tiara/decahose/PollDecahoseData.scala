package com.tiara.decahose

import java.io.File
import java.net.URL
import java.text.SimpleDateFormat

import akka.actor.{Terminated, Props, Actor}
import com.tiara.decahose.PollDecahoseData.StartDownloadingData
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.{FileUtils,IOUtils}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.Logging
import java.util.{TimeZone, Calendar, Date}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by barbaragomes on 4/1/16.
 */
class PollDecahoseData extends Actor with Logging{

  val delay = Config.appConf.getInt("poll-decahose-actor.startDownloadingAfter").seconds
  val interval = Config.appConf.getInt("poll-decahose-actor.timeInterval").seconds

  context.system.scheduler.schedule(delay, interval) {
    downloadNewFile()
  }

  //Set timezone
  TimeZone.setDefault(TimeZone.getTimeZone(Config.appConf.getString("poll-decahose-actor.timezone")))

  val header = getHeader(Config.appConf.getString("poll-decahose-actor.user"), Config.appConf.getString("poll-decahose-actor.password"))
  val hostname = Config.appConf.getString("poll-decahose-actor.hostname")
  /* Changes default file system to HDFS */
  val conf = new Configuration()
  conf.set("fs.defaultFS", Config.appConf.getString("hadoop-default-fs"))
  val fs= FileSystem.get(conf)

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
      logInfo("Decahose actor received message. Download of Decahose data started.")
    }
    case Terminated => {
      logInfo("Download of Decahose data stopped. Last downloaded file: ")
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
      logInfo(s"Downloading: $url")
      val connection = new URL(url).openConnection
      connection.setRequestProperty("Authorization", header)
      //writes on hadoop file system
      fs.create(new Path(s"${Config.appConf.getString("paths.decahose-dir")}/$fileName")).write(IOUtils.toByteArray(connection.getInputStream))
      //To write on local file system
      //FileUtils.copyInputStreamToFile(connection.getInputStream, new File(s"${Config.appConf.getString("paths.decahose-dir")}/$fileName"))
      logInfo(s"File $fileName downloaded.")
    }catch {
      case e: Exception => logError(s"Could not download file: $fileName => URL: $url", e)
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
  val URLFormat:SimpleDateFormat = new SimpleDateFormat(Config.appConf.getString("poll-decahose-actor.fileNameFormatForURL"))
  val fileNameFormat:SimpleDateFormat = new SimpleDateFormat(Config.appConf.getString("poll-decahose-actor.fileNameFormat"))
  val fileNameExtensionJson:String = Config.appConf.getString("poll-decahose-actor.fileNameExtensionJson")
  val fileNameExtensionGson:String = Config.appConf.getString("poll-decahose-actor.fileNameExtensionGson")


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
