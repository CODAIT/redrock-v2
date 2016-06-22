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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


/**
 * Created by barbaragomes on 4/1/16.
 */
class PollDecahoseData extends Actor with akka.actor.ActorLogging {

  import PollDecahoseData._

  val delay = Config.processorConf.getInt("poll-decahose-actor.startDownloadingAfter").seconds
  val interval = Config.processorConf.getInt("poll-decahose-actor.timeInterval").seconds

  val conf = new Configuration()
  conf.set("fs.defaultFS", Config.appConf.getString("hadoop-default-fs"))
  val hadoopFS = FileSystem.get(conf)

  context.system.scheduler.schedule(delay, interval) {
    downloadNewFile()
  }

  // Set timezone
  TimeZone.setDefault(TimeZone.getTimeZone(
    Config.processorConf.getString("poll-decahose-actor.timezone")))

  val header = getHeader(Config.processorConf.getString("poll-decahose-actor.user"),
    Config.processorConf.getString("poll-decahose-actor.password"))
  val hostname = Config.processorConf.getString("poll-decahose-actor.hostname")

  // Bypassing handshacking validation. The certificate CN (Common Name) should be the same as host
  // name in the URL. For bluemix archive that is not true, the DN on the certificate is localhost.
  // Talked to Viktor and seems like they will not fix it for the archive server
  javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
    new javax.net.ssl.HostnameVerifier(){
      def verify(hostname: String , sslSession: javax.net.ssl.SSLSession): Boolean = {
        if (hostname == "cde-archive.services.dal.bluemix.net") {
          return true
        }
        else {
          return false
        }
      }
    })

  override def receive: Receive = {
    case StartDownloadingData => {
      log.info("Decahose actor received message. Download of Decahose data started.")
    }
    case Terminated => {
      log.info("Download of Decahose data stopped. Last downloaded file: ")
    }
  }

  def encodeCredentials(username: String, password: String): String = {
    new String(Base64.encodeBase64String((username + ":" + password).getBytes))
  }

  def getHeader(username: String, password: String): String =
    "Basic " + encodeCredentials(username, password)

  def downloadNewFile(): Unit = {
    val (fileName, url) = getNextFileNameAndURL()
    try {
      log.info(s"Downloading: $url")
      val connection = new URL(url).openConnection
      connection.setRequestProperty("Authorization", header)
      // writes on hadoop file system - Same hadoop begin used by Spark
      val hdfs_path = s"${Config.processorConf.getString("decahose-dir")}/${fileName}"
      val writing_mode_str = Config.processorConf.getString("writing-mode-string")
      val hdfs_file = hadoopFS.create(new Path(s"${hdfs_path}${writing_mode_str}"))
      hdfs_file.write(IOUtils.toByteArray(connection.getInputStream))
      hdfs_file.close()
      // Rename File To indicate that the file is ready to be processed by Spark
      hadoopFS.rename(new Path(s"${hdfs_path}${writing_mode_str}"), new Path(s"${hdfs_path}"))
      // To write on local file system:
      // FileUtils.copyInputStreamToFile(connection.getInputStream,
      //   new File(s"${Config.appConf.getString("paths.decahose-dir")}/$fileName"))
      log.info(s"File $fileName downloaded.")
    } catch {
      case e: Exception =>
        log.info(s"Could not download file: $fileName => URL: $url"); log.info(e.toString)
    }
  }

  def getNextFileNameAndURL(): (String, String) = {

    val fileName = StoreDateTimeAndConfig.fileNameFormat.format(StoreDateTimeAndConfig.currDatetime)
    var response: (String, String) =
      (s"${fileName}_${StoreDateTimeAndConfig.fileNameExtensionJson}", s"$hostname" ++
        s"${StoreDateTimeAndConfig.URLFormat.format(StoreDateTimeAndConfig.currDatetime)}_" ++
        s"${StoreDateTimeAndConfig.fileNameExtensionJson}")

    // Use GSON extension
    if (StoreDateTimeAndConfig.currDatetime.getYear() <= (1900 - 2015) &&
      StoreDateTimeAndConfig.currDatetime.getMonth() <= 5) {
      response = (s"${fileName}_${StoreDateTimeAndConfig.fileNameExtensionGson}", s"$hostname" ++
        s"${StoreDateTimeAndConfig.URLFormat.format(StoreDateTimeAndConfig.currDatetime)}_" ++
        s"${StoreDateTimeAndConfig.fileNameExtensionGson}")
    }

    // Next datetime file name to be downloaded
    StoreDateTimeAndConfig.currDatetime =
      DateUtils.addMinutes(StoreDateTimeAndConfig.currDatetime, 10)
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
  val URLFormat: SimpleDateFormat =
    new SimpleDateFormat(Config.processorConf.getString("poll-decahose-actor.fileNameFormatForURL"))
  val fileNameFormat: SimpleDateFormat =
    new SimpleDateFormat(Config.processorConf.getString("poll-decahose-actor.fileNameFormat"))
  val fileNameExtensionJson: String =
    Config.processorConf.getString("poll-decahose-actor.fileNameExtensionJson")
  val fileNameExtensionGson: String =
    Config.processorConf.getString("poll-decahose-actor.fileNameExtensionGson")


  var currDatetime: Date = {
    /* Get the latest 10min date */
    val datenow = Calendar.getInstance().getTime()
    if (datenow.getMinutes() < 10) {
      datenow.setMinutes(0)
    } else {
      datenow.setMinutes((datenow.getMinutes/10)*10)
    }
    datenow
  }
}
