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
 * Created by barbaragomes on 4/4/16.
 */

import java.io.InputStream

import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types.{DataType, StructType}

object ApplicationContext {

  val sparkConf = new SparkConf()
  sparkConf.setAppName(Config.appConf.getString("app-name") + " - Decahose")
  sparkConf.set("spark.scheduler.mode", "FAIR")

  // Spark master resources
  sparkConf.set("spark.executor.memory", Config.processorConf.getString("spark.executor-memory"))
  sparkConf.set("spark.executor.cores", s"${Config.processorConf.getInt("spark.executor-cores")}")
  sparkConf.set("spark.ui.port", Config.processorConf.getString("spark.UI-port"))
  sparkConf.set("spark.cores.max", s"${Config.processorConf.getInt("spark.cores-max")}")
  sparkConf.set("spark.io.compression.codec", "lz4")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sparkContext)

  // config sqlContext
  sqlContext.setConf("spark.sql.shuffle.partitions",
    s"""${Config.processorConf.getInt("spark.sql-shuffle-partitions")}""")
  sqlContext.setConf("spark.sql.tungsten.enabled", "true")
  sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

  // generic class to access and manage HDFS files/directories located in distributed environment.
  sparkContext.hadoopConfiguration.set("fs.defaultFS",
    Config.appConf.getString("hadoop-default-fs"))
  val hadoopFS: FileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

  // create the schema object, which significantly speeds up
  // conversion of the json.gz files to dataframes
  val schema = {
    val stream: InputStream = getClass.getResourceAsStream("/schema.json")
    val lines = scala.io.Source.fromInputStream(stream).getLines.mkString("")
    DataType.fromJson(lines).asInstanceOf[StructType]
  }

}
