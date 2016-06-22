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
import sbt._
import sbt.Keys._
import org.scalastyle.sbt.ScalastylePlugin._
import sbtassembly.AssemblyPlugin.autoImport._

object BuildSettings {

  val ParentProject = "tiara-parent"
  val RestAPIName = "tiara-restapi"
  val DecahoseName = "tiara-decahose-processor"
  val Word2VecName = "tiara-word2vec"
  val DecahoseActorName = "tiara-poll-decahose"

  val Version = "1.0"
  val ScalaVersion = "2.10.4"

  lazy val rootbuildSettings = Defaults.coreDefaultSettings ++ Seq (
    name          := ParentProject,
    version       := Version,
    scalaVersion  := ScalaVersion,
    organization  := "com.ibm.sparktc.tiara",
    description   := "TIARA External Project",
    scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
  )

  lazy val restAPIbuildSettings = Defaults.coreDefaultSettings ++ Seq (
    name          := RestAPIName,
    version       := Version,
    scalaVersion  := ScalaVersion,
    organization  := "com.ibm.sparktc.tiara",
    description   := "TIARA REST API Application",
    scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
  )

  lazy val decahosebuildSettings = Defaults.coreDefaultSettings ++ Seq (
    name          := DecahoseName,
    version       := Version,
    scalaVersion  := ScalaVersion,
    organization  := "com.ibm.sparktc.tiara",
    description   := "TIARA decahose processor application",
    scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
  )

  lazy val word2VecSettings = Defaults.coreDefaultSettings ++ Seq (
    name          := Word2VecName,
    version       := Version,
    scalaVersion  := ScalaVersion,
    organization  := "com.ibm.sparktc.tiara",
    description   := "TIARA word2vec model generation application",
    scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
  )

  lazy val decahoseActorbuildSettings = Defaults.coreDefaultSettings ++ Seq (
    name          := DecahoseActorName,
    version       := Version,
    scalaVersion  := ScalaVersion,
    organization  := "com.ibm.sparktc.tiara",
    description   := "TIARA decahose actor to poll new files application",
    scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
  )
}

object Resolvers {
  
  val typesafe = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
  val sonatype = "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases"
  val mvnrepository = "MVN Repo" at "http://mvnrepository.com/artifact"

  val allResolvers = Seq(typesafe, sonatype, mvnrepository)

}

object Dependency {
  
  object Version {
    val Spark                       = "1.6.1"
    val akkaV                       = "2.3.14"
    val sprayV                      = "1.3.3"
    val typesafeConfig              = "1.3.0"
    val typesafePlayJSON            = "2.4.0"
    val HttpClientVersion           = "4.2.2"
  }

  // Spark dependencies
  /* Do not remove "provided" - We do not need to include spark dependency 
  on the jar because the jar is gonna be executed by spark-submit*/
  val sparkCore      = "org.apache.spark"  %% "spark-core"      % Version.Spark  % "provided"
  val sparkStreaming = "org.apache.spark"  %% "spark-streaming" % Version.Spark  % "provided"
  val sparkSQL       = "org.apache.spark"  %% "spark-sql"       % Version.Spark  % "provided"
  val sparkHive      = "org.apache.spark"  %% "spark-hive"      % Version.Spark  % "provided"
  val sparkMlLib     = "org.apache.spark"  %% "spark-mllib"     % Version.Spark  % "provided"

  val sprayCan       = "io.spray"          %%  "spray-can"      % Version.sprayV
  val sprayRouting   = "io.spray"          %%  "spray-routing"  % Version.sprayV
  val akkaActor      = "com.typesafe.akka" %%  "akka-actor"     % Version.akkaV
  val spec           = "org.specs2"        %%  "specs2-core"     % "2.3.7" % "test"
  
  //Config library
  val configLib      = "com.typesafe" % "config" % Version.typesafeConfig

  // Json library
  val playJson       = "com.typesafe.play" %% "play-json"       % Version.typesafePlayJSON

  //csv library
  val readCSV       = "com.databricks"    %% "spark-csv"  % "1.1.0"

  //Download files from bluemix
  val codec           = "commons-codec" % "commons-codec" % "1.6"
  val apacheIO        = "commons-io" % "commons-io" % "2.4"
  val apacheLang      = "org.apache.commons" % "commons-lang3" % "3.4"

  //hadoop
  val hadoopCom = "org.apache.hadoop" % "hadoop-common" % "2.7.2"
  val hadoophdfs = "org.apache.hadoop" % "hadoop-hdfs" % "2.7.2"


  val jedis           = "redis.clients" % "jedis" % "2.8.1"
}

object Dependencies {
  import Dependency._

  val decahoseDependencies = Seq(sparkCore, sparkSQL, sparkHive, sparkStreaming, readCSV, configLib, akkaActor,
                                codec,apacheLang,apacheIO,jedis)

  val decahoseActorDependencies = Seq(configLib, akkaActor, codec,apacheLang,apacheIO,hadoopCom, hadoophdfs)

  val restAPIDependecies = Seq(sparkCore, sparkSQL, sparkHive, sparkMlLib, playJson,
    sprayCan, sprayRouting, akkaActor, configLib,spec, jedis)

  val word2VecDependencies = Seq(sparkCore, sparkSQL, sparkHive, sparkMlLib, akkaActor)
}

object TiaraBuild extends Build{
  import Resolvers._
  import Dependencies._
  import BuildSettings._

  lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

  lazy val parent = Project(
    id = "tiara-parent",
    base = file("."),
    aggregate = Seq(restapi, decahoseProcessor, word2vecModelGeneration, decahosePollActor),
    settings = rootbuildSettings ++ Seq(
      aggregate in update := false,
      scalastyleConfig in Compile :=  file(".") / "project" / "scalastyle-config.xml"
    )
  )

  lazy val restapi = Project(
    id = "tiara-restapi",
    base = file("./rest-api"),
    settings = restAPIbuildSettings ++ Seq(
      maxErrors := 5,
      ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
      triggeredMessage := Watched.clearWhenTriggered,
      resolvers := allResolvers,
      libraryDependencies ++= Dependencies.restAPIDependecies,
      unmanagedBase in Compile := file(".") / "lib",
      unmanagedResourceDirectories in Compile += file(".") / "conf",
      mainClass := Some("com.tiara.restapi.Application"),
      fork := true,
      connectInput in run := true,
      scalastyleConfig in Compile :=  file(".") / "project" / "scalastyle-config.xml",
      assemblyJarName in assembly := "tiara-restapi.jar",
      assemblyMergeStrategy in assembly := {
        case PathList("org", "joda", "time",xs @ _*) => MergeStrategy.last
        case x =>
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
      }
    ))

  lazy val decahoseProcessor = Project(
    id = "tiara-decahose",
    base = file("./decahose-processor"),
    settings = decahosebuildSettings ++ Seq(
      maxErrors := 5,
      ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
      triggeredMessage := Watched.clearWhenTriggered,
      resolvers := allResolvers,
      libraryDependencies ++= Dependencies.decahoseDependencies,
      unmanagedResourceDirectories in Compile += file(".") / "conf",
      mainClass := Some("com.tiara.decahose.Application"),
      fork := true,
      connectInput in run := true,
      scalastyleConfig in Compile :=  file(".") / "project" / "scalastyle-config.xml",
      assemblyJarName in assembly := "tiara-decahose-processor.jar"
    ))

  lazy val word2vecModelGeneration = Project(
    id = "tiara-word2vec",
    base = file("./word2vec-models"),
    settings = word2VecSettings ++ Seq(
      maxErrors := 5,
      ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
      triggeredMessage := Watched.clearWhenTriggered,
      resolvers := allResolvers,
      libraryDependencies ++= Dependencies.word2VecDependencies,
      unmanagedResourceDirectories in Compile += file(".") / "conf",
      mainClass := Some("com.tiara.word2vec.Application"),
      fork := true,
      connectInput in run := true,
      scalastyleConfig in Compile :=  file(".") / "project" / "scalastyle-config.xml",
      assemblyJarName in assembly := "tiara-word2vec-model.jar",
      compileScalastyle := scalastyle.in(Compile).toTask("").value,
      (compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle
    ))

  lazy val decahosePollActor = Project(
    id = "tiara-poll-decahose",
    base = file("./decahose-poll-actor"),
    settings = decahoseActorbuildSettings ++ Seq(
      maxErrors := 5,
      ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
      triggeredMessage := Watched.clearWhenTriggered,
      resolvers := allResolvers,
      libraryDependencies ++= Dependencies.decahoseActorDependencies,
      unmanagedResourceDirectories in Compile += file(".") / "conf",
      mainClass := Some("com.tiara.decahoseactor.Application"),
      fork := true,
      connectInput in run := true,
      scalastyleConfig in Compile :=  file(".") / "project" / "scalastyle-config.xml",
      assemblyJarName in assembly := "tiara-decahose-poll-actor.jar",
      javaOptions += "-Xmx2G",
      compileScalastyle := scalastyle.in(Compile).toTask("").value,
      (compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle
    ))

}
