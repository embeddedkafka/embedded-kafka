import sbtrelease.Version

parallelExecution in ThisBuild := false

val kafkaVersion = "1.1.0"
val zookeeperVersion = "3.4.11"
val akkaVersion = "2.4.20"

val slf4jLog4jOrg = "org.slf4j"
val slf4jLog4jArtifact = "slf4j-log4j12"

lazy val commonSettings = Seq(
  organization := "net.manub",
  scalaVersion := "2.12.5",
  crossScalaVersions := Seq("2.12.5", "2.11.12"),
  homepage := Some(url("https://github.com/manub/scalatest-embedded-kafka")),
  parallelExecution in Test := false,
  logBuffered in Test := false,
  fork in Test := true,
  javaOptions += "-Xmx1G",
  scalacOptions += "-deprecation"
)

lazy val commonLibrarySettings = libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4",
  "org.apache.kafka" %% "kafka" % kafkaVersion exclude(slf4jLog4jOrg, slf4jLog4jArtifact),
  "org.apache.zookeeper" % "zookeeper" % zookeeperVersion exclude(slf4jLog4jOrg, slf4jLog4jArtifact),
  "org.apache.avro" % "avro" % "1.8.2" exclude(slf4jLog4jOrg, slf4jLog4jArtifact),
  "com.typesafe.akka" %% "akka-actor" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  slf4jLog4jOrg % slf4jLog4jArtifact % "1.7.25" % Test
)

lazy val publishSettings = Seq(
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ =>
    false
  },
  pomExtra :=
    <scm>
      <url>https://github.com/manub/scalatest-embedded-kafka</url>
      <connection>scm:git:git@github.com:manub/scalatest-embedded-kafka.git</connection>
    </scm>
      <developers>
        <developer>
          <id>manub</id>
          <name>Emanuele Blanco</name>
          <url>http://twitter.com/manub</url>
        </developer>
      </developers>
)

lazy val releaseSettings = Seq(
  releaseVersionBump := Version.Bump.Minor,
  releaseCrossBuild := true
)

lazy val root = (project in file("."))
  .settings(name := "scalatest-embedded-kafka-root")
  .settings(commonSettings: _*)
  .settings(publishArtifact := false)
  .settings(publish := {})
  .settings(releaseSettings: _*)
  .disablePlugins(BintrayPlugin)
  .settings(publishTo := Some(Resolver.defaultLocal))
  .aggregate(embeddedKafka, kafkaStreams)

lazy val embeddedKafka = (project in file("embedded-kafka"))
  .settings(name := "scalatest-embedded-kafka")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(libraryDependencies += "org.mockito" % "mockito-core" % "2.7.22" % Test)
  .settings(releaseSettings: _*)

lazy val kafkaStreams = (project in file("kafka-streams"))
  .settings(name := "scalatest-embedded-kafka-streams")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(releaseSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-streams" % kafkaVersion exclude(slf4jLog4jOrg, slf4jLog4jArtifact)
  ))
  .dependsOn(embeddedKafka)
