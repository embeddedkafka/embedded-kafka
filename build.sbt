import sbtrelease.Version

parallelExecution in ThisBuild := false

val kafkaVersion = "2.1.1"
val akkaVersion = "2.5.21"

lazy val commonSettings = Seq(
  organization := "io.github.embeddedkafka",
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.12.8", "2.11.12"),
  homepage := Some(url("https://github.com/embeddedkafka/embedded-kafka")),
  parallelExecution in Test := false,
  logBuffered in Test := false,
  fork in Test := true,
  javaOptions ++= Seq("-Xms512m", "-Xmx2048m"),
  scalacOptions += "-deprecation",
  scalafmtOnCompile := true
)

lazy val commonLibrarySettings = libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.8.2",
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.slf4j" % "slf4j-log4j12" % "1.7.26" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
)

lazy val publishSettings = Seq(
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  publishArtifact in Test := false,
  // https://github.com/sbt/sbt/issues/3570#issuecomment-432814188
  updateOptions := updateOptions.value.withGigahorse(false),
  developers := List(
    Developer(
      "manub",
      "Emanuele Blanco",
      "emanuele.blanco@gmail.com",
      url("http://twitter.com/manub")
    )
  )
)

import ReleaseTransformations._

lazy val releaseSettings = Seq(
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    setNextVersion,
    commitNextVersion,
    pushChanges
  ),
  releaseVersionBump := Version.Bump.Minor,
  releaseCrossBuild := true
)

lazy val root = (project in file("."))
  .settings(name := "embedded-kafka-root")
  .settings(commonSettings: _*)
  .settings(publishArtifact := false)
  .settings(releaseSettings: _*)
  .settings(skip in publish := true)
  .aggregate(embeddedKafka, kafkaStreams)

lazy val embeddedKafka = (project in file("embedded-kafka"))
  .settings(name := "embedded-kafka")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(
    libraryDependencies += "org.mockito" % "mockito-core" % "2.24.5" % Test)
  .settings(releaseSettings: _*)

lazy val kafkaStreams = (project in file("kafka-streams"))
  .settings(name := "embedded-kafka-streams")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(releaseSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-streams" % kafkaVersion,
    "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1"
  ))
  .dependsOn(embeddedKafka)
