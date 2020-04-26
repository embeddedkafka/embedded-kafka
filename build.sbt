import sbtrelease.Version

parallelExecution in ThisBuild := false

val kafkaVersion = "2.5.0"

lazy val commonSettings = Seq(
  organization := "io.github.embeddedkafka",
  scalaVersion := "2.13.1",
  crossScalaVersions := Seq("2.12.10", "2.13.1"),
  homepage := Some(url("https://github.com/embeddedkafka/embedded-kafka")),
  parallelExecution in Test := false,
  logBuffered in Test := false,
  fork in Test := true,
  javaOptions ++= Seq("-Xms512m", "-Xmx2048m"),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "utf8",
    "-Xlint:missing-interpolator",
    "-Xlint:private-shadow",
    "-Xlint:type-parameter-shadow",
    "-Ywarn-dead-code",
    "-Ywarn-unused"
  ),
  scalafmtOnCompile := true,
  coverageEnabled := true,
  coverageMinimum := 80,
  coverageFailOnMinimum := false
)

lazy val commonLibrarySettings = libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.slf4j" % "slf4j-log4j12" % "1.7.30" % Test,
  "org.scalatest" %% "scalatest" % "3.1.1" % Test
)

lazy val publishSettings = Seq(
  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
  publishArtifact in Test := false,
  developers := List(
    Developer(
      "manub",
      "Emanuele Blanco",
      "emanuele.blanco@gmail.com",
      url("https://twitter.com/manub")
    ),
    Developer(
      "francescopellegrini",
      "Francesco Pellegrini",
      "francesco.pelle@gmail.com",
      url("https://github.com/francescopellegrini")
    ),
    Developer(
      "NeQuissimus",
      "Tim Steinbach",
      "steinbach.tim@gmail.com",
      url("https://github.com/NeQuissimus")
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
  .settings(libraryDependencies ++= Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.0" % Test,
    "org.mockito" % "mockito-core" % "3.3.3" % Test,
    "org.scalatestplus" %% "mockito-1-10" % "3.1.0.0" % Test
  ))
  .settings(releaseSettings: _*)

lazy val kafkaStreams = (project in file("kafka-streams"))
  .settings(name := "embedded-kafka-streams")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(releaseSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-streams" % kafkaVersion
  ))
  .dependsOn(embeddedKafka)
