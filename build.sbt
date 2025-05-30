import Dependencies._

ThisBuild / parallelExecution := false
ThisBuild / versionScheme     := Some("semver-spec")

lazy val compileSettings = Seq(
  Compile / compile := (Compile / compile)
    .dependsOn(
      Compile / scalafmtSbt,
      Compile / scalafmtAll
    )
    .value,
  libraryDependencies ++= Common.testDeps,
  javaOptions ++= Seq("-Xms512m", "-Xmx2048m"),
  scalacOptions -= "-Xfatal-warnings"
)

lazy val coverageSettings = Seq(
  coverageMinimumStmtTotal := 80,
  coverageFailOnMinimum    := true
)

lazy val publishSettings = Seq(
  homepage := Some(url("https://github.com/embeddedkafka/embedded-kafka")),
  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
  Test / publishArtifact := false,
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
  releaseVersionBump := sbtrelease.Version.Bump.Minor,
  releaseCrossBuild  := true
)

lazy val testSettings = Seq(
  Test / fork              := true,
  Test / logBuffered       := false,
  Test / parallelExecution := false
)

lazy val commonSettings = Seq(
  organization := "io.github.embeddedkafka",
  scalaVersion := Versions.Scala213,
  crossScalaVersions := Seq(
    Versions.Scala213,
    Versions.Scala3
  )
) ++ compileSettings ++ coverageSettings ++ publishSettings ++ releaseSettings ++ testSettings

lazy val root = (project in file("."))
  .settings(name := "embedded-kafka-root")
  .settings(commonSettings: _*)
  .settings(publishArtifact := false)
  .settings(publish / skip := true)
  .aggregate(embeddedKafka, kafkaStreams, kafkaConnect)

lazy val embeddedKafka = (project in file("embedded-kafka"))
  .settings(name := "embedded-kafka")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= EmbeddedKafka.prodDeps)
  .settings(libraryDependencies ++= EmbeddedKafka.testDeps)

lazy val kafkaStreams = (project in file("kafka-streams"))
  .settings(name := "embedded-kafka-streams")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= KafkaStreams.prodDeps)
  .dependsOn(embeddedKafka)

lazy val kafkaConnect = (project in file("kafka-connect"))
  .settings(name := "embedded-kafka-connect")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= KafkaConnect.prodDeps)
  .dependsOn(embeddedKafka % "compile->compile;test->test")
