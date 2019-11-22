import sbtrelease.Version

parallelExecution in ThisBuild := false

val kafkaVersion = "2.4.0"
val akkaVersion = "2.5.27"

lazy val commonSettings = Seq(
  organization := "io.github.embeddedkafka",
  scalaVersion := "2.13.1",
  crossScalaVersions := Seq("2.12.10", "2.11.12", "2.13.1"),
  homepage := Some(url("https://github.com/embeddedkafka/embedded-kafka")),
  parallelExecution in Test := false,
  logBuffered in Test := false,
  fork in Test := true,
  javaOptions ++= Seq("-Xms512m", "-Xmx2048m"),
  scalacOptions += "-deprecation",
  scalafmtOnCompile := true,
  // for release candidate builds of Apache Kafka
  resolvers += "Apache Staging" at "https://repository.apache.org/content/groups/staging/"
)

lazy val commonLibrarySettings = libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.9.1",
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.slf4j" % "slf4j-log4j12" % "1.7.29" % Test,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
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
  ),
  // TODO: remove sbt-bintray sbt keys
  bintrayOrganization := Some("seglo"),
  bintrayRepository := "maven"
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
    "org.mockito" % "mockito-core" % "3.2.0" % Test,
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
