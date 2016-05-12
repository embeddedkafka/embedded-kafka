import sbtrelease.Version

val slf4jLog4jOrg = "org.slf4j"
val slf4jLog4jArtifact = "slf4j-log4j12"

lazy val commonSettings = Seq(
  name := "scalatest-embedded-kafka",
  organization := "net.manub",
  scalaVersion := "2.11.8",
  homepage := Some(url("https://github.com/manub/scalatest-embedded-kafka")),
  parallelExecution in Test := false,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.5",
    "org.apache.kafka" %% "kafka" % "0.9.0.1" exclude(slf4jLog4jOrg, slf4jLog4jArtifact),
    "org.apache.zookeeper" % "zookeeper" % "3.4.7" exclude(slf4jLog4jOrg, slf4jLog4jArtifact),
    "org.apache.avro" % "avro" % "1.7.7" exclude(slf4jLog4jOrg, slf4jLog4jArtifact),
    "com.typesafe.akka" %% "akka-actor" % "2.3.14" % Test,
    "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % Test
  )
)

lazy val publishSettings = Seq(
  licenses +=("MIT", url("http://opensource.org/licenses/MIT")),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
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
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(releaseSettings: _*)
