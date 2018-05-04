import sbtrelease.Version

parallelExecution in ThisBuild := false

val kafkaVersion = "1.1.0"
val confluentVersion = "4.1.0"
val akkaVersion = "2.5.11"

lazy val confluentMavenRepo = "confluent" at "https://packages.confluent.io/maven/"

lazy val commonSettings = Seq(
  organization := "net.manub",
  scalaVersion := "2.12.6",
  crossScalaVersions := Seq("2.12.6", "2.11.12"),
  resolvers ++= Seq(confluentMavenRepo),
  homepage := Some(url("https://github.com/manub/scalatest-embedded-kafka")),
  parallelExecution in Test := false,
  logBuffered in Test := false,
  fork in Test := true,
  javaOptions += "-Xmx1G",
  scalacOptions += "-deprecation",
  scalafmtOnCompile := true
)

lazy val commonLibrarySettings = libraryDependencies ++= Seq(
  "io.confluent" % "kafka-avro-serializer" % confluentVersion,
  "io.confluent" % "kafka-schema-registry" % confluentVersion,
  "io.confluent" % "kafka-schema-registry" % confluentVersion classifier "tests",
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
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
  .settings(libraryDependencies += "org.mockito" % "mockito-core" % "2.18.0" % Test)
  .settings(releaseSettings: _*)

lazy val kafkaStreams = (project in file("kafka-streams"))
  .settings(name := "scalatest-embedded-kafka-streams")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(releaseSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-streams" % kafkaVersion
  ))
  .dependsOn(embeddedKafka)
