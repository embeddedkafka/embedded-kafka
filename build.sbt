import sbtrelease.Version

parallelExecution in ThisBuild := false

val kafkaVersion = "2.0.1"
val confluentVersion = "5.0.1"
val akkaVersion = "2.5.18"

lazy val commonSettings = Seq(
  organization := "io.github.embeddedkafka",
  scalaVersion := "2.12.7",
  crossScalaVersions := Seq("2.12.7", "2.11.12"),
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
  "org.slf4j" % "slf4j-log4j12" % "1.7.25" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
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

// https://github.com/sbt/sbt/issues/3618
// [error] (kafkaStreams / update) sbt.librarymanagement.ResolveException: download failed: javax.ws.rs#javax.ws.rs-api;2.1!javax.ws.rs-api.${packaging.type}
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

lazy val root = (project in file("."))
  .settings(name := "embeddedkafka-root")
  .settings(commonSettings: _*)
  .settings(publishArtifact := false)
  .settings(publish := {})
  .settings(releaseSettings: _*)
  .disablePlugins(BintrayPlugin)
  .settings(publishTo := Some(Resolver.defaultLocal))
  .aggregate(embeddedKafka, kafkaStreams, schemaRegistry)

lazy val embeddedKafka = (project in file("embedded-kafka"))
  .settings(name := "embeddedkafka")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(
    libraryDependencies += "org.mockito" % "mockito-core" % "2.19.1" % Test)
  .settings(releaseSettings: _*)

lazy val kafkaStreams = (project in file("kafka-streams"))
  .settings(name := "embeddedkafka-streams")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(releaseSettings: _*)
  .settings(libraryDependencies +=
    "org.apache.kafka" % "kafka-streams" % kafkaVersion)
  .dependsOn(embeddedKafka)

lazy val schemaRegistry = (project in file("schema-registry"))
  .settings(name := "embeddedkafka-schema-registry")
  .settings(publishSettings: _*)
  .settings(commonSettings: _*)
  .settings(commonLibrarySettings)
  .settings(releaseSettings: _*)
  .settings(resolvers ++= Seq(
    "confluent" at "https://packages.confluent.io/maven/"))
  .settings(libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-streams" % kafkaVersion,
    "io.confluent" % "kafka-avro-serializer" % confluentVersion,
    "io.confluent" % "kafka-schema-registry" % confluentVersion,
    "io.confluent" % "kafka-schema-registry" % confluentVersion classifier "tests",
  ))
  .dependsOn(embeddedKafka % "compile->compile;test->test",
             kafkaStreams % "compile->compile;test->test")
