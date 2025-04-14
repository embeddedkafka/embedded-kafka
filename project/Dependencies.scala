import sbt._

object Dependencies {

  object Versions {
    val Scala3    = "3.3.5"
    val Scala213  = "2.13.16"
    val Kafka     = "4.0.0"
    val Slf4j     = "1.7.36"
    val ScalaTest = "3.2.19"
    val Jackson   = "2.18.3" // Keep consistent with the one provided by Kafka
  }

  object Common {
    lazy val testDeps: Seq[ModuleID] = Seq(
      "org.slf4j"      % "slf4j-reload4j"           % Versions.Slf4j,
      "org.scalatest" %% "scalatest-wordspec"       % Versions.ScalaTest,
      "org.scalatest" %% "scalatest-shouldmatchers" % Versions.ScalaTest
    ).map(_ % Test)
  }

  object EmbeddedKafka {
    lazy val prodDeps: Seq[ModuleID] = Seq(
      "org.apache.kafka" %% "kafka" % Versions.Kafka cross CrossVersion.for3Use2_13
    )
    lazy val testDeps: Seq[ModuleID] = Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.Jackson
    ).map(_ % Test)
  }

  object KafkaStreams {
    lazy val prodDeps: Seq[ModuleID] = Seq(
      "org.apache.kafka" % "kafka-streams" % Versions.Kafka
    )
  }

  object KafkaConnect {
    lazy val prodDeps: Seq[ModuleID] = Seq(
      "org.apache.kafka" % "connect-runtime" % Versions.Kafka
    )
  }

}
