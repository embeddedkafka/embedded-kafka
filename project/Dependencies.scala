import sbt._

object Dependencies {

  object Versions {
    val Scala3    = "3.2.2"
    val Scala213  = "2.13.10"
    val Scala212  = "2.12.17"
    val Kafka     = "3.4.1"
    val Slf4j     = "1.7.36"
    val ScalaTest = "3.2.16"
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
