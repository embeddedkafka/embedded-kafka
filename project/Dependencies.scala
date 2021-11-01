import sbt._

object Dependencies {

  object Versions {
    val Scala     = "2.13.7"
    val Scala212  = "2.12.15"
    val Kafka     = "3.0.0"
    val Slf4j     = "1.7.32"
    val ScalaTest = "3.2.10"
  }

  object Common {
    lazy val testDeps: Seq[ModuleID] = Seq(
      "org.slf4j"      % "slf4j-log4j12"            % Versions.Slf4j,
      "org.scalatest" %% "scalatest-wordspec"       % Versions.ScalaTest,
      "org.scalatest" %% "scalatest-shouldmatchers" % Versions.ScalaTest
    ).map(_ % Test)
  }

  object EmbeddedKafka {
    lazy val prodDeps: Seq[ModuleID] = Seq(
      "org.apache.kafka" %% "kafka" % Versions.Kafka
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
