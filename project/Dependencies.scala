import sbt._

object Dependencies {

  object Versions {
    val Scala        = "2.13.1"
    val Scala212     = "2.12.11"
    val Kafka        = "2.5.0"
    val Slf4j        = "1.7.30"
    val ScalaTest    = "3.1.2"
    val Mockito      = "3.3.3"
    val MockitoScala = "3.1.0.0"
  }

  object Common {
    lazy val testDeps: Seq[ModuleID] = Seq(
      "org.slf4j"      % "slf4j-log4j12" % Versions.Slf4j,
      "org.scalatest" %% "scalatest"     % Versions.ScalaTest
    ).map(_ % Test)
  }

  object EmbeddedKafka {
    lazy val prodDeps: Seq[ModuleID] = Seq(
      "org.apache.kafka" %% "kafka" % Versions.Kafka
    )

    lazy val testDeps: Seq[ModuleID] = Seq(
      "org.mockito"        % "mockito-core" % Versions.Mockito,
      "org.scalatestplus" %% "mockito-1-10" % Versions.MockitoScala
    ).map(_ % Test)
  }

  object KafkaStreams {
    lazy val prodDeps: Seq[ModuleID] = Seq(
      "org.apache.kafka" % "kafka-streams" % Versions.Kafka
    )
  }

}
