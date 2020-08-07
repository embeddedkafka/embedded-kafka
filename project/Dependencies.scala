import sbt._

object Dependencies {

  object Versions {
    val Scala                = "2.13.2"
    val Scala212             = "2.12.11"
    val Kafka                = "2.5.0"
    val Slf4j                = "1.7.30"
    val ScalaTest            = "3.2.1"
    val ScalaTestPlusMockito = "3.2.0.0"
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

    lazy val testDeps: Seq[ModuleID] = Seq(
      "org.scalatestplus" %% "mockito-3-3" % Versions.ScalaTestPlusMockito
    ).map(_ % Test)
  }

  object KafkaStreams {
    lazy val prodDeps: Seq[ModuleID] = Seq(
      "org.apache.kafka" % "kafka-streams" % Versions.Kafka
    )
  }

}
