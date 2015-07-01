name := "scalatest-embedded-kafka"

organization := "net.manub"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.5",
  "org.apache.kafka" %% "kafka" % "0.8.2.1",
  "org.apache.zookeeper" % "zookeeper" % "3.4.6",

  "com.typesafe.akka" %% "akka-actor" % "2.3.11" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.11" % "test"
)