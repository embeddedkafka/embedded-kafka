# scalatest-embedded-kafka
A library that provides an in-memory Kafka broker to run your ScalaTest specs against. It uses Kafka 0.8.2.1 and ZooKeeper 3.4.6.

Inspired by https://github.com/chbatey/kafka-unit

## How to use

scalatest-embedded-kafka is available on Bintray and Maven Central, compiled for both Scala 2.10 and 2.11

* In your `build.sbt` file add the following dependency: `"org.scalatest" %% "scalatest" % "0.2.3"`
* Have your `Spec` extend the `EmbeddedKafka` trait.
* Enclose the code that needs a running instance of Kafka within the `withRunningKafka` closure.

        class MySpec extends WordSpec with EmbeddedKafka {
    
        "runs with embedded kafka" should {

            withRunningKafka {
                // ... code goes here
            }
        
        }

* In-memory Zookeeper and Kafka will be instantiated respectively on port 6000 and 6001 and automatically shutdown at the end of the test.

## Configuration

It's possible to change the ports on which Zookeeper and Kafka are started by providing an implicit `EmbeddedKafkaConfig`

        class MySpec extends WordSpec with EmbeddedKafka {
    
        "runs with embedded kafka on a specific port" should {

            implicit val config = EmbeddedKafkaConfig(kafkaPort = 12345)

            withRunningKafka {
                // now a kafka broker is listening on port 12345
            }
        
        }
        
## Utility methods

The `EmbeddedKafka` trait provides also an utility method to publish a message to a topic:

        publishToKafka("topic", "message")
        
At the moment it's only possible to publish a message as a `String`. There's no utility method for consuming, but this will be part of a next release.

## Badges 

![Codeship](https://codeship.com/projects/f3a53210-021d-0133-d900-2e03a244558b/status?branch=master)

[![Codacy Badge](https://www.codacy.com/project/badge/c7b26292335d4331b49a81317884dd17)](https://www.codacy.com/app/emanuele-blanco/scalatest-embedded-kafka)
