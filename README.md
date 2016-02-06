# scalatest-embedded-kafka
A library that provides an in-memory Kafka broker to run your ScalaTest specs against. It uses Kafka 0.9.0.0 and ZooKeeper 3.4.7.

**NOTE** the _master_ branch is now using Apache Kafka 0.9.x. The version supporting Kafka 0.8.x can be found [here](https://github.com/manub/scalatest-embedded-kafka/tree/kafka-0.8)

Inspired by https://github.com/chbatey/kafka-unit

## How to use

scalatest-embedded-kafka is available on Bintray and Maven Central, compiled for both Scala 2.10 and 2.11

* In your `build.sbt` file add the following dependency: `"net.manub" %% "scalatest-embedded-kafka" % "0.4.2-kafka09" % "test"`
* Have your `Spec` extend the `EmbeddedKafka` trait.
* Enclose the code that needs a running instance of Kafka within the `withRunningKafka` closure.

        class MySpec extends WordSpec with EmbeddedKafka {
    
        "runs with embedded kafka" should {

            withRunningKafka {
                // ... code goes here
            }
        
        }

* In-memory Zookeeper and Kafka will be instantiated respectively on port 6000 and 6001 and automatically shutdown at the end of the test.

### Use without the `withRunningKafka` method

A `EmbeddedKafka` companion object is provided for usage without the `EmbeddedKafka` trait. Zookeeper and Kafka can be started an stopped in a programmatic way.

        class MySpec extends WordSpec {
    
        "runs with embedded kafka" should {
        
            EmbeddedKafka.start()
            
            // ... code goes here
            
            EmbeddedKafka.stop() 
        
        }
        
Please note that in order to avoid Kafka instances not shutting down properly, it's recommended to call `EmbeddedKafka.stop()` in a `after` block or in a similar teardown logic. 

## Configuration

It's possible to change the ports on which Zookeeper and Kafka are started by providing an implicit `EmbeddedKafkaConfig`

        class MySpec extends WordSpec with EmbeddedKafka {
    
        "runs with embedded kafka on a specific port" should {

            implicit val config = EmbeddedKafkaConfig(kafkaPort = 12345)

            withRunningKafka {
                // now a kafka broker is listening on port 12345
            }
        
        }
        
This works for both `withRunningKafka` and `EmbeddedKafka.start()`
        
## Utility methods

The `EmbeddedKafka` trait provides also some utility methods to interact with the embedded kafka, in order to set preconditions or verifications in your specs:

        def publishToKafka(topic: String, message: String): Unit
        
        def consumeFirstMessageFrom(topic: String): String
        
## Custom producers

It is possible to create producers for custom types in two ways:

* Using the syntax `aKafkaProducer thatSerializesValuesWith classOf[Serializer[V]]`. This will return a `KafkaProducer[String, V]`
* Using the syntax `aKafkaProducer[V]`. This will return a `KafkaProducer[String, V]`, using an implicit `Serializer[V]`.

For more information about how to use the utility methods, you can either look at the Scaladocs or at the tests of this project.

## Badges 

![Codeship](https://codeship.com/projects/f3a53210-021d-0133-d900-2e03a244558b/status?branch=master)

[![Codacy Badge](https://www.codacy.com/project/badge/c7b26292335d4331b49a81317884dd17)](https://www.codacy.com/app/emanuele-blanco/scalatest-embedded-kafka)
