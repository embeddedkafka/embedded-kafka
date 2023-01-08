# embedded-kafka

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.embeddedkafka/embedded-kafka_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.embeddedkafka/embedded-kafka_2.13)
[![Test](https://github.com/embeddedkafka/embedded-kafka/actions/workflows/test.yml/badge.svg)](https://github.com/embeddedkafka/embedded-kafka/actions/workflows/test.yml)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e6fea84592574f7dabffd44117ceb9e2)](https://www.codacy.com/gh/embeddedkafka/embedded-kafka)
[![Codacy Coverage Badge](https://api.codacy.com/project/badge/Coverage/e6fea84592574f7dabffd44117ceb9e2)](https://www.codacy.com/gh/embeddedkafka/embedded-kafka)
[![Mergify Status](https://img.shields.io/endpoint.svg?url=https://api.mergify.com/v1/badges/embeddedkafka/embedded-kafka&style=flat)](https://mergify.io)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

A library that provides an in-memory Kafka instance to run your tests against.

Inspired by [kafka-unit](https://github.com/chbatey/kafka-unit).

## Version compatibility matrix

embedded-kafka is available on Maven Central, compiled for Scala 2.12 and 2.13.

Versions match the version of Kafka they're built against.

## Important known limitation (prior to v2.8.0)

[Prior to v2.8.0](https://github.com/apache/kafka/pull/10174) Kafka core was inlining the Scala library, so you couldn't use a different Scala **patch** version than [what Kafka used to compile its jars](https://github.com/apache/kafka/blob/trunk/gradle/dependencies.gradle#L30)!

## Breaking change: new package name

From v2.8.0 onwards package name has been updated to reflect the library group id (i.e. `io.github.embeddedkafka`).

Aliases to the old package name have been added, along with a one-time [Scalafix rule](https://github.com/embeddedkafka/embedded-kafka-scalafix) to ensure the smoothest migration.

## embedded-kafka

### How to use

* In your `build.sbt` file add the following dependency (replace `x.x.x` with the appropriate version): `"io.github.embeddedkafka" %% "embedded-kafka" % "x.x.x" % Test`
* Have your class extend the `EmbeddedKafka` trait.
* Enclose the code that needs a running instance of Kafka within the `withRunningKafka` closure.

An example, using ScalaTest:

```scala
class MySpec extends AnyWordSpecLike with Matchers with EmbeddedKafka {

  "runs with embedded kafka" should {

    "work" in {

      withRunningKafka {
        // ... code goes here
      }
    }
  }
}
```

* In-memory Zookeeper and Kafka will be instantiated respectively on port 6000 and 6001 and automatically shutdown at the end of the test.

### Use without the `withRunningKafka` method

A `EmbeddedKafka` companion object is provided for usage without extending the `EmbeddedKafka` trait. Zookeeper and Kafka can be started and stopped in a programmatic way. This is the recommended usage if you have more than one test in your file and you don't want to start and stop Kafka and Zookeeper on every test.

```scala
class MySpec extends AnyWordSpecLike with Matchers {

  "runs with embedded kafka" should {

    "work" in {
      EmbeddedKafka.start()

      // ... code goes here

      EmbeddedKafka.stop()
    }
  }
}
```

Please note that in order to avoid Kafka instances not shutting down properly, it's recommended to call `EmbeddedKafka.stop()` in a `after` block or in a similar teardown logic.

### Configuration

It's possible to change the ports on which Zookeeper and Kafka are started by providing an implicit `EmbeddedKafkaConfig`

```scala
class MySpec extends AnyWordSpecLike with Matchers with EmbeddedKafka {

  "runs with embedded kafka on a specific port" should {

    "work" in {
      implicit val config = EmbeddedKafkaConfig(kafkaPort = 12345)

      withRunningKafka {
        // now a kafka broker is listening on port 12345
      }
    }
  }
}
```

If you want to run ZooKeeper and Kafka on arbitrary available ports, you can
use the `withRunningKafkaOnFoundPort` method. This is useful to make tests more
reliable, especially when running tests in parallel or on machines where other
tests or services may be running with port numbers you can't control.

```scala
class MySpec extends AnyWordSpecLike with Matchers with EmbeddedKafka {

  "runs with embedded kafka on arbitrary available ports" should {

    "work" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        // now a kafka broker is listening on actualConfig.kafkaPort
        publishStringMessageToKafka("topic", "message")
        consumeFirstStringMessageFrom("topic") shouldBe "message"
      }
    }
  }
}
```

The same implicit `EmbeddedKafkaConfig` is used to define custom consumer or producer properties

```scala
class MySpec extends AnyWordSpecLike with Matchers with EmbeddedKafka {

  "runs with custom producer and consumer properties" should {
    "work" in {
      val customBrokerConfig = Map("replica.fetch.max.bytes" -> "2000000",
        "message.max.bytes" -> "2000000")

      val customProducerConfig = Map("max.request.size" -> "2000000")
      val customConsumerConfig = Map("max.partition.fetch.bytes" -> "2000000")

      implicit val customKafkaConfig = EmbeddedKafkaConfig(
        customBrokerProperties = customBrokerConfig,
        customProducerProperties = customProducerConfig,
        customConsumerProperties = customConsumerConfig)

      withRunningKafka {
        // now a kafka broker is listening on port 12345
      }
    }
  }
}
```

This works for `withRunningKafka`, `withRunningKafkaOnFoundPort`, and `EmbeddedKafka.start()`

Also, it is now possible to provide custom properties to the broker while starting Kafka. `EmbeddedKafkaConfig` has a
`customBrokerProperties` field which can be used to provide extra properties contained in a `Map[String, String]`.
Those properties will be added to the broker configuration, be careful some properties are set by the library itself and
in case of conflict the `customBrokerProperties` values will take precedence. Please look at the source code to see what these properties
are.

### Utility methods

The `EmbeddedKafka` trait provides also some utility methods to interact with the embedded kafka, in order to set preconditions or verifications in your specs:

```scala
def publishToKafka(topic: String, message: String): Unit

def consumeFirstMessageFrom(topic: String): String

def createCustomTopic(topic: String, topicConfig: Map[String,String], partitions: Int, replicationFactor: Int): Unit
```

### Custom producers

Given implicits `Deserializer`s for each type and an `EmbeddedKafkaConfig` it is possible to use `withProducer[A, B, R] { your code here }` where R is the code return type.

For more information about how to use the utility methods, you can either look at the Scaladocs or at the tests of this project.

### Custom consumers

Given implicits `Serializer`s for each type and an `EmbeddedKafkaConfig` it is possible to use `withConsumer[A, B, R] { your code here }` where R is the code return type.

### Loan methods example

A simple test using loan methods can be as simple as this:

```scala
  implicit val serializer: Serializer[String]     = new StringSerializer()
  implicit val deserializer: Deserializer[String] = new StringDeserializer()
  val key                                         = "key"
  val value                                       = "value"
  val topic                                       = "loan_method_example"

  EmbeddedKafka.withProducer[String, String, Unit](producer =>
    producer.send(new ProducerRecord[String, String](topic, key, value)))

  EmbeddedKafka.withConsumer[String, String, Assertion](consumer => {
    consumer.subscribe(Collections.singletonList(topic))

    eventually {
      val records = consumer.poll(java.time.Duration.ofMillis(1.seconds.toMillis)).asScala
      records should have size 1
      records.head.key shouldBe key
      records.head.value shouldBe value
    }
  })
```

## embedded-kafka-streams

A library that builds on top of `embedded-kafka` to offer easy testing of [Kafka Streams](https://kafka.apache.org/documentation/streams).

It takes care of instantiating and starting your streams as well as closing them after running your test-case code.

### How to use

* In your `build.sbt` file add the following dependency (replace `x.x.x` with the appropriate version): `"io.github.embeddedkafka" %% "embedded-kafka-streams" % "x.x.x" % Test`
* Have a look at the [example test](kafka-streams/src/test/scala/io/github/embeddedkafka/streams/ExampleKafkaStreamsSpec.scala)
* For most of the cases have your class extend the `EmbeddedKafkaStreams` trait. This offers both streams management and easy loaning of producers and consumers for asserting resulting messages in output/sink topics.
* Use `EmbeddedKafkaStreams.runStreams` and `EmbeddedKafka.withConsumer` and `EmbeddedKafka.withProducer`. This allows you to create your own consumers of custom types as seen in the [example test](kafka-streams/src/test/scala/io/github/embeddedkafka/streams/ExampleKafkaStreamsSpec.scala).

## embedded-kafka-connect

A library that builds on top of `embedded-kafka` to offer easy testing of [Kafka Connect](https://kafka.apache.org/documentation/#connect).

It takes care of instantiating and starting a Kafka Connect server as well as closing it after running your test-case code.

### How to use

* In your `build.sbt` file add the following dependency (replace `x.x.x` with the appropriate version): `"io.github.embeddedkafka" %% "embedded-kafka-connect" % "x.x.x" % Test`
* Have a look at the [example test](kafka-connect/src/test/scala/io/github/embeddedkafka/connect/ExampleKafkaConnectSpec.scala)
* For most of the cases have your class extend the `EmbeddedKafkaConnect` trait.
* Use `EmbeddedKafkaConnect.startConnect`. This allows you to start a Kafka Connect server to interact with as seen in the [example test](kafka-connect/src/test/scala/io/github/embeddedkafka/connect/ExampleKafkaConnectSpec.scala).
