# embedded-kafka

[![Build Status](https://travis-ci.org/embeddedkafka/embedded-kafka.svg?branch=master)](https://travis-ci.org/embeddedkafka/embedded-kafka)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d8264e794fc041448f57c2afb3c0c028?branch=master)](https://www.codacy.com/app/francescopellegrini/embedded-kafka)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.embeddedkafka/embedded-kafka_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.embeddedkafka/embedded-kafka_2.12)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/manub/scalatest-embedded-kafka)
[![License](http://img.shields.io/:license-mit-blue.svg)](http://doge.mit-license.org)

A library that provides an in-memory Kafka instance to run your tests against.

Inspired by [kafka-unit](https://github.com/chbatey/kafka-unit).

## Version compatibility matrix

embedded-kafka is available on Maven Central, compiled for Scala 2.11, 2.12, and 2.13.

Versions match the version of Kafka they're built against.

## embedded-kafka

### How to use

* In your `build.sbt` file add the following dependency: `"io.github.embeddedkafka" %% "embedded-kafka" % "2.3.1" % "test"`
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

It is possible to create producers for custom types in two ways:

* Using the syntax `aKafkaProducer thatSerializesValuesWith classOf[Serializer[V]]`. This will return a `KafkaProducer[String, V]`
* Using the syntax `aKafkaProducer[V]`. This will return a `KafkaProducer[String, V]`, using an implicit `Serializer[V]`.

For more information about how to use the utility methods, you can either look at the Scaladocs or at the tests of this project.

### Custom consumers

Use the `Consumers` trait that easily creates consumers of arbitrary key-value types and manages their lifecycle (via a loaner pattern).

* For basic String consumption use `Consumers.withStringConsumer { your code here }`.
* For arbitrary key and value types, expose implicit `Deserializer`s for each type and use `Consumers.withConsumer { your code here }`.
* If you just want to create a consumer and manage its lifecycle yourself then try `Consumers.newConsumer()`.

### Easy message consumption

With `ConsumerExtensions` you can turn a consumer to a Scala lazy Stream of `T` and treat it as a collection for easy assertion.

* Just import the extensions.
* Bring an implicit `ConsumerRecord[_, _] => T` transform function into scope (some common functions are provided in `Codecs`).
* On any `KafkaConsumer` instance you can now do:

```scala
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder
...
consumer.consumeLazily[(String, String)]("from-this-topic").take(3).toList should be (Seq(
  "1" -> "one",
  "2" -> "two",
  "3" -> "three"
)
```

## embedded-kafka-streams

A library that builds on top of `embedded-kafka` to offer easy testing of [Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams).

It takes care of instantiating and starting your streams as well as closing them after running your test-case code.

### How to use

* In your `build.sbt` file add the following dependency: `"io.github.embeddedkafka" %% "embedded-kafka-streams" % "2.3.1" % "test"`
* Have a look at the [example test](kafka-streams/src/test/scala/net/manub/embeddedkafka/streams/ExampleKafkaStreamsSpec.scala)
* For most of the cases have your class extend the `EmbeddedKafkaStreamsAllInOne` trait. This offers both streams management and easy creation of consumers for asserting resulting messages in output/sink topics.
* If you only want to use the streams management without the test consumers just have the class extend the `EmbeddedKafkaStreams` trait.
* Use the `runStreamsWithStringConsumer` to:
  * Create any topics that need to exist for the streams to operate (usually sources and sinks).
  * Pass the Topology that will be used to instantiate and start the Kafka Streams. This will be done while using the `withRunningKafka` closure internally so that your stream runs with an embedded Kafka and Zookeeper.
  * Pass the `{code block}` that needs a running instance of your streams. This is where your actual test code will sit. You can publish messages to your source topics and consume messages from your sink topics that the Kafka Streams should have generated. This method also offers a pre-instantiated consumer that can read String keys and values.
* For more flexibility, use `runStreams` and `withConsumer`. This allows you to create your own consumers of custom types as seen in the [example test](kafka-streams/src/test/scala/net/manub/embeddedkafka/streams/ExampleKafkaStreamsSpec.scala).
