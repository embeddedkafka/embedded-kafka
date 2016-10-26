package net.manub.embeddedkafka.streams

import net.manub.embeddedkafka.{Consumers, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.streams.processor.TopologyBuilder
import org.scalatest.Suite

/** Convenience trait for testing Kafka Streams with ScalaTest.
  * It exposes `EmbeddedKafkaStreams.runStreams` as well as `Consumers` api
  * for easily creating and querying consumers in tests.
  *
  * e.g.
  * {{{
  *runStreams(Seq("inputTopic", "outputTopic", streamBuilder) {
  *  withConsumer[String, String, Unit] { consumer =>
  *    // here you can publish and consume messages and make assertions
  *    publishToKafka(in, Seq("one-string", "another-string"))
  *    consumeLazily(out).take(2).toList should be (
  *      Seq("one-string" -> "true", "another-string" -> "true")
  *    )
  *  }
  *}
  * }}}
  *
  * @see [[Consumers]]
  * @see [[EmbeddedKafkaStreams]]
  */
trait EmbeddedKafkaStreamsAllInOne extends EmbeddedKafkaStreams with Consumers {
  this: Suite =>

  /** Run Kafka Streams while offering a String-based consumer for easy testing of stream output.
    *
    * @param topicsToCreate the topics that should be created. Usually these should be the topics
    *                       that the Streams-under-test use for inputs and outputs. They need to be
    *                       created before running the streams and
    *                       this is automatically taken care of.
    * @param builder        the streams builder that contains the stream topology that will be instantiated
    * @param block          the block of testing code that will be executed by passing the simple
    *                       String-based consumer.
    * @return the result of the testing code
    */
  def runStreamsWithStringConsumer(topicsToCreate: Seq[String], builder: TopologyBuilder)
                                  (block: KafkaConsumer[String, String] => Any)
                                  (implicit config: EmbeddedKafkaConfig): Any =
    runStreams(topicsToCreate, builder)(withStringConsumer[Any](block))(config)
}
