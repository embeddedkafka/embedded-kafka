package net.manub.embeddedkafka.streams

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig, UUIDs}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.processor.TopologyBuilder
import org.apache.log4j.Logger
import org.scalatest.Suite

/** Helper trait for testing Kafka Streams.
  * It creates an embedded Kafka Instance for each test case.
  * Use `runStreams` to execute your streams.
  */
trait EmbeddedKafkaStreams extends EmbeddedKafka with TestStreamsConfig {
  this: Suite =>

  private val logger = Logger.getLogger(classOf[EmbeddedKafkaStreams])

  /** Execute Kafka streams and pass a block of code that can
    * operate while the streams are active.
    * The code block can be used for publishing and consuming messages in Kafka.
    * The block gets a pre-initialized kafka consumer that can be used implicitly for
    * util methods such as `consumeLazily(String)`.
    *
    * e.g.
    *
    * {{{
    *runStreams(Seq("inputTopic", "outputTopic", streamBuilder) {
    *  // here you can publish and consume messages and make assertions
    *  publishToKafka(in, Seq("one-string", "another-string"))
    *  consumeFirstStringMessageFrom(in) should be ("one-string")
    *}
    * }}}
    *
    * @param topicsToCreate the topics that should be created in Kafka before launching the streams.
    * @param builder        the streams builder that will be used to instantiate the streams with
    *                       a default configuration (all state directories are different and
    *                       in temp folders)
    * @param block          the code block that will executed while the streams are active.
    *                       Once the block has been executed the streams will be closed.
    */
  def runStreams(topicsToCreate: Seq[String], builder: TopologyBuilder)
                (block: => Any)
                (implicit config: EmbeddedKafkaConfig): Any =
    withRunningKafka {
      topicsToCreate.foreach(topic => createCustomTopic(topic))
      val streamId = UUIDs.newUuid().toString
      logger.debug(s"Creating stream with Application ID: [$streamId]")
      val streams = new KafkaStreams(builder, streamConfig(streamId))
      streams.start()
      try {
        block
      } finally {
        streams.close()
      }
    }(config)
}
