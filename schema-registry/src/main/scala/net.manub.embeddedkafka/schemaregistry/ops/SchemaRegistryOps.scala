package net.manub.embeddedkafka.schemaregistry.ops

import java.util.Properties

import io.confluent.kafka.schemaregistry.RestApp
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.serializers.{
  AbstractKafkaAvroSerDeConfig,
  KafkaAvroDeserializerConfig
}
import net.manub.embeddedkafka.EmbeddedServer
import net.manub.embeddedkafka.ops.RunningServersOps
import net.manub.embeddedkafka.schemaregistry.{
  EmbeddedKafkaConfigWithSchemaRegistry,
  EmbeddedSR
}

/**
  * Trait for Schema Registry-related actions.
  * Relies on [[RestApp]].
  */
trait SchemaRegistryOps {

  /**
    * @param config an implicit [[EmbeddedKafkaConfigWithSchemaRegistry]].
    * @return a map of configuration to grant Schema Registry support
    */
  protected[embeddedkafka] def configForSchemaRegistry(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] =
    Map(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> s"http://localhost:${config.schemaRegistryPort}")

  /**
    * @param config an implicit [[EmbeddedKafkaConfigWithSchemaRegistry]].
    * @return a map of Kafka Consumer configuration to grant Schema Registry support
    */
  protected[embeddedkafka] def consumerConfigForSchemaRegistry(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] =
    configForSchemaRegistry ++ Map(
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> true.toString
    )

  /**
    * Starts a Schema Registry instance.
    *
    * @param schemaRegistryPort     the port to run Schema Registry on
    * @param zooKeeperPort          the port ZooKeeper is running on
    * @param avroCompatibilityLevel the default [[AvroCompatibilityLevel]] of schemas
    * @param properties             additional [[Properties]]
    */
  def startSchemaRegistry(schemaRegistryPort: Int,
                          zooKeeperPort: Int,
                          avroCompatibilityLevel: AvroCompatibilityLevel =
                            AvroCompatibilityLevel.NONE,
                          properties: Properties = new Properties): RestApp = {
    val server = new RestApp(schemaRegistryPort,
                             s"localhost:$zooKeeperPort",
                             "_schemas",
                             avroCompatibilityLevel.name,
                             properties)
    server.start()
    server
  }

}

/**
  * [[SchemaRegistryOps]] extension relying on [[RunningServersOps]] for
  * keeping track of running [[EmbeddedSR]] instances.
  *
  * @see [[RunningServersOps]]
  */
trait RunningSchemaRegistryOps {
  this: SchemaRegistryOps with RunningServersOps =>

  def startSchemaRegistry(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry): EmbeddedSR = {
    val restApp = EmbeddedSR(
      startSchemaRegistry(config.schemaRegistryPort, config.zooKeeperPort))
    runningServers.add(restApp)
    restApp
  }

  /**
    * Stops all in memory Schema Registry instances.
    */
  def stopSchemaRegistry(): Unit =
    runningServers.stopAndRemove(isEmbeddedSR)

  private[embeddedkafka] def isEmbeddedSR(server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedSR]

}
