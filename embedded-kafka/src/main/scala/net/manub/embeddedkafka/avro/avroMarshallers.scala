package net.manub.embeddedkafka.avro

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.io._
import org.apache.avro.specific.{
  SpecificDatumReader,
  SpecificDatumWriter,
  SpecificRecord
}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class KafkaAvroDeserializer[T <: SpecificRecord](schema: Schema)
    extends Deserializer[T] {

  private val reader = new SpecificDatumReader[T](schema)

  override def deserialize(topic: String, data: Array[Byte]): T = {
    val decoder = DecoderFactory.get().binaryDecoder(data, null)
    reader.read(null.asInstanceOf[T], decoder)
  }
}

class KafkaAvroSerializer[T <: SpecificRecord]() extends Serializer[T] {

  private def toBytes(nullableData: T): Array[Byte] =
    Option(nullableData).fold[Array[Byte]](null) { data =>
      val writer: DatumWriter[T] = new SpecificDatumWriter[T](data.getSchema)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, null)

      writer.write(data, encoder)
      encoder.flush()
      out.close()

      out.toByteArray
    }

  override def serialize(topic: String, data: T): Array[Byte] =
    toBytes(data)
}
