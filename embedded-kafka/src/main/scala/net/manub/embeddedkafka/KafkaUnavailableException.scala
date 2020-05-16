package net.manub.embeddedkafka

class KafkaUnavailableException(msg: String, cause: Throwable)
    extends RuntimeException(msg, cause) {
  def this(msg: String) = this(msg, null)
  def this(cause: Throwable) = this(null, cause)
}
