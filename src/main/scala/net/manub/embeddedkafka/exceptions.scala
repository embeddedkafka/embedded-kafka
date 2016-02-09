package net.manub.embeddedkafka

class KafkaUnavailableException(cause: Throwable) extends RuntimeException(cause)

class KafkaSpecException(msg: String) extends RuntimeException(msg)