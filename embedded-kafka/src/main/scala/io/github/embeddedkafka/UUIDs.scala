package io.github.embeddedkafka

import java.util.UUID

/**
  * Utility object for creating unique test IDs.
  * Useful for separating IDs and directories across test cases.
  */
object UUIDs {

  /**
    * Create a new unique ID.
    * @return the unique ID
    */
  def newUuid(): UUID = UUID.randomUUID()
}
