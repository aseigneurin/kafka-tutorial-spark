package com.ippontech.kafkatutorials.spark

import java.time.format.DateTimeFormatter

object Constants {
  val personsTopic = "persons"
  val personsAvroTopic = "persons-avro"
  val agesTopic = "ages"

  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
}
