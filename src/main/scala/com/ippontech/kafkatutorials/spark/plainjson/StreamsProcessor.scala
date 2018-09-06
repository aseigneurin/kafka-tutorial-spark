package com.ippontech.kafkatutorials.spark.plainjson

import java.time.{LocalDate, Period}

import com.ippontech.kafkatutorials.spark.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

object StreamsProcessor {
  def main(args: Array[String]): Unit = {
    new StreamsProcessor("localhost:9092").process()
  }
}

class StreamsProcessor(brokers: String) {

  def process(): Unit = {

    val spark = SparkSession.builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", Constants.personsTopic)
      .load()

    val personJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("firstName", DataTypes.StringType)
      .add("lastName", DataTypes.StringType)
      .add("birthDate", DataTypes.StringType)
    val personNestedDf = personJsonDf.select(from_json($"value", struct).as("person"))

    val personFlattenedDf = personNestedDf.selectExpr("person.firstName", "person.lastName", "person.birthDate")

    val personDf = personFlattenedDf.withColumn("birthDate", to_timestamp($"birthDate", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

    val ageFunc: java.sql.Timestamp => Int = birthDate => {
      val birthDateLocal = birthDate.toLocalDateTime().toLocalDate()
      val age = Period.between(birthDateLocal, LocalDate.now()).getYears()
      age
    }
    val ageUdf: UserDefinedFunction = udf(ageFunc, DataTypes.IntegerType)
    val processedDf = personDf.withColumn("age", ageUdf.apply($"birthDate"))

    val resDf = processedDf.select(
      concat($"firstName", lit(" "), $"lastName").as("key"),
      processedDf.col("age").cast(DataTypes.StringType).as("value"))

//    val consoleOutput = processedDf.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()

    val kafkaOutput = resDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", "ages")
      .option("checkpointLocation", "/Users/aseigneurin/dev/kafka-tutorials/spark/checkpoints")
      .start()

    spark.streams.awaitAnyTermination()
  }

}