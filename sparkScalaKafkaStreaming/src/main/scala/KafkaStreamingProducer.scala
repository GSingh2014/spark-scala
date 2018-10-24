package com.sparkScala

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object KafkaStreamingProducer extends SparkSessionWrapper {

  def main(args: Array[String]) {
    println("elasticsearchHost = " + elasticsearchHost + " port = " + elasticsearchPort)

    val vehicleSchema = StructType(
      Seq(
        StructField("event_id", IntegerType, true),
        StructField("vehicle_id", IntegerType, true),
        StructField("vehicle_speed", FloatType, true),
        StructField("engine_speed", IntegerType, true),
        StructField("tire_pressure", IntegerType, true),
        StructField("direction", ArrayType(FloatType, true), true),
        StructField("timestamp", TimestampType, true)
      )
    )

    val vehicleRawStreamDF = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "vehicle-topic")
      // .option("kafka.enable.auto.commit","false") // All Kafka configurations should be set with kafka. prefix. Hence the correct option key is kafka.auto.offset.reset.
      .option("kafka.startingOffsets", "latest")
      .option("failOnDataLoss", "false") //Not recommended - Some data may have been lost because they are not available in Kafka any more; either the data was aged out by Kafka or the topic may have been deleted before all the data in the topic was processed.
      .load()

    //// You should never set auto.offset.reset. Instead, "set the source option startingOffsets to specify where to start instead.This will ensure that no
    //data is missed when new topics/partitions are dynamically subscribed. Note that
    //'startingoffsets' only applies when a new Streaming query is started, and
    //that resuming will always pick up from where the query left off.

    val vehicleJsonStream = vehicleRawStreamDF
      .selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"), vehicleSchema).as("data"))
      .select("data.*")

    /*  vehicleJsonStream.writeStream
    .outputMode(outputMode)
    .format("console")
    .start().awaitTermination()*/

    vehicleJsonStream.writeStream
      .outputMode(outputMode)
      .format(destination)
      .option("checkpointLocation", checkpointLocation)
      .start(indexAndDocType).awaitTermination()

  }
}