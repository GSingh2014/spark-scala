package com.sparkScala

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

trait SparkSessionWrapper {

  val config = ConfigFactory.load()

  val master = config.getString("spark.master")
  // private val appName = config.getString("spark.app.name")

  val elasticsearchUser = config.getString("spark.elasticsearch.username")
  val elasticsearchPass = config.getString("spark.elasticsearch.password")
  val elasticsearchHost = config.getString("spark.elasticsearch.host")
  val elasticsearchPort = config.getString("spark.elasticsearch.port")

  val outputMode = config.getString("spark.elasticsearch.output.mode")
  val destination = config.getString("spark.elasticsearch.data.source")
  val checkpointLocation = config.getString("spark.elasticsearch.checkpoint.location")
  val index = config.getString("spark.elasticsearch.index")
  val docType = config.getString("spark.elasticsearch.doc.type")
  val indexAndDocType = s"$index/$docType"

  lazy  val sparkSession = SparkSession.builder()
    .config(ConfigurationOptions.ES_NODES, elasticsearchHost)
    .config(ConfigurationOptions.ES_PORT, elasticsearchPort)
    .config(ConfigurationOptions.ES_INDEX_AUTO_CREATE, true)
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, elasticsearchUser)
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, elasticsearchPass)
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", true)
    //.config(ConfigurationOptions.ES_NODES_WAN_ONLY, true) // For this error to connect to docker container "I/O exception (java.net.ConnectException) caught when processing request: Connection timed out: connect"
    .master(master)
    .appName("Spark-Structured-Streaming-Kafka-Producer")
    .getOrCreate()

}
