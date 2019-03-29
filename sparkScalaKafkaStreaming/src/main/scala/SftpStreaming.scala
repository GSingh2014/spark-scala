package com.sparkScala

import java.io.ByteArrayOutputStream

import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable
import scala.util.Try

//import scala.util.parsing.json.JSONFormat.ValueFormatter
//import scala.util.parsing.json.{JSONArray, JSONFormat, JSONObject}

//import scala.io.Source
//import com.jcraft.jsch.Channel
import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
//import com.jcraft.jsch.Session

import org.apache.spark.sql.{Row, SparkSession, functions, DataFrame}
import com.databricks.spark.avro._
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import scala.reflect.runtime.universe.TypeTag

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, BinaryEncoder}

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._
import scala.collection.JavaConversions._

//import com.sparkScala.Schema2CaseClass

// --packages com.springml:spark-sftp_2.11:1.1.3
// --packages com.databricks:spark-avro_2.11:4.0.0

// DOES NOT DELETE THE FILES FROM SFTP AFTER THEY HAVE BEEN PUSHED TO KAFKA



object SftpStreaming extends SparkSessionWrapper {

  val SFTPHOST = "localhost"
  val SFTPPORT = 2233
  val SFTPUSER = "gsingh"
  val SFTPPASS = "tc@5f^p"
  val SFTPWORKINGDIR = "/upload/bus_traffic/"
  val schemaRegistryURL = "http://localhost:8081"


  def getSchema(schema:String) : Schema ={
    val avroschema = new Schema.Parser().parse(schema)
    avroschema
  }

  def getSchema(schemaRegistryURL:String, topicName:String) : Schema = {
    val restService = new RestService(schemaRegistryURL)
    val subjectValueName = topicName
    val valueRestResponseSchema = restService.getLatestVersion(subjectValueName)
    val parser = new Schema.Parser
    val topicValueAvroSchema: Schema = parser.parse(valueRestResponseSchema.getSchema)
    /*val keySchemaString = "\"string\""
    val keySchema = parser.parse(keySchemaString)
    keySchema*/
    topicValueAvroSchema
  }



  /*val schema = "{\"name\": \"sftp\",\"namespace\": \"nifi\","  +
    "\"type\": \"record\"," +
    "\"fields\": [" +
    "{\"name\": \"filename\", \"type\": \"string\" }," +
    "{\"name\": \"filedate\", \"type\": \"string\" }," +
    "{\"name\": \"content\", \"type\": \"string\" }]}"*/

  val schema = "{\"name\": \"sftp\",\"namespace\": \"nifi\","  +
    "\"type\": \"record\"," +
    "\"fields\": [" +
    "{\"name\": \"id\", \"type\": \"int\" }," +
    "{\"name\": \"username\", \"type\": \"string\" }," +
    "{ \"name\": \"dateofcreation\", \"type\": \"long\",\"default\": 19700101 }," +
    "{ \"name\": \"status\", \"type\": \"string\",\"default\": \"active\" }," +
    "{ \"name\": \"returncode\", \"type\": \"int\",\"default\": 0 }," +
    "{ \"name\": \"phone\", \"type\": \"int\",\"default\": 1234567 }" +
    "]}"

  /*def getValuesMap[T](row: Row, schema: StructType): Map[String,Any] = {
    schema.fields.map {
      field =>
        try{
          if (field.dataType.typeName.equals("struct")){
            field.name -> getValuesMap(row.getAs[Row](field.name),   field.dataType.asInstanceOf[StructType])
          }else{
            field.name -> row.getAs[T](field.name)
          }
        }catch {case e : Exception =>{field.name -> null.asInstanceOf[T]}}
    }.filter(xy => xy._2 != null).toMap
  }

  def convertRowToJSON(row: Row, schema: StructType): JSONObject = {
    val m: Map[String, Any] = getValuesMap(row, schema)
    JSONObject(m)
  }

  val defaultFormatter : ValueFormatter = (x : Any) => x match {
    case s : String => "\"" + JSONFormat.quoteString(s) + "\""
    case jo : JSONObject => jo.toString(defaultFormatter)
    case jmap : Map[String,Any] => JSONObject(jmap).toString(defaultFormatter)
    case ja : JSONArray => ja.toString(defaultFormatter)
    case other => other.toString
  }*/

  //val avroSchema = getSchema(schema)
  val avroSchema = getSchema(schemaRegistryURL,"bus-topic")

/*  def encode(schema: org.apache.avro.Schema, row: Row): Array[Byte] = {
    val gr: GenericRecord = new GenericData.Record(schema)
    row.schema.fieldNames.foreach(name => gr.put(name, row.getAs(name)))

    val writer = new GenericDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(gr, encoder)
    encoder.flush()
    out.close()

    out.toByteArray()
  }

  val encodeUDF = functions.udf(encode(avroSchema,_))*/

  def getListOfFiles() : List[String]= {

    var listOfFileslb = new ListBuffer[String]()
      try{
      println(SFTPUSER, SFTPHOST)
      val jsch = new JSch()
      val session = jsch.getSession(SFTPUSER, SFTPHOST, SFTPPORT)
      session.setPassword(SFTPPASS)
     session.setConfig(
        "PreferredAuthentications",
        "publickey,keyboard-interactive,password")
      //jsch.addIdentity("C:\\Users\\singhgo\\.ssh\\id_rsa","P0cHd1@101")
      jsch.addIdentity("~/.ssh/id_rsa",SFTPPASS)
      val config = new java.util.Properties()
      config.put("StrictHostKeyChecking", "no")
      session.setConfig(config)
      session.connect()

      val channel = session.openChannel("sftp")
      channel.connect()
      val channelSftp:ChannelSftp = channel.asInstanceOf[ChannelSftp]
      channelSftp.cd(SFTPWORKINGDIR)

      val listOfFiles = channelSftp.ls("*.csv")

      var filename:String =""

      for(i<- 0 to listOfFiles.size() - 1){
        filename = listOfFiles.get(i).toString().split(" ").last
        println(filename)
        listOfFileslb += filename
      }

      channelSftp.disconnect()
      channel.disconnect()
      session.disconnect()

    }catch {
      case e: Exception => println(e.printStackTrace())
    }

    listOfFileslb.toList
  }

/*  def getSchemafromSchemaRegistry() ={

    val restService = new RestService(schemaRegistryURL)

    val valueRestResponseSchema = restService.getLatestVersion("sftp")

    val parser = new Schema.Parser()
    val avroSchema = parser.parse(valueRestResponseSchema.getSchema)

    println(avroSchema)


  }*/

  private val myFlatMapFunction: String => Seq[String] = { input: String =>
    println(input)
    input.split("(\\r\\n)|\\r|\\n")
  }

  val flatMapUdf = functions.udf(myFlatMapFunction)

/*  private val convertToJson:(Row, StructType) => String = {
    (row: Row, structSchema: StructType) =>
      row.toString()
  }*/

  def serialize(schemaString: String)(row: Row): Array[Byte] = {
      println(row.get(0))
      val schema = getSchema(schemaString)
      val record: GenericRecord = new GenericData.Record(schema)
      row.schema.fieldNames.foreach(name => record.put(name, row.getAs(name)))

      val writer = new GenericDatumWriter[GenericRecord](schema)
      val out = new ByteArrayOutputStream()
      val binaryencoder = EncoderFactory.get.binaryEncoder(out,null)
      writer.write(record, binaryencoder)
      binaryencoder.flush()
      out.close()

      //println(out.toByteArray())

      out.toByteArray()
  }


  def deserialize[T : TypeTag](bytes: Array[Byte]): List[Row] = {
    import org.apache.avro.file.SeekableByteArrayInput
    import org.apache.avro.generic.{GenericDatumReader, GenericRecord}


    val datumReader = new GenericDatumReader[GenericRecord](avroSchema)
    val inputStream = new SeekableByteArrayInput(bytes)
    val decoder = DecoderFactory.get.binaryDecoder(inputStream, null)

    val struct_schema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    val result = new mutable.MutableList[Row]
    while (!decoder.isEnd) {
      val item = datumReader.read(null, decoder)

      print(item.getClass)

      val objectArray = new Array[Any](item.asInstanceOf[GenericRecord].getSchema.getFields.size)

      for (field <- item.getSchema.getFields) {
        objectArray(field.pos) = item.get(field.pos)
      }

      //result += sql_item
      result += new GenericRowWithSchema(objectArray, struct_schema)
     }
    result.toList
  }


  def saveToKafka(structSchema: StructType, filename: String, filenumber: Int, topicname : String) = {
    println("***Saving contents of file#" + filenumber + " -> " + filename +" to kafka***")
    //dbutils.fs.ls("wasb://spark-opensource@opensourcestore.blob.core.windows.net/tmp/")
    val df = sparkSession.read.
      format("com.springml.spark.sftp").
      schema(structSchema).
      //("wasb://spark-opensource@opensourcestore.blob.core.windows.net/tmp/").
      option("host", SFTPHOST).
      option("port", SFTPPORT).
      option("username", SFTPUSER).
      option("password", SFTPPASS).
      //option("pem", "C:\\Users\\singhgo\\.ssh\\id_rsa").
      //option("pem","~/.ssh/id_rsa").
      //option("pemPassphrase", SFTPPASS).
      option("fileType", "csv").
      option("delimiter", ",").
      option("header", false).
      option("inferSchema", false).
      load(SFTPWORKINGDIR + filename)

    df.printSchema()

    df.show(1)

    println(structSchema.typeName)


    //val serializeUDF = functions.udf(serialize(schema) _)

    val serializeUDF = functions.udf(serialize(avroSchema.toString) _)

    val column = df.columns.map{c => functions.lit(c)}

    val avrodf = df.select(
      serializeUDF(functions.struct(df.columns map functions.col:_*)).alias("value")
    )

    println("Loading to Kafka " + kafkabrokers)
    avrodf.write
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkabrokers)
      .option("topic",kafkatopic)
      .save()

  }


  def WriteToElastic(structSchema:StructType) = {
    println(kafkabrokers, kafkatopic, structSchema)
    val sftpReadStream = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkabrokers)
      .option("subscribe", kafkatopic)
      //.option("kafka.enable.auto.commit","false") // All Kafka configurations should be set with kafka. prefix. Hence the correct option key is kafka.auto.offset.reset.
      .option("startingOffsets", "earliest") //"latest" for streaming, "earliest" for batch
      //.option("endingOffsets", "latest")
      //.option("max.partition.fetch.bytes", "204857600")
      .load()

    import sparkSession.implicits._

/*    val s2cc = new Schema2CaseClass
    import s2cc.implicits._

    val schemaClass = s2cc.schemaToCaseClass(structSchema, "schemaClass")

    println(schemaClass)*/

  /*  val convertToJsonUDF = functions.udf((row:Seq[Row]) => {
      //println(structSchema)
      //println(row)
      val strColumns = structSchema.fieldNames.mkString(",")
     // val data = sparkSession.sparkContext.parallelize(row).map(r => (r.getString(0), r.getString(1)))
      val data: DataFrame = sparkSession.sqlContext.createDataFrame(sparkSession.sparkContext.parallelize(row),structSchema)
      println(strColumns)
      //row.map(_.mkString(",").split(","))
      //println(data.toDF("id","username").toJSON.toString())
      //data.toDF("id","username").toJSON.toString()
      ""
    })*/

   /* val arrayToString = ((arr: collection.mutable.WrappedArray[String]) =>
      arr.flatten.mkString(",")) */

/*    val getColumnsUDF = functions.udf((details: Seq[String]) => {
      //val columnList = List("id","username")
      val columnList = structSchema.fieldNames.toList
      val split_details = details.mkString(",").split(",")
      //split_details.foreach(println)
      val zip_details_columnList = columnList zip split_details
      //println(zip_details_columnList)
      val map_details_columnList = zip_details_columnList.toMap
      //println(map_details_columnList)
      map_details_columnList
    })

    val outDF = sftpReadStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").as[(String,String)]
      //.withColumn("value",functions.explode(functions.split($"value","\n")))
      .withColumn("delimitedRow", functions.explode(flatMapUdf(functions.col("value"))))
      .withColumn("newcol", functions.split(functions.col("delimitedRow"),","))
      .withColumn("mapcol", getColumnsUDF($"newcol"))
      .withColumn("id",functions.lit($"mapcol.id"))
      //.withColumn("jsoncol", functions.to_json($"mapcol"))
      .drop("value","delimitedRow","newcol")

      //.withColumn("jsonCol", convertToJsonUDF(functions.col("newcol")))

      // columnlist.foreach(elt => tempDF.withColumn(elt, $"newcol"(elt.indexOf(elt))))

    //.withColumn("id", $"newcol"(0))
     // .withColumn("username", $"newcol"(1))

      outDF.printSchema()*/

 //   val newDF = tempDF.withColumn("jsonCol", convertToJsonUDF(tempDF("newcol"),structSchema))
      //.withColumn("jspnCol",functions.to_json( functions.struct( tempDF.columns.map(functions.col(_)):_* ) ) )
      //.withColumn("avroCol", encodeUDF(functions.col("newcol")))

      //.select(structSchema.map(field => functions.col(field.name).cast(field.dataType)): _*)
      //.map(row => convertRowToJSON(row, structSchema)).toJSON

    val deserializeUDF = functions.udf(deserialize _)

    val es_df = sftpReadStream.withColumn("value", deserializeUDF($"value"))

    val consoleQuery = es_df
        //.withColumn("structRow", functions.split(sftpReadStream.columns.map(c=> _)))
       .writeStream
       .outputMode(outputMode)
       .format("console")
       .option("truncate", false)
       .start()

    consoleQuery.awaitTermination()



/*    val esQuery = outDF.writeStream
      .outputMode(outputMode)
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", checkpointLocation)
     // .option("es.read.metadata", "true")
     // .option("es.mapping.id","id")
     // .option("es.write.operation","upsert")
      .start(indexAndDocType)


    esQuery.awaitTermination()*/
  }

  def main(args: Array[String]): Unit = {

/*    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark-Structured-Streaming-SFTP")
      .getOrCreate()*/

    //getSchemafromSchemaRegistry()

    val structSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    //println(avroSchema)
    println(structSchema)


    val listOfFiles = getListOfFiles() //List("users.conf")

    println("---Printing ListOfFiles----")
    println(listOfFiles)

   //// for (file <- listOfFiles) saveToKafka(structSchema, file, listOfFiles.indexOf(file) + 1, "bus-topic")
    //Consume Kafka and write to elasticSearch

    WriteToElastic(structSchema)

  }
}
