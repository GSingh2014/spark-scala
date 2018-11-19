import java.io.ByteArrayOutputStream

import org.apache.spark.sql.{Row, SparkSession, functions}
import com.databricks.spark.avro._
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType


import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory

// --packages com.springml:spark-sftp_2.11:1.1.3
// --packages com.databricks:spark-avro_2.11:4.0.0

object SftpStreaming extends Serializable {

  def getSchema(schema:String) : Schema ={
    val avroschema = new Schema.Parser().parse(schema)
    avroschema
  }


  val schema = "{\"name\": \"sftp\",\"namespace\": \"nifi\","  +
    "\"type\": \"record\"," +
    "\"fields\": [" +
    "{\"name\": \"filename\", \"type\": \"string\" }," +
    "{\"name\": \"filedate\", \"type\": \"string\" }," +
    "{\"name\": \"content\", \"type\": \"string\" }]}"


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

      println(out.toByteArray())

      out.toByteArray()
  }


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark-Structured-Streaming-SFTP")
      .getOrCreate()


    val avroSchema = getSchema(schema)
    val structSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    println(avroSchema)
    println(structSchema)

    val df = sparkSession.read.
      format("com.springml.spark.sftp").
      schema(structSchema).
      option("host", "localhost").
      option("port", "2233").
      option("username", "gsingh").
      option("password", "tc@5f^p").
      option("fileType", "csv").
      option("delimiter", ":").
      option("header", false).
      option("inferSchema", false).
      load("/upload/users.conf")

    df.printSchema()

    df.show(10)

    val serializeUDF = functions.udf(serialize(schema) _)

    val column = df.columns.map{c => functions.lit(c)}

   val avrodf = df.select(
      serializeUDF(functions.struct(df.columns map functions.col:_*)).alias("value")
    )

    avrodf.write
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:29092")
      .option("topic","sftp-topic")
      .save()

  }
}
