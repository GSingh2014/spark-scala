package com.sparkScala

import java.io.File

import com.databricks.spark.avro.SchemaConverters
import com.vividsolutions.jts.geom.Geometry
import org.apache.avro.generic.GenericRecord
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.geosparksql.expressions.ST_GeomFromWKT
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD._
import org.datasyslab.geosparksql.utils.Adapter

object GeoSpatialShapefileIngestion extends SparkSessionWrapper{

  def main(args: Array[String]): Unit = {

   // val shapefileInputLocation1="C:\\Users\\singhgo\\Documents\\work\\dev\\shapefiles\\shapefiles_1\\*"
   // val shapefileInputLocation2="C:\\Users\\singhgo\\Documents\\work\\dev\\shapefiles\\shapefiles_2\\*"

    val dir = new File("C:\\Users\\singhgo\\Documents\\work\\dev\\shapefiles")

    val subdirList = getListOfSubDirectories(dir)

    println(subdirList)
/*
    var initSpatialRDD: RDD[Geometry] = sparkSession.sparkContext.emptyRDD
    var unionRDD: RDD[Geometry] = sparkSession.sparkContext.emptyRDD*/

    val schema = StructType(Seq(
      StructField("geometry", StringType, nullable = true),
      StructField("recnum", StringType, nullable = true),
      StructField("featurecla", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("scalerank", StringType, nullable = true),
      StructField("min_zoom", StringType, nullable = true)
    ))

    var rawSpatialDfMap = Map[String, (DataType, Boolean)]()
    var schemaMap = Map[String, (DataType, Boolean)]()

    var initDF: DataFrame = sparkSession.sqlContext.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
    var unionDF: DataFrame = sparkSession.sqlContext.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)


    for (subdir <- subdirList){
      val spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, dir + "\\" + subdir)
      println(spatialRDD.getClass)
      val rawSpatialDf = Adapter.toDf(spatialRDD, sparkSession)
      rawSpatialDf.printSchema()
      rawSpatialDf.show(2)
      if (schema != rawSpatialDf.schema)
      {
        rawSpatialDfMap =  rawSpatialDf.schema.map{ (structField: StructField) =>
          structField.name.toLowerCase -> (structField.dataType, structField.nullable)
        }.toMap
        schemaMap = schema.map{ (structField: StructField) =>
          structField.name.toLowerCase -> (structField.dataType, structField.nullable)
        }.toMap

        var schemadiff = getSchemaDifference(rawSpatialDfMap, schemaMap)

        println("***Schema Diff****")
        println(schemadiff)

      }
      val schema_aligned_DF = sparkSession.sqlContext.createDataFrame(rawSpatialDf.rdd, schema)
      schema_aligned_DF.printSchema()
      schema_aligned_DF.show(10)
     /* unionRDD = sparkSession.sparkContext.union(initSpatialRDD, spatialRDD.getRawSpatialRDD)
      initSpatialRDD = unionRDD*/
      unionDF = unionDF.union(schema_aligned_DF)
      initDF = unionDF
    }

    //unionRDD.foreach(println)
    /*print("****************")
    println(unionRDD.count())

    println(unionRDD.getClass)

    val schema_aligned_DF = Adapter.toDf(unionRDD.toJavaRDD().asInstanceOf[SpatialRDD[Geometry]], sparkSession)

    schema_aligned_DF.show(1000)*/

    print("****************")
    unionDF.printSchema()
    unionDF.show(10000)



    /*val a = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation1)
    val b = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation2)
    val spatialRDD = a.getRawSpatialRDD.union(b.getRawSpatialRDD)

    val c = sparkSession.sparkContext.union(a.rawSpatialRDD, b.rawSpatialRDD)

    spatialRDD.rdd.foreach(println)
    print("****************")
    println(spatialRDD.rdd.count())
    println(spatialRDD.take(1))

    c.foreach(println)
    print("****************")
    println(c.count())
    c.take(1).foreach(println)
*/
    //val rawSpatialDf = Adapter.toDf(spatialRDD, sparkSession)
    //print(spatialRDD.getRawSpatialRDD().collect())

    /*rawSpatialDf.show()
    rawSpatialDf.printSchema()
    print(rawSpatialDf.count())*/

  }

  def getListOfSubDirectories(dir: File): List[String] = {
    val files = dir.listFiles
    val dirs = for {
      file <- files
      if file.isDirectory
    } yield file.getName
    dirs.toList
  }

   // Compare relevant information
  def getSchemaDifference(schema1: Map[String, (DataType, Boolean)],
                          schema2: Map[String, (DataType, Boolean)]
                         ): Map[String, (Option[(DataType, Boolean)], Option[(DataType, Boolean)])] = {
    (schema1.keys ++ schema2.keys).
      map(_.toLowerCase).
      toList.distinct.
      flatMap { (columnName: String) =>
        val schema1FieldOpt: Option[(DataType, Boolean)] = schema1.get(columnName)
        val schema2FieldOpt: Option[(DataType, Boolean)] = schema2.get(columnName)

        if (schema1FieldOpt == schema2FieldOpt) None
        else Some(columnName -> (schema1FieldOpt, schema2FieldOpt))
      }.toMap
  }

}

