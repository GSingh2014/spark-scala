package com.sparkScala

import java.io.File

import org.apache.spark.sql.{DataFrame, Row, SaveMode, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.Adapter
import com.microsoft.azure.sqldb.spark.connect._


object GeoSpatialShapefileIngestion extends SparkSessionWrapper{

  def main(args: Array[String]): Unit = {


    val dir = new File("C:\\Users\\singhgo\\Documents\\work\\dev\\shapefiles")

    val subdirList = getListOfSubDirectories(dir)



    val schema = StructType(Seq(
      StructField("geometry", StringType, nullable = true),
      StructField("recnum", StringType, nullable = true),
      StructField("featurecla", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("scalerank", StringType, nullable = true),
      StructField("min_zoom", StringType, nullable = true),
      StructField("depth", StringType, nullable = true),
      StructField("geojson", StringType, nullable = true)
    ))

    /*var rawSpatialDfMap = Map[String, (DataType, Boolean)]()
    var schemaMap = Map[String, (DataType, Boolean)]()
    var schemaDiff = Map[String, (Option[(DataType, Boolean)], Option[(DataType, Boolean)])]()*/

    var initDF: DataFrame = sparkSession.sqlContext.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
    var unionDF: DataFrame = sparkSession.sqlContext.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)


    for (subdir <- subdirList) {
      val spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, dir + "\\" + subdir)
      println(spatialRDD.getClass)
      var rawSpatialDf = Adapter.toDf(spatialRDD, sparkSession)
      rawSpatialDf.printSchema()
      rawSpatialDf.show(2)

      /*      var schema_aligned_DF: DataFrame = sparkSession.sqlContext.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], rawSpatialDf.schema)

      if (schema != rawSpatialDf.schema)
      {
        rawSpatialDfMap =  rawSpatialDf.schema.map{ (structField: StructField) =>
          structField.name.toLowerCase -> (structField.dataType, structField.nullable)
        }.toMap
        schemaMap = schema.map{ (structField: StructField) =>
          structField.name.toLowerCase -> (structField.dataType, structField.nullable)
        }.toMap

        schemaDiff = getSchemaDifference(rawSpatialDfMap, schemaMap)

        println("***Schema Diff****")
        println(schemaDiff)

        for ((k,v) <- schemaDiff) {
          println(k)
          schema_aligned_DF = rawSpatialDf.withColumn(k, lit("-1"))
          rawSpatialDf = schema_aligned_DF
        }

        println("Schema after difference")
        schema_aligned_DF.printSchema()
      }
      /*val schema_aligned_DF = sparkSession.sqlContext.createDataFrame(rawSpatialDf.rdd, schema)
      schema_aligned_DF.printSchema()
      schema_aligned_DF.show(10)*/
     /* unionRDD = sparkSession.sparkContext.union(initSpatialRDD, spatialRDD.getRawSpatialRDD)
      initSpatialRDD = unionRDD*/
      unionDF = initDF.union(rawSpatialDf)
      initDF = unionDF
    }*/

      val initCols = initDF.columns.toSet
      val rawSpatialCols = rawSpatialDf.columns.toSet
      val total = initCols ++ rawSpatialCols // union

      unionDF = initDF.select(expr(initCols , total): _*).union(rawSpatialDf.select(expr(rawSpatialCols, total): _*))
      initDF = unionDF
    }

    // Convert geometry to geoJSON
    val geojsonDF = unionDF.withColumn("geojson", strGeometry2JsonUDF(unionDF.col("geometry")))
    print("****************")
    geojsonDF.printSchema()
    geojsonDF.select("geojson").show(10, false)

    geojsonDF.write.mode(SaveMode.Append).sqlDB(sqldbconfig)

  }

    private val strGeometry2JsonUDF = udf((geometry: String) => {
      val keyMap = Map("MULTILINESTRING" -> "MultiLineString", "LINESTRING" -> "LineString", "POLYGON" -> "Polygon", "POINT" -> "Point", "MULTIPOLYGON" -> "MultiPolygon")
      val arrGeom = geometry.replace("(","[").replace(")", "]")
        .split("\\b [\\[]")
      val coordinatesList = arrGeom{1}.split(",")
      var updt_coordinatesList = ""
      coordinatesList.foreach(x => updt_coordinatesList = updt_coordinatesList + "[" + x.replaceAll("\\d\\s", ",") + "]")
      updt_coordinatesList = updt_coordinatesList.replaceAll("\\]\\[", "\\],\\[")
      val geojson = "{\"type\": \"FeatureCollection\",\"features\": [{\"type\": \"Feature\",\"geometry\":{\"type\": \"" + keyMap(arrGeom{0}) + "\", \"coordinates\": ["  + updt_coordinatesList + "}}]}"
        .stripMargin
      geojson.trim
    })


    def expr(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }

  def getListOfSubDirectories(dir: File): List[String] = {
    val files = dir.listFiles
    val dirs = for {
      file <- files
      if file.isDirectory
    } yield file.getName
    dirs.toList
  }

   /*// Compare relevant information
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
*/
}

