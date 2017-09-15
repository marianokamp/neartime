package persistence

import java.io.File

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer
//import com.databricks.spark.avro._
//import com.databricks.spark.avro._


object PersistenceDemo {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("PersistenceDemo")
      .master("local")
      .getOrCreate()

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block // call-by-name
      val t1 = System.currentTimeMillis()
      val elapsedTime = (t1 - t0) / 1000
      println("Elapsed time: " + elapsedTime + "s")
      result
    }

    val results = ListBuffer.empty[(String, Double, Long, Long, Long)]

    def renderResults(results: ListBuffer[(String, Double, Long, Long, Long)]) {
      results.foreach { case (f, e, before, after, delta) =>
        println(f"${f}%40s $e%6.2fs ${before/1024.0/1024.0}%6.2f MB ${after/1024.0/1024.0}%6.2f MB ${delta/1024.0/1024.0}%6.2f MB")
      }
    }

    def performTestOverwrite(fileName: String, format: String, action: (String, String) => Unit) = {
      val started = System.currentTimeMillis()

      action(fileName, format)

      val elapsedTime = (System.currentTimeMillis()-started) / 1000.0
      val newSize = calcFileSize(fileName)

      (fileName, elapsedTime, 0L, newSize, newSize)
    }

    def performTestAppend2(spark: SparkSession, appendDf: DataFrame, schema: StructType, fileName: String, format: String) = {
      import scala.collection.JavaConverters._

      val initialSizeKb = calcFileSize(fileName)

      val started = System.currentTimeMillis()
      var counter = 0
      println()
      appendDf.foreach { row =>
        counter += 1
        if (counter % 100 == 0) print(".")
        val newDf = spark.createDataFrame(List(row).asJava,
          schema)

        newDf.write.option("compression", "snappy").format(format).mode("append").save(fileName)
      }
      println()

      val elapsedTime = (System.currentTimeMillis() - started) / 1000.0
      val size = calcFileSize(fileName)
      val r = (fileName, elapsedTime, size-initialSizeKb)
      r
    }

    def calcFileSize(fileName: String): Long = {
      val f = new File(fileName)

      if (f.isDirectory)
        return (f.list().map{ child => calcFileSize(f+"/"+child) }).sum
      else
        f.length
    }

    def performTestAppend(spark: SparkSession, appendDf: DataFrame, schema: StructType, fileName: String, format: String) = {
      import scala.collection.JavaConverters._

      val initialSize = calcFileSize(fileName)

      val rows = appendDf.collect()

      val started = System.currentTimeMillis()

      rows.foreach{ row =>

        val newDf = spark.createDataFrame(List(row).asJava,
                          schema)

        newDf.write.option("compression", "snappy").format(format).mode("append").save(fileName)
      }

      val elapsedTime = (System.currentTimeMillis() - started) / 1000.0

      val r = (fileName, elapsedTime, initialSize, calcFileSize(fileName), calcFileSize(fileName)-initialSize)
      r
    }

    if (true) {
      val df = time {
        spark.read.option("header", true).
          option("inferSchema", true).
          csv("/Users/mkamp/Downloads/yellow_tripdata_2016-01.csv").
          coalesce(1)
      } // sample(true, 0.01).

      df.printSchema()

      Logger.getRootLogger.info("Test!")

      time {
        println("count=" + df.count())
      }

      println("-----")


      results += performTestOverwrite("/tmp/x/outAvroNotCompressed", "com.databricks.spark.avro", {
        spark.conf.set("spark.sql.avro.compression.codec", "uncompressed")
        (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
      })

      results += performTestOverwrite("/tmp/x/outAvroCompressedDeflate5", "com.databricks.spark.avro", {
        spark.conf.set("spark.sql.avro.compression.codec", "deflate")
        spark.conf.set("spark.sql.avro.deflate.level", "5")
        (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
      })

      results += performTestOverwrite("/tmp/x/outAvroCompressedDeflate9", "com.databricks.spark.avro", {
        spark.conf.set("spark.sql.avro.compression.codec", "deflate")
        spark.conf.set("spark.sql.avro.deflate.level", "9")
        (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
      })

      results += performTestOverwrite("/tmp/x/outAvroCompressedSnappy5", "com.databricks.spark.avro", {
        spark.conf.set("spark.sql.avro.compression.codec", "snappy")
        spark.conf.set("spark.sql.avro.deflate.level", "5")
        (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
      })

      results += performTestOverwrite("/tmp/x/outParquetNotCompressed", "parquet", {
        (fileName, format) => df.write.option("compression", "uncompressed").format(format).mode("overwrite").save(fileName)
      })

      results += performTestOverwrite("/tmp/x/outParquetCompressed", "parquet", {
        (fileName, format) => df.write.option("compression", "snappy").format(format).mode("overwrite").save(fileName)
      })


      println("num Partitions=" + df.rdd.getNumPartitions)

      // Scenario 2.

      val N = 100
      //val records = spark.createDataFrame()
      import spark.implicits._

      val appendDf = spark.createDataFrame(df.takeAsList(N), df.schema)

      results += performTestOverwrite("/tmp/x/outParquetCompressedAppend", "parquet", {
        (fileName, format) => appendDf.write.option("compression", "snappy").format(format).mode("append").save(fileName)
      })

      //val r: (String, Double, Double) =
      results += performTestAppend(spark, appendDf, df.schema, "/tmp/x/outParquetCompressedAppend", "parquet")

      results += performTestOverwrite("/tmp/x/outParquetCompressedAppend", "parquet", {
        (fileName, format) => appendDf.write.option("compression", "snappy").format(format).mode("append").save(fileName)
      })

      //val r: (String, Double, Double) =

      spark.conf.set("spark.sql.avro.compression.codec", "snappy")
      spark.conf.set("spark.sql.avro.deflate.level", "5")

      results += performTestAppend(spark, appendDf, df.schema, "/tmp/x/outAvroCompressedAppend", "com.databricks.spark.avro")

      results += performTestOverwrite("/tmp/x/outAvroCompressedAppend", "com.databricks.spark.avro", {
        (fileName, format) => appendDf.write.option("compression", "snappy").format(format).mode("append").save(fileName)
      })

      renderResults(results)

    }

    spark.stop()
  }
}
