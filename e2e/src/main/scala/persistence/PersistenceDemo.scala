package persistence

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object PersistenceDemo {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    val elapsedTime = (t1 - t0) / 1000
    println("Elapsed time: " + elapsedTime + "s")
    result
  }

  def renderResults(results: ListBuffer[(String, String, Double, Long, Long, Long)]) {
    results.foreach { case (t, f, e, before, after, delta) =>
      println(f"${t}%-40s ${f}%-60s $e%10.5fs ${before / 1024.0 / 1024.0}%10.5f MB ${after / 1024.0 / 1024.0}%10.5f MB ${delta / 1024.0 / 1024.0}%10.5f MB")
    }

    println()
    println()

    import java.io._
    val pw = new PrintWriter("/tmp/x/out.csv")

    pw.println(f"Title; File; Elapsed (s); Before (MB); After (MB); Delta (MB)")

    results.foreach { case (t, f, e, before, after, delta) =>
      pw.println(f"${t}%-40s; ${f}%-60s; $e%10.5f; ${before / 1024.0 / 1024.0}%10.5f; ${after / 1024.0 / 1024.0}%10.5f; ${delta / 1024.0 / 1024.0}%10.5f")
    }
    pw.close()
  }

  def performTest(title: String, fileName: String, format: String, action: (String, String) => Unit) = {
    println("perform test "+title+".")
    val f = new File(fileName)

    val oldSize = if (f.exists()) calcFileSize(f.getAbsolutePath) else 0L
    val started = System.currentTimeMillis()

    action(fileName, format)

    val elapsedTime = (System.currentTimeMillis() - started) / 1000.0
    val newSize = calcFileSize(fileName)

    (title, fileName, elapsedTime, oldSize, newSize, newSize - oldSize)
  }


  def calcFileSize(fileName: String): Long = {
    val f = new File(fileName)

    if (f.isDirectory)
      return (f.list().map { child => calcFileSize(f + "/" + child) }).sum
    else
      f.length
  }

  def performIndividualAppends(title: String, spark: SparkSession, appendDf: DataFrame, schema: StructType, fileName: String, format: String) = {
    println("perform Individual Appends  "+title+" fileName:"+fileName+" format:"+format+".")
    import scala.collection.JavaConverters._

    val initialSize = calcFileSize(fileName)

    val rows = appendDf.collect()

    val started = System.currentTimeMillis()

    rows.foreach { row =>

      val newDf = spark.createDataFrame(List(row).asJava,
        schema)

      val compression = if (format.equals("json")) "gzip" else "snappy"
      newDf.write.option("compression", compression).format(format).mode("append").save(fileName)
    }

    val elapsedTime = (System.currentTimeMillis() - started) / 1000.0

    (title, fileName, elapsedTime, initialSize, calcFileSize(fileName), calcFileSize(fileName) - initialSize)
  }

  def performQueryTest(spark: SparkSession, sql: String) = {
    println("perform querz test"+sql+".")
    val started = System.currentTimeMillis()

    import spark.implicits._
    val results = spark.sql(sql)

    val size = results.map { row => 1 }.reduce(_ + _)

    println("sql=" + sql + " size=" + size) //+" "+records.length+ " "+size)

    val elapsed = System.currentTimeMillis() - started
    (sql, elapsed / 1000.0)

  }

  def performWriteTests(spark: SparkSession, df: DataFrame, results: ListBuffer[(String, String, Double, Long, Long, Long)]) = {
    results += performTest("CSV uncompressed write", "/tmp/x/outCSV", "csv", {
      (fileName, format) => df.write.option("compression", "none").option("header", true).format(format).mode("overwrite").save(fileName)
    })

    results += performTest("CSV compressed gzip write", "/tmp/x/outCSVCompressed", "csv", {
      (fileName, format) => df.write.option("compression", "gzip").option("header", true).format(format).mode("overwrite").save(fileName)
    })


    results += performTest("Avro uncompressed write", "/tmp/x/outAvroNotCompressed", "com.databricks.spark.avro", {
      spark.conf.set("spark.sql.avro.compression.codec", "uncompressed")
      (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
    })

    results += performTest("Avro compressed d5 write", "/tmp/x/outAvroCompressedDeflate5", "com.databricks.spark.avro", {
      spark.conf.set("spark.sql.avro.compression.codec", "deflate")
      spark.conf.set("spark.sql.avro.deflate.level", "5")
      (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
    })

    results += performTest("Avro compressed d9 write", "/tmp/x/outAvroCompressedDeflate9", "com.databricks.spark.avro", {
      spark.conf.set("spark.sql.avro.compression.codec", "deflate")
      spark.conf.set("spark.sql.avro.deflate.level", "9")
      (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
    })

    results += performTest("Avro compressed s write", "/tmp/x/outAvroCompressedSnappy5", "com.databricks.spark.avro", {
      spark.conf.set("spark.sql.avro.compression.codec", "snappy")
      spark.conf.set("spark.sql.avro.deflate.level", "5")
      (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
    })


    results += performTest("JSON uncompressed write", "/tmp/x/outJsonNotCompressed", "json", {
      (fileName, format) => df.write.option("compression", "uncompressed").format(format).mode("overwrite").save(fileName)
    })

    results += performTest("JSON compressed gzip write", "/tmp/x/outJsonCompressedGzip", "json", {
      (fileName, format) => df.write.option("compression", "gzip").format(format).mode("overwrite").save(fileName)
    })

    results += performTest("JSON compressed bzip2 write", "/tmp/x/outJsonCompressedBzip2", "json", {
      (fileName, format) => df.write.option("compression", "bzip2").format(format).mode("overwrite").save(fileName)
    })

    results += performTest("JSON compressed deflate write", "/tmp/x/outJsonCompressedDeflate", "json", {
      (fileName, format) => df.write.option("compression", "deflate").format(format).mode("overwrite").save(fileName)
    })

    /*   results += performTest("JSON compressed s write", "/tmp/x/outJsonCompressed", "json", {
           (fileName, format) => df.write.option("compression", "snappy").format(format).mode("overwrite").save(fileName)
         })
   */

    results += performTest("Parquet uncompressed write", "/tmp/x/outParquetNotCompressed", "parquet", {
      (fileName, format) => df.write.option("compression", "uncompressed").format(format).mode("overwrite").save(fileName)
    })

    results += performTest("Parquet compressed s write", "/tmp/x/outParquetCompressed", "parquet", {
      (fileName, format) => df.write.option("compression", "snappy").format(format).mode("overwrite").save(fileName)
    })

    results += performTest("Parquet compressed gzip write", "/tmp/x/outParquetCompressedGzip", "parquet", {
      (fileName, format) => df.write.option("compression", "gzip").format(format).mode("overwrite").save(fileName)
    })
  }

  def performAndReportQueryTests(spark: SparkSession, df: DataFrame) = {
    val queryResults = ListBuffer.empty[(String, Double)]

    println("df_.count=" + df.count())
    println("Preparing query files.")


    val avroQueryFile = "/tmp/x/outAvroQueryFile"

    performTest("Avro queryfile write", avroQueryFile, "com.databricks.spark.avro", {
      spark.conf.set("spark.sql.avro.compression.codec", "snappy")
      (fileName, format) => df.write.format(format).mode("overwrite").save(fileName)
    })

    val parquetQueryFile = "/tmp/x/outParquetQueryFile"

    performTest("Parquet queryfile write", parquetQueryFile, "parquet", {

      (fileName, format) => df.write.option("compression", "snappy").format(format).mode("overwrite").save(fileName)
    })

    val jsonQueryFile = "/tmp/x/outJsonQueryFile"

    performTest("JSON queryfile write", jsonQueryFile, "json", {
      (fileName, format) => df.write.option("compression", "gzip").mode("overwrite").json(fileName)
    })

    val csvQueryFile = "/tmp/x/outCsvQueryFile"

    performTest("CSV queryfile write", csvQueryFile, "csv", {
      (fileName, format) => df.write.option("compression", "gzip").option("header", true).mode("overwrite").csv(fileName)
    })

    spark.read.parquet(parquetQueryFile).createOrReplaceTempView("taxi_parquet")
    spark.read.format("com.databricks.spark.avro").load(avroQueryFile).createOrReplaceTempView("taxi_avro")
    spark.read.json(jsonQueryFile).createOrReplaceTempView("taxi_json")
    spark.read.option("header", true).csv(csvQueryFile).createOrReplaceTempView("taxi_csv")

    println("Preparing query files done.")


    val iterations = 20
    for (a <- 1 to iterations) {

      println("Running iteration " + a + "/" + iterations + " :")
      queryResults += performQueryTest(spark, "SELECT passenger_count FROM taxi_parquet")
      queryResults += performQueryTest(spark, "SELECT * FROM taxi_parquet")

      //spark.catalog.dropTempView("taxi_parquet")

      queryResults += performQueryTest(spark, "SELECT passenger_count FROM taxi_avro")
      queryResults += performQueryTest(spark, "SELECT * FROM taxi_avro")

      //spark.catalog.dropTempView("taxi_avro")

      queryResults += performQueryTest(spark, "SELECT passenger_count FROM taxi_csv")
      queryResults += performQueryTest(spark, "SELECT * FROM taxi_csv")

      //spark.catalog.dropTempView("taxi_csv")

      queryResults += performQueryTest(spark, "SELECT passenger_count FROM taxi_json")
      queryResults += performQueryTest(spark, "SELECT * FROM taxi_json")

      //spark.catalog.dropTempView("taxi_json")

      println()
    }

    println()
    queryResults.foreach { case (sql, elapsed) =>
      println(f"${sql}%-50s $elapsed%6.2fs")
    }

    println()
    println()

    queryResults.sortBy(_._1).foreach { case (sql, elapsed) =>
      println(f"${sql}%-50s $elapsed%6.2fs")
    }

    println()
    println()
    println("Averaged:")
    println()
    spark.sparkContext.parallelize(queryResults).reduceByKey(_ + _).map { case (sql, totalTime) => (sql, totalTime / iterations) }.foreach { case (sql, elapsed) =>
      println(f"${sql}%-50s $elapsed%6.2fs")
    }

    println()
    println()
    println("Median:")

    spark.sparkContext.parallelize(queryResults).map { case (s, v) => (s, List(v)) }.reduceByKey(_ ++ _).map { case (s, vs) =>
      val sorted = vs.sorted
      val median = sorted(sorted.size / 2)
      (s, median)
    }.foreach { case (sql, elapsed) =>
      println(f"${sql}%-50s $elapsed%6.2fs")
    }


    println()
    println()
  }

  def perfomAppendTests(spark: SparkSession, results: ListBuffer[(String, String, Double, Long, Long, Long)], df: Dataset[Row]) = {
    val N = 2500 // 500

    //val records = spark.createDataFrame()

    val parquetF = "/tmp/x/outParquetCompressedAppend"
    val appendDf = spark.createDataFrame(df.takeAsList(N), df.schema)

    results += performTest("Parquet compressed base write", parquetF, "parquet", {
      (fileName, format) => appendDf.write.option("compression", "snappy").format(format).mode("overwrite").save(fileName)
    })

    results += performTest("Parquet compressed large append", parquetF, "parquet", {
      (fileName, format) => appendDf.write.option("compression", "snappy").format(format).mode("append").save(fileName)
    })

    results += performIndividualAppends("Parquet compressed small appends", spark, appendDf, df.schema, parquetF, "parquet")


    //val r: (String, Double, Double) =

    spark.conf.set("spark.sql.avro.compression.codec", "snappy")
    spark.conf.set("spark.sql.avro.deflate.level", "5")
    val avroF = "/tmp/x/outAvroCompressedAppend"

    results += performTest("Avro compressed base write", avroF, "com.databricks.spark.avro", {
      (fileName, format) => appendDf.write.format(format).mode("overwrite").save(fileName)
    })

    results += performTest("Avro compressed large append", avroF, "com.databricks.spark.avro", {
      (fileName, format) => appendDf.write.format(format).mode("append").save(fileName)
    })

    results += performIndividualAppends("Avro compressed small appends", spark, appendDf,
      appendDf.schema, avroF, "com.databricks.spark.avro")


    val jsonF = "/tmp/x/outJsonCompressedAppend"

    results += performTest("JSON compressed base write", jsonF, "json", {
      (fileName, format) => appendDf.write.option("compression", "gzip").format(format).mode("overwrite").save(fileName)
    })

    results += performTest("JSON compressed large append", jsonF, "json", {
      (fileName, format) => appendDf.write.option("compression", "gzip").format(format).mode("append").save(fileName)
    })

    results += performIndividualAppends("JSON compressed small appends", spark, appendDf,
      appendDf.schema, jsonF, "json")
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName("PersistenceDemo")
      .getOrCreate()


    val inputCsv = "/Users/mkamp/Downloads/yellow_tripdata_2016-01.csv"

    val results = ListBuffer.empty[(String, String, Double, Long, Long, Long)]
    results += (("Original CSV", inputCsv, 0d, 0L, calcFileSize(inputCsv), calcFileSize(inputCsv)))
    results += (("Original CSV zipped", "/tmp/x/x.zip", 0d, 0L, calcFileSize("/tmp/x/x.zip"), calcFileSize("/tmp/x/x.zip")))

    val df = time {
      spark.read.option("header", true).
        option("inferSchema", true).
        csv(inputCsv). //sample(true, 0.001).
        coalesce(1)
    } // sample(true, 0.01).

    println()
    println("Schema inferred")
    df.printSchema()
    println()


    val dfSchemaNotInferred = time {
      spark.read.option("header", true).
        option("inferSchema", false).
        csv(inputCsv). //sample(true, 0.001).
        coalesce(1)
    }
    println("Schema not inferred")
    dfSchemaNotInferred.printSchema()

    println()


    if (true) {
      time {
        println("count Inferred =" + df.count())
      }
      performWriteTests(spark, df, results)
    }


    if (true) {

      for (f <- new File("/tmp/x").listFiles()) {

        if (f.getName.startsWith("out") && f.isDirectory) {
          for (file <- f.listFiles)
            file.delete()
          f.delete()
        }
      }

      time {
        println("count Not Inferred =" + dfSchemaNotInferred.count())
      }

      performWriteTests(spark, dfSchemaNotInferred, results)
    }

    if (true) perfomAppendTests(spark, results, df)

    println()
    println()
    renderResults(results)

    println()
    println()

    if (true) performAndReportQueryTests(spark, df)

    spark.stop()
  }


}
