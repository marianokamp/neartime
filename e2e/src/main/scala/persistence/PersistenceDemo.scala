package persistence

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
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
      val result = block    // call-by-name
      val t1 = System.currentTimeMillis()
      println("Elapsed time: " + (t1 - t0)/1000 + "s")
      result
    }

    val df = time{spark.read.option("header", true).option("inferSchema", false).csv("/Users/mkamp/Downloads/yellow_tripdata_2016-01.csv").sample(true, 0.01).coalesce(1)}
    df.printSchema()

    Logger.getRootLogger.info("Test!")

    time{println("count=" + df.count())}

    time{
      println("result1="+ df.write.option("compression", "snappy").format("parquet").mode("overwrite").save("/tmp/outParquetCompressed"))
    }

    time{
      spark.conf.set("spark.sql.avro.compression.codec", "deflate")
      spark.conf.set("spark.sql.avro.deflate.level", "5")
      println("result3="+ df.write.format("com.databricks.spark.avro").mode("overwrite").save("/tmp/outAvroCompressed"))
    }

    time{
      println("result3="+ df.write.format("com.databricks.spark.avro").mode("overwrite").save("/tmp/outAvroNotCompressed"))
    }

    time{
      println("result2="+ df.write.format("parquet").mode("overwrite").save("/tmp/outParquetNotCompressed"))
    }

    println("num Partitions=" + df.rdd.getNumPartitions)

    spark.stop()
  }
}
