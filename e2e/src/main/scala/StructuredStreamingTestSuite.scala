import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class StructuredStreamingTestSuite extends FunSuite with BeforeAndAfterAll{

  test("memory") {

    val spark = setupSpark("memory")

    import spark.implicits._

    val linesDf = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "topic")
      .load()

    val lines = linesDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]


    val wordCounts = lines.flatMap(_._2.split(" ")).groupBy("value").count()

    println("------------------------- 4 -------------------------")
    val query = wordCounts
      .writeStream
      .format("memory")
      .queryName("word_counts")
      .outputMode("complete")
      .trigger(ProcessingTime("120 seconds"))
      .start()
    println("------------------------- 6 -------------------------")

    val results = spark.sqlContext.sql("SELECT * FROM word_counts")
    results.foreach{row =>
      println(s"row=$row")
    }
    query.stop()
  }


  def setupSpark(appName: String = "StructuredStreamingTest") = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

    spark
  }

}
